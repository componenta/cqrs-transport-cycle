<?php

declare(strict_types=1);

namespace Componenta\CQRS\Command\Transport;

use Cycle\Database\DatabaseInterface;
use Cycle\Database\Query\OnConflict;
use Cycle\Database\Query\SelectQuery;
use DateTimeImmutable;
use InvalidArgumentException;
use Throwable;

/**
 * Cycle-backed transport implementation.
 *
 * Uses an opaque `<row-id>:<lease-token>` receipt handle for claimed messages.
 * Completed and failed rows are retained as idempotency tombstones.
 *
 * All queue reads are executed through the write driver. Queue consistency
 * relies on read-your-write semantics and must not depend on replica lag.
 *
 * The reference schema below is MySQL-oriented. Adapt types/index syntax for
 * other databases while preserving the uniqueness and lookup invariants.
 *
 * Schema:
 * ```sql
 * CREATE TABLE command_transport (
 *     id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
 *     queue VARCHAR(64) NOT NULL,
 *     operation_id VARCHAR(36) NOT NULL,
 *     command_class VARCHAR(255) NOT NULL,
 *     payload LONGTEXT NOT NULL,
 *     available_at TIMESTAMP NOT NULL,
 *     delivered_at TIMESTAMP NULL,
 *     lease_token VARCHAR(32) NULL,
 *     completed_at TIMESTAMP NULL,
 *     failed_at TIMESTAMP NULL,
 *     created_at TIMESTAMP NOT NULL,
 *     UNIQUE KEY uq_command_transport_operation (queue, operation_id),
 *     INDEX idx_queue_fetch (queue, completed_at, failed_at, available_at, delivered_at)
 * );
 *
 * CREATE TABLE command_transport_failed (
 *     id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
 *     queue VARCHAR(64) NOT NULL,
 *     operation_id VARCHAR(36) NOT NULL,
 *     command_class VARCHAR(255) NOT NULL,
 *     payload LONGTEXT NOT NULL,
 *     failed_at TIMESTAMP NOT NULL,
 *     UNIQUE KEY uq_command_transport_failed_operation (queue, operation_id),
 *     INDEX idx_failed_queue (queue, failed_at)
 * );
 * ```
 */
final readonly class DatabaseTransport implements TransportInterface
{
    private const string TABLE = 'command_transport';
    private const string FAILED_TABLE = 'command_transport_failed';
    private const int CLAIM_ATTEMPTS = 3;

    /**
     * @param DatabaseInterface $database Cycle connection
     * @param string $name Transport/queue name
     * @param int $redeliverTimeout Seconds before unacknowledged message becomes available again
     *
     * @throws InvalidArgumentException If name is empty or redeliverTimeout is not positive
     */
    public function __construct(
        private DatabaseInterface $database,
        private string $name,
        private int $redeliverTimeout = 300,
    ) {
        if (trim($this->name) === '') {
            throw new InvalidArgumentException('Transport name cannot be empty');
        }

        if ($this->redeliverTimeout <= 0) {
            throw new InvalidArgumentException('Redeliver timeout must be positive');
        }
    }

    public function send(Envelope $envelope, int $delay = 0): Envelope
    {
        if ($delay < 0) {
            throw new InvalidArgumentException('Transport delay must be non-negative.');
        }

        $now = $this->databaseNow();
        $availableAt = $delay > 0
            ? $now->modify("+{$delay} seconds")
            : $now;

        $this->database->insert(self::TABLE)->values([
            'queue' => $this->name,
            'operation_id' => $envelope->operationId,
            'command_class' => $envelope->commandClass,
            'payload' => $envelope->payload,
            'available_at' => self::format($availableAt),
            'delivered_at' => null,
            'lease_token' => null,
            'completed_at' => null,
            'failed_at' => null,
            'created_at' => self::format($now),
        ])->onConflict(
            OnConflict::target(['queue', 'operation_id'])->doNothing(),
        )->run();

        $row = $this->writeSelect(['id', 'command_class', 'payload'])
            ->from(self::TABLE)
            ->where('queue', $this->name)
            ->where('operation_id', $envelope->operationId)
            ->limit(1)
            ->run()
            ->fetch();

        if (!is_array($row)) {
            throw new TransportException('Transport operation was not persisted.');
        }

        $id = self::rowId($row);
        $commandClass = self::rowString($row, 'command_class');
        $payload = self::rowString($row, 'payload');

        if ($commandClass !== $envelope->commandClass || $payload !== $envelope->payload) {
            throw new TransportException(sprintf(
                'Operation ID "%s" is already used by a different command payload in queue "%s".',
                $envelope->operationId,
                $this->name,
            ));
        }

        return $envelope->withReceiptHandle($id);
    }

    public function get(): ?Envelope
    {
        for ($attempt = 0; $attempt < self::CLAIM_ATTEMPTS; ++$attempt) {
            $result = $this->getAttempt();

            if ($result !== false) {
                return $result;
            }
        }

        return null;
    }

    /** @return Envelope|false|null False means that another consumer won the claim race. */
    private function getAttempt(): Envelope|false|null
    {
        $now = $this->databaseNow();
        $redeliverLimit = self::format($now->modify("-{$this->redeliverTimeout} seconds"));
        $nowFormatted = self::format($now);

        $row = $this->writeSelect([
            'id',
            'operation_id',
            'command_class',
            'payload',
            'delivered_at',
            'lease_token',
        ])
            ->from(self::TABLE)
            ->where('queue', $this->name)
            ->where('completed_at', null)
            ->where('failed_at', null)
            ->where('available_at', '<=', $nowFormatted)
            ->where(static function (SelectQuery $query) use ($redeliverLimit): void {
                $query->where('delivered_at', null)
                    ->orWhere('delivered_at', '<', $redeliverLimit);
            })
            ->orderBy('available_at', 'ASC')
            ->limit(1)
            ->run()
            ->fetch();

        if (!is_array($row)) {
            return null;
        }

        $id = self::rowId($row);
        $operationId = self::rowString($row, 'operation_id');
        $commandClass = self::rowString($row, 'command_class');
        $payload = self::rowString($row, 'payload');
        $oldDeliveredAt = self::rowNullableString($row, 'delivered_at');
        $oldLeaseToken = self::rowNullableString($row, 'lease_token');

        try {
            $leaseToken = bin2hex(random_bytes(16));
        } catch (Throwable $exception) {
            throw new TransportException(
                'Failed to generate a transport lease token.',
                previous: $exception,
            );
        }

        if ($this->claim(
            $id,
            $oldDeliveredAt,
            $oldLeaseToken,
            $nowFormatted,
            $leaseToken,
        ) === 0) {
            return false;
        }

        return new Envelope(
            operationId: $operationId,
            commandClass: $commandClass,
            payload: $payload,
            receiptHandle: "{$id}:{$leaseToken}",
        );
    }

    public function ack(Envelope $envelope): void
    {
        [$id, $leaseToken] = self::parseReceiptHandle($envelope->receiptHandle);

        $updated = $this->database->update(self::TABLE)
            ->where('id', $id)
            ->where('queue', $this->name)
            ->where('operation_id', $envelope->operationId)
            ->where('command_class', $envelope->commandClass)
            ->where('payload', $envelope->payload)
            ->where('lease_token', $leaseToken)
            ->where('completed_at', null)
            ->where('failed_at', null)
            ->values([
                'completed_at' => self::format($this->databaseNow()),
                'lease_token' => null,
            ])
            ->run();

        if ($updated === 0 && !$this->hasDisposition($id, $envelope, 'completed_at')) {
            throw new TransportException('Cannot acknowledge a stale or invalid transport receipt handle.');
        }
    }

    public function reject(Envelope $envelope): void
    {
        [$id, $leaseToken] = self::parseReceiptHandle($envelope->receiptHandle);
        $failedAt = self::format($this->databaseNow());

        if (!$this->database->begin()) {
            throw new TransportException('Failed to begin the transport rejection transaction.');
        }

        try {
            $updated = $this->database->update(self::TABLE)
                ->where('id', $id)
                ->where('queue', $this->name)
                ->where('operation_id', $envelope->operationId)
                ->where('command_class', $envelope->commandClass)
                ->where('payload', $envelope->payload)
                ->where('lease_token', $leaseToken)
                ->where('completed_at', null)
                ->where('failed_at', null)
                ->values([
                    'failed_at' => $failedAt,
                    'lease_token' => null,
                ])
                ->run();

            if ($updated === 0) {
                if ($this->hasDisposition($id, $envelope, 'failed_at')) {
                    if (!$this->database->commit()) {
                        throw new TransportException(
                            'Failed to commit the idempotent transport rejection transaction.',
                        );
                    }

                    return;
                }

                throw new TransportException('Cannot reject a stale or invalid transport receipt handle.');
            }

            $this->database->insert(self::FAILED_TABLE)->values([
                'queue' => $this->name,
                'operation_id' => $envelope->operationId,
                'command_class' => $envelope->commandClass,
                'payload' => $envelope->payload,
                'failed_at' => $failedAt,
            ])->onConflict(
                OnConflict::target(['queue', 'operation_id'])->doNothing(),
            )->run();

            if (!$this->database->commit()) {
                throw new TransportException('Failed to commit the transport rejection transaction.');
            }
        } catch (Throwable $exception) {
            try {
                if (!$this->database->rollback()) {
                    throw new TransportException(
                        'Transport rejection rollback returned false.',
                    );
                }
            } catch (Throwable $rollbackFailure) {
                throw new DatabaseTransportRollbackException(
                    $exception,
                    $rollbackFailure,
                );
            }

            throw $exception;
        }
    }

    private function claim(
        string $id,
        ?string $oldDeliveredAt,
        ?string $oldLeaseToken,
        string $newDeliveredAt,
        string $newLeaseToken,
    ): int {
        $query = $this->database->update(self::TABLE)
            ->where('id', $id)
            ->where('queue', $this->name)
            ->where('completed_at', null)
            ->where('failed_at', null);

        $oldDeliveredAt === null
            ? $query->where('delivered_at', null)
            : $query->where('delivered_at', $oldDeliveredAt);
        $oldLeaseToken === null
            ? $query->where('lease_token', null)
            : $query->where('lease_token', $oldLeaseToken);

        return $query->values([
            'delivered_at' => $newDeliveredAt,
            'lease_token' => $newLeaseToken,
        ])->run();
    }

    private function hasDisposition(string $id, Envelope $envelope, string $column): bool
    {
        $row = $this->writeSelect([$column])
            ->from(self::TABLE)
            ->where('id', $id)
            ->where('queue', $this->name)
            ->where('operation_id', $envelope->operationId)
            ->where('command_class', $envelope->commandClass)
            ->where('payload', $envelope->payload)
            ->limit(1)
            ->run()
            ->fetch();

        return is_array($row) && is_string($row[$column] ?? null);
    }

    /** @param list<string> $columns */
    private function writeSelect(array $columns): SelectQuery
    {
        return $this->database
            ->getDriver(DatabaseInterface::WRITE)
            ->getQueryBuilder()
            ->selectQuery($this->database->getPrefix(), [], $columns);
    }

    /**
     * @param array<array-key, mixed> $row
     * @return non-empty-string
     */
    private static function rowId(array $row): string
    {
        $id = $row['id'] ?? null;

        if (is_int($id) && $id > 0) {
            return (string) $id;
        }

        if (is_string($id) && preg_match('/^[1-9][0-9]*$/D', $id) === 1) {
            return $id;
        }

        throw new TransportException('Transport row contains an invalid ID.');
    }

    /** @param array<array-key, mixed> $row */
    private static function rowString(array $row, string $column): string
    {
        $value = $row[$column] ?? null;

        if (!is_string($value)) {
            throw new TransportException(sprintf(
                'Transport row column "%s" must be a string.',
                $column,
            ));
        }

        return $value;
    }

    /** @param array<array-key, mixed> $row */
    private static function rowNullableString(array $row, string $column): ?string
    {
        $value = $row[$column] ?? null;

        if ($value !== null && !is_string($value)) {
            throw new TransportException(sprintf(
                'Transport row column "%s" must be a string or null.',
                $column,
            ));
        }

        return $value;
    }

    /** @return array{non-empty-string, non-empty-string} */
    private static function parseReceiptHandle(string|int|null $receiptHandle): array
    {
        if (!is_string($receiptHandle)
            || preg_match('/^([1-9][0-9]*):([a-f0-9]{32})$/D', $receiptHandle, $match) !== 1
        ) {
            throw new TransportException('Transport receipt handle is missing or invalid.');
        }

        return [$match[1], $match[2]];
    }

    private function databaseNow(): DateTimeImmutable
    {
        $driver = $this->database->getDriver(DatabaseInterface::WRITE);
        $statement = $driver->query('SELECT CURRENT_TIMESTAMP');

        try {
            $value = $statement->fetchColumn();
        } finally {
            $statement->close();
        }

        if (!is_string($value) || trim($value) === '') {
            throw new TransportException(sprintf(
                'Database CURRENT_TIMESTAMP must return a non-empty string; %s given.',
                get_debug_type($value),
            ));
        }

        try {
            return new DateTimeImmutable($value, $driver->getTimezone());
        } catch (Throwable $exception) {
            throw new TransportException(sprintf(
                'Database CURRENT_TIMESTAMP returned an invalid timestamp "%s".',
                $value,
            ), previous: $exception);
        }
    }

    private static function format(DateTimeImmutable $time): string
    {
        return $time->format('Y-m-d H:i:s');
    }
}
