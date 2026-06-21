<?php

declare(strict_types=1);

namespace Componenta\CQRS\Command\Transport;

use Throwable;
use InvalidArgumentException;
use DateTimeImmutable;
use DateTimeZone;
use Cycle\Database\DatabaseInterface;

/**
 * Cycle-backed transport implementation.
 *
 * Uses auto-increment ID as receiptHandle.
 *
 * Schema:
 * ```sql
 * CREATE TABLE command_transport (
 *     id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
 *     queue VARCHAR(64) NOT NULL,
 *     operation_id VARCHAR(36) NOT NULL,
 *     command_class VARCHAR(255) NOT NULL,
 *     payload TEXT NOT NULL,
 *     available_at TIMESTAMP NOT NULL,
 *     delivered_at TIMESTAMP NULL,
 *     created_at TIMESTAMP NOT NULL,
 *     INDEX idx_queue_fetch (queue, available_at, delivered_at)
 * );
 *
 * CREATE TABLE command_transport_failed (
 *     id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
 *     queue VARCHAR(64) NOT NULL,
 *     operation_id VARCHAR(36) NOT NULL,
 *     command_class VARCHAR(255) NOT NULL,
 *     payload TEXT NOT NULL,
 *     failed_at TIMESTAMP NOT NULL,
 *     INDEX idx_failed_queue (queue, failed_at)
 * );
 * ```
 */
final readonly class DatabaseTransport implements TransportInterface
{
    private const string TABLE = 'command_transport';
    private const string FAILED_TABLE = 'command_transport_failed';

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
        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));
        $delay = max(0, $delay);
        $availableAt = $delay > 0
            ? $now->modify("+{$delay} seconds")
            : $now;

        $id = $this->database->insert(self::TABLE)->values([
            'queue' => $this->name,
            'operation_id' => $envelope->operationId,
            'command_class' => $envelope->commandClass,
            'payload' => $envelope->payload,
            'available_at' => $availableAt->format('Y-m-d H:i:s'),
            'delivered_at' => null,
            'created_at' => $now->format('Y-m-d H:i:s'),
        ])->run();


        if ($id === null) {
            throw new TransportException('Failed to get last insert ID');
        }

        return $envelope->withReceiptHandle($id);
    }

    public function get(): ?Envelope
    {
        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));
        $redeliverLimit = $now->modify("-$this->redeliverTimeout seconds")->format('Y-m-d H:i:s');
        $nowFormatted = $now->format('Y-m-d H:i:s');

        $row = $this->database->select()
            ->from(self::TABLE)
            ->where('queue', $this->name)
            ->where('available_at', '<=', $nowFormatted)
            ->where(static function ($query) use ($redeliverLimit): void {
                $query->where('delivered_at', null)
                    ->orWhere('delivered_at', '<', $redeliverLimit);
            })
            ->orderBy('available_at', 'ASC')
            ->limit(1)
            ->run()
            ->fetch();

        if (!$row) {
            return null;
        }

        $claimed = $this->claim($row['id'], $row['delivered_at'], $nowFormatted);

        if ($claimed === 0) {
            return null;
        }

        return new Envelope(
            operationId: $row['operation_id'],
            commandClass: $row['command_class'],
            payload: $row['payload'],
            receiptHandle: $row['id'],
        );
    }

    public function ack(Envelope $envelope): void
    {
        if ($envelope->receiptHandle === null) {
            return;
        }

        $this->database->delete(self::TABLE)
            ->where('id', $envelope->receiptHandle)
            ->run();
    }

    public function reject(Envelope $envelope): void
    {
        if ($envelope->receiptHandle === null) {
            return;
        }

        $this->database->begin();

        try {
            $deleted = $this->database->delete(self::TABLE)
                ->where('id', $envelope->receiptHandle)
                ->run();

            if ($deleted > 0) {
                $this->database->insert(self::FAILED_TABLE)->values([
                    'queue' => $this->name,
                    'operation_id' => $envelope->operationId,
                    'command_class' => $envelope->commandClass,
                    'payload' => $envelope->payload,
                    'failed_at' => (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format('Y-m-d H:i:s'),
                ])->run();
            }

            $this->database->commit();
        } catch (Throwable $e) {
            $this->database->rollback();
            throw $e;
        }
    }

    private function claim(string|int $id, ?string $oldDeliveredAt, string $newDeliveredAt): int
    {
        $query = $this->database->update(self::TABLE)
            ->where('id', $id);

        if ($oldDeliveredAt === null) {
            $query->where('delivered_at', null);
        } else {
            $query->where('delivered_at', $oldDeliveredAt);
        }

        return $query->values(['delivered_at' => $newDeliveredAt])->run();
    }
}
