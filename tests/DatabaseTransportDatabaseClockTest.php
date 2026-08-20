<?php

declare(strict_types=1);

use Componenta\CQRS\Command\Transport\DatabaseTransport;
use Componenta\CQRS\Command\Transport\Envelope;
use Componenta\CQRS\Command\Transport\TransportException;
use Cycle\Database\Config\SQLiteDriverConfig;
use Cycle\Database\Database;
use Cycle\Database\DatabaseInterface;
use Cycle\Database\Driver\SQLite\SQLiteDriver;
use Cycle\Database\StatementInterface;

final class RedeliveryFixedClockSQLiteDriver extends SQLiteDriver
{
    public static string $now = '2040-01-02 03:04:05';

    public function query(string $statement, array $parameters = []): StatementInterface
    {
        if (trim($statement) === 'SELECT CURRENT_TIMESTAMP') {
            return parent::query('SELECT ?', [self::$now]);
        }

        return parent::query($statement, $parameters);
    }
}

function redeliveryClockDatabase(): DatabaseInterface
{
    $driver = RedeliveryFixedClockSQLiteDriver::create(new SQLiteDriverConfig());
    $database = new Database('default', '', $driver);

    $database->execute(<<<'SQL'
CREATE TABLE command_transport (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue VARCHAR(64) NOT NULL,
    operation_id VARCHAR(36) NOT NULL,
    command_class VARCHAR(255) NOT NULL,
    payload TEXT NOT NULL,
    context_payload TEXT NOT NULL,
    available_at TIMESTAMP NOT NULL,
    delivered_at TIMESTAMP NULL,
    lease_token VARCHAR(32) NULL,
    completed_at TIMESTAMP NULL,
    failed_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL,
    UNIQUE (queue, operation_id)
)
SQL);
    $database->execute(
        'CREATE INDEX idx_queue_fetch ON command_transport '
        . '(queue, completed_at, failed_at, available_at, delivered_at)',
    );
    $database->execute(<<<'SQL'
CREATE TABLE command_transport_failed (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue VARCHAR(64) NOT NULL,
    operation_id VARCHAR(36) NOT NULL,
    command_class VARCHAR(255) NOT NULL,
    payload TEXT NOT NULL,
    context_payload TEXT NOT NULL,
    failed_at TIMESTAMP NOT NULL,
    UNIQUE (queue, operation_id)
)
SQL);

    return $database;
}

function requireRedeliveryEnvelope(?Envelope $envelope): Envelope
{
    if ($envelope === null) {
        throw new RuntimeException('Expected a transport envelope.');
    }

    return $envelope;
}

it('uses the write database clock for the redelivery cutoff', function (): void {
    RedeliveryFixedClockSQLiteDriver::$now = '2040-01-02 03:04:05';
    $transport = new DatabaseTransport(
        redeliveryClockDatabase(),
        'commands',
        redeliverTimeout: 10,
    );
    $envelope = new Envelope(
        operationId: 'database-clock-redelivery',
        commandClass: stdClass::class,
        payload: '{}',
    );

    $transport->send($envelope);
    $first = requireRedeliveryEnvelope($transport->get());

    RedeliveryFixedClockSQLiteDriver::$now = '2040-01-02 03:04:15';
    expect($transport->get())->toBeNull();

    RedeliveryFixedClockSQLiteDriver::$now = '2040-01-02 03:04:16';
    $second = requireRedeliveryEnvelope($transport->get());

    expect($second->receiptHandle)->not->toBe($first->receiptHandle)
        ->and(fn() => $transport->ack($first))
        ->toThrow(TransportException::class, 'stale or invalid');

    $transport->ack($second);

    expect($transport->get())->toBeNull();
});
