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

it('rejects a delay that would overflow the platform timestamp before persistence', function (): void {
    RedeliveryFixedClockSQLiteDriver::$now = '2040-01-02 03:04:05';
    $database = redeliveryClockDatabase();
    $transport = new DatabaseTransport($database, 'commands');
    $envelope = new Envelope(
        operationId: 'overflow-delay',
        commandClass: stdClass::class,
        payload: '{}',
    );

    expect(fn() => $transport->send($envelope, PHP_INT_MAX))
        ->toThrow(InvalidArgumentException::class, 'Transport delay exceeds the supported timestamp range')
        ->and($database->select()->from('command_transport')->count())->toBe(0);
});

it('does not turn an extreme redelivery timeout into an immediate future cutoff', function (): void {
    RedeliveryFixedClockSQLiteDriver::$now = '2040-01-02 03:04:05';
    $transport = new DatabaseTransport(
        redeliveryClockDatabase(),
        'commands',
        redeliverTimeout: PHP_INT_MAX,
    );
    $envelope = new Envelope(
        operationId: 'extreme-redelivery-timeout',
        commandClass: stdClass::class,
        payload: '{}',
    );

    $transport->send($envelope);
    $claimed = requireRedeliveryEnvelope($transport->get());

    expect($claimed->receiptHandle)->toBeString()
        ->and($transport->get())->toBeNull();
});


it('rejects malformed or relative database timestamps before persisting or changing delivery state', function (string $invalid): void {
    $original = RedeliveryFixedClockSQLiteDriver::$now;
    RedeliveryFixedClockSQLiteDriver::$now = '2040-01-02 03:04:05';
    try {
        $database = redeliveryClockDatabase();
        $transport = new DatabaseTransport($database, 'commands');
        $envelope = new Envelope('clock-validation', stdClass::class, '{}');

        RedeliveryFixedClockSQLiteDriver::$now = $invalid;
        expect(fn () => $transport->send($envelope))
            ->toThrow(TransportException::class, 'invalid timestamp')
            ->and($database->select()->from('command_transport')->count())->toBe(0);
        expect(fn () => $transport->get())
            ->toThrow(TransportException::class, 'invalid timestamp');

        RedeliveryFixedClockSQLiteDriver::$now = '2040-01-02 03:04:05';
        $transport->send($envelope);
        $claimed = requireRedeliveryEnvelope($transport->get());
        RedeliveryFixedClockSQLiteDriver::$now = $invalid;
        expect(fn () => $transport->ack($claimed))
            ->toThrow(TransportException::class, 'invalid timestamp')
            ->and(fn () => $transport->reject($claimed))
            ->toThrow(TransportException::class, 'invalid timestamp');
        $row = $database->select(['completed_at', 'failed_at', 'lease_token'])
            ->from('command_transport')->run()->fetch();
        expect($row['completed_at'])->toBeNull()
            ->and($row['failed_at'])->toBeNull()
            ->and($row['lease_token'])->toBeString()
            ->and($database->select()->from('command_transport_failed')->count())->toBe(0);

        RedeliveryFixedClockSQLiteDriver::$now = '2040-01-02 03:04:05';
        $transport->ack($claimed);
        expect($transport->get())->toBeNull();
    } finally {
        RedeliveryFixedClockSQLiteDriver::$now = $original;
    }
})->with([
    'relative date' => ['tomorrow'],
    'time without date' => ['12:30:00'],
    'date without time' => ['2040-01-02'],
    'invalid calendar date' => ['2040-02-30 03:04:05'],
    'relative adjustment' => ['2040-01-02 03:04:05 +1 day'],
    'invalid time' => ['2040-01-02 25:04:05'],
]);

it('accepts a complete database timestamp with fractional seconds and an explicit offset', function (): void {
    $original = RedeliveryFixedClockSQLiteDriver::$now;
    RedeliveryFixedClockSQLiteDriver::$now = '2040-01-02 03:04:05.123456+00:00';
    try {
        $database = redeliveryClockDatabase();
        $transport = new DatabaseTransport($database, 'commands');
        $transport->send(new Envelope('fractional-clock', stdClass::class, '{}'), delay: 2);
        expect($database->select(['created_at', 'available_at'])->from('command_transport')->run()->fetch())
            ->toMatchArray([
                'created_at' => '2040-01-02 03:04:05',
                'available_at' => '2040-01-02 03:04:07',
            ])
            ->and($transport->get())->toBeNull();

        RedeliveryFixedClockSQLiteDriver::$now = '2040-01-02 03:04:07.123456+00:00';
        $claimed = requireRedeliveryEnvelope($transport->get());
        $transport->ack($claimed);
        expect($transport->get())->toBeNull();
    } finally {
        RedeliveryFixedClockSQLiteDriver::$now = $original;
    }
});
