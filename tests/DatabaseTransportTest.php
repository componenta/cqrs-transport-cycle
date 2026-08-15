<?php

declare(strict_types=1);

use Componenta\CQRS\Command\Transport\DatabaseTransport;
use Componenta\CQRS\Command\Transport\Envelope;
use Componenta\CQRS\Command\Transport\TransportException;
use Cycle\Database\Config\DatabaseConfig;
use Cycle\Database\Config\SQLiteDriverConfig;
use Cycle\Database\DatabaseInterface;
use Cycle\Database\DatabaseManager;

function transportDatabase(): DatabaseInterface
{
    $manager = new DatabaseManager(new DatabaseConfig([
        'default' => 'default',
        'databases' => [
            'default' => ['connection' => 'sqlite'],
        ],
        'connections' => [
            'sqlite' => new SQLiteDriverConfig(),
        ],
    ]));
    $database = $manager->database();

    $database->execute(<<<'SQL'
CREATE TABLE command_transport (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue VARCHAR(64) NOT NULL,
    operation_id VARCHAR(36) NOT NULL,
    command_class VARCHAR(255) NOT NULL,
    payload TEXT NOT NULL,
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
    failed_at TIMESTAMP NOT NULL,
    UNIQUE (queue, operation_id)
)
SQL);

    return $database;
}

function databaseEnvelope(string $operationId = 'operation-id', string $payload = '{}'): Envelope
{
    return new Envelope(
        operationId: $operationId,
        commandClass: stdClass::class,
        payload: $payload,
    );
}

function requireDatabaseEnvelope(?Envelope $envelope): Envelope
{
    if ($envelope === null) {
        throw new RuntimeException('Expected a transport envelope.');
    }

    return $envelope;
}

it('deduplicates sends and retains a completed operation tombstone', function (): void {
    $database = transportDatabase();
    $transport = new DatabaseTransport($database, 'commands');
    $envelope = databaseEnvelope();

    $first = $transport->send($envelope);
    $second = $transport->send($envelope);

    expect($second->receiptHandle)->toBe($first->receiptHandle)
        ->and($database->select()->from('command_transport')->count())->toBe(1);

    $claimed = requireDatabaseEnvelope($transport->get());
    $transport->ack($claimed);
    $transport->ack($claimed);
    $resent = $transport->send($envelope);

    expect($transport->get())->toBeNull()
        ->and($resent->receiptHandle)->toBe($first->receiptHandle)
        ->and($database->select()->from('command_transport')->count())->toBe(1)
        ->and($database->select('completed_at')->from('command_transport')->run()->fetchColumn())
        ->not->toBeNull();
});

it('rejects reuse of an operation ID for a different payload', function (): void {
    $transport = new DatabaseTransport(transportDatabase(), 'commands');
    $transport->send(databaseEnvelope(payload: '{"value":1}'));

    expect(fn() => $transport->send(databaseEnvelope(payload: '{"value":2}')))
        ->toThrow(TransportException::class, 'already used by a different command payload');
});

it('retries a bounded claim after losing a compare-and-swap race', function (): void {
    $database = transportDatabase();
    $transport = new DatabaseTransport($database, 'commands');
    $transport->send(databaseEnvelope());

    $database->execute('CREATE TABLE claim_gate (remaining INTEGER NOT NULL)');
    $database->execute('INSERT INTO claim_gate (remaining) VALUES (1)');
    $database->execute(<<<'SQL'
CREATE TRIGGER lose_first_transport_claim
BEFORE UPDATE OF delivered_at ON command_transport
WHEN (SELECT remaining FROM claim_gate LIMIT 1) = 1
BEGIN
    UPDATE claim_gate SET remaining = 0;
    SELECT RAISE(IGNORE);
END
SQL);

    $claimed = $transport->get();

    expect($claimed)->not->toBeNull()
        ->and($database->select('remaining')->from('claim_gate')->run()->fetchColumn())
        ->toBe(0);
});

it('prevents a stale worker from disposing a message claimed by another worker', function (): void {
    $database = transportDatabase();
    $transport = new DatabaseTransport($database, 'commands', redeliverTimeout: 1);
    $transport->send(databaseEnvelope());

    $first = requireDatabaseEnvelope($transport->get());
    $database->update('command_transport')
        ->values(['delivered_at' => '2000-01-01 00:00:00'])
        ->run();

    $second = requireDatabaseEnvelope($transport->get());
    expect($second->receiptHandle)->not->toBe($first->receiptHandle)
        ->and(fn() => $transport->ack($first))
        ->toThrow(TransportException::class, 'stale or invalid');

    $transport->ack($second);

    expect($transport->get())->toBeNull();
});

it('binds a receipt handle to the claimed command payload', function (): void {
    $database = transportDatabase();
    $transport = new DatabaseTransport($database, 'commands');
    $transport->send(databaseEnvelope(payload: '{"value":1}'));

    $claimed = requireDatabaseEnvelope($transport->get());
    $forged = new Envelope(
        operationId: $claimed->operationId,
        commandClass: $claimed->commandClass,
        payload: '{"value":2}',
        receiptHandle: $claimed->receiptHandle,
    );

    expect(fn() => $transport->ack($forged))
        ->toThrow(TransportException::class, 'stale or invalid')
        ->and(fn() => $transport->reject($forged))
        ->toThrow(TransportException::class, 'stale or invalid');

    $transport->ack($claimed);

    expect($transport->get())->toBeNull();
});

it('moves a caught failure once and keeps a failed operation tombstone', function (): void {
    $database = transportDatabase();
    $transport = new DatabaseTransport($database, 'commands');
    $envelope = databaseEnvelope();

    $transport->send($envelope);
    $claimed = requireDatabaseEnvelope($transport->get());
    $transport->reject($claimed);
    $transport->reject($claimed);
    $transport->send($envelope);

    expect($transport->get())->toBeNull()
        ->and($database->select()->from('command_transport_failed')->count())->toBe(1)
        ->and($database->select('failed_at')->from('command_transport')->run()->fetchColumn())
        ->not->toBeNull();
});
