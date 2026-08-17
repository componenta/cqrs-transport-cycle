<?php

declare(strict_types=1);

use Componenta\CQRS\Command\Transport\DatabaseTransport;
use Componenta\CQRS\Command\Transport\Envelope;
use Cycle\Database\Config\SQLiteDriverConfig;
use Cycle\Database\Database;
use Cycle\Database\DatabaseInterface;
use Cycle\Database\Driver\SQLite\SQLiteDriver;

function writeDriverOnlyQueueDatabase(): DatabaseInterface
{
    $write = SQLiteDriver::create(new SQLiteDriverConfig());
    $read = SQLiteDriver::create(new SQLiteDriverConfig());
    $database = new Database('default', '', $write, $read);

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

it('uses the write driver for all queue consistency reads', function (): void {
    $transport = new DatabaseTransport(
        writeDriverOnlyQueueDatabase(),
        'commands',
    );
    $source = new Envelope(
        operationId: 'write-driver-read-your-write',
        commandClass: stdClass::class,
        payload: '{}',
    );

    $sent = $transport->send($source);
    expect($sent->receiptHandle)->toBeString();

    $claimed = $transport->get();
    expect($claimed)->toBeInstanceOf(Envelope::class);

    if (!$claimed instanceof Envelope) {
        throw new RuntimeException('Expected a claimed envelope.');
    }

    $transport->ack($claimed);
    $transport->ack($claimed); // idempotent disposition lookup must also read WRITE.

    expect($transport->get())->toBeNull();
});
