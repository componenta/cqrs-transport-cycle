<?php

declare(strict_types=1);

use Componenta\CQRS\Command\Transport\DatabaseTransport;
use Componenta\CQRS\Command\Transport\Envelope;
use Componenta\CQRS\Command\Transport\TransportException;
use Cycle\Database\Config\DatabaseConfig;
use Cycle\Database\Config\MySQL\DsnConnectionConfig;
use Cycle\Database\Config\MySQLDriverConfig;
use Cycle\Database\DatabaseInterface;
use Cycle\Database\DatabaseManager;

function mysqlTransportDatabase(): DatabaseInterface
{
    $dsn = getenv('CQRS_TRANSPORT_TEST_MYSQL_DSN');

    if (!is_string($dsn) || $dsn === '') {
        throw new RuntimeException('CQRS_TRANSPORT_TEST_MYSQL_DSN is not configured.');
    }

    $user = getenv('CQRS_TRANSPORT_TEST_MYSQL_USER');
    $password = getenv('CQRS_TRANSPORT_TEST_MYSQL_PASSWORD');

    $manager = new DatabaseManager(new DatabaseConfig([
        'default' => 'default',
        'databases' => [
            'default' => ['connection' => 'mysql'],
        ],
        'connections' => [
            'mysql' => new MySQLDriverConfig(
                connection: new DsnConnectionConfig(
                    dsn: $dsn,
                    user: is_string($user) && $user !== '' ? $user : null,
                    password: is_string($password) && $password !== '' ? $password : null,
                ),
                timezone: 'UTC',
                queryCache: false,
            ),
        ],
    ]));

    return $manager->database('default');
}

function resetMysqlTransportSchema(DatabaseInterface $database): void
{
    $database->execute('DROP TABLE IF EXISTS command_transport_failed');
    $database->execute('DROP TABLE IF EXISTS command_transport');

    $database->execute(<<<'SQL'
CREATE TABLE command_transport (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    queue VARBINARY(64) NOT NULL,
    operation_id VARBINARY(36) NOT NULL,
    command_class VARBINARY(255) NOT NULL,
    payload LONGBLOB NOT NULL,
    context_payload LONGBLOB NOT NULL,
    available_at TIMESTAMP NOT NULL,
    delivered_at TIMESTAMP NULL,
    lease_token BINARY(32) NULL,
    completed_at TIMESTAMP NULL,
    failed_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL,
    UNIQUE KEY uq_command_transport_operation (queue, operation_id),
    INDEX idx_queue_fetch (queue, completed_at, failed_at, available_at, delivered_at)
)
SQL);

    $database->execute(<<<'SQL'
CREATE TABLE command_transport_failed (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    queue VARBINARY(64) NOT NULL,
    operation_id VARBINARY(36) NOT NULL,
    command_class VARBINARY(255) NOT NULL,
    payload LONGBLOB NOT NULL,
    context_payload LONGBLOB NOT NULL,
    failed_at TIMESTAMP NOT NULL,
    UNIQUE KEY uq_command_transport_failed_operation (queue, operation_id),
    INDEX idx_failed_queue (queue, failed_at)
)
SQL);
}

it('preserves byte-exact receipt identity on the MySQL reference schema', function (): void {
    if (!extension_loaded('pdo_mysql')
        || !is_string(getenv('CQRS_TRANSPORT_TEST_MYSQL_DSN'))
        || getenv('CQRS_TRANSPORT_TEST_MYSQL_DSN') === ''
    ) {
        $this->markTestSkipped('MySQL transport integration is not configured.');
    }

    $database = mysqlTransportDatabase();
    resetMysqlTransportSchema($database);
    $transport = new DatabaseTransport($database, 'commands');
    $source = new Envelope(
        operationId: '00000000-0000-7000-8000-000000000001',
        commandClass: stdClass::class,
        payload: '{"Value":"A"}',
        contextPayload: '{"tenant":"A"}',
    );

    $transport->send($source);
    $claimed = $transport->get();

    expect($claimed)->toBeInstanceOf(Envelope::class);

    if (!$claimed instanceof Envelope) {
        throw new RuntimeException('Expected a claimed MySQL transport envelope.');
    }

    $forgedPayload = new Envelope(
        operationId: $claimed->operationId,
        commandClass: $claimed->commandClass,
        payload: '{"Value":"a"}',
        receiptHandle: $claimed->receiptHandle,
        contextPayload: $claimed->contextPayload,
    );
    $forgedContext = new Envelope(
        operationId: $claimed->operationId,
        commandClass: $claimed->commandClass,
        payload: $claimed->payload,
        receiptHandle: $claimed->receiptHandle,
        contextPayload: '{"tenant":"a"}',
    );

    expect(fn() => $transport->ack($forgedPayload))
        ->toThrow(TransportException::class, 'stale or invalid')
        ->and(fn() => $transport->reject($forgedContext))
        ->toThrow(TransportException::class, 'stale or invalid');

    $transport->ack($claimed);

    expect($transport->get())->toBeNull();
});
