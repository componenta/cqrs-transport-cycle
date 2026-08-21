<?php

declare(strict_types=1);

use Componenta\CQRS\Command\Transport\DatabaseTransport;
use Componenta\CQRS\Command\Transport\Envelope;
use Cycle\Database\DatabaseInterface;

it('rejects a queue name exceeding the reference schema byte limit', function (): void {
    $database = $this->createStub(DatabaseInterface::class);

    expect(fn() => new DatabaseTransport($database, str_repeat('q', 65)))
        ->toThrow(InvalidArgumentException::class, 'reference schema limit of 64 bytes');
});

it('rejects oversized envelope identity fields before database access', function (): void {
    $database = $this->createStub(DatabaseInterface::class);
    $transport = new DatabaseTransport($database, 'commands');

    expect(fn() => $transport->send(new Envelope(
        operationId: str_repeat('o', 37),
        commandClass: stdClass::class,
        payload: '{}',
    )))->toThrow(InvalidArgumentException::class, 'operation ID exceeds the reference schema limit of 36 bytes')
        ->and(fn() => $transport->send(new Envelope(
            operationId: '00000000-0000-7000-8000-000000000001',
            commandClass: str_repeat('C', 256),
            payload: '{}',
        )))->toThrow(InvalidArgumentException::class, 'command class exceeds the reference schema limit of 255 bytes');
});
