# Componenta CQRS Transport Cycle

Cycle Database transport implementation for `componenta/cqrs-transport` v5. `main` is the adapter v2 line.

```bash
composer require componenta/cqrs-transport-cycle
```

The package provides `Componenta\CQRS\Command\Transport\DatabaseTransport`. The application is responsible for creating the transport tables described on that class and registering the instance in `TransportRegistryInterface`.

## Envelope persistence

The database adapter persists the complete transport envelope boundary:

- logical `queue`;
- `operation_id`;
- `command_class`;
- serialized command `payload`;
- serialized operation `context_payload`.

These values participate in idempotency and receipt binding. The MySQL reference schema therefore stores queue/identity strings with binary comparison semantics and stores opaque wire payloads as `LONGBLOB`. A case-insensitive text collation is not sufficient: SQL equality must mean byte equality for these fields.

The operation context is produced by `OperationContextSerializerInterface` in `componenta/cqrs-transport`. The database adapter treats it as opaque data and binds it to the same operation ID as the command payload.

The live table must enforce a unique `(queue, operation_id)` key. Completed and failed rows are retained as tombstones, so repeating an uncertain `send()` does not create another queue entry. Reusing an operation ID with a different command class, command payload, or operation context is rejected. If tombstones are cleaned up, choose a retention period longer than the maximum producer retry window.

## v2 schema migration

Existing non-empty transport tables must be migrated **before** deploying this adapter. Do not add a `NOT NULL` context column in one step unless the database explicitly supplies a safe value for existing rows.

For the MySQL-oriented reference schema, first add and backfill operation context:

```sql
ALTER TABLE command_transport
    ADD COLUMN context_payload LONGBLOB NULL;

ALTER TABLE command_transport_failed
    ADD COLUMN context_payload LONGBLOB NULL;

UPDATE command_transport
SET context_payload = '{}'
WHERE context_payload IS NULL;

UPDATE command_transport_failed
SET context_payload = '{}'
WHERE context_payload IS NULL;
```

Then convert all fields used for exact transport identity to binary comparison/storage and make context non-null:

```sql
ALTER TABLE command_transport
    MODIFY queue VARBINARY(64) NOT NULL,
    MODIFY operation_id VARBINARY(36) NOT NULL,
    MODIFY command_class VARBINARY(255) NOT NULL,
    MODIFY payload LONGBLOB NOT NULL,
    MODIFY context_payload LONGBLOB NOT NULL,
    MODIFY lease_token BINARY(32) NULL;

ALTER TABLE command_transport_failed
    MODIFY queue VARBINARY(64) NOT NULL,
    MODIFY operation_id VARBINARY(36) NOT NULL,
    MODIFY command_class VARBINARY(255) NOT NULL,
    MODIFY payload LONGBLOB NOT NULL,
    MODIFY context_payload LONGBLOB NOT NULL;
```

`{}` is the serialized empty operation context and is the correct backfill for messages created before operation-context propagation. Adapt the DDL syntax for other databases while preserving byte-exact equality, the unique `(queue, operation_id)` invariant, and non-null context after backfill.

The full reference schema is documented on `DatabaseTransport`.

## Delivery and consistency

Each delivery has an opaque lease-token receipt. `ack()` and `reject()` only change the row while that token still owns the lease; a worker whose lease was reclaimed cannot dispose another worker's delivery. The receipt is additionally bound to queue, operation ID, command class, command payload, and operation context.

All queue consistency reads use the Cycle **write driver**, including post-send read-back, claim candidate selection, and idempotent disposition checks. They intentionally do not use `Database::select()`, because Cycle routes that API through the optional read driver and a lagging replica cannot provide the read-your-write/lease consistency required by a queue.

All queue timestamps and redelivery cutoffs are derived from `CURRENT_TIMESTAMP` on the write driver. Producer and worker host clocks therefore do not participate in delayed availability, lease age, completion, or failure decisions. The write database/session timezone should remain stable; Cycle drivers use UTC by default.

`redeliverTimeout` is the maximum expected uninterrupted processing duration, not a heartbeat interval. The transport cannot renew a lease while a synchronous PHP handler is running. Configure it above the longest handler duration; after the timeout another worker may execute the same command concurrently. Handlers must therefore remain idempotent.

A caught worker failure is rejected into the failed table and is not visibility-redelivered. Use retry middleware around command execution or an explicit failed-message retry workflow when execution retries are required.
