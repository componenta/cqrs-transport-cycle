# Componenta CQRS Transport Cycle

Cycle Database transport implementation for `componenta/cqrs-transport`.

```bash
composer require componenta/cqrs-transport-cycle
```

The adapter supports transport v1/v2/v3 and the current transport v4 API. Its boundary is `TransportInterface` plus `Envelope`; serializer and worker internals are not part of the database adapter.

The package provides `Componenta\CQRS\Command\Transport\DatabaseTransport`.

The application is responsible for creating the transport tables described on that class and registering the instance in `TransportRegistryInterface`.

The live table must enforce a unique `(queue, operation_id)` key. Completed and failed rows are retained as tombstones, so retrying an uncertain `send()` does not create another queue entry. Reusing an operation ID with a different command class or payload is rejected. If tombstones are cleaned up, the application must choose a retention period longer than its maximum producer retry window.

Each delivery has an opaque lease-token receipt. `ack()` and `reject()` only change the row while that token still owns the lease; a worker whose lease was reclaimed cannot dispose another worker's delivery.

All queue timestamps and redelivery cutoffs are derived from `CURRENT_TIMESTAMP` on the Cycle write driver. Producer and worker host clocks therefore do not participate in delayed availability, lease age, completion, or failure decisions. The write database/session timezone should remain stable; Cycle drivers use UTC by default.

`redeliverTimeout` is the maximum expected uninterrupted processing duration, not a heartbeat interval. The transport cannot renew a lease while a synchronous PHP handler is running. Configure it above the longest handler duration; after the timeout another worker may execute the same command concurrently. Handlers must therefore remain idempotent. A caught worker failure is rejected into the failed table and is not visibility-redelivered; use retry middleware or an explicit failed-message retry workflow when retries are required.
