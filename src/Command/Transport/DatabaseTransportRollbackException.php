<?php

declare(strict_types=1);

namespace Componenta\CQRS\Command\Transport;

use Throwable;

final class DatabaseTransportRollbackException extends TransportException
{
    public function __construct(
        public readonly Throwable $primaryFailure,
        public readonly Throwable $rollbackFailure,
    ) {
        parent::__construct(
            sprintf(
                'Transport rejection failed with "%s" and rollback also failed with "%s".',
                $primaryFailure->getMessage(),
                $rollbackFailure->getMessage(),
            ),
            previous: $primaryFailure,
        );
    }
}
