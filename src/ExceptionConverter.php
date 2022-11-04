<?php

declare(strict_types=1);

namespace FOD\DBALClickHouse;

use Doctrine\DBAL\Driver\API\ExceptionConverter as ExceptionConverterInterface;
use Doctrine\DBAL\Driver\Exception;
use Doctrine\DBAL\Exception\DriverException;
use Doctrine\DBAL\Query;

class ExceptionConverter implements ExceptionConverterInterface
{
    public function convert(Exception $exception, ?Query $query): DriverException
    {
        switch ($exception->getCode()) {
            case 1:
            case 2299:
            case 38911:
                return new \Doctrine\DBAL\Exception\UniqueConstraintViolationException($exception, $query);

            case 904:
                return new \Doctrine\DBAL\Exception\InvalidFieldNameException($exception, $query);

            case 918:
            case 960:
                return new \Doctrine\DBAL\Exception\NonUniqueFieldNameException($exception, $query);

            case 923:
                return new \Doctrine\DBAL\Exception\SyntaxErrorException($exception, $query);

            case 942:
                return new \Doctrine\DBAL\Exception\TableNotFoundException($exception, $query);

            case 955:
                return new \Doctrine\DBAL\Exception\TableExistsException($exception, $query);

            case 1017:
            case 12545:
                return new \Doctrine\DBAL\Exception\ConnectionException($exception, $query);

            case 1400:
                return new \Doctrine\DBAL\Exception\NotNullConstraintViolationException($exception, $query);

            case 1918:
                return new \Doctrine\DBAL\Exception\DatabaseDoesNotExist($exception, $query);

            case 9421:
                return new \Doctrine\DBAL\Exception\DatabaseObjectNotFoundException($exception, $query);

            case 2292:
                return new \Doctrine\DBAL\Exception\ForeignKeyConstraintViolationException($exception, $query);

            default:
                return new \Doctrine\DBAL\Exception\DriverException($exception, $query);
        }
    }
}
