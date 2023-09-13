<?php

declare(strict_types=1);

/*
 * This file is part of the FODDBALClickHouse package -- Doctrine DBAL library
 * for ClickHouse (a column-oriented DBMS for OLAP <https://clickhouse.yandex/>)
 *
 * (c) FriendsOfDoctrine <https://github.com/FriendsOfDoctrine/>.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace FOD\DBALClickHouse;

use ClickHouseDB\Client;
use ClickHouseDB\Exception\ClickHouseException;
use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\ServerInfoAwareConnection;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\AbstractPlatform;

use function array_merge;

class ClickHouseConnection implements Connection, ServerInfoAwareConnection
{
    protected Client $client;

    protected AbstractPlatform $platform;

    public function __construct(
        array $params,
        string $user,
        string $password,
        AbstractPlatform $platform
    ) {
        $this->client   = new Client(
            [
                'host'     => $params['host'] ?? 'localhost',
                'port'     => $params['port'] ?? 8123,
                'username' => $user,
                'password' => $password,
            ],
            array_merge(['database' => $params['dbname'] ?? 'default'], $params['driverOptions'] ?? [])
        );
        $this->platform = $platform;
    }

    /**
     * {@inheritDoc}
     */
    public function prepare(string $sql): Statement
    {
        return new ClickHouseStatement($this->client, $sql, $this->platform);
    }

    /**
     * {@inheritDoc}
     */
    public function query(string $sql): Result
    {
        return $this->prepare($sql)->execute();
    }

    /**
     * {@inheritDoc}
     */
    public function quote($value, $type = ParameterType::STRING)
    {
        if ($type === ParameterType::STRING) {
            return $this->platform->quoteStringLiteral($value);
        }

        return $value;
    }

    /**
     * {@inheritDoc}
     */
    public function exec(string $sql): int
    {
        return $this->prepare($sql)->execute()->rowCount();
    }

    /**
     * {@inheritDoc}
     */
    public function lastInsertId($name = null)
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function beginTransaction(): bool
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function commit(): bool
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function rollBack(): bool
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getServerVersion(): string
    {
        try {
            return $this->client->getServerVersion();
        } catch (ClickHouseException) {
            return '';
        }
    }
}
