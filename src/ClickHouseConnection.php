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
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\Platforms\AbstractPlatform;

use Doctrine\DBAL\Platforms\Exception\NotSupported;
use function array_merge;

class ClickHouseConnection implements Connection
{
    protected Client $client;

    protected AbstractPlatform $platform;

    public function __construct(
        array $params,
        string $user,
        string $password,
        AbstractPlatform $platform
    ) {
        $connectParams = [
            'host'     => $params['host'] ?? 'localhost',
            'port'     => $params['port'] ?? 8123,
            'username' => $user,
            'password' => $password,
        ];

        if (isset($params['driverOptions']['sslCA'])) {
            $connectParams['sslCA'] = $params['driverOptions']['sslCA'];
            unset($params['driverOptions']['sslCA']);
        }

        $clientParams = array_merge(['database' => $params['dbname'] ?? 'default'], $params['driverOptions'] ?? []);

        $this->client = new Client($connectParams, $clientParams);
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
    public function quote(string $value): string
    {
        return $this->platform->quoteStringLiteral($value);
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
    public function lastInsertId(): int|string
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function beginTransaction(): void
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function commit(): void
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function rollBack(): void
    {
        throw NotSupported::new(__METHOD__);
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

    public function getNativeConnection()
    {
        return $this;
    }
}
