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

use Doctrine\DBAL\Connection;

/**
 * ClickHouse Driver
 */
class Driver implements \Doctrine\DBAL\Driver
{
    /**
     * {@inheritDoc}
     */
    public function connect(array $params, $username = null, $password = null, array $driverOptions = []) : ClickHouseConnection
    {
        if ($username === null) {
            if (! isset($params['user'])) {
                throw new ClickHouseException('Connection parameter `user` is required');
            }

            $username = $params['user'];
        }

        if ($password === null) {
            if (! isset($params['password'])) {
                throw new ClickHouseException('Connection parameter `password` is required');
            }

            $password = $params['password'];
        }

        if (! isset($params['host'])) {
            throw new ClickHouseException('Connection parameter `host` is required');
        }

        if (! isset($params['port'])) {
            throw new ClickHouseException('Connection parameter `port` is required');
        }

        return new ClickHouseConnection($params, (string) $username, (string) $password, $this->getDatabasePlatform());
    }

    /**
     * {@inheritDoc}
     */
    public function getDatabasePlatform() : ClickHousePlatform
    {
        return new ClickHousePlatform();
    }

    /**
     * {@inheritDoc}
     */
    public function getSchemaManager(Connection $conn) : ClickHouseSchemaManager
    {
        return new ClickHouseSchemaManager($conn);
    }

    /**
     * {@inheritDoc}
     */
    public function getName() : string
    {
        return 'clickhouse';
    }

    /**
     * {@inheritDoc}
     */
    public function getDatabase(Connection $conn) : string
    {
        $params = $conn->getParams();

        return $params['dbname'] ?? $conn->fetchOne('SELECT currentDatabase() as dbname');
    }
}
