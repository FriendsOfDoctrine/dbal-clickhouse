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
use Doctrine\DBAL\Driver\API\ExceptionConverter;
use Doctrine\DBAL\Driver\Connection as DriverConnection;
use Doctrine\DBAL\Exception\InvalidArgumentException;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\ServerVersionProvider;

class Driver implements \Doctrine\DBAL\Driver
{
    /**
     * {@inheritDoc}
     */
    public function connect(
        #[\SensitiveParameter] array $params
    ): DriverConnection {
        if (!isset($params['user'])) {
            throw new InvalidArgumentException('Connection parameter `user` is required');
        }

        $user = $params['user'];

        if (!isset($params['password'])) {
            throw new InvalidArgumentException('Connection parameter `password` is required');
        }

        $password = $params['password'];

        if (!isset($params['host'])) {
            throw new InvalidArgumentException('Connection parameter `host` is required');
        }

        if (!isset($params['port'])) {
            throw new InvalidArgumentException('Connection parameter `port` is required');
        }

        return new ClickHouseConnection($params, (string) $user, (string) $password, $this->getDatabasePlatform(new Connection\StaticServerVersionProvider('')));
    }

    /**
     * {@inheritDoc}
     */
    public function getDatabasePlatform(ServerVersionProvider $versionProvider): AbstractPlatform
    {
        return new ClickHousePlatform();
    }

    /**
     * {@inheritDoc}
     */
    public function getSchemaManager(Connection $conn, AbstractPlatform $platform): AbstractSchemaManager
    {
        return new ClickHouseSchemaManager($conn, $platform);
    }

    /**
     * {@inheritDoc}
     */
    public function getExceptionConverter(): ExceptionConverter
    {
        return new ClickHouseExceptionConverter();
    }
}
