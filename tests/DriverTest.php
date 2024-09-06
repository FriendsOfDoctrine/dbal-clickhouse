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

namespace FOD\DBALClickHouse\Tests;

use Doctrine\DBAL\Connection\StaticServerVersionProvider;
use FOD\DBALClickHouse\ClickHouseConnection;
use FOD\DBALClickHouse\ClickHousePlatform;
use FOD\DBALClickHouse\ClickHouseSchemaManager;
use FOD\DBALClickHouse\Connection;
use PHPUnit\Framework\TestCase;

/**
 * ClickHouse DBAL test class. Testing public methods of class FOD\DBALClickHouse\Driver
 *
 * @author Nikolay Mitrofanov <mitrofanovnk@gmail.com>
 */
class DriverTest extends TestCase
{
    private Connection $connection;

    public function setUp(): void
    {
        $this->connection = CreateConnectionTest::createConnection();
    }

    public function testConnect(): void
    {
        $this->assertInstanceOf(
            ClickHouseConnection::class,
            $this->connection->getDriver()->connect($this->connection->getParams())
        );
    }

    public function testGetDatabasePlatform(): void
    {
        $this->assertInstanceOf(ClickHousePlatform::class, $this->connection->getDriver()->getDatabasePlatform(new StaticServerVersionProvider('')));
    }

    public function testGetSchemaManager(): void
    {
        $this->assertInstanceOf(
            ClickHouseSchemaManager::class,
            $this->connection->getDriver()->getSchemaManager(
                $this->connection,
                $this->connection->getDatabasePlatform()
            )
        );
    }
}
