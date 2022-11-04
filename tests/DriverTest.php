<?php

declare(strict_types=1);

/*
 * This file is part of the FODDBALClickHouse package -- Doctrine DBAL library
 * for ClickHouse (a column-oriented DBMS for OLAP <https://clickhouse.yandex/>)
 *
 * (c) FriendsOfDoctrine <https://github.com/FriendsOfDoctrine/>.
 *
 * For the full copyright and license inflormation, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace FOD\DBALClickHouse\Tests;

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
    /** @var  Connection */
    protected $connection;

    public function setUp(): void
    {
        $this->connection = CreateConnectionTest::createConnection();
    }

    public function testConnect(): void
    {
        $this->assertInstanceOf(ClickHouseConnection::class, $this->connection->getDriver()->connect(
            $this->connection->getParams(),
            $this->connection->getUsername(),
            $this->connection->getPassword()
        ));
    }

    public function testGetDatabasePlatform(): void
    {
        $this->assertInstanceOf(ClickHousePlatform::class, $this->connection->getDriver()->getDatabasePlatform());
    }

    public function testcreateSchemaManager(): void
    {
        $this->assertInstanceOf(ClickHouseSchemaManager::class, $this->connection->getDriver()->createSchemaManager($this->connection));
    }

    public function testGetName(): void
    {
        $this->assertEquals('clickhouse', $this->connection->getDriver()->getName());
    }

    public function testGetDatabase(): void
    {
        $this->assertEquals(phpunit_ch_dbname, $this->connection->getDriver()->getDatabase($this->connection));
    }
}
