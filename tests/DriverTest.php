<?php
namespace FOD\DBALClickHouse\Tests;

use FOD\DBALClickHouse\ClickHouseConnection;
use FOD\DBALClickHouse\ClickHousePlatform;
use FOD\DBALClickHouse\ClickHouseSchemaManager;
use FOD\DBALClickHouse\Connection;
use PHPUnit\Framework\TestCase;

class DriverTest extends TestCase
{
    /** @var  Connection */
    protected $connection;

    public function setUp()
    {
        $this->connection = CreateConnectionTest::createConnection();
    }

    public function testConnect()
    {
        $this->assertInstanceOf(ClickHouseConnection::class, $this->connection->getDriver()->connect(
            $this->connection->getParams(),
            $this->connection->getUsername(),
            $this->connection->getPassword()
        ));
    }

    public function testGetDatabasePlatform()
    {
        $this->assertInstanceOf(ClickHousePlatform::class, $this->connection->getDriver()->getDatabasePlatform());
    }

    public function testGetSchemaManager()
    {
        $this->assertInstanceOf(ClickHouseSchemaManager::class, $this->connection->getDriver()->getSchemaManager($this->connection));
    }

    public function testGetName()
    {
        $this->assertEquals('clickhouse', $this->connection->getDriver()->getName());
    }

    public function testGetDatabase()
    {
        $this->assertEquals(phpunit_ch_dbname, $this->connection->getDriver()->getDatabase($this->connection));
    }
}
