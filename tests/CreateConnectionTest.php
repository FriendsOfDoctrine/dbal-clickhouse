<?php
namespace FOD\DBALClickHouse\Tests;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DBALException;
use PHPUnit\Framework\TestCase;

class CreateConnectionTest extends TestCase
{
    public function testCreateConnectionWithRightParams()
    {
        $this->assertInstanceOf(Connection::class, self::createConnection());
    }

    public function testCreateConnectionWithBadParams()
    {
        $this->expectException(DBALException::class);
        $this->assertInstanceOf(Connection::class, self::createConnection([]));
    }

    /**
     * @param null|array $params
     * @return Connection
     */
    public static function createConnection($params = null)
    {
        if (null === $params) {
            $params = [
                'host' => phpunit_ch_host,
                'port' => phpunit_ch_port,
                'user' => phpunit_ch_user,
                'password' => phpunit_ch_password,
                'dbname' => phpunit_ch_dbname,
                'driverClass' => phpunit_ch_driver_class,
                'wrapperClass' => phpunit_ch_wrapper_class,
            ];
        }
        return \Doctrine\DBAL\DriverManager::getConnection($params, new \Doctrine\DBAL\Configuration());
    }
}