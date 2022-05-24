<?php
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

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use PHPUnit\Framework\TestCase;

/**
 * ClickHouse DBAL test class. Tests of create connection with ClickHouse
 *
 * @author Nikolay Mitrofanov <mitrofanovnk@gmail.com>
 */
class CreateConnectionTest extends TestCase
{
    public function testCreateConnectionWithRightParams()
    {
        $this->assertInstanceOf(Connection::class, self::createConnection());
    }

    public function testCreateConnectionWithBadParams()
    {
        $this->expectException(Exception::class);
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
                'driverOptions' => [
                    'extremes'                => false,
                    'readonly'                => true,
                    'max_execution_time'      => 30,
                    'enable_http_compression' => 0,
                    'https'                   => false
                ],
            ];
        }
        return \Doctrine\DBAL\DriverManager::getConnection($params, new \Doctrine\DBAL\Configuration());
    }
}
