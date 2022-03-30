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

use Doctrine\DBAL\Driver\ServerInfoAwareConnection;
use FOD\DBALClickHouse\ClickHouseException;
use FOD\DBALClickHouse\Connection;
use PHPUnit\Framework\TestCase;

/**
 * ClickHouse DBAL test class. Testing work with public methods of FOD\DBALClickHouse\Connection class
 *
 * @author Nikolay Mitrofanov <mitrofanovnk@gmail.com>
 */
class ConnectionTest extends TestCase
{
    /** @var  Connection */
    protected $connection;

    public function setUp() : void
    {
        $this->connection = CreateConnectionTest::createConnection();
    }

    public function testExecuteUpdateDelete()
    {
        $this->expectException(ClickHouseException::class);
        $this->connection->executeUpdate('DELETE from test WHERE 1');
    }

    public function testExecuteUpdateUpdate()
    {
        $this->expectException(ClickHouseException::class);
        $this->connection->executeUpdate('UPDATE test SET name = :name WHERE id = :id', [':name' => 'test', ':id' => 1]);
    }

    public function testDelete()
    {
        $this->expectException(ClickHouseException::class);
        $this->connection->delete('test', ['id' => 1]);
    }

    public function testUpdate()
    {
        $this->expectException(ClickHouseException::class);
        $this->connection->update('test', ['name' => 'test'], ['id' => 1]);
    }

    public function testSetTransactionIsolation()
    {
        $this->expectException(ClickHouseException::class);
        $this->connection->setTransactionIsolation(1);
    }

    public function testGetTransactionIsolation()
    {
        $this->expectException(ClickHouseException::class);
        $this->connection->getTransactionIsolation();
    }

    public function testGetTransactionNestingLevel()
    {
        $this->expectException(ClickHouseException::class);
        $this->connection->getTransactionNestingLevel();
    }

    public function testTransactional()
    {
        $this->expectException(ClickHouseException::class);
        $this->connection->transactional(function () {
        });
    }

    public function testSetNestTransactionsWithSavepoints()
    {
        $this->expectException(ClickHouseException::class);
        $this->connection->setNestTransactionsWithSavepoints(true);
    }

    public function testGetNestTransactionsWithSavepoints()
    {
        $this->expectException(ClickHouseException::class);
        $this->connection->getNestTransactionsWithSavepoints();
    }

    public function testBeginTransaction()
    {
        $this->expectException(ClickHouseException::class);
        $this->connection->beginTransaction();
    }

    public function testCommit()
    {
        $this->expectException(ClickHouseException::class);
        $this->connection->commit();
    }

    public function testRollBack()
    {
        $this->expectException(ClickHouseException::class);
        $this->connection->rollBack();
    }

    public function testCreateSavepoint()
    {
        $this->expectException(ClickHouseException::class);
        $this->connection->createSavepoint('1');
    }

    public function testReleaseSavepoint()
    {
        $this->expectException(ClickHouseException::class);
        $this->connection->releaseSavepoint('1');
    }

    public function testRollbackSavepoint()
    {
        $this->expectException(ClickHouseException::class);
        $this->connection->rollbackSavepoint('1');
    }

    public function testSetRollbackOnly()
    {
        $this->expectException(ClickHouseException::class);
        $this->connection->setRollbackOnly();
    }

    public function testIsRollbackOnly()
    {
        $this->expectException(ClickHouseException::class);
        $this->connection->isRollbackOnly();
    }

    public function testPing()
    {
        $this->assertTrue($this->connection->ping());
    }

    public function testGetServerVersion()
    {
        $conn = $this->connection->getWrappedConnection();
        if ($conn instanceof ServerInfoAwareConnection) {
            $pattern = '/(^[0-9]+.[0-9]+.[0-9]+(.[0-9]$|$))/mi';
            if (method_exists($this, 'assertMatchesRegularExpression')) {
                $this->assertMatchesRegularExpression($pattern, $conn->getServerVersion());
            } else {
                $this->assertRegExp($pattern, $conn->getServerVersion());
            }
        } else {
            $this->fail(sprintf('`%s` does not implement the `%s` interface', \get_class($conn),
                ServerInfoAwareConnection::class));
        }
    }
}
