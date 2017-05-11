<?php
namespace FOD\DBALClickHouse\Tests;

use Doctrine\DBAL\DBALException;
use FOD\DBALClickHouse\ClickHouseException;
use FOD\DBALClickHouse\Connection;
use PHPUnit\Framework\TestCase;

class ConnectionTest extends TestCase
{
    /** @var  Connection */
    protected $connection;

    public function setUp()
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
        $this->expectException(DBALException::class);
        $this->connection->delete('test', ['id' => 1]);
    }

    public function testUpdate()
    {
        $this->expectException(DBALException::class);
        $this->connection->update('test', ['name' => 'test'], ['id' => 1]);
    }

    public function testSetTransactionIsolation()
    {
        $this->expectException(DBALException::class);
        $this->connection->setTransactionIsolation(1);
    }

    public function testGetTransactionIsolation()
    {
        $this->expectException(DBALException::class);
        $this->connection->getTransactionIsolation();
    }

    public function testGetTransactionNestingLevel()
    {
        $this->expectException(DBALException::class);
        $this->connection->getTransactionNestingLevel();
    }

    public function testTransactional()
    {
        $this->expectException(DBALException::class);
        $this->connection->transactional(function () {
        });
    }

    public function testSetNestTransactionsWithSavepoints()
    {
        $this->expectException(DBALException::class);
        $this->connection->setNestTransactionsWithSavepoints(true);
    }

    public function testGetNestTransactionsWithSavepoints()
    {
        $this->expectException(DBALException::class);
        $this->connection->getNestTransactionsWithSavepoints();
    }

    public function testBeginTransaction()
    {
        $this->expectException(DBALException::class);
        $this->connection->beginTransaction();
    }

    public function testCommit()
    {
        $this->expectException(DBALException::class);
        $this->connection->commit();
    }

    public function testRollBack()
    {
        $this->expectException(DBALException::class);
        $this->connection->rollBack();
    }

    public function testCreateSavepoint()
    {
        $this->expectException(DBALException::class);
        $this->connection->createSavepoint('1');
    }

    public function testReleaseSavepoint()
    {
        $this->expectException(DBALException::class);
        $this->connection->releaseSavepoint('1');
    }

    public function testRollbackSavepoint()
    {
        $this->expectException(DBALException::class);
        $this->connection->rollbackSavepoint('1');
    }

    public function testSetRollbackOnly()
    {
        $this->expectException(DBALException::class);
        $this->connection->setRollbackOnly();
    }

    public function testIsRollbackOnly()
    {
        $this->expectException(DBALException::class);
        $this->connection->isRollbackOnly();
    }
}
