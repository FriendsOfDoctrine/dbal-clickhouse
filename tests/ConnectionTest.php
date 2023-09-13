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

use Doctrine\DBAL\Driver\ServerInfoAwareConnection;
use Doctrine\DBAL\Exception;
use FOD\DBALClickHouse\Connection;
use PHPUnit\Framework\TestCase;

/**
 * ClickHouse DBAL test class. Testing work with public methods of FOD\DBALClickHouse\Connection class
 *
 * @author Nikolay Mitrofanov <mitrofanovnk@gmail.com>
 */
class ConnectionTest extends TestCase
{
    private Connection $connection;

    public function setUp(): void
    {
        $this->connection = CreateConnectionTest::createConnection();
    }

    public function testDelete(): void
    {
        $this->expectException(Exception::class);

        $this->connection->delete('test', ['id' => 1]);
    }

    public function testUpdate(): void
    {
        $this->expectException(Exception::class);

        $this->connection->update('test', ['name' => 'test'], ['id' => 1]);
    }

    public function testExecuteStatementDelete(): void
    {
        $this->expectException(Exception::class);

        $this->connection->executeStatement('DELETE FROM test WHERE id = :id', ['id' => 1]);
    }

    public function testExecuteStatementUpdate(): void
    {
        $this->expectException(Exception::class);

        $this->connection->executeStatement('UPDATE test SET name = :name WHERE id = :id', ['name' => 'test', 'id' => 1]);
    }

    public function testSetTransactionIsolation(): void
    {
        $this->expectException(Exception::class);

        $this->connection->setTransactionIsolation(1);
    }

    public function testGetTransactionIsolation(): void
    {
        $this->expectException(Exception::class);

        $this->connection->getTransactionIsolation();
    }

    public function testGetTransactionNestingLevel(): void
    {
        $this->expectException(Exception::class);

        $this->connection->getTransactionNestingLevel();
    }

    public function testTransactional(): void
    {
        $this->expectException(Exception::class);

        $this->connection->transactional(function () {
        });
    }

    public function testSetNestTransactionsWithSavepoints(): void
    {
        $this->expectException(Exception::class);

        $this->connection->setNestTransactionsWithSavepoints(true);
    }

    public function testGetNestTransactionsWithSavepoints(): void
    {
        $this->expectException(Exception::class);

        $this->connection->getNestTransactionsWithSavepoints();
    }

    public function testBeginTransaction(): void
    {
        $this->expectException(Exception::class);

        $this->connection->beginTransaction();
    }

    public function testCommit(): void
    {
        $this->expectException(Exception::class);

        $this->connection->commit();
    }

    public function testRollBack(): void
    {
        $this->expectException(Exception::class);

        $this->connection->rollBack();
    }

    public function testCreateSavepoint(): void
    {
        $this->expectException(Exception::class);

        $this->connection->createSavepoint('1');
    }

    public function testReleaseSavepoint(): void
    {
        $this->expectException(Exception::class);

        $this->connection->releaseSavepoint('1');
    }

    public function testRollbackSavepoint(): void
    {
        $this->expectException(Exception::class);

        $this->connection->rollbackSavepoint('1');
    }

    public function testSetRollbackOnly(): void
    {
        $this->expectException(Exception::class);

        $this->connection->setRollbackOnly();
    }

    public function testIsRollbackOnly(): void
    {
        $this->expectException(Exception::class);

        $this->connection->isRollbackOnly();
    }

    public function testGetServerVersion(): void
    {
        $conn = $this->connection->getWrappedConnection();

        if ($conn instanceof ServerInfoAwareConnection) {
            $pattern = '/^\d+\.\d+\.\d+\.\d+$/';

            if (method_exists($this, 'assertMatchesRegularExpression')) {
                $this->assertMatchesRegularExpression($pattern, $conn->getServerVersion());
            } else {
                $this->assertRegExp($pattern, $conn->getServerVersion());
            }
        } else {
            $this->fail(
                sprintf('`%s` does not implement the `%s` interface', \get_class($conn),
                ServerInfoAwareConnection::class)
            );
        }
    }
}
