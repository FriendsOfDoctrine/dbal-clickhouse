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

use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\TrimMode;
use FOD\DBALClickHouse\Connection;
use PHPUnit\Framework\TestCase;

/**
 * ClickHouse DBAL test class. Testing Select operations in ClickHouse
 *
 * @author Nikolay Mitrofanov <mitrofanovnk@gmail.com>
 */
class SelectTest extends TestCase
{
    private Connection $connection;

    public function setUp(): void
    {
        $this->connection = CreateConnectionTest::createConnection();

        $fromSchema = $this->connection->createSchemaManager()->introspectSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_select_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->addColumn('hits', 'integer');
        $newTable->addOption('engine', 'Memory');
        $newTable->setPrimaryKey(['id']);

        foreach ($fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform()) as $sql) {
            $this->connection->executeStatement($sql);
        }

        $this->connection->executeStatement("INSERT INTO test_select_table(id, payload, hits) VALUES (1, 'v1', 101), (2, 'v2', 202), (3, 'v3', 303), (4, 'v4', 404), (5, 'v4', 505), (6, '  t1   ', 606), (7, 'aat2aaa', 707)");
    }

    public function tearDown(): void
    {
        $this->connection->executeStatement('DROP TABLE test_select_table');
    }

    public function testFetchBothSelect(): void
    {
        $results = [];

        $result = $this->connection->executeQuery('SELECT * FROM test_select_table WHERE id = 3');

        while ($row = $result->fetchAssociative()) {
            $results[] = $row;
        }

        $this->assertEquals([['id' => 3, 'payload' => 'v3', 'hits' => 303]], $results);
    }

    public function testFetchAssocSelect(): void
    {
        $results = [];

        $result = $this->connection->executeQuery('SELECT id, hits FROM test_select_table WHERE id IN (3, 4)');

        while ($row = $result->fetchAssociative()) {
            $results[] = $row;
        }

        $this->assertEquals([['id' => 3, 'hits' => 303], ['id' => 4, 'hits' => 404]], $results);
    }

    public function testFetchNumSelect():void
    {
        $result = $this->connection->executeQuery('SELECT MAX(hits) as maxHits FROM test_select_table');

        $this->assertEquals(['maxHits' => 707], $result->fetchAssociative());
    }

    public function testFetchAllBothSelect(): void
    {
        $result = $this->connection->executeQuery("SELECT * FROM test_select_table WHERE id IN (1, 3)");

        $this->assertEquals([
            [
                'id' => 1,
                'payload' => 'v1',
                'hits' => 101,
            ],
            [
                'id' => 3,
                'payload' => 'v3',
                'hits' => 303,
            ]
        ], $result->fetchAllAssociative());
    }

    public function testFetchAllNumSelect(): void
    {
        $result = $this->connection->executeQuery("SELECT AVG(hits) FROM test_select_table");

        $this->assertEquals([[404]], $result->fetchAllNumeric());
    }

    public function testFetchColumnValidOffsetSelect(): void
    {
        $results = [];

        $result = $this->connection->executeQuery("SELECT payload, hits FROM test_select_table WHERE id > 1 ORDER BY id LIMIT 3");

        while ($row = $result->fetchNumeric()) {
            $results[] = $row[1];
        }

        $this->assertEquals([202, 303, 404], $results);
    }

    public function testFetchColumnInvalidOffsetSelect(): void
    {
        $results = [];

        $result = $this->connection->executeQuery("SELECT payload, hits FROM test_select_table WHERE id > 1 ORDER BY id");

        while ($row = $result->fetchOne()) {
            $results[] = $row;
        }

        $this->assertEquals(['v2', 'v3', 'v4', 'v4', '  t1   ', 'aat2aaa'], $results);
    }

    public function testQueryBuilderSelect(): void
    {
        $queryBuilder = $this->connection->createQueryBuilder();

        $result = $queryBuilder
            ->select('payload, uniq(hits) as uniques')
            ->from('test_select_table')
            ->where('id > :id')
            ->setParameter('id', 2, ParameterType::INTEGER)
            ->groupBy('payload')
            ->orderBy('payload')
            ->setMaxResults(2)
            ->fetchAllAssociative();

        $this->assertEquals([
            [
                'payload' => '  t1   ',
                'uniques' => '1',
            ],
            [
                'payload' => 'aat2aaa',
                'uniques' => '1',
            ]
        ], $result);
    }

    public function testDynamicParametersSelect(): void
    {
        $result = $this->connection->executeQuery(
            'SELECT payload, AVG(hits) AS avg_hits FROM test_select_table WHERE id > :id GROUP BY payload ORDER BY payload',
            ['id' => 3],
            ['id' => ParameterType::INTEGER]
        );

        $this->assertEquals([
            [
                'payload' => '  t1   ',
                'avg_hits' => 606,
            ],
            [
                'payload' => 'aat2aaa',
                'avg_hits' => 707,
            ],
            [
                'payload' => 'v4',
                'avg_hits' => 454.5,
            ],
        ], $result->fetchAllAssociative());
    }

    public function testColumnCount(): void
    {
        $result = $this->connection->executeQuery('SELECT * FROM test_select_table');

        $this->assertEquals(3, $result->columnCount());
    }

    public function testTrim(): void
    {
        $result = $this->connection->executeQuery(
            sprintf(
                'SELECT %s FROM test_select_table WHERE id = 6',
                $this->connection->getDatabasePlatform()->getTrimExpression('payload')
            )
        );

        $this->assertEquals('t1', $result->fetchOne());
    }

    public function testTrimLeft(): void
    {
        $result = $this->connection->executeQuery(
            sprintf(
                'SELECT %s FROM test_select_table WHERE id = 6',
                $this->connection->getDatabasePlatform()->getTrimExpression('payload', TrimMode::LEADING)
            )
        );

        $this->assertEquals('t1   ', $result->fetchOne());
    }

    public function testTrimRight(): void
    {
        $result = $this->connection->executeQuery(
            sprintf(
                'SELECT %s FROM test_select_table WHERE id = 6',
                $this->connection->getDatabasePlatform()->getTrimExpression('payload', TrimMode::TRAILING)
            )
        );

        $this->assertEquals('  t1', $result->fetchOne());
    }

    public function testTrimChar(): void
    {
        $result = $this->connection->executeQuery(
            sprintf(
                'SELECT %s FROM test_select_table WHERE id = 7',
                $this->connection->getDatabasePlatform()->getTrimExpression('payload', TrimMode::UNSPECIFIED, 'a')
            )
        );

        $this->assertEquals('t2', $result->fetchOne());
    }
}
