<?php
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

use FOD\DBALClickHouse\Connection;
use PHPUnit\Framework\TestCase;

/**
 * ClickHouse DBAL test class. Testing Select operations in ClickHouse
 *
 * @author Nikolay Mitrofanov <mitrofanovnk@gmail.com>
 */
class SelectTest extends TestCase
{
    /** @var  Connection */
    protected $connection;

    public function setUp()
    {
        $this->connection = CreateConnectionTest::createConnection();

        $fromSchema = $this->connection->getSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_select_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->addColumn('hits', 'integer');
        $newTable->addOption('engine', 'Memory');
        $newTable->setPrimaryKey(['id']);

        foreach ($fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform()) as $sql) {
            $this->connection->exec($sql);
        }

        $this->connection->exec("INSERT INTO test_select_table(id, payload, hits) VALUES (1, 'v1', 101), (2, 'v2', 202), (3, 'v3', 303), (4, 'v4', 404), (5, 'v4', 505)");
    }

    public function tearDown()
    {
        $this->connection->exec('DROP TABLE test_select_table');
    }

    public function testFetchBothSelect()
    {
        $results = [];
        $stmt = $this->connection->query('SELECT * FROM test_select_table WHERE id = 3');
        while ($result = $stmt->fetch()) {
            $results[] = $result;
        }
        $this->assertEquals([['id' => 3, 'payload' => 'v3', 'hits' => 303]], $results);
    }

    public function testFetchAssocSelect()
    {
        $results = [];
        $stmt = $this->connection->query('SELECT id, hits FROM test_select_table WHERE id IN (3, 4)');
        while ($result = $stmt->fetch(\PDO::FETCH_ASSOC)) {
            $results[] = $result;
        }
        $this->assertEquals([['id' => 3, 'hits' => 303], ['id' => 4, 'hits' => 404]], $results);
    }

    public function testFetchNumSelect()
    {
        $stmt = $this->connection->query('SELECT MAX(hits) FROM test_select_table');
        $result = $stmt->fetch(\PDO::FETCH_ASSOC);
        $this->assertEquals(['MAX(hits)' => 505], $result);
    }

    public function testFetchObjSelect()
    {
        $stmt = $this->connection->query('SELECT MAX(hits) as maxHits FROM test_select_table');
        $result = $stmt->fetch(\PDO::FETCH_OBJ);
        $this->assertEquals((object)['maxHits' => 505], $result);
    }

    public function testFetchKeyPairSelect()
    {
        $stmt = $this->connection->query("SELECT id, hits FROM test_select_table WHERE id = 2");
        $result = $stmt->fetch(\PDO::FETCH_KEY_PAIR);
        $this->assertEquals([2 => 202], $result);
    }

    public function testFetchAllBothSelect()
    {
        $stmt = $this->connection->query("SELECT * FROM test_select_table WHERE id IN (1, 3)");
        $result = $stmt->fetchAll();

        $this->assertEquals([[
            'id' => 1,
            'payload' => 'v1',
            'hits' => 101,
        ], [
            'id' => 3,
            'payload' => 'v3',
            'hits' => 303,
        ]], $result);
    }

    public function testFetchAllNumSelect()
    {
        $stmt = $this->connection->query("SELECT AVG(hits) FROM test_select_table");
        $result = $stmt->fetchAll(\PDO::FETCH_NUM);

        $this->assertEquals([[303]], $result);
    }

    public function testFetchAllObjSelect()
    {
        $stmt = $this->connection->query("SELECT * FROM test_select_table WHERE id IN (2, 4)");
        $result = $stmt->fetchAll(\PDO::FETCH_OBJ);

        $this->assertEquals([(object)[
            'id' => 2,
            'payload' => 'v2',
            'hits' => 202,
        ], (object)[
            'id' => 4,
            'payload' => 'v4',
            'hits' => 404,
        ]], $result);
    }

    public function testFetchAllKeyPairSelect()
    {
        $stmt = $this->connection->query("SELECT payload, hits FROM test_select_table WHERE id IN (2, 4) ORDER BY id");
        $result = $stmt->fetchAll(\PDO::FETCH_KEY_PAIR);

        $this->assertEquals([[
            'v2' => 202,
        ], [
            'v4' => 404,
        ]], $result);
    }

    public function testFetchColumnValidOffsetSelect()
    {
        $stmt = $this->connection->query("SELECT payload, hits FROM test_select_table WHERE id > 1 ORDER BY id LIMIT 3");
        $results = [];
        while ($result = $stmt->fetchColumn(1)) {
            $results[] = $result;
        }

        $this->assertEquals([202, 303, 404], $results);
    }

    public function testFetchColumnInvalidOffsetSelect()
    {
        $stmt = $this->connection->query("SELECT payload, hits FROM test_select_table WHERE id > 1 ORDER BY id");
        $results = [];
        while ($result = $stmt->fetchColumn(2)) {
            $results[] = $result;
        }

        $this->assertEquals(['v2', 'v3', 'v4', 'v4'], $results);
    }

    public function testQueryBuilderSelect()
    {
        $qb = $this->connection->createQueryBuilder();
        $result = $qb
            ->select('payload, uniq(hits) as uniques')
            ->from('test_select_table')
            ->where('id > :id')
            ->setParameter('id', 2, \PDO::PARAM_INT)
            ->groupBy('payload')
            ->orderBy('payload')
            ->setMaxResults(2)
            ->execute()
            ->fetchAll();

        $this->assertEquals([[
            'payload' => 'v3',
            'uniques' => '1',
        ], [
            'payload' => 'v4',
            'uniques' => '2',
        ]], $result);
    }

    public function testDynamicParametersSelect()
    {
        $stmt = $this->connection->prepare('SELECT payload, AVG(hits) AS avg_hits FROM test_select_table WHERE id > :id GROUP BY payload');

        $stmt->bindValue('id', 3, 'integer');
        $stmt->execute();

        $this->assertEquals([
            [
                'payload' => 'v4',
                'avg_hits' => 454.5,
            ]
        ], $stmt->fetchAll());
    }

    public function testColumnCount()
    {
        $stmt = $this->connection->prepare('SELECT * FROM test_select_table');
        $stmt->execute();

        $this->assertEquals(3, $stmt->columnCount());
    }
}

