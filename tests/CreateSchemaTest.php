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

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Types\Type;
use FOD\DBALClickHouse\ClickHouseSchemaManager;
use FOD\DBALClickHouse\Connection;
use PHPUnit\Framework\TestCase;

/**
 * ClickHouse DBAL test class. Tests of Schema manager for create table in ClickHouse
 *
 * @author Nikolay Mitrofanov <mitrofanovnk@gmail.com>
 */
class CreateSchemaTest extends TestCase
{
    /** @var  Connection */
    protected $connection;

    public function setUp()
    {
        $this->connection = CreateConnectionTest::createConnection();
    }

    public function testGetSchemaManager()
    {
        $this->assertInstanceOf(ClickHouseSchemaManager::class, $this->connection->getSchemaManager());
    }

    public function testCreateNewTableSQL()
    {
        $fromSchema = $this->connection->getSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->addColumn('oneVal', Type::FLOAT);
        $newTable->addColumn('twoVal', Type::DECIMAL);
        $newTable->addColumn('flag', Type::BOOLEAN);
        $newTable->addColumn('mask', Type::SMALLINT);
        $newTable->addColumn('hash', 'string', ['length' => 32, 'fixed' => true]);
        $newTable->setPrimaryKey(['id']);

        $migrationSQLs = $fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform());
        $this->assertEquals("CREATE TABLE test_table (EventDate Date DEFAULT today(), id UInt32, payload String, oneVal Float64, twoVal String, flag UInt8, mask Int16, hash FixedString(32)) ENGINE = ReplacingMergeTree(EventDate, (id), 8192)", implode(';', $migrationSQLs));
        foreach ($migrationSQLs as $sql) {
            $this->connection->exec($sql);
        }
        $this->connection->exec('DROP TABLE test_table');
    }

    public function testCreateDropTable()
    {
        $fromSchema = $this->connection->getSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->addColumn('oneVal', Type::FLOAT);
        $newTable->addColumn('twoVal', Type::DECIMAL);
        $newTable->addColumn('flag', Type::BOOLEAN);
        $newTable->addColumn('mask', Type::SMALLINT);
        $newTable->addColumn('hash', 'string', ['length' => 32, 'fixed' => true]);
        $newTable->setPrimaryKey(['id']);

        foreach ($fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform()) as $sql) {
            $this->connection->exec($sql);
        }

        $this->connection->exec('DROP TABLE test_table');
        $this->expectException(DBALException::class);
        $this->connection->exec('DROP TABLE test_table');
    }

    public function testIndexGranularityOption()
    {
        $fromSchema = $this->connection->getSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->setPrimaryKey(['id']);
        $newTable->addOption('indexGranularity', 4096);

        $migrationSQLs = $fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform());
        $generatedSQL = implode(';', $migrationSQLs);
        $this->assertEquals("CREATE TABLE test_table (EventDate Date DEFAULT today(), id UInt32, payload String) ENGINE = ReplacingMergeTree(EventDate, (id), 4096)", $generatedSQL);
        foreach ($migrationSQLs as $sql) {
            $this->connection->exec($sql);
        }
        $this->connection->exec('DROP TABLE test_table');
    }

    public function testEngineMergeOption()
    {
        $fromSchema = $this->connection->getSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->setPrimaryKey(['id']);
        $newTable->addOption('engine', 'MergeTree');

        $migrationSQLs = $fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform());
        $generatedSQL = implode(';', $migrationSQLs);
        $this->assertEquals("CREATE TABLE test_table (EventDate Date DEFAULT today(), id UInt32, payload String) ENGINE = MergeTree(EventDate, (id), 8192)", $generatedSQL);
        foreach ($migrationSQLs as $sql) {
            $this->connection->exec($sql);
        }
        $this->connection->exec('DROP TABLE test_table');
    }

    public function testEngineMemoryOption()
    {
        $fromSchema = $this->connection->getSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->setPrimaryKey(['id']);
        $newTable->addOption('engine', 'Memory');

        $migrationSQLs = $fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform());
        $generatedSQL = implode(';', $migrationSQLs);
        $this->assertEquals("CREATE TABLE test_table (id UInt32, payload String) ENGINE = Memory", $generatedSQL);
        foreach ($migrationSQLs as $sql) {
            $this->connection->exec($sql);
        }
        $this->connection->exec('DROP TABLE test_table');
    }

    public function testEventDateColumnOption()
    {
        $fromSchema = $this->connection->getSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->addColumn('event_date', 'date', ['default' => 'toDate(now())']);
        $newTable->addOption('eventDateColumn', 'event_date');
        $newTable->setPrimaryKey(['id']);

        $migrationSQLs = $fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform());
        $generatedSQL = implode(';', $migrationSQLs);
        $this->assertEquals("CREATE TABLE test_table (event_date Date DEFAULT today(), id UInt32, payload String) ENGINE = ReplacingMergeTree(event_date, (id), 8192)", $generatedSQL);
        foreach ($migrationSQLs as $sql) {
            $this->connection->exec($sql);
        }
        $this->connection->exec('DROP TABLE test_table');
    }

    public function testEventDateColumnBadOption()
    {
        $fromSchema = $this->connection->getSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->addColumn('event_date', Type::DATETIME, ['default' => 'toDate(now())']);
        $newTable->addOption('eventDateColumn', 'event_date');
        $newTable->setPrimaryKey(['id']);

        $this->expectException(\Exception::class);
        $migrationSQLs = $fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform());
        $generatedSQL = implode(';', $migrationSQLs);
        $this->assertEquals("CREATE TABLE test_table (event_date Date DEFAULT today(), id UInt32, payload String) ENGINE = ReplacingMergeTree(event_date, (id), 8192)", $generatedSQL);
        foreach ($migrationSQLs as $sql) {
            $this->connection->exec($sql);
        }
        $this->connection->exec('DROP TABLE test_table');
    }

    public function testEventDateProviderColumnOption()
    {
        $fromSchema = $this->connection->getSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->addColumn('updated_at', Type::DATETIME);
        $newTable->addOption('eventDateProviderColumn', 'updated_at');
        $newTable->setPrimaryKey(['id']);

        $migrationSQLs = $fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform());
        $generatedSQL = implode(';', $migrationSQLs);
        $this->assertEquals("CREATE TABLE test_table (EventDate Date DEFAULT toDate(updated_at), id UInt32, payload String, updated_at DateTime) ENGINE = ReplacingMergeTree(EventDate, (id), 8192)", $generatedSQL);
        foreach ($migrationSQLs as $sql) {
            $this->connection->exec($sql);
        }
        $this->connection->exec('DROP TABLE test_table');
    }

    public function testEventDateProviderColumnBadOption()
    {
        $fromSchema = $this->connection->getSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->addColumn('flag', Type::BOOLEAN);
        $newTable->addOption('eventDateProviderColumn', 'flag');
        $newTable->setPrimaryKey(['id']);

        $this->expectException(\Exception::class);
        $migrationSQLs = $fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform());
        $generatedSQL = implode(';', $migrationSQLs);
        $this->assertEquals("CREATE TABLE test_table (EventDate Date DEFAULT toDate(updated_at), id UInt32, payload String, updated_at DateTime) ENGINE = ReplacingMergeTree(EventDate, (id), 8192)", $generatedSQL);
        foreach ($migrationSQLs as $sql) {
            $this->connection->exec($sql);
        }
        $this->connection->exec('DROP TABLE test_table');
    }
}
