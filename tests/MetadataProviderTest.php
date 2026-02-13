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

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception\DatabaseRequired;
use Doctrine\DBAL\Platforms\Exception\NotSupported;
use Doctrine\DBAL\Schema\Exception\UnsupportedName;
use Doctrine\DBAL\Schema\Metadata\DatabaseMetadataRow;
use Doctrine\DBAL\Schema\Metadata\MetadataProvider;
use Doctrine\DBAL\Schema\Metadata\PrimaryKeyConstraintColumnRow;
use Doctrine\DBAL\Schema\Metadata\TableColumnMetadataRow;
use Doctrine\DBAL\Schema\Metadata\TableMetadataRow;
use FOD\DBALClickHouse\ClickHouseMetadataProvider;
use FOD\DBALClickHouse\ClickHousePlatform;
use PHPUnit\Framework\TestCase;

/**
 * ClickHouse DBAL test class. Testing work with public methods of FOD\DBALClickHouse\ClickHouseMetadataProvider class
 */
class ClickHouseMetadataProviderTest extends TestCase
{
    private ClickHouseMetadataProvider $metadataProvider;
    private Connection $connection;

    public function setUp(): void
    {
        $this->connection = CreateConnectionTest::createConnection();
        $platform = $this->connection->getDatabasePlatform();

        $this->metadataProvider = new ClickHouseMetadataProvider($this->connection, $platform);

        // Create test tables for testing
        $this->createTestTables();
    }

    private function createTestTables(): void
    {
        // Drop existing test tables and views
        $this->connection->executeStatement('DROP VIEW IF EXISTS test_metadata_view');
        $this->connection->executeStatement('DROP TABLE IF EXISTS test_metadata_table');

        // Create test table with primary key
        $this->connection->executeStatement("
            CREATE TABLE IF NOT EXISTS test_metadata_table (
                EventDate Date DEFAULT today(),
                id UInt32,
                name String DEFAULT 'test',
                created_at DateTime DEFAULT now(),
                nullable_field Nullable(String),
                PRIMARY KEY id
            ) ENGINE = MergeTree()
        ");

        // Create test view
        $this->connection->executeStatement("
            CREATE OR REPLACE VIEW test_metadata_view AS
            SELECT id, name FROM test_metadata_table
        ");
    }

    public function testGetAllDatabaseNames(): void
    {
        $result = $this->metadataProvider->getAllDatabaseNames();
        $this->assertInstanceOf(\Generator::class, $result);

        $results = iterator_to_array($result);
        $this->assertNotEmpty($results);

        foreach ($results as $row) {
            $this->assertInstanceOf(DatabaseMetadataRow::class, $row);
            $this->assertIsString($row->getDatabaseName());
        }
    }

    public function testGetAllSchemaNamesThrowsNotSupported(): void
    {
        $this->expectException(NotSupported::class);

        $this->metadataProvider->getAllSchemaNames();
    }

    public function testGetAllTableNames(): void
    {
        $result = $this->metadataProvider->getAllTableNames();
        $this->assertInstanceOf(\Generator::class, $result);

        $results = iterator_to_array($result);
        $this->assertNotEmpty($results);

        // Check that our test table is included
        $testTableFound = false;
        foreach ($results as $row) {
            $this->assertInstanceOf(TableMetadataRow::class, $row);
            $this->assertNull($row->getSchemaName());
            $this->assertIsString($row->getTableName());
            $this->assertEmpty($row->getOptions());

            if ($row->getTableName() === 'test_metadata_table') {
                $testTableFound = true;
            }
        }
        $this->assertTrue($testTableFound, 'test_metadata_table should be found in table list');
    }

    public function testGetTableColumnsForAllTables(): void
    {
        $result = $this->metadataProvider->getTableColumnsForAllTables();
        $this->assertInstanceOf(\Generator::class, $result);

        $results = iterator_to_array($result);
        $this->assertNotEmpty($results);

        // Find our test table columns
        $testTableColumns = array_filter($results, function ($row) {
            return $row->getTableName() === 'test_metadata_table';
        });

        $this->assertNotEmpty($testTableColumns);

        foreach ($testTableColumns as $row) {
            $this->assertInstanceOf(TableColumnMetadataRow::class, $row);
            $this->assertNull($row->getSchemaName());
            $this->assertSame('test_metadata_table', $row->getTableName());
            $this->assertNotNull($row->getColumn());

            $column = $row->getColumn();
            $this->assertIsString($column->getName());
            $this->assertNotNull($column->getType());
        }
    }

    public function testGetTableColumnsForTableThrowsExceptionWhenSchemaNameIsNotNull(): void
    {
        $this->expectException(UnsupportedName::class);

        $this->metadataProvider->getTableColumnsForTable('schema', 'test_metadata_table');
    }

    public function testGetTableColumnsForTable(): void
    {
        $result = $this->metadataProvider->getTableColumnsForTable(null, 'test_metadata_table');
        $this->assertInstanceOf(\Generator::class, $result);

        $results = iterator_to_array($result);
        $this->assertNotEmpty($results);

        $columnNames = [];
        foreach ($results as $row) {
            $this->assertInstanceOf(TableColumnMetadataRow::class, $row);
            $this->assertNull($row->getSchemaName());
            $this->assertSame('test_metadata_table', $row->getTableName());
            $this->assertNotNull($row->getColumn());

            $column = $row->getColumn();
            $columnNames[] = $column->getName();
        }

        // Check that expected columns exist
        $this->assertContains('id', $columnNames);
        $this->assertContains('name', $columnNames);
        $this->assertContains('created_at', $columnNames);
        $this->assertContains('nullable_field', $columnNames);
    }

    public function testGetIndexColumnsForAllTablesReturnsEmptyArray(): void
    {
        $result = $this->metadataProvider->getIndexColumnsForAllTables();
        $this->assertSame([], $result);
    }

    public function testGetIndexColumnsForTableThrowsExceptionWhenSchemaNameIsNotNull(): void
    {
        $this->expectException(UnsupportedName::class);

        $this->metadataProvider->getIndexColumnsForTable('schema', 'test_metadata_table');
    }

    public function testGetIndexColumnsForTableReturnsEmptyArray(): void
    {
        $result = $this->metadataProvider->getIndexColumnsForTable(null, 'test_metadata_table');
        $this->assertSame([], $result);
    }

    public function testGetPrimaryKeyConstraintColumnsForAllTables(): void
    {
        $result = $this->metadataProvider->getPrimaryKeyConstraintColumnsForAllTables();
        $this->assertInstanceOf(\Generator::class, $result);

        $results = iterator_to_array($result);

        // Find our test table primary key
        $testTablePK = array_filter($results, function ($row) {
            return $row->getTableName() === 'test_metadata_table';
        });

        $this->assertNotEmpty($testTablePK);

        foreach ($testTablePK as $row) {
            $this->assertInstanceOf(PrimaryKeyConstraintColumnRow::class, $row);
            $this->assertNull($row->getSchemaName());
            $this->assertSame('test_metadata_table', $row->getTableName());
            $this->assertSame('PRIMARY', $row->getConstraintName());
            $this->assertTrue($row->isClustered());
            $this->assertSame('id', $row->getColumnName());
        }
    }

    public function testGetPrimaryKeyConstraintColumnsForTableThrowsExceptionWhenSchemaNameIsNotNull(): void
    {
        $this->expectException(UnsupportedName::class);

        $this->metadataProvider->getPrimaryKeyConstraintColumnsForTable('schema', 'test_metadata_table');
    }

    public function testGetPrimaryKeyConstraintColumnsForTable(): void
    {
        $result = $this->metadataProvider->getPrimaryKeyConstraintColumnsForTable(null, 'test_metadata_table');
        $this->assertInstanceOf(\Generator::class, $result);

        $results = iterator_to_array($result);

        $this->assertNotEmpty($results);

        foreach ($results as $row) {
            $this->assertInstanceOf(PrimaryKeyConstraintColumnRow::class, $row);
            $this->assertNull($row->getSchemaName());
            $this->assertSame('test_metadata_table', $row->getTableName());
            $this->assertSame('PRIMARY', $row->getConstraintName());
            $this->assertTrue($row->isClustered());
            $this->assertSame('id', $row->getColumnName());
        }
    }

    public function testGetForeignKeyConstraintColumnsForAllTablesReturnsEmptyArray(): void
    {
        $result = $this->metadataProvider->getForeignKeyConstraintColumnsForAllTables();
        $this->assertSame([], $result);
    }

    public function testGetForeignKeyConstraintColumnsForTableThrowsExceptionWhenSchemaNameIsNotNull(): void
    {
        $this->expectException(UnsupportedName::class);

        $this->metadataProvider->getForeignKeyConstraintColumnsForTable('schema', 'test_metadata_table');
    }

    public function testGetForeignKeyConstraintColumnsForTableReturnsEmptyArray(): void
    {
        $result = $this->metadataProvider->getForeignKeyConstraintColumnsForTable(null, 'test_metadata_table');
        $this->assertSame([], $result);
    }

    public function testGetTableOptionsForAllTables(): void
    {
        $result = $this->metadataProvider->getTableOptionsForAllTables();
        $this->assertInstanceOf(\Generator::class, $result);

        $results = iterator_to_array($result);
        $this->assertNotEmpty($results);

        // Find our test table
        $testTable = null;
        foreach ($results as $row) {
            $this->assertInstanceOf(TableMetadataRow::class, $row);
            $this->assertNull($row->getSchemaName());
            $this->assertIsString($row->getTableName());
            $this->assertIsArray($row->getOptions());

            if ($row->getTableName() === 'test_metadata_table') {
                $testTable = $row;
            }
        }

        $this->assertNotNull($testTable);
        $options = $testTable->getOptions();
        $this->assertArrayHasKey('engine', $options);
        $this->assertSame('MergeTree', $options['engine']);
    }

    public function testGetTableOptionsForTableThrowsExceptionWhenSchemaNameIsNotNull(): void
    {
        $this->expectException(UnsupportedName::class);

        $this->metadataProvider->getTableOptionsForTable('schema', 'test_metadata_table');
    }

    public function testGetTableOptionsForTable(): void
    {
        $result = $this->metadataProvider->getTableOptionsForTable(null, 'test_metadata_table');
        $this->assertInstanceOf(\Generator::class, $result);

        $results = iterator_to_array($result);
        $this->assertCount(1, $results);

        $row = $results[0];
        $this->assertInstanceOf(TableMetadataRow::class, $row);
        $this->assertNull($row->getSchemaName());
        $this->assertSame('test_metadata_table', $row->getTableName());

        $options = $row->getOptions();
        $this->assertArrayHasKey('engine', $options);
        $this->assertSame('MergeTree', $options['engine']);
    }

    public function testGetAllViews(): void
    {
        $result = $this->metadataProvider->getAllViews();
        $this->assertInstanceOf(\Generator::class, $result);

        $results = iterator_to_array($result);

        // Find our test view
        $testView = null;
        foreach ($results as $row) {
            $this->assertInstanceOf(ViewMetadataRow::class, $row);
            $this->assertNull($row->getSchemaName());
            $this->assertIsString($row->getViewName());

            if ($row->getViewName() === 'test_metadata_view') {
                $testView = $row;
            }
        }

        $this->assertNotNull($testView);
        $this->assertSame('test_metadata_view', $testView->getViewName());
    }

    public function testGetAllSequencesThrowsNotSupported(): void
    {
        $this->expectException(NotSupported::class);

        $this->metadataProvider->getAllSequences();
    }
}
