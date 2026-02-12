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

namespace FOD\DBALClickHouse;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Exception\DatabaseRequired;
use Doctrine\DBAL\Platforms\Exception\NotSupported;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Exception\UnsupportedName;
use Doctrine\DBAL\Schema\Metadata\DatabaseMetadataRow;
use Doctrine\DBAL\Schema\Metadata\MetadataProvider;
use Doctrine\DBAL\Schema\Metadata\PrimaryKeyConstraintColumnRow;
use Doctrine\DBAL\Schema\Metadata\TableColumnMetadataRow;
use Doctrine\DBAL\Schema\Metadata\TableMetadataRow;

use function implode;
use function sprintf;
use function strpos;
use function strtolower;

class ClickHouseMetadataProvider implements MetadataProvider
{
    /** @var non-empty-string */
    private string $databaseName;

    /**
     * @internal This class can be instantiated only by a database platform.
     *
     * @throws Exception
     */
    public function __construct(private Connection $connection, private ClickHousePlatform $platform)
    {
        $databaseName = $connection->fetchOne('SELECT DATABASE()');

        if ($databaseName === null) {
            throw DatabaseRequired::new(__METHOD__);
        }

        $this->databaseName = $databaseName;
    }

    public function getAllDatabaseNames(): iterable
    {
        $sql = 'SELECT name FROM system.databases ORDER BY name';

        foreach ($this->connection->iterateColumn($sql) as $databaseName) {
            yield new DatabaseMetadataRow($databaseName);
        }
    }

    public function getAllSchemaNames(): iterable
    {
        throw NotSupported::new(__METHOD__);
    }

    public function getAllTableNames(): iterable
    {
        $sql = "SELECT name FROM system.tables WHERE database = ? AND engine != 'View' ORDER BY name";

        foreach ($this->connection->iterateNumeric($sql, [$this->databaseName]) as $row) {
            yield new TableMetadataRow(null, $row[0], []);
        }
    }

    public function getTableColumnsForAllTables(): iterable
    {
        return $this->getTableColumns(null);
    }

    public function getTableColumnsForTable(?string $schemaName, string $tableName): iterable
    {
        if ($schemaName !== null) {
            throw UnsupportedName::fromNonNullSchemaName($schemaName, __METHOD__);
        }

        return $this->getTableColumns($tableName);
    }

    private function getTableColumns(?string $tableName): iterable
    {
        $conditions = ['database = ?'];
        $params     = [$this->databaseName];

        if ($tableName !== null) {
            $conditions[] = 'table = ?';
            $params[]     = $tableName;
        }

        $sql = sprintf(
            'SELECT table, name, type, default_kind, default_expression, comment, is_in_primary_key
             FROM system.columns
             WHERE %s
             ORDER BY table, position',
            implode(' AND ', $conditions)
        );

        foreach ($this->connection->iterateNumeric($sql, $params) as $row) {
            yield $this->createTableColumn($row);
        }
    }

    private function createTableColumn(array $row): TableColumnMetadataRow
    {
        [$tableName, $columnName, $type, $defaultKind, $defaultExpression, $comment, $isInPrimaryKey] = $row;

        $typeLower = strtolower($type);

        try {
            $doctrineType = $this->platform->getDoctrineTypeMapping($typeLower);
        } catch (Exception $e) {
            $doctrineType = 'string';
        }

        $notNull = strpos($typeLower, 'nullable(') !== 0;

        $editor = Column::editor()
            ->setQuotedName($columnName)
            ->setTypeName($doctrineType)
            ->setNotNull($notNull)
            ->setComment($comment);

        if ($defaultKind === 'DEFAULT') {
            $editor->setDefaultValue($defaultExpression);
        }

        return new TableColumnMetadataRow(null, $tableName, $editor->create());
    }

    public function getIndexColumnsForAllTables(): iterable
    {
        return [];
    }

    public function getIndexColumnsForTable(?string $schemaName, string $tableName): iterable
    {
        if ($schemaName !== null) {
            throw UnsupportedName::fromNonNullSchemaName($schemaName, __METHOD__);
        }

        return [];
    }

    public function getPrimaryKeyConstraintColumnsForAllTables(): iterable
    {
        return $this->getPrimaryKeyConstraintColumns(null);
    }

    public function getPrimaryKeyConstraintColumnsForTable(?string $schemaName, string $tableName): iterable
    {
        if ($schemaName !== null) {
            throw UnsupportedName::fromNonNullSchemaName($schemaName, __METHOD__);
        }

        return $this->getPrimaryKeyConstraintColumns($tableName);
    }

    private function getPrimaryKeyConstraintColumns(?string $tableName): iterable
    {
        $conditions = ['database = ?', 'is_in_primary_key = 1'];
        $params     = [$this->databaseName];

        if ($tableName !== null) {
            $conditions[] = 'table = ?';
            $params[]     = $tableName;
        }

        $sql = sprintf(
            'SELECT table, name
             FROM system.columns
             WHERE %s
             ORDER BY table, position',
            implode(' AND ', $conditions)
        );

        foreach ($this->connection->iterateNumeric($sql, $params) as $row) {
            yield new PrimaryKeyConstraintColumnRow(
                schemaName: null,
                tableName: $row[0],
                constraintName: 'PRIMARY',
                isClustered: true,
                columnName: $row[1],
            );
        }
    }

    public function getForeignKeyConstraintColumnsForAllTables(): iterable
    {
        return [];
    }

    public function getForeignKeyConstraintColumnsForTable(?string $schemaName, string $tableName,): iterable
    {
        if ($schemaName !== null) {
            throw UnsupportedName::fromNonNullSchemaName($schemaName, __METHOD__);
        }

        return [];
    }

    public function getTableOptionsForAllTables(): iterable
    {
        return $this->getTableOptions(null);
    }

    public function getTableOptionsForTable(?string $schemaName, string $tableName): iterable
    {
        if ($schemaName !== null) {
            throw UnsupportedName::fromNonNullSchemaName($schemaName, __METHOD__);
        }

        return $this->getTableOptions($tableName);
    }

    private function getTableOptions(?string $tableName): iterable
    {
        $conditions = ['database = ?'];
        $params     = [$this->databaseName];

        if ($tableName !== null) {
            $conditions[] = 'name = ?';
            $params[]     = $tableName;
        }

        $sql = sprintf(
            'SELECT name, engine, comment FROM system.tables WHERE %s',
            implode(' AND ', $conditions)
        );

        foreach ($this->connection->iterateNumeric($sql, $params) as $row) {
            yield new TableMetadataRow(null, $row[0], [
                'engine'  => $row[1],
                'comment' => $row[2],
            ]);
        }
    }

    public function getAllViews(): iterable
    {
        $sql = "SELECT name, create_table_query FROM system.tables WHERE database = ? AND engine = 'View' ORDER BY name";

        foreach ($this->connection->iterateNumeric($sql, [$this->databaseName]) as $row) {
            yield new ClickHouseViewMetadataRow(null, $row[0], $row[1]);
        }
    }

    public function getAllSequences(): iterable
    {
        throw NotSupported::new(__METHOD__);
    }
}
