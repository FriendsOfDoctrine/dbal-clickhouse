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

use Doctrine\DBAL\Platforms\Exception\NotSupported;
use Doctrine\DBAL\Result;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\ForeignKeyConstraint;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\View;
use Doctrine\DBAL\Types\Type;

use function array_change_key_case;
use function array_filter;
use function array_key_exists;
use function array_map;
use function array_reverse;
use function current;
use function explode;
use function is_array;
use function mb_stripos;
use function mb_strtolower;
use function preg_match;
use function preg_replace;
use function str_replace;
use function trim;

use const CASE_LOWER;

class ClickHouseSchemaManager extends AbstractSchemaManager
{
    /**
     * {@inheritdoc}
     */
    protected function _getPortableTableDefinition(array $table): string
    {
        return $table['table_name'];
    }

    /**
     * {@inheritdoc}
     */
    protected function _getPortableViewDefinition(array $view): View
    {
        $statement = $this->connection->fetchOne('SHOW CREATE TABLE ' . $view['name']);

        return new View($view['name'], $statement);
    }

    /**
     * {@inheritdoc}
     */
    public function listTableIndexes(string $table): array
    {
        $tableView = $this->_getPortableViewDefinition(['name' => $table]);

        preg_match(
            '/MergeTree\(([\w+, ()]+)(?= \(((?:[^()]|\((?2)\))+)\),)/mi',
            $tableView->getSql(),
            $matches
        );

        if (is_array($matches) && array_key_exists(2, $matches)) {
            $indexColumns = array_filter(
                array_map('trim', explode(',', $matches[2])),
                fn (string $column): bool => !str_contains($column, '(')
            );

            return [
                'primary' => new Index(
                    current(array_reverse(explode('.', $table))) . '__pk',
                    $indexColumns,
                    false,
                    true
                ),
            ];
        }

        return [];
    }

    /**
     * {@inheritdoc}
     */
    protected function _getPortableTableColumnDefinition(array $tableColumn): Column
    {
        $tableColumn = array_change_key_case($tableColumn, CASE_LOWER);

        $dbType  = $columnType = trim($tableColumn['type']);
        $length  = null;
        $fixed   = false;
        $notnull = true;

        if (preg_match('/(Nullable\((\w+)\))/i', $columnType, $matches)) {
            $columnType = str_replace($matches[1], $matches[2], $columnType);
            $notnull    = false;
        }

        if (mb_stripos($columnType, 'fixedstring') === 0) {
            // get length from FixedString definition
            $length = (int) preg_replace('~.*\(([0-9]*)\).*~', '$1', $columnType);
            $dbType = 'fixedstring';
            $fixed  = true;
        }

        $unsigned = false;

        if (mb_stripos($columnType, 'uint') === 0) {
            $unsigned = true;
        }

        if (!isset($tableColumn['name'])) {
            $tableColumn['name'] = '';
        }

        $default = null;

        // @todo process not only DEFAULT type, but ALIAS and MATERIALIZED too
        if ($tableColumn['default']) {
            $default = $tableColumn['default'];
        }

        $options = [
            'length'        => $length,
            'unsigned'      => $unsigned,
            'fixed'         => $fixed,
            'default'       => $default,
            'notnull'       => $notnull,
            'autoincrement' => false,
            'comment'       => $tableColumn['comment'] ?? null,
        ];

        return new Column(
            $tableColumn['name'],
            Type::getType($this->connection->getDatabasePlatform()->getDoctrineTypeMapping($dbType)),
            $options
        );
    }

    /**
     * {@inheritdoc}
     */
    protected function _getPortableDatabaseDefinition(array $database): string
    {
        return $database['name'];
    }

    protected function selectTableNames(string $databaseName): Result
    {
        return $this->connection->executeQuery(
            <<<'SQL'
            SELECT table_name
            FROM INFORMATION_SCHEMA.TABLES
            WHERE  table_schema = ? AND table_type != 'BASE TABLE'
            SQL,
            [$databaseName]
        );
    }

    protected function selectTableColumns(string $databaseName, ?string $tableName = null): Result
    {
        $params = [$databaseName, 'BASE TABLE'];
        $extraCondition = '';

        if (null !== $tableName) {
            $extraCondition =' AND c.table_name = ?';
            $params[] = $tableName;
        }

        return $this->connection->executeQuery(
            \sprintf(<<<'SQL'
            SELECT c.column_name AS field,
                   c.table_name,
                   c.column_type as `type`,
                   c.is_nullable AS `null`,
                   c.column_name AS `key`,
                   c.column_default AS `default`,
                   c.column_comment AS `comment`,
                   c.character_set_name AS `characterset`
            FROM INFORMATION_SCHEMA.COLUMNS c
            INNER JOIN INFORMATION_SCHEMA.TABLES t ON c.table_name = t.table_name
            WHERE c.table_schema = ? AND t.table_type = ? %s
            SQL, $extraCondition),
            $params
        );
    }

    protected function selectIndexColumns(string $databaseName, ?string $tableName = null): Result
    {
        // Table information_schema.statistics do not exist return empty result
        return $this->connection->executeQuery('SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE 1 != 1');
    }

    protected function selectForeignKeyColumns(string $databaseName, ?string $tableName = null): Result
    {
        // Tables information_schema.key_column_usage and information_schema.referential_constraints do not exists return empty result
        return $this->connection->executeQuery('SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE 1 != 1');
    }

    protected function fetchTableOptionsByTable(string $databaseName, ?string $tableName = null): array
    {
        // Table information_schema.COLLATION_CHARACTER_SET_APPLICABILITY does not exist return empty result
        return [];
    }

    protected function _getPortableTableForeignKeyDefinition(array $tableForeignKey): ForeignKeyConstraint
    {
        throw NotSupported::new(__METHOD__);
    }
}
