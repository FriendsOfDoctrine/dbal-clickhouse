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

use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Schema\Column;
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
    protected function _getPortableTableDefinition($table)
    {
        return $table['name'];
    }

    /**
     * {@inheritdoc}
     */
    protected function _getPortableViewDefinition($view): View
    {
        $statement = $this->_conn->fetchOne('SHOW CREATE TABLE ' . $view['name']);

        return new View($view['name'], $statement);
    }

    /**
     * {@inheritdoc}
     */
    public function listTableIndexes($table): array
    {
        $tableView = $this->_getPortableViewDefinition(['name' => $table]);

        preg_match(
            '/MergeTree\(([\w+, \(\)]+)(?= \(((?:[^()]|\((?2)\))+)\),)/mi',
            $tableView->getSql(),
            $matches
        );

        if (is_array($matches) && array_key_exists(2, $matches)) {
            $indexColumns = array_filter(
                array_map('trim', explode(',', $matches[2])),
                fn (string $column): bool => !str_contains($column, '(')
            );

            return [
                new Index(
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
    protected function _getPortableTableColumnDefinition($tableColumn): Column
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
            $length = preg_replace('~.*\(([0-9]*)\).*~', '$1', $columnType);
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
        if ($tableColumn['default_expression'] && mb_strtolower($tableColumn['default_type']) === 'default') {
            $default = $tableColumn['default_expression'];
        }

        $options = [
            'length'        => $length,
            'unsigned'      => $unsigned,
            'fixed'         => $fixed,
            'default'       => $default,
            'notnull'       => $notnull,
            'autoincrement' => false,
            'comment'       => null,
        ];

        return new Column(
            $tableColumn['name'],
            Type::getType($this->_platform->getDoctrineTypeMapping($dbType)),
            $options
        );
    }

    /**
     * {@inheritdoc}
     */
    protected function _getPortableDatabaseDefinition($database)
    {
        return $database['name'];
    }
}
