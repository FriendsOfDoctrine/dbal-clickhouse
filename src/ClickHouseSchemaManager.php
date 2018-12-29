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
use const CASE_LOWER;
use function array_change_key_case;
use function array_filter;
use function array_key_exists;
use function array_map;
use function array_reverse;
use function current;
use function explode;
use function is_array;
use function preg_match;
use function preg_replace;
use function str_replace;
use function stripos;
use function strpos;
use function strtolower;
use function trim;

/**
 * Schema manager for the ClickHouse DBMS.
 */
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
    protected function _getPortableViewDefinition($view)
    {
        $statement = $this->_conn->fetchColumn('SHOW CREATE TABLE ' . $view['name']);

        return new View($view['name'], $statement);
    }

    /**
     * {@inheritdoc}
     */
    public function listTableIndexes($table) : array
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
                function (string $column) {
                    return strpos($column, '(') === false;
                }
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
    protected function _getPortableTableColumnDefinition($tableColumn) : Column
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

        if (stripos($columnType, 'fixedstring') === 0) {
            // get length from FixedString definition
            $length = preg_replace('~.*\(([0-9]*)\).*~', '$1', $columnType);
            $dbType = 'fixedstring';
            $fixed  = true;
        }

        $unsigned = false;

        if (stripos($columnType, 'uint') === 0) {

            $unsigned = true;

            $dbType = substr($columnType, 1);
        }

        $precision = 10;

        $scale = 0;

        if (stripos($columnType, 'decimal') === 0) {

            $unsigned = false;

            $dbType = 'decimal';

            preg_match('/([0-9]{1,2})(, )?([0-9]{1,2})?/', $columnType, $matches);

            $precision = isset($matches[1]) ? $matches[1] : 10;

            $scale = isset($matches[3]) ? $matches[3] : 0;
        }

        if (! isset($tableColumn['name'])) {
            $tableColumn['name'] = '';
        }

        $default = null;
        //TODO process not only DEFAULT type, but ALIAS and MATERIALIZED too
        if ($tableColumn['default_expression'] && strtolower($tableColumn['default_type']) === 'default') {
            $default = $tableColumn['default_expression'];
        }

        $options = [
            'length' => $length,
            'notnull' => $notnull,
            'default' => $default,
            'primary' => false,
            'fixed' => $fixed,
            'unsigned' => $unsigned,
            'autoincrement' => false,
            'comment' => null,
            'precision' => $precision,
            'scale' => $scale,
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
