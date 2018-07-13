<?php

declare(strict_types=1);

/*
 * This file is part of the FODDBALClickHouse package -- Doctrine DBAL library
 * for ClickHouse (a column-oriented DBMS for OLAP <https://clickhouse.yandex/>)
 *
 * (c) FriendsOfDoctrine <https://github.com/FriendsOfDoctrine/>.
 *
 * For the full copyright and license inflormation, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace FOD\DBALClickHouse;

use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\View;
use Doctrine\DBAL\Types\Type;

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
        return [];
    }

    /**
     * {@inheritdoc}
     */
    protected function _getPortableTableColumnDefinition($tableColumn) : Column
    {
        $tableColumn = array_change_key_case($tableColumn, CASE_LOWER);

        $dbType = $tableColumn['type'];
        $length = null;
        $fixed  = false;
        if (stripos(trim($tableColumn['type']), 'fixedstring') === 0) {
            // get length from FixedString definition
            $length = preg_replace('~.*\(([0-9]*)\).*~', '$1', $tableColumn['type']);
            $dbType = 'fixedstring';
            $fixed  = true;
        }

        $unsigned = false;
        if (stripos(trim($tableColumn['type']), 'uint') === 0) {
            $unsigned = true;
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
            'notnull' => true,
            'default' => $default,
            'primary' => false,
            'fixed' => $fixed,
            'unsigned' => $unsigned,
            'autoincrement' => false,
            'comment' => null,
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
