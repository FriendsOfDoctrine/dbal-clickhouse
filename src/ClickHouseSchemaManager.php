<?php

/**
 * Doctrine DBAL library for ClickHouse -- an open-source column-oriented DBMS for OLAP (https://clickhouse.yandex)
 */

namespace Mochalygin\DoctrineDBALClickHouse;

use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\View;

/**
 * Schema manager for ClickHouse database {@link https://clickhouse.yandex/}
 *
 * @author Mochalygin <a@mochalygin.ru>
 */
class ClickHouseSchemaManager extends AbstractSchemaManager
{

    /**
     * {@inheritdoc}
     */
    protected function _getPortableTableDefinition($table)
    {
        if ($this->_conn->getDatabase() !== $table['database']) {
            return false;
        }

        return $table['name'];
    }

    /**
     * {@inheritdoc}
     */
    protected function _getPortableViewDefinition($view)
    {
        $statement = $this->_conn->fetchColumn('SHOW CREATE TABLE ' . $view['name'] . ' FORMAT JSON');
        return new View($view['name'], $statement);
    }

    /**
     * {@inheritdoc}
     */
    public function listTableIndexes($table)
    {
        return [];
    }

    /**
     * {@inheritdoc}
     */
    protected function _getPortableTableColumnDefinition($tableColumn)
    {
        $tableColumn = array_change_key_case($tableColumn, CASE_LOWER);

        $dbType = $tableColumn['type'];
        $length = null;
        $fixed = false;
        if (substr(strtolower($tableColumn['type']), 0, 11) == 'fixedstring') {
            // get length from FixedString definition
            $length = preg_replace('~.*\(([0-9]*)\).*~', '$1', $tableColumn['type']);
            $dbType = 'fixedstring';
            $fixed = true;
        }

        $unsigned = false;
        if (substr(strtolower($tableColumn['type']), 0, 4) === 'uint') {
            $unsigned = true;
        }

        if (! isset($tableColumn['name'])) {
            $tableColumn['name'] = '';
        }

        $type = $this->_platform->getDoctrineTypeMapping($dbType);

        $default = null;
        //TODO process not only DEFAULT type, but ALIAS and MATERIALIZED too
        if ($tableColumn['default_expression'] && 'default' === strtolower($tableColumn['default_type'])) {
            $default = $tableColumn['default_expression'];
        }

        $options = array(
            'length'        => $length,
            'notnull'       => true,
            'default'       => $default,
            'primary'       => false,
            'fixed'         => $fixed,
            'unsigned'      => $unsigned,
            'autoincrement' => false,
            'comment'       => null,
        );

        return  new Column(
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
