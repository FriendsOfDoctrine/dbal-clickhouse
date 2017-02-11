<?php

/**
 * Doctrine DBAL library for ClickHouse -- an open-source column-oriented DBMS for OLAP (https://clickhouse.yandex)
 */

namespace Mochalygin\DoctrineDBALClickHouse;

use Doctrine\DBAL\Schema\AbstractSchemaManager;
//use Mochalygin\DoctrineDBALClickHouse\ClickHousePlatform;
//use Doctrine\DBAL\Types\Type;

/**
 * Schema manager for ClickHouse database {@link https://clickhouse.yandex/}
 *
 * @author Mochalygin <a@mochalygin.ru>
 */
class ClickHouseSchemaManager extends AbstractSchemaManager
{
    /**
     * Gets Table Column Definition.
     *
     * @param array $tableColumn
     *
     * @return \Doctrine\DBAL\Schema\Column
     */
    protected function _getPortableTableColumnDefinition($tableColumn)
    {
        throw new \Exception('Ololo');
    }

}
