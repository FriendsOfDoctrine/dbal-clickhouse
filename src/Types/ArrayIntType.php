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

namespace FOD\DBALClickHouse\Types;

use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Platforms\AbstractPlatform;

/**
 * Array(Int*) Types basic class
 *
 * @author Mochalygin <a@mochalygin.ru>
 */
abstract class ArrayIntType extends Type
{
    /**
     * Register Array types to the type map.
     * 
     * @return void
     */
    public static function registerArrayTypes()
    {
        self::addType('array(int8)', 'FOD\DBALClickHouse\Types\ArrayInt8Type');
        self::addType('array(int16)', 'FOD\DBALClickHouse\Types\ArrayInt16Type');
        self::addType('array(int32)', 'FOD\DBALClickHouse\Types\ArrayInt32Type');
        self::addType('array(int64)', 'FOD\DBALClickHouse\Types\ArrayInt64Type');
    }

    /**
     * {@inheritDoc}
     */
    public function getSQLDeclaration(array $fieldDeclaration, AbstractPlatform $platform)
    {
        return 'Array(' . (!empty($fieldDeclaration['unsigned']) ? 'U' : '') . 'Int' . $this->getBitness() . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getName()
    {
        return 'array(int' . $this->getBitness() . ')';
    }

    /**
     * @return int Bitness of integers in Array (Array(Int{bitness}))
     */
    protected function getBitness()
    {
        return static::BITNESS;
    }

}
