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
 * Array(*) Types basic class
 *
 * @author Mochalygin <a@mochalygin.ru>
 */
abstract class ArrayType extends Type
{

    const ARRAY_TYPES = [
        'array(int8)' => 'FOD\DBALClickHouse\Types\ArrayInt8Type',
        'array(int16)' => 'FOD\DBALClickHouse\Types\ArrayInt16Type',
        'array(int32)' => 'FOD\DBALClickHouse\Types\ArrayInt32Type',
        'array(int64)' => 'FOD\DBALClickHouse\Types\ArrayInt64Type',
        'array(uint8)' => 'FOD\DBALClickHouse\Types\ArrayUInt8Type',
        'array(uint16)' => 'FOD\DBALClickHouse\Types\ArrayUInt16Type',
        'array(uint32)' => 'FOD\DBALClickHouse\Types\ArrayUInt32Type',
        'array(uint64)' => 'FOD\DBALClickHouse\Types\ArrayUInt64Type',
        'array(float32)' => 'FOD\DBALClickHouse\Types\ArrayFloat32Type',
        'array(float64)' => 'FOD\DBALClickHouse\Types\ArrayFloat64Type',
        'array(string)' => 'FOD\DBALClickHouse\Types\ArrayStringType',
        'array(datetime)' => 'FOD\DBALClickHouse\Types\ArrayDateTimeType',
        'array(date)' => 'FOD\DBALClickHouse\Types\ArrayDateType'
    ];

    /**
     * Register Array types to the type map.
     * 
     * @return void
     */
    public static function registerArrayTypes(AbstractPlatform $platform)
    {
        foreach (self::ARRAY_TYPES as $typeName => $className) {
            self::addType($typeName, $className);
            foreach (Type::getType($typeName)->getMappedDatabaseTypes($platform) as $dbType) {
                $platform->registerDoctrineTypeMapping($dbType, $typeName);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public function getMappedDatabaseTypes(AbstractPlatform $platform)
    {
        return [
            $this->getName()
        ];
    }
}
