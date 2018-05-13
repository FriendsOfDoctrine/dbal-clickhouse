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
        'array(int8)' => ArrayInt8Type::class,
        'array(int16)' => ArrayInt16Type::class,
        'array(int32)' => ArrayInt32Type::class,
        'array(int64)' => ArrayInt64Type::class,
        'array(uint8)' => ArrayUInt8Type::class,
        'array(uint16)' => ArrayUInt16Type::class,
        'array(uint32)' => ArrayUInt32Type::class,
        'array(uint64)' => ArrayUInt64Type::class,
        'array(float32)' => ArrayFloat32Type::class,
        'array(float64)' => ArrayFloat64Type::class,
        'array(string)' => ArrayStringType::class,
        'array(datetime)' => ArrayDateTimeType::class,
        'array(date)' => ArrayDateType::class,
    ];

    /**
     * Register Array types to the type map.
     *
     * @return void
     */
    public static function registerArrayTypes(AbstractPlatform $platform)
    {
        foreach (self::ARRAY_TYPES as $typeName => $className) {
            if (!self::hasType($typeName)) {
                self::addType($typeName, $className);
                foreach (Type::getType($typeName)->getMappedDatabaseTypes($platform) as $dbType) {
                    $platform->registerDoctrineTypeMapping($dbType, $typeName);
                }
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
