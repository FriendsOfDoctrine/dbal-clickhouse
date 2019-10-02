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

namespace FOD\DBALClickHouse\Types;

/**
 * Array(UInt8) Type
 */
class ArrayUInt8Type extends ArrayType implements BitNumericalClickHouseType, UnsignedNumericalClickHouseType
{
    public function getBits() : int
    {
        return BitNumericalClickHouseType::EIGHT_BIT;
    }

    public function getBaseClickHouseType() : string
    {
        return NumericalClickHouseType::TYPE_INT;
    }
}
