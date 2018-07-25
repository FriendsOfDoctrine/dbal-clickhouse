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

namespace FOD\DBALClickHouse\Types;

/**
 * Array(UInt32) Type
 */
class ArrayUInt32Type extends AbstractArrayType implements BitInterface, UnsignedInterface
{
    public function getBits(): int
    {
        return BitInterface::THIRTY_TWO_BIT;
    }

    public function getBaseClickHouseType(): string
    {
        return NumericalTypeInterface::TYPE_INT;
    }
}