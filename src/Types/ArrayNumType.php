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

/**
 * Array(Numeric) Types basic class
 *
 * @author Mochalygin <a@mochalygin.ru>
 */
abstract class ArrayNumType extends ArrayType
{
    /**
     * @return int Bitness of integers or floats in Array (Array(Int{bitness}) or Array(Float{bitness}))
     */
    protected function getBitness()
    {
        return static::BITNESS;
    }
}
