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
 * Array(Int64) Type
 *
 * @author Mochalygin <a@mochalygin.ru>
 */
class ArrayInt64Type extends ArrayIntType
{
    const BITNESS = 64;

    /** {@inheritdoc} */
    protected function getBitness(): int
    {
        return self::BITNESS;
    }
}
