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
 * Array(Float32) Type
 *
 * @author Mochalygin <a@mochalygin.ru>
 */
class ArrayFloat32Type extends ArrayFloatType
{
    const BITNESS = 32;

    /** {@inheritdoc} */
    protected function getBitness(): int
    {
        return self::BITNESS;
    }
}
