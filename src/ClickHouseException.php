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

namespace FOD\DBALClickHouse;

use Doctrine\DBAL\Exception;

/**
 * Specific Exception for ClickHouse
 */
class ClickHouseException extends Exception
{
    public static function notSupported($method) : ClickHouseException
    {
        return new self(sprintf("Operation '%s' is not supported by platform.", $method));
    }
}
