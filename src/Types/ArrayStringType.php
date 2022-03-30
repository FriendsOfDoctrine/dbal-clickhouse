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

use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use function array_map;
use function implode;

/**
 * Array(String) Type class
 */
class ArrayStringType extends ArrayType implements StringClickHouseType
{
    public function getBaseClickHouseType() : string
    {
        return StringClickHouseType::TYPE_STRING;
    }

    /**
     * {@inheritDoc}
     */
    public function convertToDatabaseValue($value, AbstractPlatform $platform) : string
    {
        return '[' . implode(
            ', ',
            array_map(
                function (string $value) use ($platform) {
                        return $platform->quoteStringLiteral($value);
                },
                (array) $value
            )
        ) . ']';
    }

    /**
     * {@inheritDoc}
     */
    public function getBindingType() : int
    {
        return ParameterType::INTEGER;
    }
}
