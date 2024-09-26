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

class ArrayStringableType extends ArrayType implements StringableClickHouseType
{
    public function getBaseClickHouseType(): string
    {
        return StringableClickHouseType::TYPE_STRING;
    }

    /**
     * {@inheritDoc}
     */
    public function convertToDatabaseValue($value, AbstractPlatform $platform): string
    {
        return '[' . implode(
                ', ',
                array_map(
                    function (string $value) use ($platform): string {
                        return $platform->quoteStringLiteral($value);
                    },
                    (array) $value
                )
            ) . ']';
    }

    /**
     * {@inheritDoc}
     */
    public function getBindingType(): ParameterType
    {
        return ParameterType::INTEGER;
    }
}
