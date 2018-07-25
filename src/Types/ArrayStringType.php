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

use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\AbstractPlatform;

/**
 * Array(String) Type class
 */
class ArrayStringType extends AbstractArrayType implements StringTypeInterface
{
    public function getBaseClickHouseType(): string
    {
        return StringTypeInterface::TYPE_STRING;
    }

    /**
     * {@inheritDoc}
     */
    public function convertToDatabaseValue($value, AbstractPlatform $platform)
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
