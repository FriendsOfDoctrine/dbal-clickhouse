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
use function array_filter;
use function array_map;
use function implode;

/**
 * Array(DateTime) Type class
 */
class ArrayDateTimeType extends ArrayType implements DatableClickHouseType
{
    public function getBaseClickHouseType() : string
    {
        return DatableClickHouseType::TYPE_DATE_TIME;
    }

    /**
     * {@inheritDoc}
     */
    public function convertToPHPValue($value, AbstractPlatform $platform) : array
    {
        return array_map(
            function ($stringDatetime) use ($platform) {
                return \DateTime::createFromFormat($platform->getDateTimeFormatString(), $stringDatetime);
            },
            (array) $value
        );
    }

    /**
     * {@inheritDoc}
     */
    public function convertToDatabaseValue($value, AbstractPlatform $platform) : string
    {
        return '[' . implode(
            ', ',
            array_map(
                function (\DateTime $datetime) use ($platform) {
                    return "'" . $datetime->format($platform->getDateTimeFormatString()) . "'";
                },
                array_filter(
                    (array) $value,
                    function ($datetime) {
                        return $datetime instanceof \DateTime;
                    }
                )
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
