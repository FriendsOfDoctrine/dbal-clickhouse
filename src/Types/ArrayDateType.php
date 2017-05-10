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

use Doctrine\DBAL\Platforms\AbstractPlatform;

/**
 * Array(Date) Type class
 *
 * @author Mochalygin <a@mochalygin.ru>
 */
class ArrayDateType extends ArrayType
{
    /**
     * {@inheritDoc}
     */
    public function getSQLDeclaration(array $fieldDeclaration, AbstractPlatform $platform)
    {
        return 'Array(Date)';
    }

    /**
     * {@inheritDoc}
     */
    public function getName()
    {
        return 'array(date)';
    }

    /**
     * {@inheritDoc}
     */
    public function convertToPHPValue($value, AbstractPlatform $platform)
    {
        $datetimes = [];
        foreach ($value as $stringDatetime) {
            $datetimes[] = \DateTime::createFromFormat($platform->getDateFormatString(), $stringDatetime);
        }

        return $datetimes;
    }

    /**
     * {@inheritDoc}
     */
    public function convertToDatabaseValue($value, AbstractPlatform $platform)
    {
        $strings = [];
        foreach ($value as $datetime) {
            $strings[] = "'" . $datetime->format($platform->getDateFormatString()) . "'";
        }

        return '[' . implode(', ', $strings) . ']';
    }

    /**
     * {@inheritDoc}
     */
    public function getBindingType()
    {
        return \PDO::PARAM_INT;
    }
}
