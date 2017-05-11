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
 * Array(String) Type class
 *
 * @author Mochalygin <a@mochalygin.ru>
 */
class ArrayStringType extends ArrayType
{
    /**
     * {@inheritDoc}
     */
    public function getSQLDeclaration(array $fieldDeclaration, AbstractPlatform $platform)
    {
        return 'Array(String)';
    }

    /**
     * {@inheritDoc}
     */
    public function getName()
    {
        return 'array(string)';
    }

    /**
     * {@inheritDoc}
     */
    public function convertToDatabaseValue($value, AbstractPlatform $platform)
    {

        $str = array_map(
            function ($value) use ($platform) {
                return $platform->quoteStringLiteral($value);
            },
            $value
        );

        return '[' . implode(', ', $str) . ']';
    }

    /**
     * {@inheritDoc}
     */
    public function getBindingType()
    {
        return \PDO::PARAM_INT;
    }
}
