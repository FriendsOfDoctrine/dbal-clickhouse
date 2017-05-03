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
 * Array(Float*) Types basic class
 *
 * @author Mochalygin <a@mochalygin.ru>
 */
abstract class ArrayFloatType extends ArrayNumType
{
    /**
     * {@inheritDoc}
     */
    public function getSQLDeclaration(array $fieldDeclaration, AbstractPlatform $platform)
    {
        return 'Array(Float' . $this->getBitness() . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getName()
    {
        return 'array(float' . $this->getBitness() . ')';
    }
}
