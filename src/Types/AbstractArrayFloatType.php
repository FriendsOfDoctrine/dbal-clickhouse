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

use Doctrine\DBAL\Platforms\AbstractPlatform;

/**
 * Array(Float*) Types basic class
 */
abstract class AbstractArrayFloatType extends AbstractArrayNumType
{
    /**
     * {@inheritDoc}
     */
    public function getSQLDeclaration(array $fieldDeclaration, AbstractPlatform $platform) : string
    {
        return 'Array(Float' . $this->getBitness() . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getName() : string
    {
        return 'array(float' . $this->getBitness() . ')';
    }
}
