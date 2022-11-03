<?php declare(strict_types=1);

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

/**
 * Decimal Type
 */
class DecimalType extends Type
{

    public function getBaseClickHouseType() : string
    {
        return NumericalClickHouseType::TYPE_DECIMAL;
    }

    public function getName(): string
    {
        return Type::DECIMAL;
    }

    /**
     * {@inheritdoc}
     */
    protected function getDeclaration(array $fieldDeclaration = []) : string
    {

        $fieldDeclaration['precision'] = ! isset($fieldDeclaration['precision']) || empty($fieldDeclaration['precision'])
            ? 10 : $fieldDeclaration['precision'];
        $fieldDeclaration['scale']     = ! isset($fieldDeclaration['scale']) || empty($fieldDeclaration['scale'])
            ? 0 : $fieldDeclaration['scale'];

        return $this->getBaseClickHouseType(). "({$fieldDeclaration['precision']}, {$fieldDeclaration['scale']})";
    }
}
