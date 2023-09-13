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

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Types\Type;

use function array_key_exists;
use function mb_strtolower;
use function sprintf;

abstract class ArrayType extends Type implements ClickHouseType
{
    protected const ARRAY_TYPES = [
        'array(int8)'     => ArrayInt8Type::class,
        'array(int16)'    => ArrayInt16Type::class,
        'array(int32)'    => ArrayInt32Type::class,
        'array(int64)'    => ArrayInt64Type::class,
        'array(uint8)'    => ArrayUInt8Type::class,
        'array(uint16)'   => ArrayUInt16Type::class,
        'array(uint32)'   => ArrayUInt32Type::class,
        'array(uint64)'   => ArrayUInt64Type::class,
        'array(float32)'  => ArrayFloat32Type::class,
        'array(float64)'  => ArrayFloat64Type::class,
        'array(string)'   => ArrayStringableType::class,
        'array(datetime)' => ArrayDateTimeType::class,
        'array(date)'     => ArrayDateType::class,
    ];

    public static function registerArrayTypes(AbstractPlatform $platform): void
    {
        foreach (self::ARRAY_TYPES as $typeName => $className) {
            if (self::hasType($typeName)) {
                continue;
            }

            self::addType($typeName, $className);

            foreach (Type::getType($typeName)->getMappedDatabaseTypes($platform) as $dbType) {
                $platform->registerDoctrineTypeMapping($dbType, $typeName);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public function getMappedDatabaseTypes(AbstractPlatform $platform): array
    {
        return [$this->getName()];
    }

    /**
     * {@inheritDoc}
     */
    public function getSQLDeclaration(array $column, AbstractPlatform $platform): string
    {
        return $this->getDeclaration($column);
    }

    /**
     * {@inheritDoc}
     */
    public function getName(): string
    {
        return mb_strtolower($this->getDeclaration());
    }

    /**
     * @param array $fieldDeclaration
     */
    protected function getDeclaration(array $fieldDeclaration = []): string
    {
        return sprintf(
            array_key_exists(
                'notnull',
                $fieldDeclaration
            ) && $fieldDeclaration['notnull'] === false ? 'Array(Nullable(%s%s%s))' : 'Array(%s%s%s)',
            $this instanceof UnsignedNumericalClickHouseType ? 'U' : '',
            $this->getBaseClickHouseType(),
            $this instanceof BitNumericalClickHouseType ? $this->getBits() : ''
        );
    }
}
