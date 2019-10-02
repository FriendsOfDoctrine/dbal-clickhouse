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

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use function strtolower;

/**
 * Clickhouse types basic class
 */
abstract class Type extends \Doctrine\DBAL\Types\Type implements ClickHouseType
{

    public const INT8   = 'int8';
    public const INT16  = 'int16';
    public const INT32  = 'int32';
    public const INT64  = 'int64';
    public const FLOAT32 = 'float32';
    public const FLOAT64 = 'float64';
    public const DECIMAL = 'decimal';

    protected const TYPES = [
        self::INT8 => Int8Type::class,
        self::INT16 => Int16Type::class,
        self::INT32 => Int32Type::class,
        self::INT64 => Int64Type::class,
        self::FLOAT32 => Float32Type::class,
        self::FLOAT64 => Float64Type::class,
        self::DECIMAL => DecimalType::class
    ];

    /**
     * Register Array types to the type map.
     *
     * @param AbstractPlatform $platform
     * @return void
     *
     * @throws DBALException
     */
    public static function registerTypes(AbstractPlatform $platform) : void
    {
        foreach (self::TYPES as $typeName => $className) {
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
    public function getMappedDatabaseTypes(AbstractPlatform $platform) : array
    {
        return [$this->getName()];
    }

    /**
     * {@inheritDoc}
     */
    public function getSQLDeclaration(array $fieldDeclaration, AbstractPlatform $platform) : string
    {
        return $this->getDeclaration($fieldDeclaration);
    }

    /**
     * {@inheritDoc}
     */
    public function getName() : string
    {
        return strtolower($this->getDeclaration());
    }

    /**
     * @param mixed[] $fieldDeclaration
     *
     * @return string $declaration
     *
     * @throws DBALException
     */
    abstract protected function getDeclaration(array $fieldDeclaration = []) : string;
}
