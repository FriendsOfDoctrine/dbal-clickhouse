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

namespace FOD\DBALClickHouse;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception\InvalidColumnDeclaration;
use Doctrine\DBAL\Exception\InvalidColumnType;
use Doctrine\DBAL\LockMode;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\DateIntervalUnit;
use Doctrine\DBAL\Platforms\Exception\NotSupported;
use Doctrine\DBAL\Platforms\Keywords\KeywordList;
use Doctrine\DBAL\Platforms\TrimMode;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Schema\ForeignKeyConstraint;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\TableDiff;
use Doctrine\DBAL\Schema\UniqueConstraint;
use Doctrine\DBAL\TransactionIsolationLevel;
use Doctrine\DBAL\Types\BigIntType;
use Doctrine\DBAL\Types\BlobType;
use Doctrine\DBAL\Types\DateTimeType;
use Doctrine\DBAL\Types\DateType;
use Doctrine\DBAL\Types\DecimalType;
use Doctrine\DBAL\Types\FloatType;
use Doctrine\DBAL\Types\IntegerType;
use Doctrine\DBAL\Types\SmallIntType;
use Doctrine\DBAL\Types\StringType;
use Doctrine\DBAL\Types\TextType;
use Doctrine\DBAL\Types\Type;
use FOD\DBALClickHouse\Types\BitNumericalClickHouseType;
use FOD\DBALClickHouse\Types\DatableClickHouseType;
use FOD\DBALClickHouse\Types\NumericalClickHouseType;
use FOD\DBALClickHouse\Types\StringableClickHouseType;
use FOD\DBALClickHouse\Types\UnsignedNumericalClickHouseType;

use function addslashes;
use function array_filter;
use function array_key_exists;
use function array_keys;
use function array_merge;
use function array_unique;
use function array_values;
use function count;
use function get_class;
use function implode;
use function in_array;
use function mb_stripos;
use function sprintf;
use function trim;

class ClickHousePlatform extends AbstractPlatform
{
    /**
     * {@inheritDoc}
     */
    public function getBooleanTypeDeclarationSQL(array $column): string
    {
        return $this->prepareDeclarationSQL(
            UnsignedNumericalClickHouseType::UNSIGNED_CHAR .
            NumericalClickHouseType::TYPE_INT . BitNumericalClickHouseType::EIGHT_BIT,
            $column
        );
    }

    /**
     * {@inheritDoc}
     */
    public function getIntegerTypeDeclarationSQL(array $column): string
    {
        return $this->prepareDeclarationSQL(
            $this->_getCommonIntegerTypeDeclarationSQL($column) .
            NumericalClickHouseType::TYPE_INT . BitNumericalClickHouseType::THIRTY_TWO_BIT,
            $column
        );
    }

    /**
     * {@inheritDoc}
     */
    public function getBigIntTypeDeclarationSQL(array $column): string
    {
        return $this->prepareDeclarationSQL(StringableClickHouseType::TYPE_STRING, $column);
    }

    /**
     * {@inheritDoc}
     */
    public function getSmallIntTypeDeclarationSQL(array $column): string
    {
        return $this->prepareDeclarationSQL(
            $this->_getCommonIntegerTypeDeclarationSQL($column) .
            NumericalClickHouseType::TYPE_INT . BitNumericalClickHouseType::SIXTEEN_BIT,
            $column
        );
    }

    /**
     * {@inheritDoc}
     */
    protected function _getCommonIntegerTypeDeclarationSQL(array $column): string
    {
        if (!empty($column['autoincrement'])) {
            throw NotSupported::new('Clickhouse does not support AUTO_INCREMENT fields');
        }

        return empty($column['unsigned']) ? '' : UnsignedNumericalClickHouseType::UNSIGNED_CHAR;
    }

    /**
     * {@inheritDoc}
     */
    protected function initializeDoctrineTypeMappings(): void
    {
        $this->doctrineTypeMapping = [
            'int8'    => 'smallint',
            'int16'   => 'integer',
            'int32'   => 'integer',
            'int64'   => 'bigint',
            'uint8'   => 'smallint',
            'uint16'  => 'integer',
            'uint32'  => 'integer',
            'uint64'  => 'bigint',
            'float32' => 'decimal',
            'float64' => 'float',

            'string'      => 'string',
            'fixedstring' => 'string',
            'date'        => 'date',
            'datetime'    => 'datetime',

            'array(int8)'    => 'array',
            'array(int16)'   => 'array',
            'array(int32)'   => 'array',
            'array(int64)'   => 'array',
            'array(uint8)'   => 'array',
            'array(uint16)'  => 'array',
            'array(uint32)'  => 'array',
            'array(uint64)'  => 'array',
            'array(float32)' => 'array',
            'array(float64)' => 'array',

            'array(string)'   => 'array',
            'array(date)'     => 'array',
            'array(datetime)' => 'array',

            'enum8'  => 'string',
            'enum16' => 'string',

            'nullable(int8)'    => 'smallint',
            'nullable(int16)'   => 'integer',
            'nullable(int32)'   => 'integer',
            'nullable(int64)'   => 'bigint',
            'nullable(uint8)'   => 'smallint',
            'nullable(uint16)'  => 'integer',
            'nullable(uint32)'  => 'integer',
            'nullable(uint64)'  => 'bigint',
            'nullable(float32)' => 'decimal',
            'nullable(float64)' => 'float',

            'nullable(string)'      => 'string',
            'nullable(fixedstring)' => 'string',
            'nullable(date)'        => 'date',
            'nullable(datetime)'    => 'datetime',

            'array(nullable(int8))'    => 'array',
            'array(nullable(int16))'   => 'array',
            'array(nullable(int32))'   => 'array',
            'array(nullable(int64))'   => 'array',
            'array(nullable(uint8))'   => 'array',
            'array(nullable(uint16))'  => 'array',
            'array(nullable(uint32))'  => 'array',
            'array(nullable(uint64))'  => 'array',
            'array(nullable(float32))' => 'array',
            'array(nullable(float64))' => 'array',

            'array(nullable(string))'   => 'array',
            'array(nullable(date))'     => 'array',
            'array(nullable(datetime))' => 'array',
        ];
    }

    /**
     * {@inheritDoc}
     */
    protected function getVarcharTypeDeclarationSQLSnippet(?int $length): string
    {
        return StringableClickHouseType::TYPE_STRING;
    }

    public function getStringTypeDeclarationSQL(array $column): string
    {
        $length = $column['length'] ?? null;

        if (empty($column['fixed'])) {
            try {
                return $this->prepareDeclarationSQL($this->getVarcharTypeDeclarationSQLSnippet($length), $column);
            } catch (InvalidColumnType $e) {
                throw InvalidColumnDeclaration::fromInvalidColumnType($column['name'], $e);
            }
        }

        return $this->prepareDeclarationSQL($this->getCharTypeDeclarationSQLSnippet($length), $column);
    }

    /**
     * @param int|null $length The length of the column in characters
     *                         or NULL if the length should be omitted.
     */
    protected function getCharTypeDeclarationSQLSnippet(?int $length): string
    {
        $sql = StringableClickHouseType::TYPE_FIXED_STRING;

        if ($length !== null) {
            $sql .= sprintf('(%d)', $length);
        }

        return $sql;
    }

    /**
     * {@inheritDoc}
     */
    protected function getBinaryTypeDeclarationSQLSnippet(?int $length): string
    {
        if (null === $length) {
            return StringableClickHouseType::TYPE_STRING;
        }

        return StringableClickHouseType::TYPE_FIXED_STRING . '(' . $length . ')';
    }

    protected function getVarbinaryTypeDeclarationSQLSnippet(?int $length): string
    {
        if (null === $length) {
            return StringableClickHouseType::TYPE_STRING;
        }

        return StringableClickHouseType::TYPE_FIXED_STRING . '(' . $length . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getClobTypeDeclarationSQL(array $column): string
    {
        return $this->prepareDeclarationSQL(StringableClickHouseType::TYPE_STRING, $column);
    }

    /**
     * {@inheritDoc}
     */
    public function getBlobTypeDeclarationSQL(array $column): string
    {
        return $this->prepareDeclarationSQL(StringableClickHouseType::TYPE_STRING, $column);
    }

    // scalar functions

    /**
     * {@inheritDoc}
     */
    public function getLengthExpression(string $string): string
    {
        return 'lengthUTF8(CAST(' . $string . ' AS String))';
    }

    /**
     * {@inheritDoc}
     */
    public function getModExpression(string $dividend, string $divisor): string
    {
        return 'modulo(' . $dividend . ', ' . $divisor . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getTrimExpression(string $str, TrimMode $mode = TrimMode::UNSPECIFIED, ?string $char = null): string
    {
        if (null === $char) {
            return match ($mode) {
                TrimMode::LEADING => sprintf("replaceRegexpAll(%s, '(^\\\s+)', '')", $str),
                TrimMode::TRAILING => sprintf("replaceRegexpAll(%s, '(\\\s+$)', '')", $str),
                default => sprintf("replaceRegexpAll(%s, '(^\\\s+|\\\s+$)', '')", $str),
            };
        }

        return sprintf("replaceRegexpAll(%s, '(^%s+|%s+$)', '')", $str, $char, $char);
    }

    /**
     * {@inheritDoc}
     */
    public function getLocateExpression(string $string, string $substring, ?string $start = null): string
    {
        return 'positionUTF8(' . $string . ', ' . $substring . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getSubstringExpression(string $string, string $start, ?string $length = null): string
    {
        if ($length === null) {
            throw new \InvalidArgumentException("'length' argument must be a constant");
        }

        return 'substringUTF8(' . $string . ', ' . $start . ', ' . $length . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getConcatExpression(string ...$string): string
    {
        return 'concat(' . implode(', ', $string) . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getDateDiffExpression(string $date1, string $date2): string
    {
        return 'CAST(' . $date1 . ' AS Date) - CAST(' . $date2 . ' AS Date)';
    }

    /**
     * {@inheritDoc}
     */
    public function getDateAddSecondsExpression(string $date, string $seconds): string
    {
        return $this->getDateArithmeticIntervalExpression($date, '+', $seconds, DateIntervalUnit::SECOND);
    }

    /**
     * {@inheritDoc}
     */
    public function getDateSubSecondsExpression(string $date, string $seconds): string
    {
        return $this->getDateArithmeticIntervalExpression($date, '-', $seconds, DateIntervalUnit::SECOND);
    }

    /**
     * {@inheritDoc}
     */
    public function getDateAddMinutesExpression(string $date, string $minutes): string
    {
        return $this->getDateArithmeticIntervalExpression($date, '+', $minutes, DateIntervalUnit::MINUTE);
    }

    /**
     * {@inheritDoc}
     */
    public function getDateSubMinutesExpression(string $date, string $minutes): string
    {
        return $this->getDateArithmeticIntervalExpression($date, '-', $minutes, DateIntervalUnit::MINUTE);
    }

    /**
     * {@inheritDoc}
     */
    public function getDateAddHourExpression(string $date, string $hours): string
    {
        return $this->getDateArithmeticIntervalExpression($date, '+', $hours, DateIntervalUnit::HOUR);
    }

    /**
     * {@inheritDoc}
     */
    public function getDateSubHourExpression(string $date, string $hours): string
    {
        return $this->getDateArithmeticIntervalExpression($date, '-', $hours, DateIntervalUnit::HOUR);
    }

    /**
     * {@inheritDoc}
     */
    public function getDateAddDaysExpression(string $date, string $days): string
    {
        return $this->getDateArithmeticIntervalExpression($date, '+', $days, DateIntervalUnit::DAY);
    }

    /**
     * {@inheritDoc}
     */
    public function getDateSubDaysExpression(string $date, string $days): string
    {
        return $this->getDateArithmeticIntervalExpression($date, '-', $days, DateIntervalUnit::DAY);
    }

    /**
     * {@inheritDoc}
     */
    public function getDateAddWeeksExpression(string $date, string $weeks): string
    {
        return $this->getDateArithmeticIntervalExpression($date, '+', $weeks, DateIntervalUnit::WEEK);
    }

    /**
     * {@inheritDoc}
     */
    public function getDateSubWeeksExpression(string $date, string $weeks): string
    {
        return $this->getDateArithmeticIntervalExpression($date, '-', $weeks, DateIntervalUnit::WEEK);
    }

    /**
     * {@inheritDoc}
     */
    public function getBitAndComparisonExpression(string $value1, string $value2): string
    {
        return 'bitAnd(' . $value1 . ', ' . $value2 . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getBitOrComparisonExpression(string $value1, string $value2): string
    {
        return 'bitOr(' . $value1 . ', ' . $value2 . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function appendLockHint(string $fromClause, LockMode $lockMode): string
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getDropIndexSQL(string $name, string $table): string
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getDropConstraintSQL(string $name, string $table): string
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getDropForeignKeySQL(string $foreignKey, string $table): string
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getCommentOnColumnSQL(string $tableName, string $columnName, string $comment): string
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    protected function _getCreateTableSQL(string $name, array $columns, array $options = []): array
    {
        $engine        = !empty($options['engine']) ? $options['engine'] : 'ReplacingMergeTree';
        $engineOptions = '';

        $columns = \array_column($columns, null, 'name');

        if (isset($options['uniqueConstraints']) && !empty($options['uniqueConstraints'])) {
            throw NotSupported::new('uniqueConstraints');
        }

        if (isset($options['indexes']) && !empty($options['indexes'])) {
            throw NotSupported::new('uniqueConstraints');
        }

        /**
         * MergeTree* specific section
         */
        if (in_array(
            $engine,
            [
                'MergeTree',
                'CollapsingMergeTree',
                'SummingMergeTree',
                'AggregatingMergeTree',
                'ReplacingMergeTree',
                'GraphiteMergeTree',
            ],
            true
        )) {
            $indexGranularity   = !empty($options['indexGranularity']) ? $options['indexGranularity'] : 8192;
            $samplingExpression = '';

            /**
             * eventDateColumn section
             */
            $dateColumnParams = [
                'type'    => Type::getType('date'),
                'default' => 'today()',
            ];

            if (!empty($options['eventDateProviderColumn'])) {
                $options['eventDateProviderColumn'] = trim($options['eventDateProviderColumn']);

                if (!isset($columns[$options['eventDateProviderColumn']])) {
                    throw new \Exception(
                        'Table `' . $name . '` has not column with name: `' . $options['eventDateProviderColumn']
                    );
                }

                if (!($columns[$options['eventDateProviderColumn']]['type'] instanceof DateType) &&
                    !($columns[$options['eventDateProviderColumn']]['type'] instanceof DateTimeType) &&
                    !($columns[$options['eventDateProviderColumn']]['type'] instanceof TextType) &&
                    !($columns[$options['eventDateProviderColumn']]['type'] instanceof IntegerType) &&
                    !($columns[$options['eventDateProviderColumn']]['type'] instanceof SmallIntType) &&
                    !($columns[$options['eventDateProviderColumn']]['type'] instanceof BigIntType) &&
                    !($columns[$options['eventDateProviderColumn']]['type'] instanceof FloatType) &&
                    !($columns[$options['eventDateProviderColumn']]['type'] instanceof DecimalType) &&
                    (
                        !($columns[$options['eventDateProviderColumn']]['type'] instanceof StringType) ||
                        $columns[$options['eventDateProviderColumn']]['fixed']
                    )
                ) {
                    throw new \Exception(
                        'Column `' . $options['eventDateProviderColumn'] . '` with type `' .
                        $columns[$options['eventDateProviderColumn']]['type']->lookupName($columns[$options['eventDateProviderColumn']]['type']) .
                        '`, defined in `eventDateProviderColumn` option, has not valid DBAL Type'
                    );
                }

                $dateColumnParams['default'] =
                    $columns[$options['eventDateProviderColumn']]['type'] instanceof IntegerType ||
                    $columns[$options['eventDateProviderColumn']]['type'] instanceof SmallIntType ||
                    $columns[$options['eventDateProviderColumn']]['type'] instanceof FloatType ?
                        ('toDate(toDateTime(' . $options['eventDateProviderColumn'] . '))') :
                        ('toDate(' . $options['eventDateProviderColumn'] . ')');
            }

            if (empty($options['eventDateColumn'])) {
                $dateColumns = array_filter($columns, fn (array $column): bool => $column['type'] instanceof DateType);

                if ($dateColumns) {
                    throw new \Exception(
                        'Table `' . $name . '` has DateType columns: `' . implode(
                            '`, `',
                            array_keys($dateColumns)
                        ) .
                        '`, but no one of them is setted as `eventDateColumn` with 
                        $table->addOption("eventDateColumn", "%eventDateColumnName%")'
                    );
                }

                $eventDateColumnName = 'EventDate';
            } elseif (isset($columns[$options['eventDateColumn']])) {
                if (!($columns[$options['eventDateColumn']]['type'] instanceof DateType)) {
                    throw new \Exception(
                        'In table `' . $name . '` you have set field `' .
                        $options['eventDateColumn'] .
                        '` (' . get_class($columns[$options['eventDateColumn']]['type']) . ')
                         as `eventDateColumn`, but it is not instance of DateType'
                    );
                }

                $eventDateColumnName = $options['eventDateColumn'];

                unset($columns[$options['eventDateColumn']]);
            } else {
                $eventDateColumnName = $options['eventDateColumn'];
            }

            $dateColumnParams['name'] = $eventDateColumnName;
            // insert into very beginning
            $columns = [$eventDateColumnName => $dateColumnParams] + $columns;

            /**
             * Primary key section
             */
            if (empty($options['primary'])) {
                throw new \Exception('You need specify PrimaryKey for MergeTree* tables');
            }

            $primaryIndex = array_values($options['primary']);

            if (!empty($options['samplingExpression'])) {
                $samplingExpression = ', ' . $options['samplingExpression'];
                $primaryIndex[]     = $options['samplingExpression'];
            }

            $engineOptions = sprintf(
                '(%s%s, (%s), %d',
                $eventDateColumnName,
                $samplingExpression,
                implode(
                    ', ',
                    array_unique($primaryIndex)
                ),
                $indexGranularity
            );

            /**
             * any specific MergeTree* table parameters
             */
            if ($engine === 'ReplacingMergeTree' && !empty($options['versionColumn'])) {
                if (!isset($columns[$options['versionColumn']])) {
                    throw new \Exception(
                        'If you specify `versionColumn` for ReplacingMergeTree table -- 
                        you must add this column manually (any of UInt*, Date or DateTime types)'
                    );
                }

                if (!$columns[$options['versionColumn']]['type'] instanceof IntegerType &&
                    !$columns[$options['versionColumn']]['type'] instanceof BigIntType &&
                    !$columns[$options['versionColumn']]['type'] instanceof SmallIntType &&
                    !$columns[$options['versionColumn']]['type'] instanceof DateType &&
                    !$columns[$options['versionColumn']]['type'] instanceof DateTimeType
                ) {
                    throw new \Exception(
                        'For ReplacingMergeTree tables `versionColumn` must be any of UInt* family, 
                        or Date, or DateTime types. ' .
                        get_class($columns[$options['versionColumn']]['type']) . ' given.'
                    );
                }

                $engineOptions .= ', ' . $columns[$options['versionColumn']]['name'];
            }

            $engineOptions .= ')';
        }

        $sql[] = sprintf(
            'CREATE TABLE %s (%s) ENGINE = %s%s',
            $name,
            $this->getColumnDeclarationListSQL($columns),
            $engine,
            $engineOptions
        );

        return $sql;
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateForeignKeySQL(ForeignKeyConstraint $foreignKey, string $table): string
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getAlterTableSQL(TableDiff $diff): array
    {
        $columnSql  = [];
        $queryParts = [];

        foreach ($diff->getChangedColumns() as $column) {
            if ($column->getNewColumn()->getName() !== $column->getOldColumn()->getName()) {
                throw NotSupported::new('RENAME COLUMN');
            }
        }

        foreach ($diff->getAddedColumns() as $column) {
            $columnArray  = $column->toArray();
            $queryParts[] = 'ADD COLUMN ' . $this->getColumnDeclarationSQL($column->getQuotedName($this), $columnArray);
        }

        foreach ($diff->getDroppedColumns() as $column) {
            $queryParts[] = 'DROP COLUMN ' . $column->getQuotedName($this);
        }

        foreach ($diff->getChangedColumns() as $columnDiff) {
            $column      = $columnDiff->getNewColumn();
            $columnArray = $column->toArray();

            // Don't propagate default value changes for unsupported column types.
            if (($columnArray['type'] instanceof TextType || $columnArray['type'] instanceof BlobType)
                && $columnDiff->hasDefaultChanged()
                && $columnDiff->countChangedProperties() === 1
            ) {
                continue;
            }

            $queryParts[] = 'MODIFY COLUMN ' . $this->getColumnDeclarationSQL(
                    $column->getQuotedName($this),
                    $columnArray
                );
        }

        $sql      = [];
        $tableSql = [];

        if ((count($queryParts) > 0)) {
            $sql[] = 'ALTER TABLE ' . $diff->getOldTable()->getQuotedName($this) . ' ' . implode(', ', $queryParts);
        }

        return array_merge($sql, $tableSql, $columnSql);
    }

    /**
     * {@inheritDoc}
     */
    protected function getPreAlterTableIndexForeignKeySQL(TableDiff $diff): array
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    protected function getPostAlterTableIndexForeignKeySQL(TableDiff $diff): array
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    protected function getRenameIndexSQL(string $oldIndexName, Index $index, string $tableName): array
    {
        throw NotSupported::new(__METHOD__);
    }

    protected function prepareDeclarationSQL(string $declarationSQL, array $columnDef): string
    {
        if (array_key_exists('notnull', $columnDef) && $columnDef['notnull'] === false) {
            return 'Nullable(' . $declarationSQL . ')';
        }

        return $declarationSQL;
    }

    /**
     * {@inheritDoc}
     */
    public function getColumnDeclarationSQL(string $name, array $column): string
    {
        if (isset($column['columnDefinition'])) {
            $columnDef = $column['columnDefinition'];
        } else {
            $default = $this->getDefaultValueDeclarationSQL($column);

            $columnDef = $column['type']->getSqlDeclaration($column, $this) . $default;
        }

        return $name . ' ' . $columnDef;
    }

    /**
     * {@inheritDoc}
     */
    public function getDecimalTypeDeclarationSQL(array $column): string
    {
        return $this->prepareDeclarationSQL(StringableClickHouseType::TYPE_STRING, $column);
    }

    /**
     * {@inheritDoc}
     */
    public function getCheckDeclarationSQL(array $definition): string
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getUniqueConstraintDeclarationSQL(UniqueConstraint $constraint): string
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getIndexDeclarationSQL(Index $index): string
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getForeignKeyDeclarationSQL(ForeignKeyConstraint $foreignKey): string
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getAdvancedForeignKeyOptionsSQL(ForeignKeyConstraint $foreignKey): string
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getForeignKeyReferentialActionSQL(string $action): string
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getForeignKeyBaseDeclarationSQL(ForeignKeyConstraint $foreignKey): string
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getCurrentDateSQL(): string
    {
        return 'today()';
    }

    /**
     * {@inheritDoc}
     */
    public function getCurrentTimeSQL(): string
    {
        return 'now()';
    }

    /**
     * {@inheritDoc}
     */
    public function getCurrentTimestampSQL(): string
    {
        return 'toUnixTimestamp(now())';
    }

    /**
     * {@inheritDoc}
     */
    public function getListDatabasesSQL(): string
    {
        return 'SHOW DATABASES';
    }

    /**
     * {@inheritDoc}
     */
    public function getListViewsSQL(string $database): string
    {
        return "SELECT name FROM system.tables WHERE database != 'system' AND engine = 'View'";
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateViewSQL(string $name, string $sql): string
    {
        return 'CREATE VIEW ' . $this->quoteIdentifier($name) . ' AS ' . $sql;
    }

    /**
     * {@inheritDoc}
     */
    public function getDropViewSQL(string $name): string
    {
        return 'DROP TABLE ' . $this->quoteIdentifier($name);
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateDatabaseSQL(string $name): string
    {
        return 'CREATE DATABASE ' . $name;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTimeTypeDeclarationSQL(array $column): string
    {
        return $this->prepareDeclarationSQL(DatableClickHouseType::TYPE_DATE_TIME, $column);
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTimeTzTypeDeclarationSQL(array $column): string
    {
        return $this->prepareDeclarationSQL(DatableClickHouseType::TYPE_DATE_TIME, $column);
    }

    /**
     * {@inheritDoc}
     */
    public function getTimeTypeDeclarationSQL(array $column): string
    {
        return $this->prepareDeclarationSQL(StringableClickHouseType::TYPE_STRING, $column);
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTypeDeclarationSQL(array $column): string
    {
        return $this->prepareDeclarationSQL(DatableClickHouseType::TYPE_DATE, $column);
    }

    /**
     * {@inheritDoc}
     */
    public function getFloatDeclarationSQL(array $column): string
    {
        return $this->prepareDeclarationSQL(
            NumericalClickHouseType::TYPE_FLOAT . BitNumericalClickHouseType::SIXTY_FOUR_BIT,
            $column
        );
    }

    /**
     * {@inheritDoc}
     */
    public function getDefaultTransactionIsolationLevel(): TransactionIsolationLevel
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function supportsSavepoints(): bool
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    protected function doModifyLimitQuery(string $query, ?int $limit, int $offset): string
    {
        if ($limit === null) {
            return $query;
        }

        $query .= ' LIMIT ';

        if ($offset > 0) {
            $query .= $offset . ', ';
        }

        $query .= $limit;

        return $query;
    }

    /**
     * {@inheritDoc}
     */
    public function getEmptyIdentityInsertSQL(string $quotedTableName, string $quotedIdentifierColumnName): string
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getTruncateTableSQL(string $tableName, bool $cascade = false): string
    {
        /**
         * For MergeTree* engines may be done with next workaround:
         *
         * SELECT partition FROM system.parts WHERE table= '$tableName';
         * ALTER TABLE $tableName DROP PARTITION $partitionName
         */
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function createSavePoint(string $savepoint): string
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function releaseSavePoint(string $savepoint): string
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function rollbackSavePoint(string $savepoint): string
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getDefaultValueDeclarationSQL(array $column): string
    {
        if (!isset($column['default'])) {
            return '';
        }

        $defaultValue = $this->quoteStringLiteral($column['default']);
        $fieldType    = $column['type'] ?: null;

        if ($fieldType !== null) {
            if ($fieldType === DatableClickHouseType::TYPE_DATE ||
                $fieldType instanceof DateType ||
                in_array($fieldType, [
                    'Integer',
                    'SmallInt',
                    'Float',
                ]) ||
                (
                    $fieldType === 'BigInt'
                    && Type::getType('BigInt')->getBindingType() === ParameterType::INTEGER
                )
            ) {
                $defaultValue = $column['default'];
            } elseif ($fieldType === DatableClickHouseType::TYPE_DATE_TIME &&
                $column['default'] === $this->getCurrentTimestampSQL()
            ) {
                $defaultValue = $this->getCurrentTimestampSQL();
            }
        }

        return sprintf(' DEFAULT %s', $defaultValue);
    }

    /**
     * {@inheritDoc}
     */
    public function getDoctrineTypeMapping(string $dbType): string
    {
        if (mb_stripos($dbType, 'fixedstring') === 0) {
            $dbType = 'fixedstring';
        }

        if (mb_stripos($dbType, 'enum8') === 0) {
            $dbType = 'enum8';
        }

        if (mb_stripos($dbType, 'enum16') === 0) {
            $dbType = 'enum16';
        }

        return parent::getDoctrineTypeMapping($dbType);
    }

    /**
     * {@inheritDoc}
     */
    public function quoteStringLiteral(string $str): string
    {
        return parent::quoteStringLiteral(addslashes($str));
    }

    /**
     * {@inheritDoc}
     */
    public function quoteSingleIdentifier(string $str): string
    {
        return parent::quoteSingleIdentifier(addslashes($str));
    }

    /**
     * {@inheritDoc}
     */
    public function getCurrentDatabaseExpression(): string
    {
        return 'DATABASE()';
    }

    protected function getDateArithmeticIntervalExpression(string $date, string $operator, string $interval, DateIntervalUnit $unit): string
    {
        $operation = '+' === $operator ? 'date_add' : 'date_sub';
        $toDateFunction = strlen($date) > 10 ? 'toDateTime' : 'toDate';

        return \sprintf('%s(%s, %d, %s(\'%s\'))', $operation, $unit->value, (int) $interval, $toDateFunction, $date);
    }

    public function getSetTransactionIsolationSQL(TransactionIsolationLevel $level): string
    {
        throw NotSupported::new(__METHOD__);
    }

    protected function createReservedKeywordsList(): KeywordList
    {
        return new ClickHouseKeywords();
    }

    public function createSchemaManager(Connection $connection): AbstractSchemaManager
    {
        return new ClickHouseSchemaManager($connection, $this);
    }
}
