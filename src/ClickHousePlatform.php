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

use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\TrimMode;
use Doctrine\DBAL\Schema\ForeignKeyConstraint;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\TableDiff;
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
use FOD\DBALClickHouse\Types\StringClickHouseType;
use FOD\DBALClickHouse\Types\UnsignedNumericalClickHouseType;
use function addslashes;
use function array_filter;
use function array_key_exists;
use function array_keys;
use function array_merge;
use function array_unique;
use function array_values;
use function count;
use function func_get_args;
use function get_class;
use function implode;
use function in_array;
use function sprintf;
use function stripos;
use function trim;

/**
 * Provides the behavior, features and SQL dialect of the ClickHouse database platform.
 */
class ClickHousePlatform extends AbstractPlatform
{
    protected const TIME_MINUTE = 60;
    protected const TIME_HOUR   = self::TIME_MINUTE * 60;
    protected const TIME_DAY    = self::TIME_HOUR * 24;
    protected const TIME_WEEK   = self::TIME_DAY * 7;

    /**
     * {@inheritDoc}
     */
    public function getBooleanTypeDeclarationSQL(array $columnDef) : string
    {
        return $this->prepareDeclarationSQL(
            UnsignedNumericalClickHouseType::UNSIGNED_CHAR .
            NumericalClickHouseType::TYPE_INT . BitNumericalClickHouseType::EIGHT_BIT,
            $columnDef
        );
    }

    /**
     * {@inheritDoc}
     */
    public function getIntegerTypeDeclarationSQL(array $columnDef) : string
    {
        return $this->prepareDeclarationSQL(
            $this->_getCommonIntegerTypeDeclarationSQL($columnDef) .
            NumericalClickHouseType::TYPE_INT . BitNumericalClickHouseType::THIRTY_TWO_BIT,
            $columnDef
        );
    }

    /**
     * {@inheritDoc}
     */
    public function getBigIntTypeDeclarationSQL(array $columnDef) : string
    {
        return $this->prepareDeclarationSQL(StringClickHouseType::TYPE_STRING, $columnDef);
    }

    /**
     * {@inheritDoc}
     */
    public function getSmallIntTypeDeclarationSQL(array $columnDef) : string
    {
        return $this->prepareDeclarationSQL(
            $this->_getCommonIntegerTypeDeclarationSQL($columnDef) .
            NumericalClickHouseType::TYPE_INT . BitNumericalClickHouseType::SIXTEEN_BIT,
            $columnDef
        );
    }

    /**
     * {@inheritDoc}
     */
    protected function _getCommonIntegerTypeDeclarationSQL(array $columnDef) : string
    {
        if (! empty($columnDef['autoincrement'])) {
            throw ClickHouseException::notSupported('Clickhouse do not support AUTO_INCREMENT fields');
        }

        return empty($columnDef['unsigned']) ? '' : UnsignedNumericalClickHouseType::UNSIGNED_CHAR;
    }

    /**
     * {@inheritDoc}
     */
    protected function initializeDoctrineTypeMappings() : void
    {
        $this->doctrineTypeMapping = [
            'int8' => 'smallint',
            'int16' => 'integer',
            'int32' => 'integer',
            'int64' => 'bigint',
            'uint8' => 'smallint',
            'uint16' => 'integer',
            'uint32' => 'integer',
            'uint64' => 'bigint',
            'float32' => 'decimal',
            'float64' => 'float',

            'string' => 'string',
            'fixedstring' => 'string',
            'date' => 'date',
            'datetime' => 'datetime',

            'array(int8)' => 'array',
            'array(int16)' => 'array',
            'array(int32)' => 'array',
            'array(int64)' => 'array',
            'array(uint8)' => 'array',
            'array(uint16)' => 'array',
            'array(uint32)' => 'array',
            'array(uint64)' => 'array',
            'array(float32)' => 'array',
            'array(float64)' => 'array',

            'array(string)' => 'array',
            'array(date)' => 'array',
            'array(datetime)' => 'array',

            'enum8' => 'string',
            'enum16' => 'string',

            'nullable(int8)' => 'smallint',
            'nullable(int16)' => 'integer',
            'nullable(int32)' => 'integer',
            'nullable(int64)' => 'bigint',
            'nullable(uint8)' => 'smallint',
            'nullable(uint16)' => 'integer',
            'nullable(uint32)' => 'integer',
            'nullable(uint64)' => 'bigint',
            'nullable(float32)' => 'decimal',
            'nullable(float64)' => 'float',

            'nullable(string)' => 'string',
            'nullable(fixedstring)' => 'string',
            'nullable(date)' => 'date',
            'nullable(datetime)' => 'datetime',

            'array(nullable(int8))' => 'array',
            'array(nullable(int16))' => 'array',
            'array(nullable(int32))' => 'array',
            'array(nullable(int64))' => 'array',
            'array(nullable(uint8))' => 'array',
            'array(nullable(uint16))' => 'array',
            'array(nullable(uint32))' => 'array',
            'array(nullable(uint64))' => 'array',
            'array(nullable(float32))' => 'array',
            'array(nullable(float64))' => 'array',

            'array(nullable(string))' => 'array',
            'array(nullable(date))' => 'array',
            'array(nullable(datetime))' => 'array',
        ];
    }

    /**
     * {@inheritDoc}
     */
    protected function getVarcharTypeDeclarationSQLSnippet($length, $fixed) : string
    {
        return $fixed
            ? (StringClickHouseType::TYPE_FIXED_STRING . '(' . $length . ')')
            : StringClickHouseType::TYPE_STRING;
    }

    /**
     * {@inheritDoc}
     */
    public function getVarcharTypeDeclarationSQL(array $field) : string
    {
        if (! isset($field['length'])) {
            $field['length'] = $this->getVarcharDefaultLength();
        }

        $fixed = $field['fixed'] ?? false;

        $maxLength = $fixed
            ? $this->getCharMaxLength()
            : $this->getVarcharMaxLength();

        if ($field['length'] > $maxLength) {
            return $this->getClobTypeDeclarationSQL($field);
        }

        return $this->prepareDeclarationSQL(
            $this->getVarcharTypeDeclarationSQLSnippet($field['length'], $fixed),
            $field
        );
    }

    /**
     * {@inheritDoc}
     */
    protected function getBinaryTypeDeclarationSQLSnippet($length, $fixed) : string
    {
        return StringClickHouseType::TYPE_STRING;
    }

    /**
     * {@inheritDoc}
     */
    public function getClobTypeDeclarationSQL(array $field) : string
    {
        return $this->prepareDeclarationSQL(StringClickHouseType::TYPE_STRING, $field);
    }

    /**
     * {@inheritDoc}
     */
    public function getBlobTypeDeclarationSQL(array $field) : string
    {
        return $this->prepareDeclarationSQL(StringClickHouseType::TYPE_STRING, $field);
    }

    /**
     * {@inheritDoc}
     */
    public function getName() : string
    {
        return 'clickhouse';
    }

    /**
     * {@inheritDoc}
     */
    public function getIdentifierQuoteCharacter() : string
    {
        return '`';
    }

    /**
     * {@inheritDoc}
     */
    public function getVarcharDefaultLength() : int
    {
        return 512;
    }

    /**
     * {@inheritDoc}
     */
    public function getCountExpression($column) : string
    {
        return 'COUNT()';
    }

    // scalar functions

    /**
     * {@inheritDoc}
     */
    public function getMd5Expression($column) : string
    {
        return 'MD5(CAST(' . $column . ' AS String))';
    }

    /**
     * {@inheritDoc}
     */
    public function getLengthExpression($column) : string
    {
        return 'lengthUTF8(CAST(' . $column . ' AS String))';
    }

    /**
     * {@inheritDoc}
     */
    public function getSqrtExpression($column) : string
    {
        return 'sqrt(' . $column . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getRoundExpression($column, $decimals = 0) : string
    {
        return 'round(' . $column . ', ' . $decimals . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getModExpression($expression1, $expression2) : string
    {
        return 'modulo(' . $expression1 . ', ' . $expression2 . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getTrimExpression($str, $pos = TrimMode::UNSPECIFIED, $char = false) : string
    {
        if (! $char) {
            switch ($pos) {
                case TrimMode::LEADING:
                    return $this->getLtrimExpression($str);
                case TrimMode::TRAILING:
                    return $this->getRtrimExpression($str);
                default:
                    return sprintf("replaceRegexpAll(%s, '(^\\\s+|\\\s+$)', '')", $str);
            }
        }

        return sprintf("replaceRegexpAll(%s, '(^%s+|%s+$)', '')", $str, $char, $char);
    }

    /**
     * {@inheritDoc}
     */
    public function getRtrimExpression($str) : string
    {
        return sprintf("replaceRegexpAll(%s, '(\\\s+$)', '')", $str);
    }

    /**
     * {@inheritDoc}
     */
    public function getLtrimExpression($str) : string
    {
        return sprintf("replaceRegexpAll(%s, '(^\\\s+)', '')", $str);
    }

    /**
     * {@inheritDoc}
     */
    public function getUpperExpression($str) : string
    {
        return 'upperUTF8(' . $str . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getLowerExpression($str) : string
    {
        return 'lowerUTF8(' . $str . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getLocateExpression($str, $substr, $startPos = false) : string
    {
        return 'positionUTF8(' . $str . ', ' . $substr . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getNowExpression() : string
    {
        return 'now()';
    }

    /**
     * {@inheritDoc}
     */
    public function getSubstringExpression($value, $from, $length = null) : string
    {
        if ($length === null) {
            throw new \InvalidArgumentException("'length' argument must be a constant");
        }

        return 'substringUTF8(' . $value . ', ' . $from . ', ' . $length . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getConcatExpression() : string
    {
        return 'concat(' . implode(', ', func_get_args()) . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getIsNullExpression($expression) : string
    {
        return 'isNull(' . $expression . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getIsNotNullExpression($expression) : string
    {
        return 'isNotNull(' . $expression . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getAcosExpression($value) : string
    {
        return 'acos(' . $value . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getSinExpression($value) : string
    {
        return 'sin(' . $value . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getPiExpression() : string
    {
        return 'pi()';
    }

    /**
     * {@inheritDoc}
     */
    public function getCosExpression($value) : string
    {
        return 'cos(' . $value . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getDateDiffExpression($date1, $date2) : string
    {
        return 'CAST(' . $date1 . ' AS Date) - CAST(' . $date2 . ' AS Date)';
    }

    /**
     * {@inheritDoc}
     */
    public function getDateAddSecondsExpression($date, $seconds) : string
    {
        return $date . ' + ' . $seconds;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateSubSecondsExpression($date, $seconds) : string
    {
        return $date . ' - ' . $seconds;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateAddMinutesExpression($date, $minutes) : string
    {
        return $date . ' + ' . $minutes * self::TIME_MINUTE;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateSubMinutesExpression($date, $minutes) : string
    {
        return $date . ' - ' . $minutes * self::TIME_MINUTE;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateAddHourExpression($date, $hours) : string
    {
        return $date . ' + ' . $hours * self::TIME_HOUR;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateSubHourExpression($date, $hours) : string
    {
        return $date . ' - ' . $hours * self::TIME_HOUR;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateAddDaysExpression($date, $days) : string
    {
        return $date . ' + ' . $days * self::TIME_DAY;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateSubDaysExpression($date, $days) : string
    {
        return $date . ' - ' . $days * self::TIME_DAY;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateAddWeeksExpression($date, $weeks) : string
    {
        return $date . ' + ' . $weeks * self::TIME_WEEK;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateSubWeeksExpression($date, $weeks) : string
    {
        return $date . ' - ' . $weeks * self::TIME_WEEK;
    }

    /**
     * {@inheritDoc}
     */
    public function getBitAndComparisonExpression($value1, $value2) : string
    {
        return 'bitAnd(' . $value1 . ', ' . $value2 . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getBitOrComparisonExpression($value1, $value2) : string
    {
        return 'bitOr(' . $value1 . ', ' . $value2 . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getForUpdateSQL() : string
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function appendLockHint($fromClause, $lockMode) : string
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getReadLockSQL() : string
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getWriteLockSQL() : string
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getDropIndexSQL($index, $table = null) : string
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getDropConstraintSQL($constraint, $table) : string
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getDropForeignKeySQL($foreignKey, $table) : string
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getCommentOnColumnSQL($tableName, $columnName, $comment) : string
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    protected function _getCreateTableSQL($tableName, array $columns, array $options = []) : array
    {
        $engine        = ! empty($options['engine']) ? $options['engine'] : 'ReplacingMergeTree';
        $engineOptions = '';

        if (isset($options['uniqueConstraints']) && ! empty($options['uniqueConstraints'])) {
            throw ClickHouseException::notSupported('uniqueConstraints');
        }

        if (isset($options['indexes']) && ! empty($options['indexes'])) {
            throw ClickHouseException::notSupported('uniqueConstraints');
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
            $indexGranularity   = ! empty($options['indexGranularity']) ? $options['indexGranularity'] : 8192;
            $samplingExpression = '';

            /**
             * eventDateColumn section
             */
            $dateColumnParams = [
                'type' => Type::getType('date'),
                'default' => 'today()',
            ];
            if (! empty($options['eventDateProviderColumn'])) {
                $options['eventDateProviderColumn'] = trim($options['eventDateProviderColumn']);
                if (! isset($columns[$options['eventDateProviderColumn']])) {
                    throw new \Exception(
                        'Table `' . $tableName . '` has not column with name: `' . $options['eventDateProviderColumn']
                    );
                }

                if (! ($columns[$options['eventDateProviderColumn']]['type'] instanceof DateType) &&
                    ! ($columns[$options['eventDateProviderColumn']]['type'] instanceof DateTimeType) &&
                    ! ($columns[$options['eventDateProviderColumn']]['type'] instanceof TextType) &&
                    ! ($columns[$options['eventDateProviderColumn']]['type'] instanceof IntegerType) &&
                    ! ($columns[$options['eventDateProviderColumn']]['type'] instanceof SmallIntType) &&
                    ! ($columns[$options['eventDateProviderColumn']]['type'] instanceof BigIntType) &&
                    ! ($columns[$options['eventDateProviderColumn']]['type'] instanceof FloatType) &&
                    ! ($columns[$options['eventDateProviderColumn']]['type'] instanceof DecimalType) &&
                    (
                        ! ($columns[$options['eventDateProviderColumn']]['type'] instanceof StringType) ||
                        $columns[$options['eventDateProviderColumn']]['fixed']
                    )
                ) {
                    throw new \Exception(
                        'Column `' . $options['eventDateProviderColumn'] . '` with type `' .
                        $columns[$options['eventDateProviderColumn']]['type']->getName() .
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
                $dateColumns = array_filter($columns, function ($column) {
                    return $column['type'] instanceof DateType;
                });

                if ($dateColumns) {
                    throw new \Exception(
                        'Table `' . $tableName . '` has DateType columns: `' . implode(
                            '`, `',
                            array_keys($dateColumns)
                        ) .
                        '`, but no one of them is setted as `eventDateColumn` with 
                        $table->addOption("eventDateColumn", "%eventDateColumnName%")'
                    );
                }

                $eventDateColumnName = 'EventDate';
            } elseif (isset($columns[$options['eventDateColumn']])) {
                if (! ($columns[$options['eventDateColumn']]['type'] instanceof DateType)) {
                    throw new \Exception(
                        'In table `' . $tableName . '` you have set field `' .
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
            if (! empty($options['samplingExpression'])) {
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
            if ($engine === 'ReplacingMergeTree' && ! empty($options['versionColumn'])) {
                if (! isset($columns[$options['versionColumn']])) {
                    throw new \Exception(
                        'If you specify `versionColumn` for ReplacingMergeTree table -- 
                        you must add this column manually (any of UInt*, Date or DateTime types)'
                    );
                }

                if (! $columns[$options['versionColumn']]['type'] instanceof IntegerType &&
                    ! $columns[$options['versionColumn']]['type'] instanceof BigIntType &&
                    ! $columns[$options['versionColumn']]['type'] instanceof SmallIntType &&
                    ! $columns[$options['versionColumn']]['type'] instanceof DateType &&
                    ! $columns[$options['versionColumn']]['type'] instanceof DateTimeType
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
            $tableName,
            $this->getColumnDeclarationListSQL($columns),
            $engine,
            $engineOptions
        );

        return $sql;
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateForeignKeySQL(ForeignKeyConstraint $foreignKey, $table) : string
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getAlterTableSQL(TableDiff $diff) : array
    {
        $columnSql  = [];
        $queryParts = [];
        if ($diff->newName !== false || ! empty($diff->renamedColumns)) {
            throw ClickHouseException::notSupported('RENAME COLUMN');
        }

        foreach ($diff->addedColumns as $column) {
            if ($this->onSchemaAlterTableAddColumn($column, $diff, $columnSql)) {
                continue;
            }

            $columnArray  = $column->toArray();
            $queryParts[] = 'ADD COLUMN ' . $this->getColumnDeclarationSQL($column->getQuotedName($this), $columnArray);
        }

        foreach ($diff->removedColumns as $column) {
            if ($this->onSchemaAlterTableRemoveColumn($column, $diff, $columnSql)) {
                continue;
            }

            $queryParts[] = 'DROP COLUMN ' . $column->getQuotedName($this);
        }

        foreach ($diff->changedColumns as $columnDiff) {
            if ($this->onSchemaAlterTableChangeColumn($columnDiff, $diff, $columnSql)) {
                continue;
            }

            $column      = $columnDiff->column;
            $columnArray = $column->toArray();

            // Don't propagate default value changes for unsupported column types.
            if (($columnArray['type'] instanceof TextType || $columnArray['type'] instanceof BlobType) &&
                $columnDiff->hasChanged('default') &&
                count($columnDiff->changedProperties) === 1
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

        if (! $this->onSchemaAlterTable($diff, $tableSql) && (count($queryParts) > 0)) {
            $sql[] = 'ALTER TABLE ' . $diff->getName($this)->getQuotedName($this) . ' ' . implode(
                ', ',
                $queryParts
            );
        }

        return array_merge($sql, $tableSql, $columnSql);
    }

    /**
     * {@inheritDoc}
     */
    protected function getPreAlterTableIndexForeignKeySQL(TableDiff $diff) : array
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    protected function getPostAlterTableIndexForeignKeySQL(TableDiff $diff) : array
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    protected function getRenameIndexSQL($oldIndexName, Index $index, $tableName) : array
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    protected function _getAlterTableIndexForeignKeySQL(TableDiff $diff) : array
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    protected function prepareDeclarationSQL(string $declarationSQL, array $columnDef) : string
    {
        if (array_key_exists('notnull', $columnDef) && $columnDef['notnull'] === false) {
            return 'Nullable(' . $declarationSQL . ')';
        }

        return $declarationSQL;
    }

    /**
     * {@inheritDoc}
     */
    public function getColumnDeclarationSQL($name, array $field) : string
    {
        if (isset($field['columnDefinition'])) {
            $columnDef = $this->getCustomTypeDeclarationSQL($field);
        } else {
            $default = $this->getDefaultValueDeclarationSQL($field);

            $columnDef = $field['type']->getSqlDeclaration($field, $this) . $default;
        }

        return $name . ' ' . $columnDef;
    }

    /**
     * {@inheritDoc}
     */
    public function getDecimalTypeDeclarationSQL(array $columnDef) : string
    {
        return $this->prepareDeclarationSQL(StringClickHouseType::TYPE_STRING, $columnDef);
    }

    /**
     * {@inheritDoc}
     */
    public function getCheckDeclarationSQL(array $definition) : string
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getUniqueConstraintDeclarationSQL($name, Index $index) : string
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getIndexDeclarationSQL($name, Index $index) : string
    {
        // Index declaration in statements like CREATE TABLE is not supported.
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getForeignKeyDeclarationSQL(ForeignKeyConstraint $foreignKey) : string
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getAdvancedForeignKeyOptionsSQL(ForeignKeyConstraint $foreignKey) : string
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getForeignKeyReferentialActionSQL($action) : string
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getForeignKeyBaseDeclarationSQL(ForeignKeyConstraint $foreignKey) : string
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getUniqueFieldDeclarationSQL() : string
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getCurrentDateSQL() : string
    {
        return 'today()';
    }

    /**
     * {@inheritDoc}
     */
    public function getCurrentTimeSQL() : string
    {
        return 'now()';
    }

    /**
     * {@inheritDoc}
     */
    public function getCurrentTimestampSQL() : string
    {
        return 'toUnixTimestamp(now())';
    }

    /**
     * {@inheritDoc}
     */
    public function getListDatabasesSQL() : string
    {
        return 'SHOW DATABASES';
    }

    /**
     * {@inheritDoc}
     */
    public function getListTableColumnsSQL($table, $database = null) : string
    {
        return sprintf(
            'DESCRIBE TABLE %s',
            ($database ? $this->quoteSingleIdentifier($database) . '.' : '') . $this->quoteSingleIdentifier($table)
        );
    }

    /**
     * {@inheritDoc}
     */
    public function getListTablesSQL() : string
    {
        return "SELECT database, name FROM system.tables WHERE database != 'system' AND engine != 'View'";
    }

    /**
     * {@inheritDoc}
     */
    public function getListViewsSQL($database) : string
    {
        return "SELECT name FROM system.tables WHERE database != 'system' AND engine = 'View'";
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateViewSQL($name, $sql) : string
    {
        return 'CREATE VIEW ' . $this->quoteIdentifier($name) . ' AS ' . $sql;
    }

    /**
     * {@inheritDoc}
     */
    public function getDropViewSQL($name) : string
    {
        return 'DROP TABLE ' . $this->quoteIdentifier($name);
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateDatabaseSQL($database) : string
    {
        return 'CREATE DATABASE ' . $database;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTimeTypeDeclarationSQL(array $fieldDeclaration) : string
    {
        return $this->prepareDeclarationSQL(DatableClickHouseType::TYPE_DATE_TIME, $fieldDeclaration);
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTimeTzTypeDeclarationSQL(array $fieldDeclaration) : string
    {
        return $this->prepareDeclarationSQL(DatableClickHouseType::TYPE_DATE_TIME, $fieldDeclaration);
    }

    /**
     * {@inheritDoc}
     */
    public function getTimeTypeDeclarationSQL(array $fieldDeclaration) : string
    {
        return $this->prepareDeclarationSQL(StringClickHouseType::TYPE_STRING, $fieldDeclaration);
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTypeDeclarationSQL(array $fieldDeclaration) : string
    {
        return $this->prepareDeclarationSQL(DatableClickHouseType::TYPE_DATE, $fieldDeclaration);
    }

    /**
     * {@inheritDoc}
     */
    public function getFloatDeclarationSQL(array $fieldDeclaration) : string
    {
        return $this->prepareDeclarationSQL(
            NumericalClickHouseType::TYPE_FLOAT . BitNumericalClickHouseType::SIXTY_FOUR_BIT,
            $fieldDeclaration
        );
    }

    /**
     * {@inheritDoc}
     */
    public function getDefaultTransactionIsolationLevel() : int
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /* supports*() methods */

    /**
     * {@inheritDoc}
     */
    public function supportsTransactions() : bool
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public function supportsSavepoints() : bool
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public function supportsPrimaryConstraints() : bool
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public function supportsForeignKeyConstraints() : bool
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public function supportsGettingAffectedRows() : bool
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    protected function doModifyLimitQuery($query, $limit, $offset) : string
    {
        if ($limit === null) {
            return $query;
        }

        $query .= ' LIMIT ';
        if ($offset !== null) {
            $query .= $offset . ', ';
        }

        $query .= $limit;

        return $query;
    }

    /**
     * {@inheritDoc}
     */
    public function getEmptyIdentityInsertSQL($tableName, $identifierColumnName) : string
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getTruncateTableSQL($tableName, $cascade = false) : string
    {
        /**
         * For MergeTree* engines may be done with next workaround:
         *
         * SELECT partition FROM system.parts WHERE table= '$tableName';
         * ALTER TABLE $tableName DROP PARTITION $partitionName
         */
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function createSavePoint($savepoint) : string
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function releaseSavePoint($savepoint) : string
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function rollbackSavePoint($savepoint) : string
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    protected function getReservedKeywordsClass() : string
    {
        return ClickHouseKeywords::class;
    }

    /**
     * {@inheritDoc}
     */
    public function getDefaultValueDeclarationSQL($field) : string
    {
        if (! isset($field['default'])) {
            return '';
        }
        $defaultValue = $this->quoteStringLiteral($field['default']);
        $fieldType    = $field['type'] ?: null;
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
                $defaultValue = $field['default'];
            } elseif ($fieldType === DatableClickHouseType::TYPE_DATE_TIME &&
                $field['default'] === $this->getCurrentTimestampSQL()
            ) {
                $defaultValue = $this->getCurrentTimestampSQL();
            }
        }

        return sprintf(' DEFAULT %s', $defaultValue);
    }

    /**
     * {@inheritDoc}
     */
    public function getDoctrineTypeMapping($dbType) : string
    {
        // FixedString
        if (stripos($dbType, 'fixedstring') === 0) {
            $dbType = 'fixedstring';
        }

        //Enum8
        if (stripos($dbType, 'enum8') === 0) {
            $dbType = 'enum8';
        }

        //Enum16
        if (stripos($dbType, 'enum16') === 0) {
            $dbType = 'enum16';
        }
        return parent::getDoctrineTypeMapping($dbType);
    }

    /**
     * {@inheritDoc}
     */
    public function quoteStringLiteral($str) : string
    {
        $c = $this->getStringLiteralQuoteCharacter();

        return $c . addslashes($str) . $c;
    }

    /**
     * {@inheritDoc}
     */
    public function quoteSingleIdentifier($str) : string
    {
        $c = $this->getIdentifierQuoteCharacter();

        return $c . addslashes($str) . $c;
    }
}
