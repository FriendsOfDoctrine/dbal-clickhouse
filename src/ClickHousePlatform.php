<?php

/**
 * Doctrine DBAL library for ClickHouse -- an open-source column-oriented DBMS for OLAP (https://clickhouse.yandex)
 */

namespace Mochalygin\DoctrineDBALClickHouse;

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Identifier;
use Doctrine\DBAL\Types;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\DateType;
use Doctrine\DBAL\Schema\Constraint;
use Doctrine\DBAL\Schema\Sequence;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\ForeignKeyConstraint;
use Doctrine\DBAL\Schema\TableDiff;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\ColumnDiff;
use Doctrine\DBAL\Events;
use Doctrine\Common\EventManager;
use Doctrine\DBAL\Event\SchemaCreateTableEventArgs;
use Doctrine\DBAL\Event\SchemaCreateTableColumnEventArgs;
use Doctrine\DBAL\Event\SchemaDropTableEventArgs;
use Doctrine\DBAL\Event\SchemaAlterTableEventArgs;
use Doctrine\DBAL\Event\SchemaAlterTableAddColumnEventArgs;
use Doctrine\DBAL\Event\SchemaAlterTableRemoveColumnEventArgs;
use Doctrine\DBAL\Event\SchemaAlterTableChangeColumnEventArgs;
use Doctrine\DBAL\Event\SchemaAlterTableRenameColumnEventArgs;

/**
 * Platform for ClickHouse database {@link https://clickhouse.yandex/}
 *
 * @author mochalygin <a@mochalygin.ru>
 */
class ClickHousePlatform extends \Doctrine\DBAL\Platforms\AbstractPlatform
{
    
    /**
     * {@inheritDoc}
     */
    public function getBooleanTypeDeclarationSQL(array $columnDef)
    {
        return 'UInt8';
    }

    /**
     * {@inheritDoc}
     */
    public function getIntegerTypeDeclarationSQL(array $columnDef)
    {
        return $this->_getCommonIntegerTypeDeclarationSQL($columnDef) . 'Int32';
    }

    /**
     * {@inheritDoc}
     */
    public function getBigIntTypeDeclarationSQL(array $columnDef)
    {
        return $this->_getCommonIntegerTypeDeclarationSQL($columnDef) . 'Int64';
    }

    /**
     * {@inheritDoc}
     */
    public function getSmallIntTypeDeclarationSQL(array $columnDef)
    {
        return $this->_getCommonIntegerTypeDeclarationSQL($columnDef) . 'Int16';
    }

    /**
     * {@inheritDoc}
     */
    protected function _getCommonIntegerTypeDeclarationSQL(array $columnDef)
    {
        if (! empty($columnDef['autoincrement']))
            throw new \Exception('Clickhouse do not support AUTO_INCREMENT fields');

        return empty($columnDef['unsigned']) ? '' : 'U'; 
    }

    /**
     * {@inheritDoc}
     */    
    protected function initializeDoctrineTypeMappings()
    {
        $this->doctrineTypeMapping = [
            'uint8' => 'smallint',
            'uint16' => 'integer',
            'uint32' => 'integer',
            'uint64' => 'bigint',
            'int8' => 'smallint',
            'int16' => 'integer',
            'int32' => 'integer',
            'int64' => 'bigint',
            'float32' => 'decimal',
            'float64' => 'float',
            'string' => 'string',//'text',
            'fixedstring' => 'string',
            'date' => 'date',
            'datetime' => 'datetime',
//            'enum' => 'simple_array', //TODO ???
//            'array' => 'array', //TODO ???
//            'tuple' => 'json_array' //TODO ???
        ];
    }

    /**
     * {@inheritDoc}
     */   
    protected function getVarcharTypeDeclarationSQLSnippet($length, $fixed)
    {
        return $fixed 
                ? 'FixedString(' . $length . ')'
                : 'String';
    }

    /**
     * {@inheritDoc}
     */   
    protected function getBinaryTypeDeclarationSQLSnippet($length, $fixed)
    {
        return 'String';
    }

    /**
     * {@inheritDoc}
     */   
    public function getClobTypeDeclarationSQL(array $field)
    {
        return 'String';
    }

    /**
     * {@inheritDoc}
     */
    public function getBlobTypeDeclarationSQL(array $field)
    {
        return 'String';
    }

    /**
     * {@inheritDoc}
     */
    public function getName()
    {
        return 'clickhouse';
    }

    /**
     * {@inheritDoc}
     */
    public function getIdentifierQuoteCharacter()
    {
        return '`';
    }

    /**
     * {@inheritDoc}
     */
    public function getVarcharDefaultLength()
    {
        return 512;
    }

    /**
     * {@inheritDoc}
     */
    public function getCountExpression($column)
    {
        return 'COUNT()';
    }

    // scalar functions

    /**
     * {@inheritDoc}
     */
    public function getMd5Expression($column)
    {
        return 'MD5(CAST(' . $column . ' AS String))';
    }

    /**
     * {@inheritDoc}
     */
    public function getLengthExpression($column)
    {
        return 'lengthUTF8(CAST(' . $column . ' AS String))';
    }

    /**
     * {@inheritDoc}
     */
    public function getSqrtExpression($column)
    {
        return 'sqrt(' . $column . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getRoundExpression($column, $decimals = 0)
    {
        return 'round(' . $column . ', ' . $decimals . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getModExpression($expression1, $expression2)
    {
        return 'modulo(' . $expression1 . ', ' . $expression2 . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getTrimExpression($str, $pos = self::TRIM_UNSPECIFIED, $char = false)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getRtrimExpression($str)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getLtrimExpression($str)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getUpperExpression($str)
    {
        return 'upperUTF8(' . $str . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getLowerExpression($str)
    {
        return 'lowerUTF8(' . $str . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getLocateExpression($str, $substr, $startPos = false)
    {
        return 'positionUTF8(' . $str . ', ' . $substr . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getNowExpression()
    {
        return 'now()';
    }

    /**
     * {@inheritDoc}
     */
    public function getSubstringExpression($value, $from, $length = null)
    {
        if ( isnull($length) ) {
            throw new \InvalidArgumentException("'length' argument must be a constant");
        }

        return 'substringUTF8(' . $value . ', ' . $from . ', ' . $length . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getConcatExpression()
    {
        return 'concat(' . implode(', ', func_get_args()) . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getIsNullExpression($expression)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getIsNotNullExpression($expression)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getAcosExpression($value)
    {
        return 'acos(' . $value . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getSinExpression($value)
    {
        return 'sin(' . $value . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getPiExpression()
    {
        return 'pi()';
    }

    /**
     * {@inheritDoc}
     */
    public function getCosExpression($value)
    {
        return 'cos(' . $value . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getDateDiffExpression($date1, $date2)
    {
        return 'CAST(' . $date1 . ' AS Date) - CAST(' . $date2 . ' AS Date)';
    }
    
    
    
    
    

    /**
     * Returns the SQL to add the number of given seconds to a date.
     *
     * @param string  $date
     * @param integer $seconds
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getDateAddSecondsExpression($date, $seconds)
    {
        return $this->getDateArithmeticIntervalExpression($date, '+', $seconds, self::DATE_INTERVAL_UNIT_SECOND);
    }

    /**
     * Returns the SQL to subtract the number of given seconds from a date.
     *
     * @param string  $date
     * @param integer $seconds
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getDateSubSecondsExpression($date, $seconds)
    {
        return $this->getDateArithmeticIntervalExpression($date, '-', $seconds, self::DATE_INTERVAL_UNIT_SECOND);
    }

    /**
     * Returns the SQL to add the number of given minutes to a date.
     *
     * @param string  $date
     * @param integer $minutes
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getDateAddMinutesExpression($date, $minutes)
    {
        return $this->getDateArithmeticIntervalExpression($date, '+', $minutes, self::DATE_INTERVAL_UNIT_MINUTE);
    }

    /**
     * Returns the SQL to subtract the number of given minutes from a date.
     *
     * @param string  $date
     * @param integer $minutes
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getDateSubMinutesExpression($date, $minutes)
    {
        return $this->getDateArithmeticIntervalExpression($date, '-', $minutes, self::DATE_INTERVAL_UNIT_MINUTE);
    }

    /**
     * Returns the SQL to add the number of given hours to a date.
     *
     * @param string  $date
     * @param integer $hours
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getDateAddHourExpression($date, $hours)
    {
        return $this->getDateArithmeticIntervalExpression($date, '+', $hours, self::DATE_INTERVAL_UNIT_HOUR);
    }

    /**
     * Returns the SQL to subtract the number of given hours to a date.
     *
     * @param string  $date
     * @param integer $hours
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getDateSubHourExpression($date, $hours)
    {
        return $this->getDateArithmeticIntervalExpression($date, '-', $hours, self::DATE_INTERVAL_UNIT_HOUR);
    }

    /**
     * Returns the SQL to add the number of given days to a date.
     *
     * @param string  $date
     * @param integer $days
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getDateAddDaysExpression($date, $days)
    {
        return $this->getDateArithmeticIntervalExpression($date, '+', $days, self::DATE_INTERVAL_UNIT_DAY);
    }

    /**
     * Returns the SQL to subtract the number of given days to a date.
     *
     * @param string  $date
     * @param integer $days
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getDateSubDaysExpression($date, $days)
    {
        return $this->getDateArithmeticIntervalExpression($date, '-', $days, self::DATE_INTERVAL_UNIT_DAY);
    }

    /**
     * Returns the SQL to add the number of given weeks to a date.
     *
     * @param string  $date
     * @param integer $weeks
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getDateAddWeeksExpression($date, $weeks)
    {
        return $this->getDateArithmeticIntervalExpression($date, '+', $weeks, self::DATE_INTERVAL_UNIT_WEEK);
    }

    /**
     * Returns the SQL to subtract the number of given weeks from a date.
     *
     * @param string  $date
     * @param integer $weeks
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getDateSubWeeksExpression($date, $weeks)
    {
        return $this->getDateArithmeticIntervalExpression($date, '-', $weeks, self::DATE_INTERVAL_UNIT_WEEK);
    }

    /**
     * Returns the SQL to add the number of given months to a date.
     *
     * @param string  $date
     * @param integer $months
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getDateAddMonthExpression($date, $months)
    {
        return $this->getDateArithmeticIntervalExpression($date, '+', $months, self::DATE_INTERVAL_UNIT_MONTH);
    }

    /**
     * Returns the SQL to subtract the number of given months to a date.
     *
     * @param string  $date
     * @param integer $months
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getDateSubMonthExpression($date, $months)
    {
        return $this->getDateArithmeticIntervalExpression($date, '-', $months, self::DATE_INTERVAL_UNIT_MONTH);
    }

    /**
     * Returns the SQL to add the number of given quarters to a date.
     *
     * @param string  $date
     * @param integer $quarters
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getDateAddQuartersExpression($date, $quarters)
    {
        return $this->getDateArithmeticIntervalExpression($date, '+', $quarters, self::DATE_INTERVAL_UNIT_QUARTER);
    }

    /**
     * Returns the SQL to subtract the number of given quarters from a date.
     *
     * @param string  $date
     * @param integer $quarters
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getDateSubQuartersExpression($date, $quarters)
    {
        return $this->getDateArithmeticIntervalExpression($date, '-', $quarters, self::DATE_INTERVAL_UNIT_QUARTER);
    }

    /**
     * Returns the SQL to add the number of given years to a date.
     *
     * @param string  $date
     * @param integer $years
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getDateAddYearsExpression($date, $years)
    {
        return $this->getDateArithmeticIntervalExpression($date, '+', $years, self::DATE_INTERVAL_UNIT_YEAR);
    }

    /**
     * Returns the SQL to subtract the number of given years from a date.
     *
     * @param string  $date
     * @param integer $years
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getDateSubYearsExpression($date, $years)
    {
        return $this->getDateArithmeticIntervalExpression($date, '-', $years, self::DATE_INTERVAL_UNIT_YEAR);
    }

    /**
     * Returns the SQL for a date arithmetic expression.
     *
     * @param string  $date     The column or literal representing a date to perform the arithmetic operation on.
     * @param string  $operator The arithmetic operator (+ or -).
     * @param integer $interval The interval that shall be calculated into the date.
     * @param string  $unit     The unit of the interval that shall be calculated into the date.
     *                          One of the DATE_INTERVAL_UNIT_* constants.
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    protected function getDateArithmeticIntervalExpression($date, $operator, $interval, $unit)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getBitAndComparisonExpression($value1, $value2)
    {
        return 'bitAnd(' . $value1 . ', ' . $value2 . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getBitOrComparisonExpression($value1, $value2)
    {
        return 'bitOr(' . $value1 . ', ' . $value2 . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getForUpdateSQL()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function appendLockHint($fromClause, $lockMode)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getReadLockSQL()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getWriteLockSQL()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getDropIndexSQL($index, $table = null)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getDropConstraintSQL($constraint, $table)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getDropForeignKeySQL($foreignKey, $table)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getCommentOnColumnSQL($tableName, $columnName, $comment)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    protected function _getCreateTableSQL($tableName, array $columns, array $options = [])
    {
        $engine = !empty($options['engine']) ? $options['engine'] : 'MergeTree'; //TODO is it good decision? 

        if (isset($options['uniqueConstraints']) && ! empty($options['uniqueConstraints'])) {
            throw DBALException::notSupported('uniqueConstraints');
        }

        if (isset($options['indexes']) && ! empty($options['indexes'])) {
            throw DBALException::notSupported('uniqueConstraints');
        }


        /**
         * MergeTree* specific section
         */
        if ( in_array($engine, ['MergeTree', 'CollapsingMergeTree', 'SummingMergeTree', 'AggregatingMergeTree', 'ReplacingMergeTree']) ) {
            $indexGranularity = !empty($options['indexGranularity']) ? $options['indexGranularity'] : 8192;

            /**
             * eventDateColumn section
             */
            if ( empty($options['eventDateColumn']) ) {
                $dateColumns = array_filter($columns, function($column) {
                    return $column['type'] instanceof DateType;
                });

                if ($dateColumns) {
                    throw new \Exception('Table `' . $tableName . '` has DateType columns: `' . implode('`, `', array_keys($dateColumns)) . '`, but no one of them is setted as `eventDateColumn` with $table->addOption("eventDateColumn", "%eventDateColumnName%")');
                } else {
                    $eventDateColumnName = 'EventDate';
                    $dateColumn = [$eventDateColumnName => [
                        'name' => $eventDateColumnName,
                        'type' => Type::getType('date'),
                        'default' => 'today()'
                    ]];
                }
            } else {
                if (isset($columns[$options['eventDateColumn']])) {
                    if ($columns[$options['eventDateColumn']]['type'] instanceof DateType) {
                        $eventDateColumnName = $options['eventDateColumn'];
                        $dateColumn = [$options['eventDateColumn'] => $columns[$options['eventDateColumn']]];
                        unset($columns[$options['eventDateColumn']]);
                    } else {
                        throw new \Exception('In table `' . $tableName . '` you have set field `' . $options['eventDateColumn'] . '` (' . get_class($columns[$options['eventDateColumn']]['type']) . ') as `eventDateColumn`, but it is not instance of DateType');
                    }
                } else {
                    $eventDateColumnName = $options['eventDateColumn'];
                    $dateColumn = [$eventDateColumnName => [
                        'name' => $eventDateColumnName,
                        'type' => Type::getType('date'),
                        'default' => 'today()'
                    ]];

                }
            }
            $columns = $dateColumn + $columns; // insert into very beginning
            
            /**
             * Primary key section
             */
            if ( empty($options['primary']) ) {
                throw new \Exception('You need specify PrimaryKey for MergeTree* tables');
            }
        
        }

        $columnListSql = $this->getColumnDeclarationListSQL($columns);
        $query = 'CREATE TABLE ' . $tableName . ' (' . $columnListSql . ') ENGINE = ' . $engine;

        if ( in_array($engine, ['MergeTree', 'CollapsingMergeTree', 'SummingMergeTree', 'AggregatingMergeTree', 'ReplacingMergeTree']) ) {
            $query .=  '(' . $eventDateColumnName . ', (' . implode(', ', array_unique(array_values($options['primary']))) . '), ' . $indexGranularity;
            //TODO any special MergeTree* table parameters
            $query .= ')';
        }

        $sql[] = $query;

        return $sql;
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateForeignKeySQL(ForeignKeyConstraint $foreignKey, $table)
    {
        throw DBALException::notSupported(__METHOD__);
    }    

    /**
     * {@inheritDoc}
     */
    public function getAlterTableSQL(TableDiff $diff)
    {
        $columnSql = [];
        $queryParts = [];
        if ($diff->newName !== false) {
            throw DBALException::notSupported('RENAME COLUMN');
        }

        foreach ($diff->addedColumns as $column) {
            if ($this->onSchemaAlterTableAddColumn($column, $diff, $columnSql)) {
                continue;
            }

            $columnArray = $column->toArray();
            $queryParts[] = 'ADD COLUMN ' . $this->getColumnDeclarationSQL($column->getQuotedName($this), $columnArray);
        }

        foreach ($diff->removedColumns as $column) {
            if ($this->onSchemaAlterTableRemoveColumn($column, $diff, $columnSql)) {
                continue;
            }

            $queryParts[] =  'DROP COLUMN ' . $column->getQuotedName($this);
        }

        foreach ($diff->changedColumns as $columnDiff) {
            if ($this->onSchemaAlterTableChangeColumn($columnDiff, $diff, $columnSql)) {
                continue;
            }

            /* @var $columnDiff \Doctrine\DBAL\Schema\ColumnDiff */
            $column = $columnDiff->column;
            $columnArray = $column->toArray();

            // Don't propagate default value changes for unsupported column types.
            if ($columnDiff->hasChanged('default') &&
                count($columnDiff->changedProperties) === 1 &&
                ($columnArray['type'] instanceof TextType || $columnArray['type'] instanceof BlobType)
            ) {
                continue;
            }

            $queryParts[] =  'MODIFY COLUMN ' . $this->getColumnDeclarationSQL($column->getQuotedName($this), $columnArray);
        }

        foreach ($diff->renamedColumns as $oldColumnName => $column) {
            throw DBALException::notSupported('RENAME COLUMN');
        }

        $sql = [];
        $tableSql = [];

        if ( ! $this->onSchemaAlterTable($diff, $tableSql)) {
            if (count($queryParts) > 0) {
                $sql[] = 'ALTER TABLE ' . $diff->getName($this)->getQuotedName($this) . ' ' . implode(', ', $queryParts);
            }
        }

        return array_merge($sql, $tableSql, $columnSql);
    }

    /**
     * {@inheritDoc}
     */
    protected function getPreAlterTableIndexForeignKeySQL(TableDiff $diff)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    protected function getPostAlterTableIndexForeignKeySQL(TableDiff $diff)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    protected function getRenameIndexSQL($oldIndexName, Index $index, $tableName)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    protected function _getAlterTableIndexForeignKeySQL(TableDiff $diff)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getColumnDeclarationSQL($name, array $field)
    {
        if (isset($field['columnDefinition'])) {
            $columnDef = $this->getCustomTypeDeclarationSQL($field);
        } else {
            $default = $this->getDefaultValueDeclarationSQL($field);

            $typeDecl = $field['type']->getSqlDeclaration($field, $this);
            $columnDef = $typeDecl . $default;
        }

        return $name . ' ' . $columnDef;
    }

    /**
     * {@inheritDoc}
     */
    public function getDecimalTypeDeclarationSQL(array $columnDef)
    {
        return 'Float64';
    }

    /**
     * {@inheritDoc}
     */
    public function getCheckDeclarationSQL(array $definition)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getUniqueConstraintDeclarationSQL($name, Index $index)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getIndexDeclarationSQL($name, Index $index)
    {
        // Index declaration in statements like CREATE TABLE is not supported.
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getForeignKeyDeclarationSQL(ForeignKeyConstraint $foreignKey)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getAdvancedForeignKeyOptionsSQL(ForeignKeyConstraint $foreignKey)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getForeignKeyReferentialActionSQL($action)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getForeignKeyBaseDeclarationSQL(ForeignKeyConstraint $foreignKey)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getUniqueFieldDeclarationSQL()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getCurrentDateSQL()
    {
        return 'today()';
    }

    /**
     * {@inheritDoc}
     * @todo check it! time with 1970 year prefix...
     */
    public function getCurrentTimeSQL()
    {
        return 'toTime(now())';
    }

    /**
     * {@inheritDoc}
     */
    public function getCurrentTimestampSQL()
    {
        return 'now()';
    }

    /**
     * {@inheritDoc}
     */
    public function getListDatabasesSQL()
    {
        return 'SHOW DATABASES FORMAT JSON';
    }

    /**
     * {@inheritDoc}
     */
    public function getListTableColumnsSQL($table, $database = null)
    {
        return 'DESCRIBE TABLE ' . ($database ? $this->quoteStringLiteral($database) . '.' : '') . $this->quoteStringLiteral($table) . ' FORMAT JSON';
    }

    /**
     * {@inheritDoc}
     */
    public function getListTablesSQL()
    {
        return "SELECT name FROM system.tables WHERE database != 'system' AND engine != 'View'";
    }

    /**
     * {@inheritDoc}
     */
    public function getListViewsSQL($database)
    {
        return "SELECT name FROM system.tables WHERE database != 'system' AND engine = 'View'";
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateViewSQL($name, $sql)
    {
        return 'CREATE VIEW ' . $this->quoteStringLiteral($name) . ' AS ' . $sql;
    }

    /**
     * {@inheritDoc}
     */
    public function getDropViewSQL($name)
    {
        return 'DROP TABLE ' . $this->quoteStringLiteral($name);
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateDatabaseSQL($database)
    {
        return 'CREATE DATABASE ' . $this->quoteStringLiteral($database);
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTimeTypeDeclarationSQL(array $fieldDeclaration)
    {
        return 'DateTime';
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTimeTzTypeDeclarationSQL(array $fieldDeclaration)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTypeDeclarationSQL(array $fieldDeclaration)
    {
        return 'Date';
    }

    /**
     * {@inheritDoc}
     */
    public function getFloatDeclarationSQL(array $fieldDeclaration)
    {
        return 'Float64';
    }

    /**
     * {@inheritDoc}
     */
    public function getDefaultTransactionIsolationLevel()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /* supports*() methods */

    /**
     * {@inheritDoc}
     */
    public function supportsTransactions()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public function supportsSavepoints()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public function supportsPrimaryConstraints()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public function supportsForeignKeyConstraints()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public function supportsGettingAffectedRows()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    protected function doModifyLimitQuery($query, $limit, $offset)
    {
        if ( is_null($limit) )
            return $query;

        $query .= ' LIMIT ';
        if (! is_null($offset)) {
            $query .= $offset . ', ';
        }

        $query .= $limit;

        return $query;
    }

    /**
     * {@inheritDoc}
     */
    public function getEmptyIdentityInsertSQL($tableName, $identifierColumnName)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getTruncateTableSQL($tableName, $cascade = false)
    {
        /**
         * For MergeTree* engines may be done with next workaround:
         *
         * SELECT partition FROM system.parts WHERE table= '$tableName';
         * ALTER TABLE $tableName DROP PARTITION $partitionName
         */
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function createSavePoint($savepoint)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function releaseSavePoint($savepoint)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function rollbackSavePoint($savepoint)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getStringLiteralQuoteCharacter()
    {
        return '`';
    }

    /**
     * {@inheritDoc}
     */
    protected function getReservedKeywordsClass()
    {
        return 'Mochalygin\DoctrineDBALClickHouse\ClickHouseKeywords';
    }

    /**
     * {@inheritDoc}
     */
    public function getDefaultValueDeclarationSQL($field)
    {
        if (! isset($field['default'])) {
            return '';
        }
            
        $default = " DEFAULT '" . $field['default'] . "'";
        if ( isset($field['type']) ) {
            if (in_array((string)$field['type'], ['Integer', 'BigInt', 'SmallInt', 'Float'])) {
                $default = ' DEFAULT ' . $field['default'];
            } else if (in_array((string)$field['type'], ['DateTime']) && $field['default'] == $this->getCurrentTimestampSQL()) {
                $default = ' DEFAULT ' . $this->getCurrentTimestampSQL();
            } else if ('Date' == (string)$field['type'] && $field['default'] == $this->getCurrentDateSQL()) {
                $default = ' DEFAULT ' . $this->getCurrentDateSQL();
            }
        }

        return $default;
    }

}
