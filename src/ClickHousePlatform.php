<?php

/**
 * Doctrine DBAL library for ClickHouse -- an open-source column-oriented DBMS for OLAP (https://clickhouse.yandex)
 */

namespace Mochalygin\DoctrineDBALClickHouse;

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Identifier;
use Doctrine\DBAL\Types;
use Doctrine\DBAL\Schema\Constraint;
use Doctrine\DBAL\Schema\Sequence;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\ForeignKeyConstraint;
use Doctrine\DBAL\Schema\TableDiff;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\ColumnDiff;
use Doctrine\DBAL\Types\Type;
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
            'UInt8' => 'smallint',
            'UInt16' => 'integer',
            'UInt32' => 'integer',
            'UInt64' => 'bigint',
            'Int8' => 'smallint',
            'Int16' => 'integer',
            'Int32' => 'integer',
            'Int64' => 'bigint',
            'Float32' => 'decimal',
            'Float64' => 'float',
            'String' => 'text',
            'FixedString' => 'string',
            'Date' => 'date',
            'DateTime' => 'datetime',
            'Enum' => 'simple_array', //TODO ???
            'Array' => 'array', //TODO ???
            'Tuple' => 'json_array' //TODO ???
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
     * Gets the default length of a varchar field.
     * ClickHouse length in bytes, not symbols
     *
     * @return integer
     */
    public function getVarcharDefaultLength()
    {
        return 1000;
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
        //TODO length() or lengthUTF8()?
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
//        if ($length === null) {
//            return 'substringUTF8(' . $value . ' FROM ' . $from . ')';
//        }

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
     * @todo there '1' string may be return (always true
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
     * Returns the SQL bit AND comparison expression.
     *
     * @param string $value1
     * @param string $value2
     *
     * @return string
     */
    public function getBitAndComparisonExpression($value1, $value2)
    {
        return '(' . $value1 . ' & ' . $value2 . ')';
    }

    /**
     * Returns the SQL bit OR comparison expression.
     *
     * @param string $value1
     * @param string $value2
     *
     * @return string
     */
    public function getBitOrComparisonExpression($value1, $value2)
    {
        return '(' . $value1 . ' | ' . $value2 . ')';
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
     * Returns the SQL statement(s) to create a table with the specified name, columns and constraints
     * on this platform.
     *
     * @param \Doctrine\DBAL\Schema\Table   $table
     * @param integer                       $createFlags
     *
     * @return array The sequence of SQL statements.
     *
     * @throws \Doctrine\DBAL\DBALException
     * @throws \InvalidArgumentException
     * 
     * @todo implement this!
     */
    public function getCreateTableSQL(Table $table, $createFlags = self::CREATE_INDEXES)
    {
        if ( ! is_int($createFlags)) {
            throw new \InvalidArgumentException("Second argument of AbstractPlatform::getCreateTableSQL() has to be integer.");
        }

        if (count($table->getColumns()) === 0) {
            throw DBALException::noColumnsSpecifiedForTable($table->getName());
        }

        $tableName = $table->getQuotedName($this);
        $options = $table->getOptions();
        $options['uniqueConstraints'] = array();
        $options['indexes'] = array();
        $options['primary'] = array();

        if (($createFlags&self::CREATE_INDEXES) > 0) {
            foreach ($table->getIndexes() as $index) {
                /* @var $index Index */
                if ($index->isPrimary()) {
                    $options['primary']       = $index->getQuotedColumns($this);
                    $options['primary_index'] = $index;
                } else {
                    $options['indexes'][$index->getQuotedName($this)] = $index;
                }
            }
        }

        $columnSql = array();
        $columns = array();

        foreach ($table->getColumns() as $column) {
            /* @var \Doctrine\DBAL\Schema\Column $column */

            if (null !== $this->_eventManager && $this->_eventManager->hasListeners(Events::onSchemaCreateTableColumn)) {
                $eventArgs = new SchemaCreateTableColumnEventArgs($column, $table, $this);
                $this->_eventManager->dispatchEvent(Events::onSchemaCreateTableColumn, $eventArgs);

                $columnSql = array_merge($columnSql, $eventArgs->getSql());

                if ($eventArgs->isDefaultPrevented()) {
                    continue;
                }
            }

            $columnData = $column->toArray();
            $columnData['name'] = $column->getQuotedName($this);
            $columnData['version'] = $column->hasPlatformOption("version") ? $column->getPlatformOption('version') : false;
            $columnData['comment'] = $this->getColumnComment($column);

            if (strtolower($columnData['type']) == "string" && $columnData['length'] === null) {
                $columnData['length'] = 255;
            }

            if (in_array($column->getName(), $options['primary'])) {
                $columnData['primary'] = true;
            }

            $columns[$columnData['name']] = $columnData;
        }

        if (($createFlags&self::CREATE_FOREIGNKEYS) > 0) {
            $options['foreignKeys'] = array();
            foreach ($table->getForeignKeys() as $fkConstraint) {
                $options['foreignKeys'][] = $fkConstraint;
            }
        }

        if (null !== $this->_eventManager && $this->_eventManager->hasListeners(Events::onSchemaCreateTable)) {
            $eventArgs = new SchemaCreateTableEventArgs($table, $columns, $options, $this);
            $this->_eventManager->dispatchEvent(Events::onSchemaCreateTable, $eventArgs);

            if ($eventArgs->isDefaultPrevented()) {
                return array_merge($eventArgs->getSql(), $columnSql);
            }
        }

        $sql = $this->_getCreateTableSQL($tableName, $columns, $options);
        if ($this->supportsCommentOnStatement()) {
            foreach ($table->getColumns() as $column) {
                $comment = $this->getColumnComment($column);

                if (null !== $comment && '' !== $comment) {
                    $sql[] = $this->getCommentOnColumnSQL($tableName, $column->getQuotedName($this), $comment);
                }
            }
        }

        return array_merge($sql, $columnSql);
    }

    /**
     * {@inheritDoc}
     */
    public function getCommentOnColumnSQL($tableName, $columnName, $comment)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * Returns the SQL used to create a table.
     *
     * @param string $tableName
     * @param array  $columns
     * @param array  $options
     *
     * @return array
     * 
     * @todo implement this!
     */
    protected function _getCreateTableSQL($tableName, array $columns, array $options = array())
    {
        $columnListSql = $this->getColumnDeclarationListSQL($columns);

        if (isset($options['uniqueConstraints']) && ! empty($options['uniqueConstraints'])) {
            foreach ($options['uniqueConstraints'] as $name => $definition) {
                $columnListSql .= ', ' . $this->getUniqueConstraintDeclarationSQL($name, $definition);
            }
        }

        if (isset($options['primary']) && ! empty($options['primary'])) {
            $columnListSql .= ', PRIMARY KEY(' . implode(', ', array_unique(array_values($options['primary']))) . ')';
        }

        if (isset($options['indexes']) && ! empty($options['indexes'])) {
            foreach ($options['indexes'] as $index => $definition) {
                $columnListSql .= ', ' . $this->getIndexDeclarationSQL($index, $definition);
            }
        }

        $query = 'CREATE TABLE ' . $tableName . ' (' . $columnListSql;

        $check = $this->getCheckDeclarationSQL($columns);
        if ( ! empty($check)) {
            $query .= ', ' . $check;
        }
        $query .= ')';

        $sql[] = $query;

        if (isset($options['foreignKeys'])) {
            foreach ((array) $options['foreignKeys'] as $definition) {
                $sql[] = $this->getCreateForeignKeySQL($definition, $tableName);
            }
        }

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
     * @todo implement it!
     */
    public function getAlterTableSQL(TableDiff $diff)
    {
        throw DBALException::notSupported(__METHOD__);
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
     * @todo test it!
     */
    public function getColumnDeclarationSQL($name, array $field)
    {
        if (isset($field['columnDefinition'])) {
            $columnDef = $this->getCustomTypeDeclarationSQL($field);
        } else {
            $default = $this->getDefaultValueDeclarationSQL($field);

            $charset = (isset($field['charset']) && $field['charset']) ?
                    ' ' . $this->getColumnCharsetDeclarationSQL($field['charset']) : '';

            $collation = (isset($field['collation']) && $field['collation']) ?
                    ' ' . $this->getColumnCollationDeclarationSQL($field['collation']) : '';

            $notnull = (isset($field['notnull']) && $field['notnull']) ? ' NOT NULL' : '';

            $unique = (isset($field['unique']) && $field['unique']) ?
                    ' ' . $this->getUniqueFieldDeclarationSQL() : '';

            $check = (isset($field['check']) && $field['check']) ?
                    ' ' . $field['check'] : '';

            $typeDecl = $field['type']->getSqlDeclaration($field, $this);
            $columnDef = $typeDecl . $charset . $default . $notnull . $unique . $check . $collation;
        }

        if ($this->supportsInlineColumnComments() && isset($field['comment']) && $field['comment'] !== '') {
            $columnDef .= " COMMENT " . $this->quoteStringLiteral($field['comment']);
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
     * Obtains DBMS specific SQL code portion needed to set an index
     * declaration to be used in statements like CREATE TABLE.
     *
     * @param string                      $name  The name of the index.
     * @param \Doctrine\DBAL\Schema\Index $index The index definition.
     *
     * @return string DBMS specific SQL code portion needed to set an index.
     *
     * @throws \InvalidArgumentException
     */
    public function getIndexDeclarationSQL($name, Index $index)
    {
        $columns = $index->getQuotedColumns($this);
        $name = new Identifier($name);

        if (count($columns) === 0) {
            throw new \InvalidArgumentException("Incomplete definition. 'columns' required.");
        }

        return $this->getCreateIndexSQLFlags($index) . 'INDEX ' . $name->getQuotedName($this) . ' ('
            . $this->getIndexFieldDeclarationListSQL($columns)
            . ')' . $this->getPartialIndexSQL($index);
    }

    /**
     * Obtains DBMS specific SQL code portion needed to set an index
     * declaration to be used in statements like CREATE TABLE.
     *
     * @param array $fields
     *
     * @return string
     */
    public function getIndexFieldDeclarationListSQL(array $fields)
    {
        $ret = array();

        foreach ($fields as $field => $definition) {
            if (is_array($definition)) {
                $ret[] = $field;
            } else {
                $ret[] = $definition;
            }
        }

        return implode(', ', $ret);
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
        return 'CAST(today() AS Date)';
    }
    
    
    
    
    
    
    

    /**
     * Returns the SQL specific for the platform to get the current time.
     *
     * @return string
     */
    public function getCurrentTimeSQL()
    {
        return 'CURRENT_TIME';
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
        //SELECT * FROM columns WHERE database='default' AND table='summing_url_views'
        return 'DESCRIBE TABLE ' . ($database ? $this->quoteStringLiteral($database) . '.' : '') . $this->quoteStringLiteral($table) . ' FORMAT JSON';
    }

    /**
     * {@inheritDoc}
     */
    public function getListTablesSQL()
    {
        return 'SHOW TABLES FORMAT JSON';
    }





    /**
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getListUsersSQL()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * Returns the SQL to list all views of a database or user.
     *
     * @param string $database
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getListViewsSQL($database)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * Returns the list of indexes for the current database.
     *
     * The current database parameter is optional but will always be passed
     * when using the SchemaManager API and is the database the given table is in.
     *
     * Attention: Some platforms only support currentDatabase when they
     * are connected with that database. Cross-database information schema
     * requests may be impossible.
     *
     * @param string $table
     * @param string $currentDatabase
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getListTableIndexesSQL($table, $currentDatabase = null)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @param string $name
     * @param string $sql
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getCreateViewSQL($name, $sql)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @param string $name
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getDropViewSQL($name)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateDatabaseSQL($database)
    {
        return 'CREATE DATABASE ' . $database;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTimeTypeDeclarationSQL(array $fieldDeclaration)
    {
        return 'DateTime';
    }

    /**
     * Obtains DBMS specific SQL to be used to create datetime with timezone offset fields.
     *
     * @param array $fieldDeclaration
     *
     * @return string
     */
    public function getDateTimeTzTypeDeclarationSQL(array $fieldDeclaration)
    {
        return $this->getDateTimeTypeDeclarationSQL($fieldDeclaration);
    }


    /**
     * {@inheritDoc}
     */
    public function getDateTypeDeclarationSQL(array $fieldDeclaration)
    {
        return 'Date';
    }

    /**
     * Obtains DBMS specific SQL to be used to create time fields in statements
     * like CREATE TABLE.
     *
     * @param array $fieldDeclaration
     *
     * @return string
     *
     * @throws \Doctrine\DBAL\DBALException If not supported on this platform.
     */
    public function getTimeTypeDeclarationSQL(array $fieldDeclaration)
    {
        throw DBALException::notSupported(__METHOD__);
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
     * Whether the platform supports indexes.
     *
     * @return boolean
     */
    public function supportsIndexes()
    {
        return true;
    }

    
    
    
    
    
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
     * @todo check it!
     */
    public function supportsGettingAffectedRows()
    {
        return false;
    }





    
    

    /**
     * Does this platform have native JSON type.
     *
     * @return boolean
     */
    public function hasNativeJsonType()
    {
        return false;
    }

    /**
     * @deprecated
     * @todo Remove in 3.0
     */
    public function getIdentityColumnNullInsertSQL()
    {
        return "";
    }

    /**
     * Whether this platform supports views.
     *
     * @return boolean
     */
    public function supportsViews()
    {
        return true;
    }







    /**
     * {@inheritDoc}
     */
    protected function doModifyLimitQuery($query, $limit, $offset)
    {
        if (is_null($limit))
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
     * Generates a Truncate Table SQL statement for a given table.
     *
     * Cascade is not supported on many platforms but would optionally cascade the truncate by
     * following the foreign keys.
     *
     * @param string  $tableName
     * @param boolean $cascade
     *
     * @return string
     */
    public function getTruncateTableSQL($tableName, $cascade = false)
    {
        $tableIdentifier = new Identifier($tableName);

        return 'TRUNCATE ' . $tableIdentifier->getQuotedName($this);
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
}
