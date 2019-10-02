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

use ClickHouseDB\Client;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\FetchMode;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use function array_key_exists;
use function array_keys;
use function array_map;
use function array_replace;
use function array_shift;
use function array_values;
use function array_walk;
use function count;
use function current;
use function explode;
use function implode;
use function in_array;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function preg_replace;
use function stripos;
use function trim;

/**
 * ClickHouse Statement
 */
class ClickHouseStatement implements \IteratorAggregate, Statement
{
    /** @var Client */
    protected $smi2CHClient;

    /** @var string */
    protected $statement;

    /** @var AbstractPlatform */
    protected $platform;

    /** @var mixed[] */
    protected $rows = [];

    /**
     * Query parameters for prepared statement (key => value)
     * @var mixed[]
     */
    protected $values = [];

    /**
     * Query parameters' types for prepared statement (key => value)
     * @var mixed[]
     */
    protected $types = [];

    /** @var \ArrayIterator|null */
    protected $iterator;

    /** @var int */
    private $fetchMode = FetchMode::MIXED;

    public function __construct(Client $client, string $statement, AbstractPlatform $platform)
    {
        $this->smi2CHClient = $client;
        $this->statement    = $statement;
        $this->platform     = $platform;
    }

    /**
     * {@inheritDoc}
     */
    public function getIterator() : \ArrayIterator
    {
        if (! $this->iterator) {
            $this->iterator = new \ArrayIterator($this->rows);
        }

        return $this->iterator;
    }

    /**
     * {@inheritDoc}
     */
    public function closeCursor()
    {
        $this->rows     = [];
        $this->iterator = null;

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function columnCount()
    {
        return $this->rows
            ? count(current($this->rows))
            : 0;
    }

    /**
     * {@inheritDoc}
     */
    public function setFetchMode($fetchMode, $arg2 = null, $arg3 = null)
    {
        $this->fetchMode = $this->assumeFetchMode($fetchMode);

        return true;
    }

    protected function assumeFetchMode(?int $fetchMode = null) : int
    {
        $mode = $fetchMode ?: $this->fetchMode;
        if (! in_array($mode, [
            FetchMode::ASSOCIATIVE,
            FetchMode::NUMERIC,
            FetchMode::STANDARD_OBJECT,
            \PDO::FETCH_KEY_PAIR,
        ], true)) {
            $mode = FetchMode::MIXED;
        }

        return $mode;
    }

    /**
     * {@inheritDoc}
     */
    public function fetch($fetchMode = null, $cursorOrientation = \PDO::FETCH_ORI_NEXT, $cursorOffset = 0)
    {
        $data = $this->getIterator()->current();

        if ($data === null) {
            return false;
        }

        $this->getIterator()->next();

        if ($this->assumeFetchMode($fetchMode) === FetchMode::NUMERIC) {
            return array_values($data);
        }

        if ($this->assumeFetchMode($fetchMode) === FetchMode::MIXED) {
            return array_values($data) + $data;
        }

        if ($this->assumeFetchMode($fetchMode) === FetchMode::STANDARD_OBJECT) {
            return (object) $data;
        }

        if ($this->assumeFetchMode($fetchMode) === \PDO::FETCH_KEY_PAIR) {
            if (count($data) < 2) {
                throw new \Exception(
                    'To fetch in \PDO::FETCH_KEY_PAIR mode, result set must contain at least 2 columns'
                );
            }

            return [array_shift($data) => array_shift($data)];
        }

        return $data;
    }

    /**
     * {@inheritDoc}
     */
    public function fetchAll($fetchMode = null, $fetchArgument = null, $ctorArgs = null)
    {
        if ($this->assumeFetchMode($fetchMode) === FetchMode::NUMERIC) {
            return array_map(
                'array_values',
                $this->rows
            );
        }

        if ($this->assumeFetchMode($fetchMode) === FetchMode::MIXED) {
            return array_map(
                function ($row) {
                    return array_values($row) + $row;
                },
                $this->rows
            );
        }

        if ($this->assumeFetchMode($fetchMode) === FetchMode::STANDARD_OBJECT) {
            return array_map(
                function ($row) {
                    return (object) $row;
                },
                $this->rows
            );
        }

        if ($this->assumeFetchMode($fetchMode) === \PDO::FETCH_KEY_PAIR) {
            return array_map(
                function ($row) {
                    if (count($row) < 2) {
                        throw new \Exception(
                            'To fetch in \PDO::FETCH_KEY_PAIR mode, result set must contain at least 2 columns'
                        );
                    }

                    return [array_shift($row) => array_shift($row)];
                },
                $this->rows
            );
        }

        return $this->rows;
    }

    /**
     * {@inheritDoc}
     */
    public function fetchColumn($columnIndex = 0)
    {
        $elem = $this->fetch(FetchMode::NUMERIC);
        if (is_array($elem)) {
            return $elem[$columnIndex] ?? $elem[0];
        }

        return false;
    }

    /**
     * {@inheritDoc}
     */
    public function bindValue($param, $value, $type = null)
    {
        $this->values[$param] = $value;
        $this->types[$param]  = $type;
    }

    /**
     * {@inheritDoc}
     */
    public function bindParam($column, &$variable, $type = null, $length = null)
    {
        $this->values[$column] = &$variable;
        $this->types[$column]  = $type;
    }

    public function errorCode() : void
    {
        throw new ClickHouseException('You need to implement ClickHouseStatement::' . __METHOD__ . '()');
    }

    public function errorInfo() : void
    {
        throw new ClickHouseException('You need to implement ClickHouseStatement::' . __METHOD__ . '()');
    }

    /**
     * {@inheritDoc}
     */
    public function execute($params = null) : bool
    {
        $hasZeroIndex = false;
        if (is_array($params)) {
            $this->values = array_replace($this->values, $params);//TODO array keys must be all strings or all integers?
            $hasZeroIndex = array_key_exists(0, $params);
        }

        $sql = $this->statement;

        if ($hasZeroIndex) {
            $statementParts = explode('?', $sql);
            array_walk($statementParts, function (&$part, $key) : void {
                if (! array_key_exists($key, $this->values)) {
                    return;
                }

                $part .= $this->getTypedParam($key);
            });
            $sql = implode('', $statementParts);
        } else {
            foreach (array_keys($this->values) as $key) {
                $sql = preg_replace(
                    '/(' . (is_int($key) ? '\?' : ':' . $key) . ')/i',
                    $this->getTypedParam($key),
                    $sql,
                    1
                );
            }
        }

        $this->processViaSMI2($sql);

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function rowCount() : int
    {
        return 1; // ClickHouse do not return amount of inserted rows, so we will return 1
    }

    public function getSql() : string
    {
        return $this->statement;
    }

    /**
     * Specific SMI2 ClickHouse lib statement execution
     * If you want to use any other lib for working with CH -- just update this method
     *
     */
    protected function processViaSMI2(string $sql) : void
    {
        $sql = trim($sql);

        $this->rows =
            stripos($sql, 'select') === 0 ||
            stripos($sql, 'show') === 0 ||
            stripos($sql, 'describe') === 0 ?
                $this->smi2CHClient->select($sql)->rows() :
                $this->smi2CHClient->write($sql)->rows();
    }

    /**
     * @param string|int $key
     * @throws ClickHouseException
     */
    protected function getTypedParam($key) : string
    {
        if ($this->values[$key] === null) {
            return 'NULL';
        }

        $type = $this->types[$key] ?? null;

        // if param type was not setted - trying to get db-type by php-var-type
        if ($type === null) {
            if (is_bool($this->values[$key])) {
                $type = ParameterType::BOOLEAN;
            } elseif (is_int($this->values[$key]) || is_float($this->values[$key])) {
                $type = ParameterType::INTEGER;
            } elseif (is_array($this->values[$key])) {
                /*
                 * ClickHouse Arrays
                 */
                $values = $this->values[$key];
                if (is_int(current($values)) || is_float(current($values))) {
                    array_map(
                        function ($value) : void {
                            if (! is_int($value) && ! is_float($value)) {
                                throw new ClickHouseException(
                                    'Array values must all be int/float or string, mixes not allowed'
                                );
                            }
                        },
                        $values
                    );
                } else {
                    $values = array_map(function ($value) {
                        return $value === null ? 'NULL' : $this->platform->quoteStringLiteral($value);
                    }, $values);
                }

                return '[' . implode(', ', $values) . ']';
            }
        }

        if ($type === ParameterType::INTEGER) {
            return (string) $this->values[$key];
        }

        if ($type === ParameterType::BOOLEAN) {
            return (string) (int) (bool) $this->values[$key];
        }

        return $this->platform->quoteStringLiteral((string) $this->values[$key]);
    }
}
