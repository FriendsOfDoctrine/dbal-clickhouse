<?php

/**
 * Doctrine DBAL library for ClickHouse -- an open-source column-oriented DBMS for OLAP (https://clickhouse.yandex)
 */

namespace Mochalygin\DoctrineDBALClickHouse;

/**
 * Statement for ClickHouse database (http://clickhouse.yandex)
 * 
 * @author mochalygin <a@mochalygin.ru>
 */
class ClickHouseStatement implements \IteratorAggregate, \Doctrine\DBAL\Driver\Statement
{
    /** 
     * @var \ClickHouseDB\Client
     */
    protected $smi2CHClient;

    /** 
     * @var string 
     */
    protected $statement;
    
    /**
     * @var array|null
     */
    protected $rows = null;

    /**
     * Query parameters for prepared statement (key => value)
     * @var array 
     */
    protected $values = [];
    
    /**
     * @var \ArrayIterator|null
     */
    protected $iterator = null;

    /**
     * @var integer
     */
    private $fetchMode = \PDO::FETCH_BOTH;

    /**
     * @param \ClickHouseDB\Client $client
     * @param string $statement
     */
    public function __construct(\ClickHouseDB\Client $client, $statement)
    {
        $this->smi2CHClient = $client;
        $this->statement = $statement;
    }

    /**
     * {@inheritDoc} 
     */
    public function getIterator()
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
        $this->rows = null;
        $this->iterator = null;

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function columnCount()
    {
        return $this->rows 
                ? count(array_slice($this->rows, 0, 1)[0])
                : null;
    }

    /**
     * {@inheritDoc}
     */
    public function setFetchMode($fetchMode, $arg2 = null, $arg3 = null)
    {
        $this->fetchMode = $this->assumeFetchMode($fetchMode);
        return true;
    }

    /**
     * @param int|null $fetchMode
     * @return int
     */
    protected function assumeFetchMode($fetchMode = null)
    {
        $mode = $fetchMode ?: $this->fetchMode;
        if (! in_array($mode, [
                    \PDO::FETCH_ASSOC,
                    \PDO::FETCH_NUM,
                    \PDO::FETCH_BOTH,
                    \PDO::FETCH_OBJ,
                    \PDO::FETCH_KEY_PAIR,
        ])) {
            $mode = \PDO::FETCH_BOTH;
        }

        return $mode;
    }

    /**
     * {@inheritDoc}
     */
    public function fetch($fetchMode = null)
    {
        $data = $this->getIterator()->current();
        $this->getIterator()->next();

        if (\PDO::FETCH_NUM == $this->assumeFetchMode($fetchMode)) {
            return array_values($data);
        }

        if (\PDO::FETCH_BOTH == $this->assumeFetchMode($fetchMode)) {
            return array_values($data) + $data;
        }

        if (\PDO::FETCH_OBJ == $this->assumeFetchMode($fetchMode)) {
            return (object)$data;
        }

        if (\PDO::FETCH_KEY_PAIR == $this->assumeFetchMode($fetchMode)) {
            if (count($data) < 2) {
                throw new \Exception('To fetch in \PDO::FETCH_KEY_PAIR mode, result set must contain at least 2 columns');
            }

            return [array_shift($data) => array_shift($data)];
        }

        return $data;
    }

    /**
     * {@inheritDoc}
     */
    public function fetchAll($fetchMode = null)
    {
        if (\PDO::FETCH_NUM == $this->assumeFetchMode($fetchMode)) {
            return  array_map(
                        function ($row) {return array_values($row);},
                        $this->rows
                    );
        }

        if (\PDO::FETCH_BOTH == $this->assumeFetchMode($fetchMode)) {
            return  array_map(
                        function ($row) {return array_values($row) + $row;},
                        $this->rows
                    );
        }

        if (\PDO::FETCH_OBJ == $this->assumeFetchMode($fetchMode)) {
            return  array_map(
                        function ($row) {return (object)$row;},
                        $this->rows
                    );
        }

        if (\PDO::FETCH_KEY_PAIR == $this->assumeFetchMode($fetchMode)) {
            return  array_map(
                        function ($row) {
                            if (count($row) < 2) {
                                throw new \Exception('To fetch in \PDO::FETCH_KEY_PAIR mode, result set must contain at least 2 columns');
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
        if ($elem = $this->fetch()) {
            if (array_key_exists($columnIndex, $elem)) {
                return $elem[$columnIndex];
            } else {
                return array_values($elem)[0];
            }
        }

        return false;
    }

    /**
     * {@inheritDoc}
     * @todo regard $type
     */
    public function bindValue($param, $value, $type = null)
    {
        $this->values[$param] = $value;
    }

    /**
     * {@inheritDoc}
     * @todo regard $type and $length
     */
    public function bindParam($column, &$variable, $type = null, $length = null)
    {
        $this->values[$column] = &$variable;
    }

    function errorCode()
    {
        throw new \Exception('Implement it!');
        return (int)$this->getChStatement()->isError();
    }

    function errorInfo()
    {
        throw new \Exception('Implement it!');
        return implode(PHP_EOL, $this->getChStatement()->info());
    }

    /**
     * {@inheritDoc}
     */
    public function execute($params = null)
    {
        if ( is_array($params) ) {
            $this->values = array_replace($this->values, $params);//TODO array keys must be all strings or all integers?
        }

        $sql = $this->statement;
        foreach ($this->values as $key => $value) {
            $value = is_string($value) ? "'" . addslashes($value) . "'" : $value;
            $sql = preg_replace('/(' . (is_int($key) ? '\?' : ':' . $key) . ')/i', $value, $sql, 1);
        }

        $this->processViaSMI2($sql);

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function rowCount()
    {
        return 1; // ClickHouse do not return amount of inserted rows, so we will return 1
    }

    /**
     * @return mixed
     */
    public function getSql()
    {
        return $this->statement;
    }

    /**
     * Specific SMI2 ClickHouse lib statement execution
     * If you want to use any other lib for working with CH -- just update this method
     *
     * @param string $sql
     */
    protected function processViaSMI2($sql)
    {
        //smi2 CH Driver works only with FORMAT JSON, so add suffix if it is SELECT statement
        $sql = trim($sql);
        if (strtoupper(substr($sql, 0, 6)) === 'SELECT') {
            if (strtoupper(substr($sql, -11)) !== 'FORMAT JSON') {
                $sql .= ' FORMAT JSON';
            }
        }

        //TODO catch in Driver and convert into DBALExceptions all SMI2's exceptions (need to implement ExceptionConverterDriver)
        $this->rows = $this->smi2CHClient->write($sql)->rows();
    }

}