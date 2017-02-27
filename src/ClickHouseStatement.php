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
    protected $sql;
    
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
    
    private $fetchMode = \PDO::FETCH_BOTH;

    public function __construct(\ClickHouseDB\Client $client, $sql)
    {
        $this->smi2CHClient = $client;
        $this->sql = $sql;
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
     * @todo check it!
     */
    public function closeCursor()
    {
        $this->rows = null;
        $this->iterator = null;
        $this->fetchMode = \PDO::FETCH_BOTH;

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
                    \PDO::FETCH_BOTH
        ])) {
            $mode = \PDO::FETCH_BOTH;
        }

        return $mode;
    }

    /**
     * {@inheritDoc}
     * @todo other FetchModes
     */
    public function fetch($fetchMode = null)
    {
        $data = $this->getIterator()->current();
        $this->getIterator()->next();

        if (\PDO::FETCH_NUM == $this->assumeFetchMode($fetchMode))
            $data = array_values($data);

        return $data;
    }

    /**
     * {@inheritDoc}
     * @todo regard fetchMode
     */
    public function fetchAll($fetchMode = null)
    {
        if (\PDO::FETCH_NUM == $this->assumeFetchMode($fetchMode)) {
            return  array_map(
                        function ($row) {return array_values($row);},
                        $this->rows
                    );
        }

        return $this->rows;
    }

    /**
     * {@inheritDoc}
     * @todo test it!
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
        //@todo to catch \ClickHouseDB\QueryException?
//        try {
            if ( is_array($params) )
                $this->values = array_replace($this->values, $params);//TODO array keys must be all strings or all integers?

            $completeSql = $this->sql;
            foreach ($this->values as $key => $value) {
                $value = is_string($value) ? "'" . addslashes($value) . "'" : $value;
                
                $completeSql = preg_replace('/(' . (is_int($key) ? '\?' : ':' . $key) . ')/i', $value, $completeSql, 1);
            }

            //TODO smi2 works only with FORMAT JSON, so add it if SELECT statement
            if (strtoupper(substr($completeSql, 0, 6)) == 'SELECT') {
                $completeSql .= ' FORMAT JSON';
            }

            $smi2CHStatement = $this->getClient()->write($completeSql);
//            var_dump($smi2CHStatement->getRequest()->response()->body());
            $this->rows = $smi2CHStatement->rows();

//            var_dump($this->rows);

            return true; //TODO fix it!
//        } catch (\ClickHouseDB\QueryException $ex) {
//            echo 'Exception... ' . $ex->getMessage();
//            return false;
//        }
    }

    /**
     * {@inheritDoc}
     */
    public function rowCount()
    {
        //actually ClickHouse server do not return amount of inserted rows, so we will return 1
        //TODO research
        return 1;
    }

    /**
     * @return \ClickHouseDB\Client
     */
    protected function getClient()
    {
        return $this->smi2CHClient;
    }

    /**
     * @return mixed
     */
    public function getSql()
    {
        return $this->sql;
    }

}