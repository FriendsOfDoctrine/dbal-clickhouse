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
    
    private $defaultFetchMode = \PDO::FETCH_BOTH;

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
        $this->defaultFetchMode = $fetchMode;
        return true; //@todo check allowed fetch modes
    }

    /**
     * {@inheritDoc}
     * @todo regard fetchMode
     */
    public function fetch($fetchMode = null)
    {
        $data = $this->getIterator()->current();
        $this->getIterator()->next();

        return $data;
    }

    /**
     * {@inheritDoc}
     * @todo regard fetchMode
     */
    public function fetchAll($fetchMode = null)
    {
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
        throw new \Exception('Implement it!');
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