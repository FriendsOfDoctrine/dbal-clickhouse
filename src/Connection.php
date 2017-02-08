<?php

/**
 * Doctrine DBAL library for ClickHouse -- an open-source column-oriented DBMS for OLAP (https://clickhouse.yandex)
 */

namespace Mochalygin\DoctrineDBALClickHouse;

use Doctrine\Common\EventManager;
use Doctrine\DBAL\DBALException;

/**
 * Connection for ClickHouse database {@link https://clickhouse.yandex/}
 * 
 * @author mochalygin <a@mochalygin.ru>
 */
class Connection extends \Doctrine\DBAL\Connection
{

    /**
     * {@inheritDoc}
     */
    public function executeUpdate($query, array $params = array(), array $types = array())
    {
        //ClickHouse has no UPDATE (CollapsingMergeTree???) and DELETE statement, so we may do only INSERT with this method
        if (strtoupper(substr(trim($query), 0, 6) != 'INSERT')) {
            throw new \Exception('DELETE and UPDATE are not allowed in ClickHouse');
        }

        return parent::executeUpdate($query, $params, $types);
    }
    /**
     * @throws \Exception
     */
    public function delete($tableExpression, array $identifier, array $types = array())
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws \Exception
     */
    public function setTransactionIsolation($level)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws \Exception
     */
    public function getTransactionIsolation()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws \Exception
     */
    public function update($tableExpression, array $data, array $identifier, array $types = array())
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @todo тут немного говнокодца, метод сработает, поскольку Statement лежит в том же неймспейсе, но это очень и очень уродливый подход
     */
//    public function prepare($statement)
//    {
//        $this->connect();
//
//        try {
//            $stmt = new Statement($statement, $this);
//        } catch (\Exception $ex) {
//            throw DBALException::driverExceptionDuringQuery($this->_driver, $ex, $statement);
//        }
//
//        $stmt->setFetchMode($this->defaultFetchMode);
//
//        return $stmt;
//    }

    /**
     * @throws \Exception
     */
    public function getTransactionNestingLevel()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws \Exception
     */
    public function transactional(\Closure $func)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws \Exception
     */
    public function setNestTransactionsWithSavepoints($nestTransactionsWithSavepoints)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws \Exception
     */
    public function getNestTransactionsWithSavepoints()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws \Exception
     */
    public function beginTransaction()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws \Exception
     */
    public function commit()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws \Exception
     */
    public function rollBack()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws \Exception
     */
    public function createSavepoint($savepoint)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws \Exception
     */
    public function releaseSavepoint($savepoint)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws \Exception
     */
    public function rollbackSavepoint($savepoint)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws \Exception
     */
    public function setRollbackOnly()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws \Exception
     */
    public function isRollbackOnly()
    {
        throw DBALException::notSupported(__METHOD__);
    }

}
