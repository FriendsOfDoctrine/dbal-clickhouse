<?php
/*
 * This file is part of the FODDBALClickHouse package -- Doctrine DBAL library
 * for ClickHouse (a column-oriented DBMS for OLAP <https://clickhouse.yandex/>)
 *
 * (c) FriendsOfDoctrine <https://github.com/FriendsOfDoctrine/>.
 *
 * For the full copyright and license inflormation, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Mochalygin\DoctrineDBALClickHouse;

use Doctrine\Common\EventManager;
use Doctrine\DBAL\DBALException;

/**
 * ClickHouse Connection
 *
 * @author Mochalygin <a@mochalygin.ru>
 */
class Connection extends \Doctrine\DBAL\Connection
{
    /**
     * {@inheritDoc}
     */
    public function executeUpdate($query, array $params = array(), array $types = array())
    {
        // ClickHouse has no UPDATE or DELETE statements
        $command = strtoupper(substr(trim($query), 0, 6));
        if ('UPDATE' == $command || 'DELETE' == $command) {
            throw new ClickHouseException('UPDATE and DELETE are not allowed in ClickHouse');
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
    public function update($tableExpression, array $data, array $identifier, array $types = array())
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * all methods below throw exceptions, because ClickHouse has not transactions
     */

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
