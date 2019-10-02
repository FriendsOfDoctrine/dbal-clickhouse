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

use Doctrine\DBAL\DBALException;
use function strtoupper;
use function substr;
use function trim;

/**
 * ClickHouse Connection
 */
class Connection extends \Doctrine\DBAL\Connection
{
    /**
     * {@inheritDoc}
     */
    public function executeUpdate($query, array $params = [], array $types = []) : int
    {
        // ClickHouse has no UPDATE or DELETE statements
        $command = strtoupper(substr(trim($query), 0, 6));
        if ($command === 'UPDATE' || $command === 'DELETE') {
            throw new ClickHouseException('UPDATE and DELETE are not allowed in ClickHouse');
        }

        return parent::executeUpdate($query, $params, $types);
    }

    /**
     * @throws DBALException
     */
    public function delete($tableExpression, array $identifier, array $types = []) : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function update($tableExpression, array $data, array $identifier, array $types = []) : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * all methods below throw exceptions, because ClickHouse has not transactions
     */

    /**
     * @throws DBALException
     */
    public function setTransactionIsolation($level) : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function getTransactionIsolation() : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function getTransactionNestingLevel() : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function transactional(\Closure $func) : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function setNestTransactionsWithSavepoints($nestTransactionsWithSavepoints) : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function getNestTransactionsWithSavepoints() : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function beginTransaction() : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function commit() : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function rollBack() : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function createSavepoint($savepoint) : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function releaseSavepoint($savepoint) : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function rollbackSavepoint($savepoint) : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function setRollbackOnly() : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function isRollbackOnly() : void
    {
        throw DBALException::notSupported(__METHOD__);
    }
}
