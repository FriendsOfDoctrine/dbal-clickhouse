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
     * @throws ClickHouseException
     */
    public function delete($tableExpression, array $identifier, array $types = []) : int
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * @throws ClickHouseException
     */
    public function update($tableExpression, array $data, array $identifier, array $types = []) : int
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * all methods below throw exceptions, because ClickHouse has not transactions
     */

    /**
     * @throws ClickHouseException
     */
    public function setTransactionIsolation($level) : int
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * @throws ClickHouseException
     */
    public function getTransactionIsolation() : int
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * @throws ClickHouseException
     */
    public function getTransactionNestingLevel() : int
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * @throws ClickHouseException
     */
    public function transactional(\Closure $func) : void
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * @throws ClickHouseException
     */
    public function setNestTransactionsWithSavepoints($nestTransactionsWithSavepoints) : void
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * @throws ClickHouseException
     */
    public function getNestTransactionsWithSavepoints() : bool
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * @throws ClickHouseException
     */
    public function beginTransaction() : bool
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * @throws ClickHouseException
     */
    public function commit() : bool
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * @throws ClickHouseException
     */
    public function rollBack() : bool
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * @throws ClickHouseException
     */
    public function createSavepoint($savepoint) : void
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * @throws ClickHouseException
     */
    public function releaseSavepoint($savepoint) : void
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * @throws ClickHouseException
     */
    public function rollbackSavepoint($savepoint) : void
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * @throws ClickHouseException
     */
    public function setRollbackOnly() : void
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }

    /**
     * @throws ClickHouseException
     */
    public function isRollbackOnly() : bool
    {
        throw ClickHouseException::notSupported(__METHOD__);
    }
}
