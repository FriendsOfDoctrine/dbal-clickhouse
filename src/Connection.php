<?php

declare(strict_types=1);

/*
 * This file is part of the FODDBALClickHouse package -- Doctrine DBAL library
 * for ClickHouse (a column-oriented DBMS for OLAP <https://clickhouse.yandex/>)
 *
 * (c) FriendsOfDoctrine <https://github.com/FriendsOfDoctrine/>.
 *
 * For the full copyright and license inflormation, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace FOD\DBALClickHouse;

use Doctrine\DBAL\ConnectionException;

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
    public function executeUpdate($query, array $params = [], array $types = []): int
    {
        // ClickHouse has no UPDATE or DELETE statements
        $command = strtoupper(substr(trim($query), 0, 6));
        if ($command === 'UPDATE' || $command === 'DELETE') {
            throw new ClickHouseException('UPDATE and DELETE are not allowed in ClickHouse');
        }

        return parent::executeUpdate($query, $params, $types);
    }

    /**
     * @throws ConnectionException
     */
    public function delete($tableExpression, array $identifier, array $types = []): void
    {
        throw ConnectionException::notSupported(__METHOD__);
    }

    /**
     * @throws ConnectionException
     */
    public function update($tableExpression, array $data, array $identifier, array $types = []): void
    {
        throw ConnectionException::notSupported(__METHOD__);
    }

    /**
     * all methods below throw exceptions, because ClickHouse has not transactions
     */

    /**
     * @throws ConnectionException
     */
    public function setTransactionIsolation($level): void
    {
        throw ConnectionException::notSupported(__METHOD__);
    }

    /**
     * @throws ConnectionException
     */
    public function getTransactionIsolation(): void
    {
        throw ConnectionException::notSupported(__METHOD__);
    }

    /**
     * @throws ConnectionException
     */
    public function getTransactionNestingLevel(): void
    {
        throw ConnectionException::notSupported(__METHOD__);
    }

    /**
     * @throws ConnectionException
     */
    public function transactional(\Closure $func): void
    {
        throw ConnectionException::notSupported(__METHOD__);
    }

    /**
     * @throws ConnectionException
     */
    public function setNestTransactionsWithSavepoints($nestTransactionsWithSavepoints): void
    {
        throw ConnectionException::notSupported(__METHOD__);
    }

    /**
     * @throws ConnectionException
     */
    public function getNestTransactionsWithSavepoints(): void
    {
        throw ConnectionException::notSupported(__METHOD__);
    }

    /**
     * @throws ConnectionException
     */
    public function beginTransaction(): void
    {
        throw ConnectionException::notSupported(__METHOD__);
    }

    /**
     * @throws ConnectionException
     */
    public function commit(): void
    {
        throw ConnectionException::notSupported(__METHOD__);
    }

    /**
     * @throws ConnectionException
     */
    public function rollBack(): void
    {
        throw ConnectionException::notSupported(__METHOD__);
    }

    /**
     * @throws ConnectionException
     */
    public function createSavepoint($savepoint): void
    {
        throw ConnectionException::notSupported(__METHOD__);
    }

    /**
     * @throws ConnectionException
     */
    public function releaseSavepoint($savepoint): void
    {
        throw ConnectionException::notSupported(__METHOD__);
    }

    /**
     * @throws ConnectionException
     */
    public function rollbackSavepoint($savepoint): void
    {
        throw ConnectionException::notSupported(__METHOD__);
    }

    /**
     * @throws ConnectionException
     */
    public function setRollbackOnly(): void
    {
        throw ConnectionException::notSupported(__METHOD__);
    }

    /**
     * @throws ConnectionException
     */
    public function isRollbackOnly(): void
    {
        throw ConnectionException::notSupported(__METHOD__);
    }
}
