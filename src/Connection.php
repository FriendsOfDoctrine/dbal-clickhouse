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

use Doctrine\DBAL\Exception;

use function mb_strtoupper;
use function mb_substr;
use function trim;

class Connection extends \Doctrine\DBAL\Connection
{
    /**
     * {@inheritDoc}
     */
    public function delete($table, array $criteria, array $types = []): int
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function update($table, array $data, array $criteria, array $types = []): int
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function executeStatement($sql, array $params = [], array $types = []): int|string
    {
        $command = mb_strtoupper(mb_substr(trim($sql), 0, 6));

        if (in_array($command, ['DELETE', 'UPDATE'], true)) {
            throw Exception::notSupported($command);
        }

        return parent::executeStatement($sql, $params, $types);
    }

    /**
     * all methods below throw exceptions, because ClickHouse has not transactions
     */

    /**
     * {@inheritDoc}
     */
    public function setTransactionIsolation($level): int
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getTransactionIsolation(): int
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getTransactionNestingLevel(): int
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function transactional(\Closure $func): void
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function setNestTransactionsWithSavepoints($nestTransactionsWithSavepoints): void
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getNestTransactionsWithSavepoints(): bool
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function beginTransaction(): bool
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function commit(): bool
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function rollBack(): bool
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function createSavepoint($savepoint): void
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function releaseSavepoint($savepoint): void
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function rollbackSavepoint($savepoint): void
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function setRollbackOnly(): void
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function isRollbackOnly(): bool
    {
        throw Exception::notSupported(__METHOD__);
    }
}
