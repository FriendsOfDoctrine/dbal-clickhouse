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

use Doctrine\DBAL\Platforms\Exception\NotSupported;
use Doctrine\DBAL\TransactionIsolationLevel;
use function mb_strtoupper;
use function mb_substr;
use function trim;

class Connection extends \Doctrine\DBAL\Connection
{
    /**
     * {@inheritDoc}
     */
    public function delete(string $table, array $criteria = [], array $types = []): int|string
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function update(string $table, array $data, array $criteria = [], array $types = []): int|string
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function executeStatement(string $sql, array $params = [], array $types = []): int|string
    {
        $command = mb_strtoupper(mb_substr(trim($sql), 0, 6));

        if (in_array($command, ['DELETE', 'UPDATE'], true)) {
            throw NotSupported::new($command);
        }

        return parent::executeStatement($sql, $params, $types);
    }

    /**
     * all methods below throw exceptions, because ClickHouse has not transactions
     */

    /**
     * {@inheritDoc}
     */
    public function setTransactionIsolation(TransactionIsolationLevel $level): void
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getTransactionIsolation(): TransactionIsolationLevel
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getTransactionNestingLevel(): int
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function transactional(\Closure $func): mixed
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function setNestTransactionsWithSavepoints($nestTransactionsWithSavepoints): void
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getNestTransactionsWithSavepoints(): bool
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function beginTransaction(): void
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function commit(): void
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function rollBack(): void
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function createSavepoint($savepoint): void
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function releaseSavepoint($savepoint): void
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function rollbackSavepoint($savepoint): void
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function setRollbackOnly(): void
    {
        throw NotSupported::new(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function isRollbackOnly(): bool
    {
        throw NotSupported::new(__METHOD__);
    }
}
