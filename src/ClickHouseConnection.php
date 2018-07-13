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

use ClickHouseDB\Client as Smi2CHClient;
use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\AbstractPlatform;

/**
 * ClickHouse implementation for the Connection interface.
 */
class ClickHouseConnection implements Connection
{
    /** @var Smi2CHClient */
    protected $smi2CHClient;

    /** @var AbstractPlatform */
    protected $platform;

    /**
     * Connection constructor
     *
     * @param array $params Array with connection params.
     */
    public function __construct(
        array $params,
        string $username,
        string $password,
        AbstractPlatform $platform
    ) {
        $this->smi2CHClient = new Smi2CHClient([
            'host' => $params['host'] ?? 'localhost',
            'port' => $params['port'] ?? 8123,
            'username' => $username,
            'password' => $password,
            'settings' => array_merge([
                'database' => $params['dbname'] ?? 'default',
            ], $params['driverOptions'] ?? []),
        ]);

        $this->platform = $platform;
    }

    /**
     * {@inheritDoc}
     */
    public function prepare($prepareString)
    {
        return new ClickHouseStatement($this->smi2CHClient, $prepareString, $this->platform);
    }

    /**
     * {@inheritDoc}
     */
    public function query()
    {
        $args = \func_get_args();
        $stmt = $this->prepare($args[0]);
        $stmt->execute();

        return $stmt;
    }

    /**
     * {@inheritDoc}
     */
    public function quote($input, $type = ParameterType::STRING)
    {
        if ($type === ParameterType::INTEGER) {
            return $input;
        }

        return $this->platform->quoteStringLiteral($input);
    }

    /**
     * {@inheritDoc}
     */
    public function exec($statement) : int
    {
        $stmt = $this->prepare($statement);
        $stmt->execute();

        return $stmt->rowCount();
    }

    /**
     * {@inheritDoc}
     */
    public function lastInsertId($name = null)
    {
        throw new \LogicException('Unable to get last insert id in ClickHouse');
    }

    /**
     * {@inheritDoc}
     */
    public function beginTransaction()
    {
        throw new \LogicException('Transactions are not allowed in ClickHouse');
    }

    /**
     * {@inheritDoc}
     */
    public function commit()
    {
        throw new \LogicException('Transactions are not allowed in ClickHouse');
    }

    /**
     * {@inheritDoc}
     */
    public function rollBack()
    {
        throw new \LogicException('Transactions are not allowed in ClickHouse');
    }

    /**
     * {@inheritDoc}
     */
    public function errorCode()
    {
        throw new \LogicException('You need to implement ClickHouseConnection::errorCode()');
    }

    /**
     * {@inheritDoc}
     */
    public function errorInfo()
    {
        throw new \LogicException('You need to implement ClickHouseConnection::errorInfo()');
    }
}
