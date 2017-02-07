<?php

/**
 * Doctrine DBAL library for ClickHouse -- an open-source column-oriented DBMS for OLAP (https://clickhouse.yandex)
 */
        
namespace Mochalygin\DoctrineDBALClickHouse;

use Doctrine\DBAL\ConnectionException;
use ClickHouseDB\Client as Smi2CHClient;

/**
 * Connection class for ClickHouse database {@link https://clickhouse.yandex/}
 * 
 * @author mochalygin <a@mochalygin.ru>
 */
class ClickHouseConnection implements \Doctrine\DBAL\Driver\Connection
{
    /**
     * @var Smi2CHClient
     */
    protected $client;

    /**
     * @var int
     */
    protected $errorCode;

    /**
     * @var string
     */
    protected $errorInfo;

    /**
     * Connection constructor
     * 
     * @param string $username      The username to use when connecting.
     * @param string $password      The password to use when connecting.
     * @param string $host
     * @param int $port
     * @param string $database
     */
    public function __construct($username = 'default', $password = '', $host = 'localhost', $port = 8123, $database = 'default')
    {
        $this->client = new Smi2CHClient([
            'host' => $host,
            'port' => $port,
            'username' => $username,
            'password' => $password,
            'settings' => ['database' => $database]
        ]);
    }

    /**
     * @param string $prepareString
     * @return Statement
     */
    public function prepare($prepareString)
    {
        return new ClickHouseStatement($this->getSmi2CHClient(), $prepareString);
    }

    /**
     * @return Statement
     */
    public function query()
    {
        $args = func_get_args();
        $sql = $args[0];
        $stmt = $this->prepare($sql);
        $stmt->execute();
        return $stmt;
    }

    /**
     * @param string $input
     * @param int $type
     * @return string
     */
    public function quote($input, $type = \PDO::PARAM_STR)
    {
        throw new \Exception('You need to implement ClickHouseConnection::quote()');
//        if (\PDO::PARAM_STR == $type)
//            return "'" . addslashes($input) . "'";
//        else if (\PDO::PARAM_INT == $type)
//            return $input;
//        else
//            throw new \Exception('Only strings and integers accepted to quoteing');
    }

    /**
     * @param string $statement
     * @return int
     */
    public function exec($statement)
    {
        $stmt = $this->query($statement);
        if (false === $stmt->execute()) {
            throw new \RuntimeException('Unable to execute query: ' . $statement);
        }
        return $stmt->rowCount();
    }

    /**
     * {@inheritDoc}
     */
    public function lastInsertId($name = null)
    {
        throw new \Exception("Unable to get last insert id in ClickHouse");
    }

    /**
     * {@inheritDoc}
     */
    public function beginTransaction()
    {
        throw new \Exception("Transactions are not allowed in ClickHouse");
    }

    /**
     * {@inheritDoc}
     */
    public function commit()
    {
        throw new \Exception("Transactions are not allowed in ClickHouse");
    }

    /**
     * {@inheritDoc}
     */
    public function rollBack()
    {
        throw new \Exception("Transactions are not allowed in ClickHouse");
    }

    /**
     * {@inheritDoc}
     */
    public function errorCode()
    {
        throw new \Exception('You need to implement ClickHouseConnection::errorCode()');
//        return $this->errorCode;
    }

    /**
     * {@inheritDoc}
     */
    public function errorInfo()
    {
        throw new \Exception('You need to implement ClickHouseConnection::errorInfo()');
//        return $this->errorInfo;
    }

    /**
     * @return Smi2CHClient
     * @throws \Exception
     */
    protected function getSmi2CHClient()
    {
        if (! $this->client)
            throw new \Exception('ClickHouse\Client was not initialized');

        return $this->client;
    }

}