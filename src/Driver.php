<?php

/**
 * Doctrine DBAL library for ClickHouse -- an open-source column-oriented DBMS for OLAP (https://clickhouse.yandex)
 */

namespace Mochalygin\DoctrineDBALClickHouse;

/**
 * DBAL Driver for ClickHouse database {@link https://clickhouse.yandex/}
 *
 * @author mochalygin <a@mochalygin.ru>
 */
class Driver implements \Doctrine\DBAL\Driver/*, \Doctrine\DBAL\Driver\ExceptionConverterDriver*/
{
    
    
//    public function convertException($message, \Doctrine\DBAL\Driver\DriverException $exception)
//    {
//        echo $exception->getMessage();
//        exit;
//    }
    
    /**
     * {@inheritDoc}
     */
    public function connect(array $params, $username = null, $password = null, array $driverOptions = []) 
    {
        //TODO проверка на существование индексов в массиве
        return new ClickHouseConnection($params['user'], $params['password'], $params['host'], $params['port'], $params['dbname']);
    }

    /**
     * {@inheritDoc}
     */
    public function getDatabasePlatform()
    {
        return new ClickHousePlatform;
    }

    /**
     * {@inheritDoc}
     */
    public function getSchemaManager(\Doctrine\DBAL\Connection $conn)
    {
        return new ClickHouseSchemaManager($conn);
    }

    /**
     * {@inheritDoc}
     */
    public function getName()
    {
        return 'clickhouse';
    }

    /**
     * {@inheritDoc}
     */
    public function getDatabase(\Doctrine\DBAL\Connection $conn)
    {
        return $conn->fetchColumn('SELECT currentDatabase() as dbname');
    }
}
