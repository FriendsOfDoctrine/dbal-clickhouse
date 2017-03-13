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
//        TODO: implement it!
//    }

    /**
     * {@inheritDoc}
     */
    public function connect(array $params, $user = null, $password = null, array $driverOptions = [])
    {
        if ( is_null($user) ) {
            if (! isset($params['user']))
                throw new ClickHouseException('Connection parameter `user` is required');

            $user = $params['user'];
        }

        if ( is_null($password) ) {
            if (! isset($params['password']))
                throw new ClickHouseException('Connection parameter `password` is required');

            $password = $params['password'];
        }

        if (! isset($params['host'])) {
            throw new ClickHouseException('Connection parameter `host` is required');
        }

        if (! isset($params['port'])) {
            throw new ClickHouseException('Connection parameter `port` is required');
        }

        if (! isset($params['dbname'])) {
            throw new ClickHouseException('Connection parameter `dbname` is required');
        }

        return new ClickHouseConnection($user, $password, $params['host'], $params['port'], $params['dbname']);
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
        $params = $conn->getParams();
        if ( isset($params['dbname']) ) {
            return $params['dbname'];
        }

        return $conn->fetchColumn('SELECT currentDatabase() as dbname');
    }
}
