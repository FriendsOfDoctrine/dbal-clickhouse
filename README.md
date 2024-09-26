# Doctrine DBAL ClickHouse Driver

[![Latest Version on Packagist](https://img.shields.io/packagist/v/FriendsOfDoctrine/dbal-clickhouse.svg?style=flat-square)](https://packagist.org/packages/FriendsOfDoctrine/dbal-clickhouse)
[![GitHub Tests Status](https://img.shields.io/github/actions/workflow/status/FriendsOfDoctrine/dbal-clickhouse/tests.yml?branch=master&label=tests&style=flat-square)](https://github.com/FriendsOfDoctrine/dbal-clickhouse/actions/workflows/tests.yml?query=branch%3Amaster)
[![GitHub Code Style Status](https://img.shields.io/github/actions/workflow/status/FriendsOfDoctrine/dbal-clickhouse/phpstan.yml?branch=master&label=code%20style&style=flat-square)](https://github.com/FriendsOfDoctrine/dbal-clickhouse/actions/workflows/phpstan.yml?query=branch%3Amaster)
[![Total Downloads](https://img.shields.io/packagist/dt/FriendsOfDoctrine/dbal-clickhouse.svg?style=flat-square)](https://packagist.org/packages/FriendsOfDoctrine/dbal-clickhouse)
[![Licence](https://img.shields.io/packagist/l/FriendsOfDoctrine/dbal-clickhouse.svg?style=flat-square)](https://packagist.org/packages/FriendsOfDoctrine/dbal-clickhouse)

Doctrine DBAL driver for ClickHouse - an open-source column-oriented database management system by [Yandex](https://clickhouse.yandex/)

**Driver is suitable for Symfony or any other framework using Doctrine.**

* [v3](https://github.com/FriendsOfDoctrine/dbal-clickhouse/releases?q=v3.) supports Doctrine DBAL 4+ and PHP 8.1+
* [v2](https://github.com/FriendsOfDoctrine/dbal-clickhouse/releases?q=v2.) supports Doctrine DBAL 3+ and PHP 8.0+
* [v1](https://github.com/FriendsOfDoctrine/dbal-clickhouse/releases?q=v1.) supports Doctrine DBAL 2+ and PHP 7.1+

## Installation

```
composer require friendsofdoctrine/dbal-clickhouse
```

## Initialization
### Custom PHP script
```php
$connectionParams = [
    'host' => 'localhost',
    'port' => 8123,
    'user' => 'default',
    'password' => '',
    'dbname' => 'default',
    'driverClass' => 'FOD\DBALClickHouse\Driver',
    'wrapperClass' => 'FOD\DBALClickHouse\Connection',
    'driverOptions' => [
        'extremes' => false,
        'readonly' => true,
        'max_execution_time' => 30,
        'enable_http_compression' => 0,
        'https' => false,
    ],
];
$conn = \Doctrine\DBAL\DriverManager::getConnection($connectionParams, new \Doctrine\DBAL\Configuration());
```
`driverOptions` are special `smi2/phpclickhouse` client [settings](https://github.com/smi2/phpClickHouse#settings)

### Symfony
configure...
```yml
# app/config/config.yml
doctrine:
    dbal:
        connections:
            clickhouse:
                host:     localhost
                port:     8123
                user:     default
                password: ""
                dbname:   default
                driver_class: FOD\DBALClickHouse\Driver
                wrapper_class: FOD\DBALClickHouse\Connection
                options:
                    enable_http_compression: 1
                    max_execution_time: 60
                    sslCA: '/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt'
            #mysql:
            #   ...
```
...and get from the service container
```php
$conn = $this->get('doctrine.dbal.clickhouse_connection');
```

## Usage

### Create database
```php
php bin/console doctrine:database:create --connection=clickhouse --if-not-exists
```

### Create new table
```php
// ***quick start***
$fromSchema = $conn->getSchemaManager()->createSchema();
$toSchema = clone $fromSchema;


// create new table object
$newTable = $toSchema->createTable('new_table');

// add columns
$newTable->addColumn('id', 'integer', ['unsigned' => true]);
$newTable->addColumn('payload', 'string', ['notnull' => false]);
// *option 'notnull' in false mode allows you to insert NULL into the column; 
//                   in this case, the column will be represented in the ClickHouse as Nullable(String)
$newTable->addColumn('hash', 'string', ['length' => 32, 'fixed' => true]);
// *option 'fixed' sets the fixed length of a string column as specified; 
//                 if specified, the type of the column is FixedString

//set primary key
$newTable->setPrimaryKey(['id']);


// execute migration SQLs to create table in ClickHouse
$sqlArray = $fromSchema->getMigrateToSql($toSchema, $conn->getDatabasePlatform());
foreach ($sqlArray as $sql) {
    $conn->exec($sql);
}
```

```php
// ***more options (optional)***

//specify table engine
$newTable->addOption('engine', 'MergeTree');
// *if not specified -- default engine 'ReplacingMergeTree' will be used


// add Date column for partitioning
$newTable->addColumn('event_date', 'date', ['default' => 'toDate(now())']);
$newTable->addOption('eventDateColumn', 'event_date');
// *if not specified -- default Date column named EventDate will be added
$newTable->addOption('eventDateProviderColumn', 'updated_at');
// *if specified -- event date column will be added with default value toDate(updated_at); 
//    if the type of the provider column is `string`, the valid format of provider column values must be either `YYYY-MM-DD` or `YYYY-MM-DD hh:mm:ss`
//    if the type of provider column is neither `string`, nor `date`, nor `datetime`, provider column values must contain a valid UNIX Timestamp
$newTable->addOption('samplingExpression', 'intHash32(id)');
// samplingExpression -- a tuple that defines the table's primary key, and the index granularity

//specify index granularity
$newTable->addOption('indexGranularity', 4096);
// *if not specified -- default value 8192 will be used
```

### Insert
```php
// 1
$conn->exec("INSERT INTO new_table (id, payload) VALUES (1, 'dummyPayload1')");
```

```php
// 2
$conn->insert('new_table', ['id' => 2, 'payload' => 'dummyPayload2']);
// INSERT INTO new_table (id, payload) VALUES (?, ?) [2, 'dummyPayload2']
```

```php
// 3 via QueryBuilder
$qb = $conn->createQueryBuilder();

$qb
    ->insert('new_table')
    ->setValue('id', ':id')
    ->setValue('payload', ':payload')
    ->setParameter('id', 3, \PDO::PARAM_INT) // need to explicitly set param type to `integer`, because default type is `string` and ClickHouse doesn't like types mismatchings
    ->setParameter('payload', 'dummyPayload3');

$qb->execute();
```
### Select
```php
echo $conn->fetchColumn('SELECT SUM(views) FROM articles');
```

### Select via Dynamic Parameters and Prepared Statements
```php
$stmt = $conn->prepare('SELECT authorId, SUM(views) AS total_views FROM articles WHERE category_id = :categoryId AND publish_date = :publishDate GROUP BY authorId');

$stmt->bindValue('categoryId', 123);
$stmt->bindValue('publishDate', new \DateTime('2017-02-29'), 'datetime');
$stmt->execute();

while ($row = $stmt->fetch()) {
    echo $row['authorId'] . ': ' . $row['total_views'] . PHP_EOL;
}
```

### Additional types

If you want to use [Array(T) type](https://clickhouse.yandex/reference_en.html#Array(T)), register additional DBAL types in your code:
```php
// register all custom DBAL Array types
ArrayType::registerArrayTypes($conn->getDatabasePlatform());
// register one custom DBAL Array(Int8) type
Type::addType('array(int8)', 'FOD\DBALClickHouse\Types\ArrayInt8Type');
```
or register them in Symfony configuration file:
```yml
# app/config/config.yml
doctrine:
    dbal:
        connections:
        ...
        types:
            array(int8): FOD\DBALClickHouse\Types\ArrayInt8Type
            array(int16): FOD\DBALClickHouse\Types\ArrayInt16Type
            array(int32): FOD\DBALClickHouse\Types\ArrayInt32Type
            array(int64): FOD\DBALClickHouse\Types\ArrayInt64Type
            array(uint8): FOD\DBALClickHouse\Types\ArrayUInt8Type
            array(uint16): FOD\DBALClickHouse\Types\ArrayUInt16Type
            array(uint32): FOD\DBALClickHouse\Types\ArrayUInt32Type
            array(uint64): FOD\DBALClickHouse\Types\ArrayUInt64Type
            array(float32): FOD\DBALClickHouse\Types\ArrayFloat32Type
            array(float64): FOD\DBALClickHouse\Types\ArrayFloat64Type
            array(string): FOD\DBALClickHouse\Types\ArrayStringableType
            array(datetime): FOD\DBALClickHouse\Types\ArrayDateTimeType
            array(date): FOD\DBALClickHouse\Types\ArrayDateType
```

Additional type `BigIntType` helps you to store bigint values as [Int64/UInt64](https://clickhouse.yandex/reference_en.html#UInt8,%20UInt16,%20UInt32,%20UInt64,%20Int8,%20Int16,%20Int32,%20Int64) value type in ClickHouse.
You can override DBAL type in your code:
```php
Type::overrideType(Type::BIGINT, 'FOD\DBALClickHouse\Types\BigIntType');
```
or use custom mapping types in Symfony configuration:
```yml
# app/config/config.yml
doctrine:
    dbal:
        types:
            bigint:  FOD\DBALClickHouse\Types\BigIntType
            ...
```

### More information in Doctrine DBAL documentation:
* [Data Retrieval And Manipulation](http://docs.doctrine-project.org/projects/doctrine-dbal/en/latest/reference/data-retrieval-and-manipulation.html)
* [SQL Query Builder](http://docs.doctrine-project.org/projects/doctrine-dbal/en/latest/reference/query-builder.html)
* [Schema-Representation](http://docs.doctrine-project.org/projects/doctrine-dbal/en/latest/reference/schema-representation.html)
