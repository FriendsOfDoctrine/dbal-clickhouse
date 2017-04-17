# Doctrine FODDBALClickHouse Driver

Doctrine DBAL driver for ClickHouse -- an open-source column-oriented database management system by Yandex (https://clickhouse.yandex/)

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
    'wrapperClass' => 'FOD\DBALClickHouse\Connection'
];
$conn = \Doctrine\DBAL\DriverManager::getConnection($connectionParams, new \Doctrine\DBAL\Configuration());
```

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
            #mysql:
            #   ...
```
...and get from the service container
```php
$conn = $this->get('doctrine.dbal.clickhouse_connection');
```


## Usage

### Create new table
```php
// ***quick start***
$fromSchema = $conn->getSchemaManager()->createSchema();
$toSchema = clone $fromSchema;


// create new table object
$newTable = $toSchema->createTable('new_table');

// add columns
$newTable->addColumn('id', 'integer', ['unsigned' => true]);
$newTable->addColumn('payload', 'string');

//set primary key
$newTable->setPrimaryKey(['id']);


// execute migration SQLs to create table in ClickHouse
$sqlArray = $fromSchema->getMigrateToSql($toSchema, $conn->getDatabasePlatform());
foreach ($sqlArray as $sql) {
    $conn->exec($sql);
}
```

```php
// ***more options***

//specify table engine
$newTable->addOption('engine', 'MergeTree');
// *if not specified -- default engine 'ReplacingMergeTree' will be used


// add Date column for partitioning
$newTable->addColumn('event_date', 'date', ['default' => 'toDate(now())']);
$newTable->addOption('eventDateColumn', 'event_date');
// *if not specified -- default Date column named EventDate will be added


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

### Update
*ClickHouse has no classical updates. Will add ersatz-updates for ReplacingMergeTree and CollapcingMergeTree engines later*

### More information in Doctrine DBAL documentation:
* [Data Retrieval And Manipulation](http://docs.doctrine-project.org/projects/doctrine-dbal/en/latest/reference/data-retrieval-and-manipulation.html)
* [SQL Query Builder](http://docs.doctrine-project.org/projects/doctrine-dbal/en/latest/reference/query-builder.html)
* [Schema-Representation](http://docs.doctrine-project.org/projects/doctrine-dbal/en/latest/reference/schema-representation.html)
