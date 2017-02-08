# doctrine-dbal-clickhouse

Doctrine DBAL driver for ClickHouse database (https://clickhouse.yandex/)
##Under construction...
```
$config = new \Doctrine\DBAL\Configuration();

$connectionParams = array(
    'dbname' => 'default',
    'user' => 'default',
    'password' => '',
    'host' => 'localhost',
    'port' => 8123,
    'driverClass' => 'Mochalygin\DoctrineDBALClickHouse\Driver',
    'wrapperClass' => 'Mochalygin\DoctrineDBALClickHouse\Connection'
);
$conn = \Doctrine\DBAL\DriverManager::getConnection($connectionParams, $config);


$stmt = $conn->query('SELECT SUM(views) FROM articles');
$stmt->fetchAll();
```
