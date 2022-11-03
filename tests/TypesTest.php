<?php
/*
 * This file is part of the FODDBALClickHouse package -- Doctrine DBAL library
 * for ClickHouse (a column-oriented DBMS for OLAP <https://clickhouse.yandex/>)
 *
 * (c) FriendsOfDoctrine <https://github.com/FriendsOfDoctrine/>.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace FOD\DBALClickHouse\Tests;

use \FOD\DBALClickHouse\Types\Type;
use FOD\DBALClickHouse\Connection;
use PHPUnit\Framework\TestCase;

/**
 * ClickHouse DBAL test class. Testing work with DBAL types
 *
 * @author Philip Shcherbanich <filippf@bk.ru>
 */
class TypesTest extends TestCase
{
    /** @var  Connection */
    protected $connection;

    protected $schemaSQLs = [];

    protected $tableName = '_test_type_table';

    public function setUp()
    {
        $this->connection = CreateConnectionTest::createConnection();
        Type::registerTypes($this->connection->getDatabasePlatform());
    }

    public function tearDown()
    {
        $this->connection->exec("DROP TABLE {$this->tableName}");
    }

    public function testInt8()
    {

        $fieldName = 'int8';

        $neededFieldSQLDeclaration = 'Int8';

        $fieldSQLDeclaration = Type::getType($fieldName)->getSQLDeclaration([], $this->connection->getDatabasePlatform());

        $this->assertEquals($fieldSQLDeclaration, $neededFieldSQLDeclaration);

        $this->createTempTable($fieldName);

        $this->assertIsNotBool(strpos($this->connection->fetchColumn("SHOW CREATE {$this->tableName}"), "field {$fieldSQLDeclaration}"));
    }

    public function testInt16()
    {

        $fieldName = 'int16';

        $neededFieldSQLDeclaration = 'Int16';

        $fieldSQLDeclaration = Type::getType($fieldName)->getSQLDeclaration([], $this->connection->getDatabasePlatform());

        $this->assertEquals($fieldSQLDeclaration, $neededFieldSQLDeclaration);

        $this->createTempTable($fieldName);

        $this->assertIsNotBool(strpos($this->connection->fetchColumn("SHOW CREATE {$this->tableName}"), "field {$fieldSQLDeclaration}"));
    }

    public function testInt32()
    {

        $fieldName = 'int32';

        $neededFieldSQLDeclaration = 'Int32';

        $fieldSQLDeclaration = Type::getType($fieldName)->getSQLDeclaration([], $this->connection->getDatabasePlatform());

        $this->assertEquals($fieldSQLDeclaration, $neededFieldSQLDeclaration);

        $this->createTempTable($fieldName);

        $this->assertIsNotBool(strpos($this->connection->fetchColumn("SHOW CREATE {$this->tableName}"), "field {$fieldSQLDeclaration}"));
    }

    public function testInt64()
    {

        $fieldName = 'int64';

        $neededFieldSQLDeclaration = 'Int64';

        $fieldSQLDeclaration = Type::getType($fieldName)->getSQLDeclaration([], $this->connection->getDatabasePlatform());

        $this->assertEquals($fieldSQLDeclaration, $neededFieldSQLDeclaration);

        $this->createTempTable($fieldName);

        $this->assertIsNotBool(strpos($this->connection->fetchColumn("SHOW CREATE {$this->tableName}"), "field {$fieldSQLDeclaration}"));
    }


    public function testUInt8()
    {

        $fieldName = 'int8';

        $neededFieldSQLDeclaration = 'UInt8';

        $options = ['unsigned' => true];

        $fieldSQLDeclaration = Type::getType($fieldName)->getSQLDeclaration($options, $this->connection->getDatabasePlatform());

        $this->assertEquals($fieldSQLDeclaration, $neededFieldSQLDeclaration);

        $this->createTempTable($fieldName, $options);

        $this->assertIsNotBool(strpos($this->connection->fetchColumn("SHOW CREATE {$this->tableName}"), "field {$fieldSQLDeclaration}"));
    }

    public function testUInt16()
    {

        $fieldName = 'int16';

        $neededFieldSQLDeclaration = 'UInt16';

        $options = ['unsigned' => true];

        $fieldSQLDeclaration = Type::getType($fieldName)->getSQLDeclaration($options, $this->connection->getDatabasePlatform());

        $this->assertEquals($fieldSQLDeclaration, $neededFieldSQLDeclaration);

        $this->createTempTable($fieldName, $options);

        $this->assertIsNotBool(strpos($this->connection->fetchColumn("SHOW CREATE {$this->tableName}"), "field {$fieldSQLDeclaration}"));
    }

    public function testUInt32()
    {

        $fieldName = 'int32';

        $neededFieldSQLDeclaration = 'UInt32';

        $options = ['unsigned' => true];

        $fieldSQLDeclaration = Type::getType($fieldName)->getSQLDeclaration($options, $this->connection->getDatabasePlatform());

        $this->assertEquals($fieldSQLDeclaration, $neededFieldSQLDeclaration);

        $this->createTempTable($fieldName, $options);

        $this->assertIsNotBool(strpos($this->connection->fetchColumn("SHOW CREATE {$this->tableName}"), "field {$fieldSQLDeclaration}"));
    }

    public function testUInt64()
    {

        $fieldName = 'int64';

        $neededFieldSQLDeclaration = 'UInt64';

        $options = ['unsigned' => true];

        $fieldSQLDeclaration = Type::getType($fieldName)->getSQLDeclaration($options, $this->connection->getDatabasePlatform());

        $this->assertEquals($fieldSQLDeclaration, $neededFieldSQLDeclaration);

        $this->createTempTable($fieldName, $options);

        $this->assertIsNotBool(strpos($this->connection->fetchColumn("SHOW CREATE {$this->tableName}"), "field {$fieldSQLDeclaration}"));
    }

    public function testFloat32()
    {

        $fieldName = 'float32';

        $neededFieldSQLDeclaration = 'Float32';

        $fieldSQLDeclaration = Type::getType($fieldName)->getSQLDeclaration([], $this->connection->getDatabasePlatform());

        $this->assertEquals($fieldSQLDeclaration, $neededFieldSQLDeclaration);

        $this->createTempTable($fieldName);

        $this->assertIsNotBool(strpos($this->connection->fetchColumn("SHOW CREATE {$this->tableName}"), "field {$fieldSQLDeclaration}"));
    }

    public function testFloat64()
    {

        $fieldName = 'float64';

        $neededFieldSQLDeclaration = 'Float64';

        $fieldSQLDeclaration = Type::getType($fieldName)->getSQLDeclaration([], $this->connection->getDatabasePlatform());

        $this->assertEquals($fieldSQLDeclaration, $neededFieldSQLDeclaration);

        $this->createTempTable($fieldName);

        $this->assertIsNotBool(strpos($this->connection->fetchColumn("SHOW CREATE {$this->tableName}"), "field {$fieldSQLDeclaration}"));
    }

    public function testDecimal()
    {

        $fieldName = 'decimal';

        $neededFieldSQLDeclaration = 'Decimal(10, 0)';

        Type::overrideType(Type::DECIMAL, 'FOD\DBALClickHouse\Types\DecimalType');

        $fieldSQLDeclaration = Type::getType($fieldName)->getSQLDeclaration([], $this->connection->getDatabasePlatform());

        $this->assertEquals($fieldSQLDeclaration, $neededFieldSQLDeclaration);

        $this->createTempTable($fieldName);

        $this->assertIsNotBool(strpos($this->connection->fetchColumn("SHOW CREATE {$this->tableName}"), "field {$fieldSQLDeclaration}"));
    }

    protected function createTempTable($type, $options = [])
    {
        $fromSchema = $this->connection->getSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;
        if ($toSchema->hasTable($this->tableName) || $fromSchema->hasTable($this->tableName)) {
            $this->connection->exec("DROP TABLE {$this->tableName}");
        }
        $newTable = $toSchema->createTable($this->tableName);

        $newTable->addColumn('field', $type, $options);
        $newTable->addOption('engine', 'Memory');

        foreach ($fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform()) as $sql) {
            $this->connection->exec($sql);
        }
    }
}
