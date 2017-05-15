<?php
/*
 * This file is part of the FODDBALClickHouse package -- Doctrine DBAL library
 * for ClickHouse (a column-oriented DBMS for OLAP <https://clickhouse.yandex/>)
 *
 * (c) FriendsOfDoctrine <https://github.com/FriendsOfDoctrine/>.
 *
 * For the full copyright and license inflormation, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace FOD\DBALClickHouse\Tests;

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Types\Type;
use FOD\DBALClickHouse\Connection;
use PHPUnit\Framework\TestCase;

/**
 * ClickHouse DBAL test class. Testing work with DBAL types
 *
 * @package FOD\DBALClickHouse\Tests
 * @author Nikolay Mitrofanov <mitrofanovnk@gmail.com>
 */
class DbalTypeTest extends TestCase
{
    /** @var  Connection */
    protected $connection;

    public function setUp()
    {
        $this->connection = CreateConnectionTest::createConnection();

        $fromSchema = $this->connection->getSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_dbal_type_table');

        $newTable->addColumn('typeArray', Type::TARRAY);
        $newTable->addColumn('typeSimpleArray', Type::SIMPLE_ARRAY);
        $newTable->addColumn('typeJsonArray', Type::JSON_ARRAY);
        $newTable->addColumn('typeBigInt', Type::BIGINT);
        $newTable->addColumn('typeBoolean', Type::BOOLEAN);
        $newTable->addColumn('typeDateTime', Type::DATETIME);
        $newTable->addColumn('typeDate', Type::DATE);
        $newTable->addColumn('typeDecimal', Type::DECIMAL);
        $newTable->addColumn('typeInteger', Type::INTEGER);
        $newTable->addColumn('typeObject', Type::OBJECT);
        $newTable->addColumn('typeSmallInt', Type::SMALLINT);
        $newTable->addColumn('typeString', Type::STRING);
        $newTable->addColumn('typeText', Type::TEXT);
        $newTable->addColumn('typeBinary', Type::BINARY);
        $newTable->addColumn('typeBlob', Type::BLOB);
        $newTable->addColumn('typeFloat', Type::FLOAT);
        $newTable->addColumn('typeGUID', Type::GUID);
        $newTable->addOption('engine', 'Memory');

        $generatedSQL = implode(';', $fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform()));

        $this->connection->exec($generatedSQL);
    }

    public function tearDown()
    {
        $this->connection->exec('DROP TABLE test_dbal_type_table');
    }

    public function testTypeArray()
    {
        $this->connection->insert('test_dbal_type_table', ['typeArray' => ['v1' => 123, 'v2' => 234]], ['typeArray' => Type::TARRAY]);
        $this->assertEquals(serialize(['v1' => 123, 'v2' => 234]), $this->connection->fetchColumn('SELECT typeArray FROM test_dbal_type_table'));
    }

    public function testTypeSimpleArray()
    {
        $this->connection->insert('test_dbal_type_table', ['typeSimpleArray' => [123, 234]], ['typeSimpleArray' => Type::SIMPLE_ARRAY]);
        $this->assertEquals(implode(',', [123, 234]), $this->connection->fetchColumn('SELECT typeSimpleArray FROM test_dbal_type_table'));
    }

    public function testTypeJsonArray()
    {
        $this->connection->insert('test_dbal_type_table', ['typeJsonArray' => [123, 'foo' => 'bar']], ['typeJsonArray' => Type::JSON_ARRAY]);
        $this->assertEquals(json_encode([123, 'foo' => 'bar']), $this->connection->fetchColumn('SELECT typeJsonArray FROM test_dbal_type_table'));
    }

    public function testTypeBigInt()
    {
        $this->connection->insert('test_dbal_type_table', ['typeBigInt' => 123123123123], ['typeBigInt' => Type::BIGINT]);
        $this->assertEquals(123123123123, $this->connection->fetchColumn('SELECT typeBigInt FROM test_dbal_type_table'));
    }

    public function testTypeBoolean()
    {
        $this->connection->insert('test_dbal_type_table', ['typeBoolean' => true], ['typeBoolean' => Type::BOOLEAN]);
        $this->assertEquals(1, $this->connection->fetchColumn('SELECT typeBoolean FROM test_dbal_type_table'));
    }

    public function testTypeDatetime()
    {
        $this->connection->insert('test_dbal_type_table', ['typeDateTime' => new \DateTime('2000-05-05')], ['typeDateTime' => Type::DATETIME]);
        $this->assertEquals('2000-05-05 00:00:00', $this->connection->fetchColumn('SELECT typeDateTime FROM test_dbal_type_table'));
    }

    public function testTypeDate()
    {
        $this->connection->insert('test_dbal_type_table', ['typeDate' => new \DateTime('2000-05-05')], ['typeDate' => Type::DATE]);
        $this->assertEquals('2000-05-05', $this->connection->fetchColumn('SELECT typeDate FROM test_dbal_type_table'));
    }

    public function testTypeDecimalFail()
    {
        // :TODO Wtf?
        $this->expectException(DBALException::class);
        $this->connection->insert('test_dbal_type_table', ['typeDecimal' => '142.15'], ['typeDecimal' => Type::DECIMAL]);
        $this->assertEquals(142.15, $this->connection->fetchColumn('SELECT typeDecimal FROM test_dbal_type_table'));
    }

    public function testTypeDecimal()
    {
        $this->connection->insert('test_dbal_type_table', ['typeDecimal' => 142.15]);
        $this->assertEquals(142.15, $this->connection->fetchColumn('SELECT typeDecimal FROM test_dbal_type_table'));
    }

    public function testTypeInteger()
    {
        $this->connection->insert('test_dbal_type_table', ['typeInteger' => 142], ['typeInteger' => Type::INTEGER]);
        $this->assertEquals(142, $this->connection->fetchColumn('SELECT typeInteger FROM test_dbal_type_table'));
    }

    public function testTypeObject()
    {
        $this->connection->insert('test_dbal_type_table', ['typeObject' => (object)['foo' => 'bar']], ['typeObject' => Type::OBJECT]);
        $this->assertEquals(serialize((object)['foo' => 'bar']), $this->connection->fetchColumn('SELECT typeObject FROM test_dbal_type_table'));
    }

    public function testTypeSmallInt()
    {
        $this->connection->insert('test_dbal_type_table', ['typeSmallInt' => 14], ['typeSmallInt' => Type::SMALLINT]);
        $this->assertEquals(14, $this->connection->fetchColumn('SELECT typeSmallInt FROM test_dbal_type_table'));
    }

    public function testTypeString()
    {
        $this->connection->insert('test_dbal_type_table', ['typeString' => 'foo bar baz'], ['typeString' => Type::STRING]);
        $this->assertEquals('foo bar baz', $this->connection->fetchColumn('SELECT typeString FROM test_dbal_type_table'));
    }

    public function testTypeText()
    {
        $this->connection->insert('test_dbal_type_table', ['typeText' => 'foo bar baz'], ['typeText' => Type::TEXT]);
        $this->assertEquals('foo bar baz', $this->connection->fetchColumn('SELECT typeText FROM test_dbal_type_table'));
    }

    public function testTypeBinary()
    {
        $this->connection->insert('test_dbal_type_table', ['typeBinary' => 1], ['typeBinary' => Type::BINARY]);
        $this->assertEquals(1, $this->connection->fetchColumn('SELECT typeBinary FROM test_dbal_type_table'));
    }

    public function testTypeBlob()
    {
        $val = md5(time());
        $this->connection->insert('test_dbal_type_table', ['typeBlob' => $val], ['typeBlob' => Type::BLOB]);
        $this->assertEquals($val, $this->connection->fetchColumn('SELECT typeBlob FROM test_dbal_type_table'));
    }

    public function testTypeGUID()
    {
        $val = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11';
        $this->connection->insert('test_dbal_type_table', ['typeGUID' => $val], ['typeGUID' => Type::GUID]);
        $this->assertEquals($val, $this->connection->fetchColumn('SELECT typeGUID FROM test_dbal_type_table'));
    }
}