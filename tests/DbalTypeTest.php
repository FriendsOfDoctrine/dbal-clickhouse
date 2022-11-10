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

namespace FOD\DBALClickHouse\Tests;

use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use FOD\DBALClickHouse\Connection;
use PHPUnit\Framework\TestCase;

/**
 * ClickHouse DBAL test class. Testing work with DBAL types
 *
 * @author Nikolay Mitrofanov <mitrofanovnk@gmail.com>
 */
class DbalTypeTest extends TestCase
{
    private Connection$connection;

    private array $schemaSQLs = [];

    public function setUp(): void
    {
        $this->connection = CreateConnectionTest::createConnection();

        $fromSchema = $this->connection->createSchemaManager()->introspectSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_dbal_type_table');

        $newTable->addColumn('typeArray', Types::ARRAY);
        $newTable->addColumn('typeSimpleArray', Types::SIMPLE_ARRAY);
        $newTable->addColumn('typeJsonArray', Types::JSON);
        $newTable->addColumn('typeBigInt', Types::BIGINT);
        $newTable->addColumn('typeBoolean', Types::BOOLEAN);
        $newTable->addColumn('typeDateTime', Types::DATETIME_MUTABLE);
        $newTable->addColumn('typeDateTimeTZ', Types::DATETIMETZ_MUTABLE);
        $newTable->addColumn('typeDate', Types::DATE_MUTABLE);
        $newTable->addColumn('typeTime', Types::TIME_MUTABLE);
        $newTable->addColumn('typeDecimal', Types::DECIMAL);
        $newTable->addColumn('typeInteger', Types::INTEGER);
        $newTable->addColumn('typeObject', Types::OBJECT);
        $newTable->addColumn('typeSmallInt', Types::SMALLINT);
        $newTable->addColumn('typeString', Types::STRING);
        $newTable->addColumn('typeText', Types::TEXT);
        $newTable->addColumn('typeBinary', Types::BINARY);
        $newTable->addColumn('typeBlob', Types::BLOB);
        $newTable->addColumn('typeFloat', Types::FLOAT);
        $newTable->addColumn('typeGUID', Types::GUID);
        $newTable->addOption('engine', 'Memory');

        $this->schemaSQLs = $fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform());

        foreach ($this->schemaSQLs as $sql) {
            $this->connection->executeStatement($sql);
        }
    }

    public function tearDown(): void
    {
        $this->connection->executeStatement('DROP TABLE test_dbal_type_table');
    }

    public function testCreateSchema(): void
    {
        $this->assertEquals('CREATE TABLE test_dbal_type_table (typeArray String, typeSimpleArray String, typeJsonArray String, typeBigInt String, typeBoolean UInt8, typeDateTime DateTime, typeDateTimeTZ DateTime, typeDate Date, typeTime String, typeDecimal String, typeInteger Int32, typeObject String, typeSmallInt Int16, typeString String, typeText String, typeBinary String, typeBlob String, typeFloat Float64, typeGUID FixedString(36)) ENGINE = Memory', implode(';', $this->schemaSQLs));
    }

    public function testTypeArray(): void
    {
        $this->connection->insert('test_dbal_type_table', ['typeArray' => ['v1' => 123, 'v2' => 234]], ['typeArray' => Types::ARRAY]);

        $this->assertEquals(serialize(['v1' => 123, 'v2' => 234]), $this->connection->fetchOne('SELECT typeArray FROM test_dbal_type_table'));
    }

    public function testTypeSimpleArray(): void
    {
        $this->connection->insert('test_dbal_type_table', ['typeSimpleArray' => [123, 234]], ['typeSimpleArray' => Types::SIMPLE_ARRAY]);

        $this->assertEquals(implode(',', [123, 234]), $this->connection->fetchOne('SELECT typeSimpleArray FROM test_dbal_type_table'));
    }

    public function testTypeJsonArray(): void
    {
        $this->connection->insert('test_dbal_type_table', ['typeJsonArray' => [123, 'foo' => 'bar']], ['typeJsonArray' => Types::JSON]);

        $this->assertEquals(json_encode([123, 'foo' => 'bar']), $this->connection->fetchOne('SELECT typeJsonArray FROM test_dbal_type_table'));
    }

    public function testTypeBigInt(): void
    {
        $this->connection->insert('test_dbal_type_table', ['typeBigInt' => 123123123123], ['typeBigInt' => Types::BIGINT]);

        $this->assertEquals('123123123123', $this->connection->fetchOne('SELECT typeBigInt FROM test_dbal_type_table'));
    }

    public function testTypeBigIntReload(): void
    {
        Type::overrideType(Types::BIGINT, 'FOD\DBALClickHouse\Types\BigIntType');

        $this->connection = CreateConnectionTest::createConnection();

        $fromSchema = $this->connection->createSchemaManager()->introspectSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_dbal_type_bigint_table');

        $newTable->addColumn('typeBigInt', Types::BIGINT);
        $newTable->addOption('engine', 'Memory');

        foreach ($fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform()) as $sql) {
            $this->connection->executeStatement($sql);

            $this->assertEquals('CREATE TABLE test_dbal_type_bigint_table (typeBigInt Int64) ENGINE = Memory', $sql);
        }

        $this->connection->insert('test_dbal_type_bigint_table', ['typeBigInt' => 123123123123], ['typeBigInt' => Types::BIGINT]);

        $this->assertEquals(123123123123, $this->connection->fetchOne('SELECT typeBigInt FROM test_dbal_type_bigint_table'));

        $this->connection->executeStatement('DROP TABLE test_dbal_type_bigint_table');
    }

    public function testTypeBoolean(): void
    {
        $this->connection->insert('test_dbal_type_table', ['typeBoolean' => true], ['typeBoolean' => Types::BOOLEAN]);

        $this->assertEquals(1, $this->connection->fetchOne('SELECT typeBoolean FROM test_dbal_type_table'));
    }

    public function testTypeDatetime(): void
    {
        $this->connection->insert('test_dbal_type_table', ['typeDateTime' => new \DateTime('2000-05-05')], ['typeDateTime' => Types::DATETIME_MUTABLE]);

        $this->assertEquals('2000-05-05 00:00:00', $this->connection->fetchOne('SELECT typeDateTime FROM test_dbal_type_table'));
    }

    public function testTypeDatetimeTZ(): void
    {
        $this->connection->insert('test_dbal_type_table', ['typeDateTimeTZ' => new \DateTime('2000-05-05')], ['typeDateTimeTZ' => Types::DATETIMETZ_MUTABLE]);

        $this->assertEquals('2000-05-05 00:00:00', $this->connection->fetchOne('SELECT typeDateTimeTZ FROM test_dbal_type_table'));
    }

    public function testTypeDate(): void
    {
        $this->connection->insert('test_dbal_type_table', ['typeDate' => new \DateTime('2000-05-05')], ['typeDate' => Types::DATE_MUTABLE]);

        $this->assertEquals('2000-05-05', $this->connection->fetchOne('SELECT typeDate FROM test_dbal_type_table'));
    }

    public function testTypeTime(): void
    {
        $this->connection->insert('test_dbal_type_table', ['typeTime' => new \DateTime('13:41:18')], ['typeTime' => Types::TIME_MUTABLE]);

        $this->assertEquals('13:41:18', $this->connection->fetchOne('SELECT typeTime FROM test_dbal_type_table'));
    }

    public function testTypeDecimalFail(): void
    {
        $this->connection->insert('test_dbal_type_table', ['typeDecimal' => 142.15], ['typeDecimal' => Types::DECIMAL]);

        $this->assertEquals('142.15', $this->connection->fetchOne('SELECT typeDecimal FROM test_dbal_type_table'));
    }

    public function testTypeInteger(): void
    {
        $this->connection->insert('test_dbal_type_table', ['typeInteger' => 142], ['typeInteger' => Types::INTEGER]);

        $this->assertEquals(142, $this->connection->fetchOne('SELECT typeInteger FROM test_dbal_type_table'));
    }

    public function testTypeObject(): void
    {
        $this->connection->insert('test_dbal_type_table', ['typeObject' => (object)['foo' => 'bar']], ['typeObject' => Types::OBJECT]);

        $this->assertEquals(serialize((object)['foo' => 'bar']), $this->connection->fetchOne('SELECT typeObject FROM test_dbal_type_table'));
    }

    public function testTypeSmallInt(): void
    {
        $this->connection->insert('test_dbal_type_table', ['typeSmallInt' => 14], ['typeSmallInt' => Types::SMALLINT]);

        $this->assertEquals(14, $this->connection->fetchOne('SELECT typeSmallInt FROM test_dbal_type_table'));
    }

    public function testTypeString(): void
    {
        $this->connection->insert('test_dbal_type_table', ['typeString' => 'foo bar baz'], ['typeString' => Types::STRING]);

        $this->assertEquals('foo bar baz', $this->connection->fetchOne('SELECT typeString FROM test_dbal_type_table'));
    }

    public function testTypeText(): void
    {
        $this->connection->insert('test_dbal_type_table', ['typeText' => 'foo bar baz'], ['typeText' => Types::TEXT]);

        $this->assertEquals('foo bar baz', $this->connection->fetchOne('SELECT typeText FROM test_dbal_type_table'));
    }

    public function testTypeBinary(): void
    {
        $this->connection->insert('test_dbal_type_table', ['typeBinary' => 1], ['typeBinary' => Types::BINARY]);

        $this->assertEquals(1, $this->connection->fetchOne('SELECT typeBinary FROM test_dbal_type_table'));
    }

    public function testTypeBlob(): void
    {
        $val = md5((string) time());

        $this->connection->insert('test_dbal_type_table', ['typeBlob' => $val], ['typeBlob' => Types::BLOB]);

        $this->assertEquals($val, $this->connection->fetchOne('SELECT typeBlob FROM test_dbal_type_table'));
    }

    public function testTypeGUID(): void
    {
        $val = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11';

        $this->connection->insert('test_dbal_type_table', ['typeGUID' => $val], ['typeGUID' => Types::GUID]);

        $this->assertEquals($val, $this->connection->fetchOne('SELECT typeGUID FROM test_dbal_type_table'));
    }
}
