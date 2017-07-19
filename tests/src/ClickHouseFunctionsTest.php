<?php
namespace ierusalim\ClickHouse;

/**
 * Generated by PHPUnit_SkeletonGenerator on 2017-07-15 at 18:53:17.
 */
class ClickHouseFunctionsTest extends \PHPUnit_Framework_TestCase
{

    /**
     * @var ClickHouseFunctions
     */
    protected $object;

    /**
     * Sets up the fixture, for example, opens a network connection.
     * This method is called before a test is executed.
     */
    protected function setUp()
    {
        $localenv = "../localenv.php";
        if (is_file($localenv)) {
            include $localenv;
        } else {
            $clickhouse_url = null;
        }
        $this->object = new ClickHouseFunctions($clickhouse_url);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::changeIfIsAlias
     */
    public function testChangeIfIsAlias()
    {
        $ch = $this->object;

        $test_type = 'testtype';
        $this->assertEquals($test_type, $ch->changeIfIsAlias($test_type));

        $canonical_types = array_merge(
            $ch->types_fix_size,
            ['String'=>0, 'FixedString'=>0, 'Array'=>0]
        );

        foreach ($ch->types_aliases as $key => $v) {
            $cano_type = $ch->changeIfIsAlias($key);
            $this->assertEquals($cano_type, $v);
            $is_canonic = isset($canonical_types[$v]);
            if (!$is_canonic) {
                echo "\nNon-canonical type $v\n";
            }
            $this->assertTrue($is_canonic);
        }
        foreach ($ch->types_fix_size as $key => $v) {
            $cano_type = $ch->changeIfIsAlias(strtoupper($key));
            $this->assertEquals($cano_type, $key);
        }
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::addTypeAlias
     */
    public function testAddTypeAlias()
    {
        $ch = $this->object;

        $this->assertFalse($ch->addTypeAlias('yyy', 'xxx'));

        $for_type = 'sTrInG';
        $alias = 'Symbolic';

        $this->assertEquals("String", $ch->addTypeAlias($for_type, $alias));
        $this->assertArrayHasKey(strtolower($alias), $ch->types_aliases);

        $this->assertEquals("String", $ch->changeIfIsAlias($alias));

        $for_type = 'Uint32';
        $alias = 'UNSIGNED INTEGER';

        $this->assertEquals("UInt32", $ch->addTypeAlias($for_type, $alias));
        $this->assertArrayHasKey(strtolower($alias), $ch->types_aliases);

        $this->assertEquals("UInt32", $ch->changeIfIsAlias($alias));
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::delTypeAlias
     */
    public function testDelTypeAlias()
    {
        $ch = $this->object;

        $this->assertFalse($ch->delTypeAlias('yyy'));

        $alias = 'double';
        $this->assertArrayHasKey($alias, $ch->types_aliases);
        $canon = $ch->types_aliases[$alias];
        $this->assertEquals($canon, $ch->changeIfIsAlias($alias));
        $this->assertTrue($ch->delTypeAlias($alias));
        $this->assertArrayNotHasKey($alias, $ch->types_aliases);
        $this->assertEquals($alias, $ch->changeIfIsAlias($alias));
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::getCurrentDatabase
     */
    public function testGetCurrentDatabase()
    {
        $ch = $this->object;

        $ch->setSession();
        $session_id_1 = $ch->setSession();
        $session_id_2 = $ch->getSession();

        $db_1 = 'default';
        $db_2 = 'system';

        $ans = $ch->setCurrentDatabase($db_1, $session_id_1);
        $this->assertTrue($ans);
        $ans = $ch->setCurrentDatabase($db_2, $session_id_2);
        $this->assertTrue($ans);

        $ch->setSession($session_id_1);
        $db_name = $ch->getCurrentDatabase();
        $this->assertEquals($db_name, $db_1);

        $ch->setSession($session_id_2);
        $db_name = $ch->getCurrentDatabase();
        $this->assertEquals($db_name, $db_2);

        $db_name = $ch->getCurrentDatabase($session_id_1);
        $this->assertEquals($db_name, $db_1);
        $db_name = $ch->getCurrentDatabase($session_id_2);
        $this->assertEquals($db_name, $db_2);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::setCurrentDatabase
     */
    public function testSetCurrentDatabase()
    {
        $ch = $this->object;

        $ch->setSession();
        $session_id_1 = $ch->setSession();
        $session_id_2 = $ch->getSession();

        $db_1 = 'default';
        $db_2 = 'system';

        $ans = $ch->setCurrentDatabase($db_1, $session_id_1);
        $this->assertTrue($ans);
        $ans = $ch->setCurrentDatabase($db_2, $session_id_2);
        $this->assertTrue($ans);

        $db_name = $ch->getCurrentDatabase($session_id_1);
        $this->assertEquals($db_name, $db_1);
        $db_name = $ch->getCurrentDatabase($session_id_2);
        $this->assertEquals($db_name, $db_2);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::getDatabasesList
     */
    public function testGetDatabasesList()
    {
        $ch = $this->object;
        $db_arr = $ch->queryColumnTab("SHOW DATABASES");
        $db_2_arr = $ch->getDatabasesList();
        $this->assertEquals($db_arr, $db_2_arr);
        $this->assertTrue(\array_search('system', $db_arr) !== false);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::getTablesList
     */
    public function testGetTablesList()
    {
        $ch = $this->object;
        $sys_tbl_arr = $ch->getTablesList('system');
        $this->assertTrue(\count($sys_tbl_arr) > 10);
        $this->assertTrue(\array_search('databases', $sys_tbl_arr) !== false);

        $data_tbl_arr = $ch->getTablesList('system', 'd%');
        $this->assertTrue(count($data_tbl_arr) > 0);
        $this->assertTrue(array_search('databases', $data_tbl_arr) !== false);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::getProcessList
     */
    public function testGetProcessList()
    {
        $ch = $this->object;
        $proc_arr = $ch->getProcessList();
        $this->assertTrue(\is_array($proc_arr));
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::getTableRowSize
     */
    public function testGetTableRowSize()
    {
        $ch = $this->object;
        $sum_arr = $ch->getTableRowSize('system.processes');
        \extract($sum_arr);
        $this->assertTrue($fixed_bytes > 10);
        $this->assertTrue($dynamic_fields > 5);

        //exceptions
        $sum_arr = $ch->getTableRowSize('notfoundtable');
        $this->assertTrue(is_string($sum_arr));

        $sum_arr = $ch->getTableRowSize(['badtype']);
        $this->assertTrue(is_string($sum_arr));
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::countRowFixedSize
     */
    public function testCountRowFixedSize()
    {
        $ch = $this->object;
        $dyna = 0;
        $sum = $ch->countRowFixedSize(['int16', 'int32', 'int64', 'String'], $dyna);
        $this->assertEquals(14, $sum);
        $this->assertEquals(1, $dyna);

        // exceptions
        $sum = $ch->countRowFixedSize([]);
        $this->assertTrue(is_string($sum));

        $sum = $ch->countRowFixedSize(['int16','badtype']);
        $this->assertTrue(is_string($sum));
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::getTableFields
     */
    public function testGetTableFields()
    {
        $ch = $this->object;
        $desc_tbl_arr = $ch->getTableFields('system.databases');
        $this->assertArrayHasKey('name', $desc_tbl_arr);
        $this->assertArrayHasKey('engine', $desc_tbl_arr);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::getSystemSettings
     */
    public function testGetSystemSettings()
    {
        $ch = $this->object;
        $arr = $ch->getSystemSettings();
        $this->assertTrue(count($arr) > 10);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::getVersion
     */
    public function testGetVersion()
    {
        $ch = $this->object;
        $version = $ch->getVersion();
        $this->assertTrue(strpos($version, '.') > 0);
        echo "Version of ClickHouse server: $version\n";
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::getUptime
     */
    public function testGetUptime()
    {
        $ch = $this->object;
        $uptime_sec = $ch->getUptime();
        $this->assertTrue(is_numeric($uptime_sec));
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::getNumbers
     */
    public function testGetNumbers()
    {
        $ch = $this->object;
        $arr = $ch->getNumbers(100);
        $this->assertEquals(100, count($arr));
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::sqlTableQuick
     */
    public function testSqlTableQuick()
    {
        $ch = $this->object;

        $sql = $ch->sqlTableQuick('temp', [
            'id' => 'Int16',
            'dt' => ['Date','now()']
        ]);

        $this->assertEquals(
            'CREATE TABLE IF NOT EXISTS temp (' .
            ' id Int16, dt DEFAULT toDate(now()) ' .
            ') ENGINE = MergeTree(dt, (id, dt), 8192)',
        $sql);

        $sql = $ch->sqlTableQuick('temp', [
            'id' => 'Int16',
            'dt' => ['Date','now()'],
            'ver' => 'int'
        ]);

        $this->assertEquals(
            'CREATE TABLE IF NOT EXISTS temp (' .
            ' id Int16, dt DEFAULT toDate(now()), ver Int32 ' .
            ') ENGINE = ReplacingMergeTree(dt, (id, dt), 8192 ,ver)',
        $sql);

        $this->setExpectedException("\Exception");
        // No date field exception
        $sql = $ch->sqlTableQuick('temp', [
            'id' => 'Int16',
            'dt' => ['Int32','toInt32(now())']
        ]);
    }
    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::sqlTableQuick
     */
    public function testSqlTableQuickException()
    {
        $ch = $this->object;
        $this->setExpectedException("\Exception");

        // No primary field
        $sql = $ch->sqlTableQuick('temp', [
            'dt' => ['Int32','toInt32(now())']
        ]);
        $this->assertFalse("This line will not be executed");
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::createTableQuick
     */
    public function testCreateTableQuick()
    {
        $ch = $this->object;

        $table = 'tempcreate';

        $ans = $ch->createTableQuick($table, [
            'id' => 'Int16',
            'dt' => ['Date', 'now()']
        ], 2);
        $this->assertFalse($ans);

        $fields_arr = $ch->getTableFields($table);

        $this->assertEquals(['id'=>'Int16', 'dt'=>'Date'], $fields_arr);

        $ans = $ch->createTableQuick("broken'\nname", [
            'id' => 'Int16',
            'dt' => ['Date', 'now()']
        ], 2);
        $this->assertTrue(is_string($ans));
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::getTableInfo
     */
    public function testGetTableInfo()
    {
        $ch = $this->object;

        $tbl = "system.columns";
        $arr = $ch->getTableInfo($tbl);
        $this->assertArrayHasKey('table_name', $arr);
        $this->assertEquals($tbl, $arr['table_name']);
        $arr = $ch->getTableInfo("notfoundthistable");
        $this->assertFalse(\is_array($arr));
        //broken request
        $arr = $ch->getTableInfo("notfound'\nthistable");
        $this->assertFalse(\is_array($arr));
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::parseFieldsArr
     */
    public function testParseFieldsArr()
    {
        $ch = $this->object;

        $arr = $ch->parseFieldsArr(['id' => 'int']);
        $this->assertEquals(['id' => [
            'create' => 'id Int32',
            'type_full' => 'Int32',
            'type_name' => 'Int32',
            'type_src' => 'int',
            'default' => '',
            'bytes' => 4
        ]], $arr);

        $arr = $ch->parseFieldsArr(['id' => 'int16 123']);
        $this->assertEquals(['id' => [
            'create' => 'id DEFAULT toInt16(123)',
            'type_full' => 'Int16',
            'type_name' => 'Int16',
            'type_src' => 'int16',
            'default' => 'toInt16(123)',
            'bytes' => 2
        ]], $arr);

        $arr = $ch->parseFieldsArr(['id' => 'Int16 DEFAULT 123']);
        $this->assertEquals(['id' => [
            'create' => 'id Int16 DEFAULT 123',
            'type_full' => 'Int16',
            'type_name' => 'Int16',
            'type_src' => 'Int16',
            'default' => '123',
            'bytes' => 2
        ]], $arr);

        $arr = $ch->parseFieldsArr(['id' => 'FixedString(55) x']);
        $this->assertEquals(['id' => [
            'create' => "id DEFAULT toFixedString('x',55)",
            'type_full' => 'FixedString(55)',
            'type_name' => 'FixedString',
            'type_src' => 'FixedString(55)',
            'default' => "toFixedString('x',55)",
            'bytes' => 55
        ]], $arr);

        $arr = $ch->parseFieldsArr(['id' => ['FixedString(55)','x']]);
        $this->assertEquals(['id' => [
            'create' => "id DEFAULT toFixedString('x',55)",
            'type_full' => 'FixedString(55)',
            'type_name' => 'FixedString',
            'type_src' => 'FixedString(55)',
            'default' => "toFixedString('x',55)",
            'bytes' => 55
        ]], $arr);

        $arr = $ch->parseFieldsArr(['id' => ["Enum8('google'=1,'bing'=2)",'google']]);
        $this->assertEquals(['id' => [
            'create' => "id Enum8('google'=1,'bing'=2) DEFAULT 'google'",
            'type_full' => "Enum8('google'=1,'bing'=2)",
            'type_name' => 'Enum8',
            'type_src' => "Enum8('google'=1,'bing'=2)",
            'default' => "'google'",
            'bytes' => 1
        ]], $arr);

        $this->setExpectedException("\Exception");
        $arr = $ch->parseFieldsArr(['id' => 'UnknownType']);
        $this->assertFalse("This line will not be executed");
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::parseType
     */
    public function testParseType()
    {
        $ch = $this->object;
        $name = $to_conv = '';

        $type = "Unknown()";
        $this->assertFalse($ch->parseType($type, $name, $to_conv));
        $this->assertEquals("Unknown", $name);

        $type = "int";
        $this->assertEquals(4, $ch->parseType($type, $name, $to_conv));
        $this->assertEquals("Int32", $name);

        $type = "enum('google' = 1, 'bing' = 2)";
        $this->assertEquals(1, $ch->parseType($type, $name, $to_conv));
        $this->assertEquals('Enum8', $name);
        $this->assertFalse($to_conv);

        $type = "Array(1,2,3)";
        $this->assertEquals(0, $ch->parseType($type, $name, $to_conv));
        $this->assertEquals('Array', $name);
        $this->assertFalse($to_conv);

        $type = "FixedString(123)";
        $this->assertEquals(123, $ch->parseType($type, $name, $to_conv));
        $this->assertEquals('FixedString', $name);
        $this->assertEquals(['toFixedString(', ',123)'], $to_conv);

        $type = "FixedString(x)";
        $this->assertFalse($ch->parseType($type, $name, $to_conv));
        $this->assertEquals('FixedString', $name);

        foreach (\array_merge(['String'], \array_keys($ch->types_fix_size)) as $type) {
            $bytes = isset($ch->types_fix_size[$type]) ? $ch->types_fix_size[$type] : 0;
            $this->assertEquals($bytes, $ch->parseType($type, $name, $to_conv));
            $this->assertEquals($type, $name);
            if ($to_conv) {
                $this->assertEquals(['to' . $type . '(', ')'], $to_conv);
            } else {
                $this->assertEquals('Enum', substr($type, 0, 4));
            }
        }
    }
}
