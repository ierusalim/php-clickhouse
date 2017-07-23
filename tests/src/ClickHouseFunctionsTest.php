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
        $db_arr = $ch->queryStrings("SHOW DATABASES");
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
        $fixed_bytes = $ch->countRowFixedSize(['int16', 'int32', 'int64', 'String']);
        \extract($fixed_bytes); // fixed_bytes, dynamic_fields
        $this->assertEquals(14, $fixed_bytes);
        $this->assertEquals(1, $dynamic_fields);

        // exceptions
        $fixed_bytes = $ch->countRowFixedSize([]);
        $this->assertTrue(is_string($fixed_bytes));

        $fixed_bytes = $ch->countRowFixedSize(['int16','badtype']);
        $this->assertTrue(is_string($fixed_bytes));
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

        $sql = implode($ch->sqlTableQuick('temp', [
            'id' => 'Int16',
            'dt' => ['Date','now()']
        ]));

        $this->assertEquals(
            'CREATE TABLE IF NOT EXISTS temp (' .
            ' id Int16, dt DEFAULT toDate(now()) ' .
            ') ENGINE = MergeTree(dt, (id, dt), 8192)',
        $sql);

        $this->setExpectedException("\Exception");
        // No date field exception
        $sql = implode($ch->sqlTableQuick('temp', [
            'id' => 'Int16',
            'dt' => ['Int32','toInt32(now())']
        ]));
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::dropTable
     */
    public function testDropTable()
    {
        $ch = $this->object;

        $table = 'testtbl123';

        $ans = $ch->createTableQuick($table, [
            'id' => 'Int16',
            'dt' => ['Date', 'now()']
        ], 2);
        $this->assertFalse($ans);

        $arr = $ch->queryInsertArray($table, ['id', 'dt'], [111, '2017-10-10']);
        $this->assertFalse($arr);

        $arr = $ch->queryValue("SELECT id FROM $table");
        $this->assertEquals(111, $arr);

        $ans = $ch->dropTable($table);
        $this->assertFalse($ans);

        $arr = $ch->queryValue("SELECT id FROM $table");
        $this->assertFalse($arr);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::renameTable
     */
    public function testRenameTable()
    {
        $ch = $this->object;

        $prefix = 'tempren';
        $postfix = 'xxx';

        $table_from = $prefix . 'from';
        $table_next = $prefix . 'next';
        $table_to = $prefix . 'to';

        $ans = $ch->createTableQuick($table_from, [
            'id' => 'Int16',
            'dt' => ['Date', 'now()']
        ], 2);
        $this->assertFalse($ans);

        $ans = $ch->renameTable($table_from, $table_to);

        $arr = $ch->getTablesList(null, $table_to);
        $this->assertEquals($table_to, $arr[0]);

        $ans = $ch->createTableQuick($table_next, [
            'id' => 'Int16',
            'dt' => ['Date', 'now()']
        ], 2);
        $this->assertFalse($ans);

        $arr = $ch->getTablesList(null, $prefix .'%');

        $ren_arr = [];
        foreach ($arr as $table) {
            if (substr($table, -strlen($postfix)) != $postfix) {
                $ren_arr[$table] = $table . $postfix;
            }
        }
        $ans = $ch->renameTable($ren_arr);

        $arr = $ch->getTablesList(null, $prefix . '%');
        $drop_count = 0;
        foreach ($arr as $drop_table) {
            if (\substr($table, -\strlen($postfix)) != $postfix) {
                $this->assertFalse($ch->dropTable($drop_table));
                $drop_count++;
            }
        }
        $this->assertGreaterThan(1, $drop_count);
        $this->assertEquals(\count($arr), $drop_count);

        // test bad param
        $this->assertTrue(\is_string($ch->renameTable($table_from)));
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::clearTable
     */
    public function testClearTable()
    {
        $ch = $this->object;

        $table = 'testtbl123';

        $ans = $ch->createTableQuick($table, [
            'id' => 'Int16',
            'dt' => ['Date', 'now()']
        ], 2);
        $this->assertFalse($ans);

        $arr = $ch->queryInsertArray($table, ['id', 'dt'], [111, '2017-10-10']);
        $this->assertFalse($arr);

        $arr = $ch->queryValue("SELECT id FROM $table");
        $this->assertEquals(111, $arr);

        $ans = $ch->clearTable($table);
        $this->assertFalse($ans);

        $arr = $ch->queryInsertArray($table, ['id', 'dt'], [222, '2017-10-10']);
        $this->assertFalse($arr);

        $arr = $ch->queryValue("SELECT id FROM $table");
        $this->assertEquals(222, $arr);

        $ans = $ch->dropTable($table);
        $this->assertFalse($ans);

        $ans = $ch->clearTable('system.numbers');
        $this->assertTrue(\is_string($ans));
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::sendFileInsert
     */
    public function testSendFileInsert()
    {
        $ch = $this->object;

        $table = 'testtbl123';

        $file = '../forpost.txt';

        $ans = $ch->createTableQuick($table, [
            'id' => 'Int16',
            'dt' => ['Date', 'now()'],
            's' => 'String',
            'when' => 'Date'
        ], 2);
        $this->assertFalse($ans);

        $row = "\t2017-11-05\tString\t1500559231\n";
        \file_put_contents($file, '1' . $row . '2'. $row);

        $ans = $ch->sendFileInsert($file, $table, true);
        $exp = [
            'file_structure' => 'id Int16, dt String, s String, when UInt32',
            'selector' => 'id, toDate(dt), s, toDate(when)'
            ];
        $this->assertEquals($exp, $ans);

        $ans = $ch->sendFileInsert($file, $table);
        $this->assertFalse($ans);

        $ans = $ch->queryArr("SELECT * FROM $table LIMIT 10", true);
        $exp = [[1, '2017-11-05', 'String', '2017-07-20'],
                [2, '2017-11-05', 'String', '2017-07-20']];
        $this->assertEquals($exp, $ans);

        // test errors
        $ans = $ch->sendFileInsert('', $table);
        $this->assertTrue(is_string($ans));
        $ans = $ch->sendFileInsert($file, '');
        $this->assertTrue(is_string($ans));

        $row = "\t2017-11-05\n";
        \file_put_contents($file, '1' . $row . '2'. $row);
        $ans = $ch->sendFileInsert($file, $table);
        $this->assertTrue(is_string($ans));
//        \chmod($file, 0222);
//        $ans = $ch->sendFileInsert($file, $table);
//        $this->assertTrue(is_string($ans));
        unlink($file);
    }
    /**
     * @covers ierusalim\ClickHouse\ClickHouseFunctions::sqlTableQuick
     */
    public function testSqlTableQuickException()
    {
        $ch = $this->object;
        $this->setExpectedException("\Exception");

        // No primary field
        $sql = implode($ch->sqlTableQuick('temp', [
            'dt' => ['Int32','toInt32(now())']
        ]));
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


        $engine = $ch->queryTableSys($table, "tables");
        $this->assertEquals('MergeTree', $engine['engine']);

        $this->assertFalse($ch->dropTable($table));

        $ans = $ch->createTableQuick($table, [
            'id' => 'Int16',
            'dt' => ['Date', 'now()'],
            'ver' => 'UInt8'
        ], 2, ', ver');
        $this->assertFalse($ans);

        $engine = $ch->queryTableSys($table, "tables");
        $this->assertEquals('ReplacingMergeTree', $engine['engine']);

        $this->assertFalse($ch->dropTable($table));

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
        $arr = $ch->getTableInfo($tbl, 0);
        $this->assertArrayHasKey('table_name', $arr);
        $this->assertEquals($tbl, $arr['table_name']);
        $this->assertArrayNotHasKey('engine', $arr);
        $this->assertArrayNotHasKey('create', $arr);
        $this->assertArrayNotHasKey('system.merges', $arr);
        $this->assertArrayNotHasKey('system.replicas', $arr);
        $this->assertArrayNotHasKey('system.parts', $arr);

        $arr = $ch->getTableInfo($tbl, 1);
        $this->assertArrayHasKey('table_name', $arr);
        $this->assertEquals($tbl, $arr['table_name']);
        $this->assertArrayHasKey('engine', $arr);
        $this->assertEquals('SystemColumns', $arr['engine']);
        $this->assertArrayNotHasKey('create', $arr);
        $this->assertArrayNotHasKey('system.merges', $arr);
        $this->assertArrayNotHasKey('system.replicas', $arr);
        $this->assertArrayNotHasKey('system.parts', $arr);

        $arr = $ch->getTableInfo($tbl, 2);
        $this->assertArrayHasKey('engine', $arr);
        $this->assertArrayHasKey('create', $arr);
        $this->assertArrayNotHasKey('system.merges', $arr);
        $this->assertArrayNotHasKey('system.replicas', $arr);
        $this->assertArrayNotHasKey('system.parts', $arr);

        $arr = $ch->getTableInfo($tbl, 3);
        $this->assertArrayHasKey('engine', $arr);
        $this->assertArrayHasKey('create', $arr);
        $this->assertArrayHasKey('system.merges', $arr);
        $this->assertArrayNotHasKey('system.replicas', $arr);
        $this->assertArrayNotHasKey('system.parts', $arr);

        $arr = $ch->getTableInfo($tbl, 4);
        $this->assertArrayHasKey('engine', $arr);
        $this->assertArrayHasKey('create', $arr);
        $this->assertArrayHasKey('system.merges', $arr);
        $this->assertArrayHasKey('system.replicas', $arr);
        $this->assertArrayNotHasKey('system.parts', $arr);

        $arr = $ch->getTableInfo($tbl, 5);
        $this->assertArrayHasKey('engine', $arr);
        $this->assertArrayHasKey('create', $arr);
        $this->assertArrayHasKey('system.merges', $arr);
        $this->assertArrayHasKey('system.replicas', $arr);
        $this->assertArrayHasKey('system.parts', $arr);

        // not found table request
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
            'create' => "id DEFAULT CAST('google' AS Enum8('google'=1,'bing'=2))",
            'type_full' => "Enum8('google'=1,'bing'=2)",
            'type_name' => 'Enum8',
            'type_src' => "Enum8('google'=1,'bing'=2)",
            'default' => "CAST('google' AS Enum8('google'=1,'bing'=2))",
            'bytes' => 1
        ]], $arr);

        $arr = $ch->parseFieldsArr(['id' => ["Array(UInt8)",'[1]']]);
        $this->assertEquals(['id' => [
            'create' => "id Array(UInt8) DEFAULT [1]",
            'type_full' => "Array(UInt8)",
            'type_name' => 'Array',
            'type_src' => "Array(UInt8)",
            'default' => "[1]",
            'bytes' => 0
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
        $this->assertEquals(['CAST(', " AS Enum8('google' = 1, 'bing' = 2))"], $to_conv);

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
                if ('Enum' !== substr($type, 0, 4)) {
                    $this->assertEquals(['to' . $type . '(', ')'], $to_conv);
                }
            }
        }
    }
}
