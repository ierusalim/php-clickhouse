<?php
namespace ierusalim\ClickHouse;

/**
 * Generated by PHPUnit_SkeletonGenerator on 2017-07-13 at 15:20:19.
 */
class ClickHouseReqTest extends \PHPUnit_Framework_TestCase
{

    /**
     * @var ClickHouseReq
     */
    protected $object;

    /**
     * Sets up the fixture, for example, opens a network connection.
     * This method is called before a test is executed.
     */
    protected function setUp()
    {
        $localenv = "localenv.php";
        if (is_file($localenv)) {
            include $localenv;
        } else {
            $clickhouse_url = null;
        }
        $this->object = new ClickHouseReq($clickhouse_url);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseReq::setCurrentDatabase
     * @todo   Implement testSetCurrentDatabase().
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
     * @covers ierusalim\ClickHouse\ClickHouseReq::getCurrentDatabase
     * @todo   Implement testGetCurrentDatabase().
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
     * @covers ierusalim\ClickHouse\ClickHouseReq::queryGood
     * @todo   Implement testQueryGood().
     */
    public function testQueryGood()
    {
       $ch = $this->object;
       $this->assertEquals($ch->queryGood("SELECT 1"), "1");
       $this->assertTrue($ch->queryGood("USE system"));
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseReq::queryValue
     * @todo   Implement testQueryValue().
     */
    public function testQueryValue()
    {
       $ch = $this->object;
       $this->assertEquals($ch->queryValue("SELECT 1"), "1");
       $this->assertEquals($ch->queryValue("USE system"), "");
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseReq::queryFullArray
     * @todo   Implement testQueryFullArray().
     */
    public function testQueryFullArray()
    {
       $ch = $this->object;
       $t_arr = $ch->queryFullArray("SHOW DATABASES");
       $this->assertArrayHasKey('meta', $t_arr);
       $this->assertArrayHasKey('data', $t_arr);
       $this->assertArrayHasKey('statistics', $t_arr);
       $this->assertArrayHasKey('rows', $t_arr);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseReq::queryData
     */
    public function testQueryData()
    {
        $ch = $this->object;
        $arr = $ch->queryData("SHOW DATABASES", false);
        $this->assertArrayHasKey('name', $arr[0]);
        $arr = $ch->queryData("SHOW DATABASES", true);
        $this->assertArrayHasKey('0', $arr[0]);
        
    }
    /**
     * @covers ierusalim\ClickHouse\ClickHouseReq::queryKeyValues
     */
    public function testQueryKeyValues()
    {
        $ch = $this->object;
        
        $arr1 = $ch->queryKeyValues("DESCRIBE TABLE system.databases", 'name', 'type', 1);
        $arr2 = $ch->getTableFields('system.databases');

        $this->assertEquals($arr1, $arr2);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseReq::getDatabasesList
     */
    public function testGetDatabasesList()
    {
        $ch = $this->object;
        $db_arr = $ch->queryColumn("SHOW DATABASES");
        $db_2_arr =$ch->getDatabasesList();
        $this->assertEquals($db_arr, $db_2_arr);
        $this->assertTrue(\array_search('system', $db_arr) !== false);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseReq::getProcessList
     */
    public function testGetProcessList()
    {
        $ch = $this->object;
        $proc_arr = $ch->getProcessList();
        $this->assertTrue(\is_array($proc_arr));
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseReq::getTablesList
     */
    public function testGetTablesList()
    {
        $ch = $this->object;
        $sys_tbl_arr = $ch->getTablesList('system');
        $this->assertTrue(\count($sys_tbl_arr)>10);
        $this->assertTrue(\array_search('databases', $sys_tbl_arr) !== false);

        $data_tbl_arr = $ch->getTablesList('system', 'd%');
        $this->assertTrue(count($data_tbl_arr)>0);
        $this->assertTrue(array_search('databases', $data_tbl_arr)!==false);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseReq::getTableFields
     */
    public function testGetTableFields()
    {
        $ch = $this->object;
        $desc_tbl_arr = $ch->getTableFields('system.databases');
        $this->assertArrayHasKey('name', $desc_tbl_arr);
        $this->assertArrayHasKey('engine', $desc_tbl_arr);
    }
    
    public function testGetSystemSettings()
    {
        $ch = $this->object;
        $arr = $ch->getSystemSettings();
        $this->assertTrue(count($arr)>10);
    }
}
