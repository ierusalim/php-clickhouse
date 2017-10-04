<?php
namespace ierusalim\ClickHouse;

/**
 * Generated by PHPUnit_SkeletonGenerator on 2017-07-13 at 13:25:25.
 */
class ClickHouseAPITest extends \PHPUnit_Framework_TestCase
{

    /**
     * @var ClickHouseAPI
     */
    protected $object;

    /**
     * Sets up the fixture, for example, opens a network connection.
     * This method is called before a test is executed.
     */
    protected function setUp()
    {
        $this->object = new ClickHouseAPI;
        $ch = $this->object;
        $this->resetServerUrl();
        $ch->session_autocreate = false;
    }

    public function testConstructEmpty()
    {
        echo "PHP " . phpversion() ."\n";
        $r = new ClickHouseAPI();
        $this->assertEquals('127.0.0.1', $r->host);
    }

    public function testConstructWithURL()
    {
        $r = new ClickHouseAPI('https://8.8.8.8:1234/');
        $this->assertEquals('8.8.8.8', $r->host);
    }

    public function testConstructWithHostEtc()
    {
        $r = new ClickHouseAPI('1.2.3.4', 5678, 'default', '');
        $this->assertEquals('1.2.3.4', $r->host);
    }

    protected function resetServerUrl()
    {
        $ch = $this->object;

        $localenv = "../localenv.php";
        if (is_file($localenv)) {
            include $localenv;
        } else {
            $clickhouse_url = null;
        }
        $ch->setServerUrl($clickhouse_url);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::setServerUrl
     */
    public function testSetServerUrl()
    {
        $ch = $this->object;
        $ch->setServerUrl("https://8.8.8.8:1234/");
        $this->assertEquals($ch->host, '8.8.8.8');
        $this->resetServerUrl();
    }

    public function testSetServerUrlException()
    {
        $ch = $this->object;
        $this->setExpectedException("\Exception");
        $ch->setServerUrl("ftp://8.8.8.8");
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::doApiCall
     * @todo   Implement testDoApiCall().
     */
    public function testDoApiCall()
    {
        $ch = $this->object;

        // $ch->toSlot()->doApiCall - clear async-mode, than do..

        $ans = $ch->toSlot()->doApiCall(0, ['query' => 'SELECT version()']);

        $curl_error = $ans['curl_error'];
        if ($curl_error) {
            echo "\nCURL_ERROR: $curl_error";
            $this->assertTrue(empty($curl_error));
        } else {
            echo "Version response: {$ans['response']}Starting tests...\n";
        }

        $slot = "tmp1";
        $ans = $ch->toSlot($slot)->doApiCall(false, ['query'=>'SELECT 123']);
        $this->assertEquals(102, $ans['code']);

        $ch->debug = true;
        $ch->hook_before_api_call = function ($url, $obj) {
            return "https://ierusalim.github.io";
        };

        $file = dirname(dirname(__DIR__)) . DIRECTORY_SEPARATOR . '.gitignore';

        $ans = $ch->doApiCall("empty", [], true, [], $file);
        $this->assertEquals($ans['code'], 405);

        $this->assertEquals("123\n", $ch->fromSlot($slot)->results);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::yiDoApiCall
     * @todo   Implement testYiDoApiCall().
     */
    public function testYiDoApiCall()
    {
        $ch = $this->object;

        if ($ch->isSupported('query')) {
            $table = "anytabletmp";

            $file = 'anyfile.txt';

            $file_data = '';
            for ($t=1; $t<100; $t++) {
                $file_data .= $t . "\t2017-12-12\tAny string data\n";
            }

            $file_size = file_put_contents($file, $file_data);

            $this->assertTrue($file_size > 0);

            $fields = '(id, dt, s)';
            $structure_excactly = 'id UInt32, dt Date, s String';

            $ch->query("DROP TABLE IF EXISTS $table")

               ->query("CREATE TABLE $table" .
                "( $structure_excactly )" .
                "ENGINE = MergeTree(dt, (id, dt), 8192)");

            $ch->is_windows = true;

            $ans = $ch->doApiCall(false,
                ['query' => "INSERT INTO $table $fields FORMAT TabSeparated"],
                true, [], $file, true);

            $ch->query("SELECT * FROM $table");
            $this->assertEquals($file_data, $ch->results);

            $ch->is_windows = false;

            try {
                $ans = $ch->doApiCall(false,
                    ['query' => "INSERT INTO $table $fields FORMAT TabSeparated"],
                    true, [], $file, true);
            } catch (\Exception $e) {
                echo $e->getMessage();
                \fclose($ch->fh);
            }


            $ch->query("DROP TABLE IF EXISTS $table");
            unlink($file);
            unlink($file . '.gz');
            $slot = "tmp1";
            $ans = $ch->toSlot($slot)->doApiCall(false, ['query'=>'SELECT 123']);
            $this->assertEquals(102, $ans['code']);

            $ch->debug = true;

            $ch->hook_before_api_call = function ($url, $obj) {
                return "https://ierusalim.github.io";
            };

            $file = dirname(dirname(__DIR__)) . \DIRECTORY_SEPARATOR . '.gitignore';

            $ans = $ch->doApiCall("empty", [], true, [], $file);
            $this->assertEquals($ans['code'], 405);

            $this->assertEquals("123\n", $ch->fromSlot($slot)->results);
        }
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::getVersion
     */
    public function testGetVersion()
    {
        $ch = $this->object;

        // erase all existing slots
        foreach($ch->multi_status as $slot => $status) {
            $ch->eraseSlot($slot);
        }

        $version = $ch->getVersion();
        if ($ch->isSupported('query')) {
            $this->assertTrue(strpos($version, '.') > 0);
        }
        echo "Version of ClickHouse server: $version\n";

        $ch1 = new ClickHouseAPI('http://github.com:22');
        $ch1->session_autocreate = true;

        $ver_bad = $ch1->getVersion(true);
        $this->assertFalse($ch1->session_autocreate);
        $this->assertEquals("Unknown", $ver_bad);

        $ver_good = $ch->getVersion(true);
        $this->assertEquals($version, $ver_good);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::versionSendQuery
     */
    public function testVersionSendQuery()
    {
        $ch = $this->object;
        if ($ch->isSupported('query')) {
            $version = $ch->getVersion(true);
        }
        echo "Version again: $version\n";

        $ch1 = new ClickHouseAPI('http://github.com:22');
        $ch1->session_autocreate = true;

        $ver_bad = $ch1->getVersion(true);
        $this->assertFalse($ch1->session_autocreate);
        $this->assertEquals("Unknown", $ver_bad);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::query
     */
    public function testQuery()
    {
        $ch = $this->object;

        if ($ch->isSupported('query')) {
            $ans = $ch->query("SELECT 123")->results;
            $this->assertEquals("123\n", $ans);
            $table = "querytesttable";
            $this->assertEquals("111\n", $ch
            ->query("CREATE TABLE IF NOT EXISTS $table (id UInt8, dt Date) ENGINE = MergeTree(dt, (id), 8192)")
            ->query("INSERT INTO $table SELECT 111 as id, toDate(now()) as dt")
            ->query("SELECT id FROM $table WHERE dt = toDate(now())")
            ->query("DROP TABLE IF EXISTS $table")
            ->results
            );
        }

        try {
            $ans = $ch->query("BAD QUERY");
        } catch (\Exception $e) {
            $ans = $e->getMessage();
        }

        $this->assertTrue(\strpos($ans, 'Syntax error') !== false);

        $ch->curl_options[\CURLOPT_TIMEOUT] = 2;

        // curl error emulation
        $ch->hook_before_api_call = function($s, $obj) {
            return 'http://github.com:22/';
        };
        try {
            $ans = $ch->query("ANY QUERY");
        } catch (\Exception $e) {
            $ans = $e->getMessage();
        }
        //$this->assertTrue(\strpos($ans, 'Syntax error') !== false);
        $ch->hook_before_api_call = false;

        try {
            $ch = new ClickHouseAPI("https://github.com:22/");
            $ch->curl_options[\CURLOPT_CONNECTTIMEOUT] = 2;
            $version = $ch->getVersion();
            $ans =$ch->query("");
        } catch (\Exception $e) {
            $ans = $e->getMessage();
        }
        $ch->curl_options[\CURLOPT_CONNECTTIMEOUT] = 7;
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::setCompression
     */
    public function testSetCompression()
    {
        $ch = $this->object;

        if ($ch->isSupported('session_id')) {
            $ch->setCompression(false);

            $ans = $ch->query("SELECT number FROM system.numbers LIMIT 100")->results;

            $size_d = $ch->curl_info[\CURLINFO_SIZE_DOWNLOAD];
            $this->assertEquals(strlen($ans), $size_d);

            $ch->setCompression(true);

            $ans = $ch->query("SELECT number FROM system.numbers LIMIT 100")->results;

            $size_d = $ch->curl_info[\CURLINFO_SIZE_DOWNLOAD];
            if ($size_d < strlen($ans)) {
                echo "http-compression supported\n";
            }
            $this->assertGreaterThan($size_d, strlen($ans));
        }
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::isSupported
     */
    public function testIsSupported()
    {
        $ch = $this->object;
        $sess_sup = $ch->isSupported('session_id');
        echo "Sessions " . ($sess_sup ? '' : 'is not ') . "supported\n";

        if (!$ch->isSupported('query', true)) {
            echo "query is not supported; ClickHouse Server is not ready\n";
            echo "Server: {$ch->host}:{$ch->port}\n";
        }

        $sess2_sup = (new ClickHouseAPI('https://google.com:443/'))->isSupported('session_id');
        $this->assertFalse($sess2_sup);
        if ($sess_sup) {
            echo ',';
            $this->assertTrue($ch->isSupported('session_id'));
        }

        $this->assertEquals($ch->isSupported('version', true), $ch->getVersion());

        $this->assertFalse($ch->isSupported('unknown'));
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::anyQuery
     * @todo   Implement testAnyQuery().
     */
    public function testAnyQuery()
    {
        $ch = $this->object;

        if ($ch->isSupported('query')) {
            //get mode
            $ans = $ch->anyQuery("SELECT 123");
            $this->assertTrue(isset($ans['response']));
            $this->assertEquals(trim($ans['response']), 123);

            //post mode
            $ans = $ch->anyQuery("SELECT 123", []);
            $this->assertTrue(isset($ans['response']));
            $this->assertEquals(trim($ans['response']), 123);
        } else {
            echo '-';
        }
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::getQuery
     * @todo   Implement testGetQuery().
     */
    public function testGetQuery()
    {
        $ch = $this->object;
        if ($ch->isSupported('query')) {
            $ans = $ch->getQuery("SELECT 456");
            $this->assertTrue(isset($ans['response']));
            $this->assertEquals(trim($ans['response']), '456');
        } else {
            echo '-';
        }
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::doGet
     * @todo   Implement testDoGet().
     */
    public function testDoGet()
    {
        $ch = $this->object;
        if ($ch->isSupported('query')) {
            $ans = $ch->doGet("SELECT 567", ['database' => 'default']);
            $this->assertEquals(200, ($ans['code']));
            $this->assertTrue(isset($ans['response']));
            $this->assertEquals(trim($ans['response']), '567');
        } else {
            echo '-';
        }
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::postQuery
     * @todo   Implement testPostQuery().
     */
    public function testPostQuery()
    {
        $ch = $this->object;
        if ($ch->isSupported('query')) {
            $ans = $ch->postQuery("CREATE TABLE t (a UInt8) ENGINE = Memory", []);
            if ($ans['code'] == 500) {
                $ans = $ch->postQuery("DROP TABLE t", []);
            }
            $this->assertEquals($ans['code'], 200);
        } else {
            echo '-';
        }
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::doQuery
     * @todo   Implement testDoQuery().
     */
    public function testDoQuery()
    {
        $ch = $this->object;

        $ch->session_autocreate = false;

        if ($ch->isSupported('query')) {
            // test default query SELECT 1
            $ans = $ch->doQuery();
            $this->assertEquals(\trim($ans['response']), 1);

            $this->assertNull($ch->getSession());

            $ch->session_autocreate = true;
            $ans = $ch->doQuery("SELECT 22");
            $this->assertEquals(\trim($ans['response']), 22);

            $session_id = $ch->getSession();
            if ($ch->isSupported('session_id')) {
                $this->assertEquals(32, strlen($session_id));
            }

            // test previous query SELECT 22
            $ans = $ch->doQuery();
            $this->assertEquals(\trim($ans['response']), 22);
        }
        if ($ch->isSupported('session_id')) {
            $sess = $ch->getSession();
            $this->assertEquals($sess, $session_id);

            // test temporary session
            $sess_tmp = md5(microtime());
            // use temporary session
            $ans = $ch->doQuery("SELECT 123", false, [], $sess_tmp);

            // session_id must not changed
            $this->assertEquals($session_id, $ch->getSession());
        } else {
            echo '-';
        }
        if ($ch->isSupported('query')) {
            // check query if not supported session
            $ch->isSupported('session_id', false, false);
            $ch->setOption('session_id', 'test');
            $ans = $ch->doQuery("SELECT 321");
            $this->assertEquals(\trim($ans['response']), 321);
            $ch->isSupported('session_id', true);
        }
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::setOption
     * @todo   Implement testSetOption().
     */
    public function testSetOption()
    {
        $ch = $this->object;
        $ch->setOption('user', 'default');
        $user = $ch->getOption('user');
        $this->assertEquals($user, 'default');
        $user = $ch->setOption('user', null);
        $this->assertFalse(isset($this->options['user']));
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::getOption
     * @todo   Implement testGetOption().
     */
    public function testGetOption()
    {
        $ch = $this->object;
        $noopt = $ch->getOption('noopt');
        $this->assertNull($noopt);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::delOption
     * @todo   Implement testDelOption().
     */
    public function testDelOption()
    {
        $ch = $this->object;
        $ch->setSession();
        $session_id = $ch->getSession();
        $this->assertEquals(strlen($session_id), 32);
        $old = $ch->delOption("session_id");
        $this->assertEquals($session_id, $old);
        $new = $ch->getSession();
        $this->assertNull($new);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::slotFinished
     * @todo   Implement testSlotFinished().
     */
    public function testSlotFinihed()
    {
        $ch = $this->object;

        if (1) {
        $ch ->toSlot("T1") -> query("SELECT 'OK 123 '")
            ->toSlot("T2") -> query("SELECT 'OK 456 '")
            ->toSlot("T3") -> query("SELECT 'OK 789 '");

        foreach ($ch->slotFinished(0) as $slot => $arr) {
            $ans = trim($arr['response']) ;
            $this->assertEquals("OK", substr($ans,0,2));
        }
        }
        if (1) {
        $slots = [];
        $max = 10;
        for($s = 1; $s<$max; $s++) {
            $slots[] = $slot = "test$s";
            //$ch->toSlot($slot)->query("SELECT hex(toString($s))");
            $ch->toSlot($slot)->query("SELECT hex(MD5(toString($s)))");
        }
        echo "Started slots: " . implode(",", $slots) . "\nResults:\n";
        $n = 1;
        foreach ($ch->slotFinished() as $slot => $arr) {
            echo "$n. $slot  = " . $arr['response'];
            if (empty($arr['response'])) {
                print_r($ch->slot_data[$slot]);
                break;
            }
            $n++;
        }
        }
    }
}
