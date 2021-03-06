<?php
namespace ierusalim\ClickHouse;

/**
 * Generated by PHPUnit_SkeletonGenerator on 2017-08-01 at 17:42:56.
 */
class ClickHouseSlotsTest extends \PHPUnit_Framework_TestCase
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
    }
    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::slotExisting
     * @todo   Implement testSlotExisting().
     */
    public function testSlotExisting()
    {
        $ch = $this->object;

        $ch->eraseAllSlots(true);

        $urls = [
        'https://www.google.com/',
        'https://pastebin.com/raw/gZC9hKMc',
        ];

        // start slots for each url
        $slots = [];
        foreach ($urls as $k => $url) {
            $curl_h = curl_init($url);
            \curl_setopt_array($curl_h, $ch->curl_options);

            $slots[$url] = $slot = 'Test' . $k;
            $slot_low = strtolower($slot);

            $arr = $ch->slotStart($slot, $curl_h);
        }
        foreach($ch->slotExisting() as $slot => $status) {
            $this->assertEquals(0, $status);
        }
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::slotFinished
     * @todo   Implement testSlotFinished().
     */
    public function testSlotFinished()
    {
        $ch = $this->object;

        $ch->eraseAllSlots(true);

        $urls = [
        'https://www.google.com/',
        'https://pastebin.com/raw/gZC9hKMc',
        ];

        // start slots for each url
        $slots = [];
        foreach ($urls as $k => $url) {
            $curl_h = curl_init($url);
            \curl_setopt_array($curl_h, $ch->curl_options);

            $slots[$url] = $slot = 'test' . $k;
            $slot_low = strtolower($slot);

            $arr = $ch->slotStart($slot, $curl_h);
        }
        foreach($ch->slotFinished() as $slot => $data) {
            $this->assertTrue(\in_array($slot, $slots));
        }
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::slotFree
     * @todo   Implement testSlotFree().
     */
    public function testSlotFree()
    {
        $ch = $this->object;
        $curl_h = curl_init();
        $slot = 'Test1';
        $arr = $ch->slotStart($slot, $curl_h);
        $this->assertArrayHasKey('code', $arr);
        $this->assertArrayHasKey('curl_error', $arr);
        $this->assertArrayHasKey('response', $arr);
        $this->assertArrayHasKey('curl_h', $arr);
        $this->assertEquals(102, $arr['code']);

        $ans = $ch->slotFree("notfound");

        $this->assertTrue(strpos($ans, 'unknow') !== false);

        $slot_low = strtolower($slot);

        $this->assertArrayHasKey($slot_low, $ch->multi_status);
        $this->assertArrayHasKey($slot_low, $ch->multi_ch);

        $ans = $ch->slotFree($slot);

        $this->assertFalse($ans);

        $this->assertArrayNotHasKey($slot_low, $ch->multi_status);
        $this->assertArrayNotHasKey($slot_low, $ch->multi_ch);
        $this->assertArrayHasKey($slot_low, $ch->slot_results);
        $this->assertArrayHasKey($slot_low, $ch->slot_hooks);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::eraseSlot
     * @todo   Implement testEraseSlot().
     */
    public function testEraseSlot()
    {
        $ch = $this->object;
        $curl_h = curl_init();
        $slot = 'Test222';
        $arr = $ch->slotStart($slot, $curl_h);
        $this->assertArrayHasKey('code', $arr);
        $this->assertArrayHasKey('curl_error', $arr);
        $this->assertArrayHasKey('response', $arr);
        $this->assertArrayHasKey('curl_h', $arr);
        $this->assertEquals(102, $arr['code']);

        $slot_low = strtolower($slot);

        $this->assertArrayHasKey($slot_low, $ch->multi_status);
        $this->assertArrayHasKey($slot_low, $ch->multi_ch);
        $this->assertArrayHasKey($slot_low, $ch->slot_results);
        $this->assertArrayHasKey($slot_low, $ch->slot_hooks);

        $ans = $ch->eraseSlot($slot);

        $this->assertEquals($ch, $ans);

        $this->assertArrayNotHasKey($slot_low, $ch->multi_status);
        $this->assertArrayNotHasKey($slot_low, $ch->multi_ch);
        $this->assertArrayNotHasKey($slot_low, $ch->slot_results);
        $this->assertArrayNotHasKey($slot_low, $ch->slot_hooks);

        $this->setExpectedException("\Exception");
        $ch->eraseSlot("notfound");
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::eraseAllSlots
     * @todo   Implement testEraseAllSlots().
     */
    public function testEraseAllSlots()
    {
        $ch = $this->object;
        $ch ->toSlot("Test2")->query("SELECT 234")
            ->toSlot("Test3")->query("SELECT 567");
        $ch->eraseAllSlots(true);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::slotEmulateResults
     * @todo   Implement testSlotEmulateResults.
     */
    public function testSlotEmulateResults()
    {
        $ch = $this->object;

        $test_string = "test string";
        $ch ->slotEmulateResults("test", $test_string);

        $ans = $ch->slotResults('test');

        $this->assertEquals($test_string, $ans);

        $ch ->slotEmulateResults("test", $test_string, ['code' => 404]);

        $ans = $ch->slotResults('test');

        $this->assertEquals($test_string, $ans);

        $this->assertEquals(404, $ch->slot_data['test']['code']);
    }
    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::toSlot
     * @todo   Implement testToSlot().
     */
    public function testToSlot()
    {
        $ch = $this->object;
        $slot = "Test1";
        $ch ->toSlot($slot)  ->query("SELECT 123")
            ->toSlot("Test2")->query("SELECT 234")
            ->toSlot("Test3")->query("SELECT 567");
        try {
            $ch->toSlot($slot);
        } catch (\Exception $e) {
            $this->assertEquals("Slot '$slot' is busy", $e->getMessage());
        }
        foreach([$slot, 'Test2', 'Test3'] as $slot) {
            echo $ch->fromSlot($slot)->results;
        }
    }


    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::fromSlot
     * @todo   Implement testFromSlot().
     */
    public function testFromSlot()
    {
        $ch = $this->object;

        $ch->eraseAllSlots(true);

        $urls = [
        'https://www.google.com/',
        'https://pastebin.com/raw/gZC9hKMc',
        ];

        // start slots for each url
        $slots = [];
        foreach ($urls as $k => $url) {
            $curl_h = curl_init($url);
            \curl_setopt_array($curl_h, $ch->curl_options);

            $slots[$url] = $slot = 'Test' . $k;
            $slot_low = strtolower($slot);

            $arr = $ch->slotStart($slot, $curl_h);
        }

        // Waiting for the completion of each url-slot
        foreach($slots as $url => $slot) {
            $code = $ch->fromSlot($slot)->last_code;
            $this->assertTrue(\is_numeric($code));
        }

        $this->setExpectedException('\Exception');
        $ch->fromSlot("badname");
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::slotResults
     * @todo   Implement testSlotResults().
     */
    public function testSlotResults()
    {
        $ch = $this->object;

        $slot = "testslot";
        $ch->toSlot($slot)->query("SELECT 123");

        // waiting for version-requests finished, if have active
        if ($ch->multi_still_run) {
            while (($arr = $ch->slotWaitReady()) != -1) {
                foreach($arr as $slot) {
                    $arr = $ch->slotResults($slot);
                    $this->assertArrayHasKey('code', $arr);
                }
            }
        } else {
            $this->assertFalse(1);
        }

        $ch->slot_data[$slot] = [false];
        try {
            $arr = $ch->slotResults($slot);
        } catch (\Exception $e) {
            $this->assertEquals("Slot '$slot' damaged", $e->getMessage());
        }

        $ch->eraseSlot($slot);
        try {
            $arr = $ch->slotResults($slot);
        } catch (\Exception $e) {
            $this->assertEquals("Unknown slot '$slot'", $e->getMessage());
        }


        $ch->from_slot = false;
        try {
            $arr = $ch->slotResults();
        } catch (\Exception $e) {
            $this->assertEquals("Illegal slot name", $e->getMessage());
        }

        $urls = [];
        for ($i=98; $i<101; $i++) {
            $urls[] = 'https://pastebin.com/raw/gZC9hKM' . chr($i);
        }

        // start slots for each url
        $slots = [];
        foreach ($urls as $k => $url) {
            $curl_h = curl_init($url);
            \curl_setopt_array($curl_h, $ch->curl_options);

            $slot = 'slot' . $k;
            $slots[] = $slot;
            $yi = function () {
                $response_arr = yield;
                yield $response_arr['code'];
            };
            $fi = function ($ans) {
                return $ans;
            };
            $ch->slotHookPush($slot, ['mode' => 2, 'fn' => $fi]);
            $arr = $ch->slotStart($slot, $curl_h, ['mode' => 1, 'fn' => $yi()]);
        }
        foreach($slots as $slot) {
            $res = $ch->slotResults($slot);
            $this->assertTrue(\is_numeric($res));
        }
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::slotContentToResults
     * @todo   Implement testSlotContentToResults().
     */
    public function testSlotContentToResults()
    {
        $ch = $this->object;

        // erase all existing slots
        $ch->eraseAllSlots(true);

        $curl_h = curl_init('http://facebook.com/');
        \curl_setopt_array($curl_h, $ch->curl_options);

        $slot = 'Test1';
        $slot_low = strtolower($slot);

        $arr = $ch->slotStart($slot, $curl_h);

        $ans = $ch->slotWaitReady($slot);
        $this->assertTrue($ans);

        $this->assertArrayHasKey($slot_low, $ch->multi_status);
        $this->assertArrayHasKey($slot_low, $ch->multi_ch);
        $this->assertArrayHasKey($slot_low, $ch->slot_results);
        $this->assertArrayHasKey($slot_low, $ch->slot_hooks);

        $this->assertFalse($ch->slotFree($slot));

        $this->assertArrayNotHasKey($slot_low, $ch->multi_status);
        $this->assertArrayNotHasKey($slot_low, $ch->multi_ch);
        $this->assertArrayHasKey($slot_low, $ch->slot_results);
        $this->assertArrayHasKey($slot_low, $ch->slot_hooks);

        $curl_h = curl_init('http://github.com:22/');
        \curl_setopt_array($curl_h, $ch->curl_options);

        $arr = $ch->slotStart($slot, $curl_h,
            [
            'mode' => 0,
            'fn' => function($obj, $slot_low, $par) {
                $obj->slot_results[$slot_low]['hook_results'] = "Hook slot=$slot_low";
            },
            'par' => 12345
        ]);

        $ans = $ch->slotWaitReady($slot);

        $this->assertEquals("Hook slot=$slot_low", $ch->slot_results[$slot_low]['hook_results']);

//        $this->assertTrue($ans !== true);

        $this->assertArrayHasKey($slot_low, $ch->multi_status);
        $this->assertArrayHasKey($slot_low, $ch->multi_ch);
        $this->assertArrayHasKey($slot_low, $ch->slot_results);
        $this->assertArrayHasKey($slot_low, $ch->slot_hooks);

        $res = $ch->slot_results[$slot_low];
        $this->assertArrayHasKey('curl_error', $res);
        $this->assertTrue(strlen($res['curl_error'])>0);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::slotHookPush
     * @todo   Implement testSlotHookPush().
     */
    public function testSlotHookPush()
    {
        $ch = $this->object;

        $slot = "abs";

        $this->assertTrue(strlen($ch->slotHookPush($slot, true))>3);

        $this->assertFalse($ch->slotHookPush($slot, false));

        $this->assertEquals([], $ch->slot_hooks[$slot]);

        $this->assertFalse($ch->slotHookPush($slot, false));

        $this->assertEquals([], $ch->slot_hooks[$slot]);

        $this->assertFalse($ch->slotHookPush($slot, [
            'mode'=>0, 'fn'=>1, 'par'=>2,
        ]));

        $this->assertEquals([[
            'mode'=>0, 'fn'=>1, 'par'=>2,
        ]], $ch->slot_hooks[$slot]);

        $this->assertFalse($ch->slotHookPush($slot, false));

        $this->assertEquals([
            ['mode'=>0, 'fn'=>1, 'par'=>2],
        ], $ch->slot_hooks[$slot]);

        $this->assertFalse($ch->slotHookPush($slot, [
            'mode'=>0, 'fn'=>2, 'par'=>3,
        ]));

        $this->assertEquals([
            ['mode'=>0, 'fn'=>1, 'par'=>2],
            ['mode'=>0, 'fn'=>2, 'par'=>3],
        ], $ch->slot_hooks[$slot]);

        $this->assertFalse($ch->slotHookPush($slot, [
            ['mode'=>1, 'fn'=>3, 'par'=>4],
            ['mode'=>2, 'fn'=>4, 'par'=>5],
        ]));

        $this->assertTrue(\strlen($ch->slotHookPush($slot, [
            'modex'=>0, 'fn'=>1, 'par'=>2,
        ]))>3);

        $this->assertEquals([
            ['mode'=>0, 'fn'=>1, 'par'=>2],
            ['mode'=>0, 'fn'=>2, 'par'=>3],
            ['mode'=>1, 'fn'=>3, 'par'=>4],
            ['mode'=>2, 'fn'=>4, 'par'=>5],
        ], $ch->slot_hooks[$slot]);
    }
    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::slotsRefresh
     * @todo   Implement testSlotsRefresh().
     */
    public function testSlotsRefresh()
    {
        $ch = $this->object;

        $ch->eraseAllSlots(true);

        $ans = $ch->slotsRefresh();

        $this->assertEquals(0, \count($ans));

        $version = $ch->getVersion();

        $curl_h = curl_init('http://github.com:22/');
        \curl_setopt_array($curl_h, $ch->curl_options);

        $slot = 'Test1';
        $slot_low = strtolower($slot);

        $arr = $ch->slotStart($slot, $curl_h);

        $ans = $ch->slotCheck($slot_low);
        $this->assertEquals($ch->multi_status[$slot_low], $ans);
        $this->assertTrue($ans !== false);

        $_tl = \microtime(true) + 5;
        while (\microtime(true) < $_tl) {
            $ans = $ch->slotsRefresh();
            if (\count($ans)) {
                $this->assertEquals($slot_low, $ans[0]);
            }
            if (!$ch->multi_still_run) {
                break;
            }
        }

        // must return empty
        $ans = $ch->slotsRefresh();
        $this->assertTrue(!count($ans));

        //\ob_end_clean();
        // test multi url parallely
        $urls = [];
        for ($i=97; $i<122; $i++) {
            $urls[] = 'https://pastebin.com/raw/gZC9hKM' . chr($i);
        }

        // start slots for each url
        foreach ($urls as $k => $url) {
            $curl_h = curl_init($url);
            \curl_setopt_array($curl_h, $ch->curl_options);

            $slot = 'test' . $k;
            $arr = $ch->slotStart($slot, $curl_h);
        }
        // waiting for slots finished
        while ($ch->slotWaitReady() != -1);

        $ch->multi_h = curl_multi_close($ch->multi_h);
        $ch->multi_h = null;
        $this->assertEquals([], $ch->slotsRefresh());
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::slotWaitReady
     * @todo   Implement testSlotWaitReady().
     */
    public function testSlotWaitReady()
    {
        $ch = $this->object;

        $ch->eraseAllSlots(true);

        $urls = [
        'https://www.google.com/',
        'https://pastebin.com/raw/gZC9hKMc',
        ];

        // start slots for each url
        $slots = [];
        foreach ($urls as $k => $url) {
            $curl_h = curl_init($url);
            \curl_setopt_array($curl_h, $ch->curl_options);

            $slots[$url] = $slot = 'Test' . $k;
            $slot_low = strtolower($slot);

            $ans = $ch->slotCheck($slot_low);
            $this->assertFalse($ans);

            $arr = $ch->slotStart($slot, $curl_h);

            $this->assertArrayHasKey($slot_low, $ch->multi_status);
            $this->assertArrayHasKey($slot_low, $ch->multi_ch);

            $ans = $ch->slotCheck($slot_low);
            $this->assertEquals($ch->multi_status[$slot_low], $ans);
            $this->assertTrue($ans !== false);
        }

        // Waiting for the completion of each url-slot
        foreach($slots as $url => $slot) {
            $slot_low = strtolower($slot);

            $ans = $ch->slotWaitReady($slot);

            if ($ans === true) {
                $res = $ch->slot_results[$slot_low];
                $this->assertArrayHasKey('code', $res);
                $this->assertArrayHasKey('response', $res);
                $this->assertArrayHasKey('curl_error', $res);
                $this->assertArrayHasKey('curl_info', $res);

                $ans = $ch->slotCheck($slot_low);
                $this->assertTrue($ans);
            } else {
                echo "CURL_ERROR # $ans ( $url )\n";

                $chk = $ch->slotCheck($slot_low);
                $this->assertEquals($ans, $chk);
            }

            $this->assertArrayHasKey($slot_low, $ch->slot_results);
            $this->assertArrayHasKey($slot_low, $ch->slot_hooks);

            $this->assertArrayHasKey($slot_low, $ch->multi_status);
            $this->assertArrayHasKey($slot_low, $ch->multi_ch);
        }

        // no active slots
        $ans = $ch->slotWaitReady();

        $this->assertEquals(-1, $ans);

        // release slots
        foreach ($slots as $slot) {
            $ans = $ch->slotWaitReady($slot);
            $this->assertTrue(!empty($ans));
            $ch->slotFree($slot);
        }

        // starting slots again
        $slots = [];
        foreach ($urls as $k => $url) {
            $curl_h = curl_init($url);
            \curl_setopt_array($curl_h, $ch->curl_options);

            $slots[$url] = $slot = 'Test' . $k;
            $slot_low = strtolower($slot);

            $ans = $ch->slotCheck($slot_low);
            $this->assertFalse($ans);

            $arr = $ch->slotStart($slot, $curl_h);

            $this->assertArrayHasKey($slot_low, $ch->multi_status);
            $this->assertArrayHasKey($slot_low, $ch->multi_ch);

            $ans = $ch->slotCheck($slot_low);
            $this->assertEquals($ch->multi_status[$slot_low], $ans);
            $this->assertTrue($ans !== false);
        }

        // waiting for any slot
        while (\count($slots)) {
            $arr = $ch->slotWaitReady();
            $this->assertTrue(\is_array($arr));
            if (!\is_array($arr)) {
                break;
            }
            foreach($arr as $slot) {
                $k = \array_search($ans, $slots);
               $this->assertTrue($k !== false);
               unset($slots[$k]);
            }
        }

        $this->assertEquals(-1, $ch->slotWaitReady());

        $ch->multi_h = false;
        $this->assertEquals(-1, $ch->slotWaitReady());
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::slotCheck
     * @todo   Implement testSlotCheck().
     */
    public function testSlotCheck()
    {
        $ch = $this->object;
        $curl_h = curl_init('http://google.com/');
        \curl_setopt_array($curl_h, $ch->curl_options);

        $slot = 'Test1';
        $slot_low = strtolower($slot);

        $ans = $ch->slotCheck($slot_low);
        $this->assertFalse($ans);

        $arr = $ch->slotStart($slot, $curl_h);

        $this->assertArrayHasKey($slot_low, $ch->multi_status);
        $this->assertArrayHasKey($slot_low, $ch->multi_ch);

        $ans = $ch->slotCheck($slot_low);
        $this->assertEquals($ch->multi_status[$slot_low], $ans);
        $this->assertTrue($ans !== false);

        $ans = $ch->slotWaitReady($slot);
        if ($ans === true) {
            $res = $ch->slot_results[$slot_low];
            $this->assertArrayHasKey('code', $res);
            $this->assertArrayHasKey('response', $res);
            $this->assertArrayHasKey('curl_error', $res);
            $this->assertArrayHasKey('curl_info', $res);

            $ans = $ch->slotCheck($slot_low);
            $this->assertTrue($ans);
        } else {
            echo "CURL_ERROR # $ans \n";

            $chk = $ch->slotCheck($slot_low);
            $this->assertEquals($ans, $chk);
        }

        $this->assertArrayHasKey($slot_low, $ch->slot_results);
        $this->assertArrayHasKey($slot_low, $ch->slot_hooks);

        $this->assertArrayHasKey($slot_low, $ch->multi_status);
        $this->assertArrayHasKey($slot_low, $ch->multi_ch);
    }

    /**
     * @covers ierusalim\ClickHouse\ClickHouseAPI::slotStart
     * @todo   Implement testSlotStart().
     */
    public function testSlotStart()
    {
        $ch = $this->object;
        $curl_h = curl_init();
        $slot = 'Test1';
        $arr = $ch->slotStart($slot, $curl_h);
        $this->assertArrayHasKey('code', $arr);
        $this->assertArrayHasKey('curl_error', $arr);
        $this->assertArrayHasKey('response', $arr);
        $this->assertArrayHasKey('curl_h', $arr);
        $this->assertEquals(102, $arr['code']);

        // slot already exists
        $slot = 'Test1';
        $arr = $ch->slotStart($slot, $curl_h);
        $this->assertArrayHasKey('code', $arr);
        $this->assertArrayHasKey('curl_error', $arr);
        $this->assertArrayHasKey('response', $arr);
        $this->assertArrayHasKey('curl_h', $arr);
        $this->assertEquals(417, $arr['code']);
        $this->assertTrue(strpos($arr['curl_error'], 'lready ex') !== false);
//        print_r($arr);
    }

    /**
    * @covers ierusalim\ClickHouse\ClickHouseAPI::slotAdd
    * @todo   Implement testSlotAdd().
    */
    public function testSlotAdd()
    {
        $ch = $this->object;

        $ch->eraseAllSlots(true);

        $max = 32;
        $wait = [];
        echo "Multi-curl test for $max urls parallel.\nslot Add: ";
        for ($i=1; $i<$max; $i++) {
            $curl_h = \curl_init();
            \curl_setopt($curl_h, \CURLOPT_URL, 'https://github.com/test' . $i);
            \curl_setopt_array($curl_h, $ch->curl_options);
            $slot = 's' . $i;
            $ans = $ch->slotAdd($slot, $curl_h);
            $this->assertFalse($ans);
            echo "[$slot]";
            $wait[] = $slot;
        }
        echo "\nFinished: ";
        for ($i=1; $i<$max+2; $i++) {
            $ans = $ch->slotWaitReady();
            if (is_array($ans)) {
                foreach($ans as $slot) {
                    echo "[$slot]";
                    $k = \array_search($slot, $wait);
                    unset($wait[$k]);
                }
            } else {
                echo " extra: $i = $ans ";
            }
        }
        $this->assertEquals(0, \count($wait));
        if (!\count($wait)) {
            echo "All slots finished\n";
        } else {
            echo "Not finished: " . \implode(", ", $wait);
        }

        $curl_h = curl_init();
        $slot = 'Test1';
        $ans = $ch->slotAdd($slot, $curl_h);
        $this->assertFalse($ans);

        // already exists slot name
        $slot = 'Test1';
        $ans = $ch->slotAdd($slot, $curl_h);
        $this->assertTrue(strpos($ans, 'lready ex') !== false);

        // already exists curl_h
        $slot = 'Test2';
        $ans = $ch->slotAdd($slot, $curl_h);
        $this->assertTrue(strpos($ans, 'curl error') !== false);

        // bad hook
        $slot = 'Test3';
        $ans = $ch->slotAdd($slot, $curl_h, 'hook');
        $this->assertTrue(strlen($ans) > 5);
    }
}
