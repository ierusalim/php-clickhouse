<?php
namespace ierusalim\ClickHouse;

class ClickHouseTableParserTest extends \PHPUnit_Framework_TestCase
{

    /**
     * @var ClickHouseSQLParser
     */
    protected $object;

    /**
     * Sets up the fixture, for example, opens a network connection.
     * This method is called before a test is executed.
     */
    protected function setUp()
    {
        $this->object = new ClickHouseSQLParser;
        //$this->object = new ClickHouseTableType; // use inside as trait
    }

    /**
     * Provider of SQL-requests for create engine
     *
     * @return array
     */
    public function createEngineProvider()
    {
        return [
            #0
            ["Engine = MergeTree(a, b, 8192)",
            ['engine' => 'MergeTree',
             'engine_param' => ['a', 'b', 8192],
             'merge_tree_fam' => true,
             'date_field' => 'a',
             'sampl' => false,
             'primary_key' => 'b',
             'granul' => 8192,
             'ver' => false
            ]],

            #1
            ["ENGINE = MergeTree(dt, (id, dt), 8192, ver)",
            ['engine' => 'MergeTree',
             'engine_param' => ['dt', '(id, dt)', 8192, 'ver'],
             'merge_tree_fam' => true,
             'date_field' => 'dt',
             'sampl' => false,
             'primary_key' => ['id', 'dt'],
             'granul' => 8192,
             'ver' => 'ver'
            ]],

            #2
            ["ENGINE = MergeTree(dt, (id, dt), 8192)",
            ['engine' => 'MergeTree',
             'engine_param' => ['dt', '(id, dt)', 8192],
             'merge_tree_fam' => true,
             'date_field' => 'dt',
             'sampl' => false,
             'primary_key' => ['id', 'dt'],
             'granul' => 8192,
             'ver' => ''
            ]],

            #3
            ["ENGINE = MergeTree(dt, sampl, (id, dt), 8192)",
            ['engine' => 'MergeTree',
             'engine_param' => ['dt', 'sampl', '(id, dt)', 8192],
             'merge_tree_fam' => true,
             'date_field' => 'dt',
             'sampl' => 'sampl',
             'primary_key' => ['id', 'dt'],
             'granul' => 8192,
             'ver' => ''
            ]],

            #4
            ["ENGINE = Log",
            ['engine' => 'Log',
             'engine_param' => [''],
             'merge_tree_fam' => false,
             'date_field' => '',
             'sampl' => '',
             'primary_key' => '',
             'granul' => '',
             'ver' => ''
            ]],

            #5
            ["ENGINE = Log(", false],

            #6
            ["IMAGINE = Log", false],

            #7
            ["Engine = MergeTree(a, (), 8192)", false],
        ];
    }

    /**
     * @dataProvider createEngineProvider
     * @covers ierusalim\ClickHouse\ClickHouseSQLParser::parseCreateEngineSql
     * @todo   Implement testParseCreateEngineSql().
     */
    public function testParseCreateEngineSql($sql, $regpars)
    {
        $ch = $this->object;
        $ans = $ch->parseCreateEngineSql($sql);
        if ($regpars) {
            foreach($regpars as $key => $val) {
                $this->assertEquals($val, $ans[$key]);
                unset($ans[$key]);
            }
            foreach($ans as $key => $val) {
                if ($val) {
                    echo "\n[$key] => '$val'; // -- unexpected in ";
                    print_r($ans);
                    break;
                }
            }
        } else {
            if (\is_array($ans)) {
                print_r($ans);
            }
            $this->assertFalse(\is_array($ans));
        }
    }

     /**
     * Provider of SQL-requests for create table
     *
     * @return array
     */
    public function tableCreaterFnParserProvider()
    {
        $fields = "(id UInt32, dt Date DEFAULT now(), s String)";
        $engine = "ENGINE = MergeTree(dt, (id, dt), 8192, ver)";
        return [
            //$sql, $regpars, $subfn, $exparr
            [   //sql
                "CREATE TABLE db.x $fields $engine",
                //regpars
                [
                    'create_fn' => 'CREATE TABLE db.x',
                    'create_par' => "$fields $engine",
                    'temporary' => false,
                    'if_not_exists' => false,
                    'database' => 'db',
                    'table' => 'x',
                    'dbtb' => 'db.x',
                    'as' => false,
                    'asdb' => false,
                    'astbl' => false,
                    'on_cluster' => false,
                    'cluster' => false,
                ],
                0,0],

            [   //sql
                "CREATE TEMPORARY TABLE IF NOT EXISTS db.x AS abc $fields $engine",
                //regpars
                [
                    'create_fn' => 'CREATE TEMPORARY TABLE IF NOT EXISTS db.x AS abc',
                    'create_par' => "$fields $engine",
                    'temporary' => true,
                    'if_not_exists' => true,
                    'database' => 'db',
                    'table' => 'x',
                    'dbtb' => 'db.x',
                    'as' => true,
                    'asdb' => false,
                    'astbl' => 'abc',
                    'on_cluster' => false,
                    'cluster' => false,
                ],
                0,0],

            [   //sql
                "CREATE TEMPORARY TABLE IF NOT EXISTS db.x AS `xxx yyy`.abc ON CLUSTER xxx $fields $engine",
                //regpars
                [
                    'create_fn' => 'CREATE TEMPORARY TABLE IF NOT EXISTS db.x AS `xxx yyy`.abc ON CLUSTER xxx',
                    'create_par' => "$fields $engine",
                    'temporary' => true,
                    'if_not_exists' => true,
                    'database' => 'db',
                    'table' => 'x',
                    'dbtb' => 'db.x',
                    'as' => true,
                    'asdb' => 'xxx yyy',
                    'astbl' => 'abc',
                    'on_cluster' => true,
                    'cluster' => 'xxx',
                ],
                0,0],

            [   //sql
                "CREATE TEMPORARY TABLE IF NOT EXISTS db.x ON CLUSTER `The cluster` $fields $engine",
                //regpars
                [
                    'create_fn' => 'CREATE TEMPORARY TABLE IF NOT EXISTS db.x ON CLUSTER `The cluster`',
                    'create_par' => "$fields $engine",
                    'temporary' => true,
                    'if_not_exists' => true,
                    'database' => 'db',
                    'table' => 'x',
                    'dbtb' => 'db.x',
                    'as' => false,
                    'asdb' => false,
                    'astbl' => false,
                    'on_cluster' => true,
                    'cluster' => 'The cluster',
                ],
                0,0],

            [   //sql
                "CREATE TABLE db.x $fields $engine",
                //regpars
                [
                    'create_fn' => 'CREATE TABLE db.x',
                    'create_par' => "$fields $engine",
                    'database' => 'db',
                    'table' => 'x',
                    'dbtb' => 'db.x',
                ],
                0,0
            ],

            [   //sql
                "CREATE TABLE tbl $fields $engine",
                //regpars
                [
                    'create_fn' => 'CREATE TABLE tbl',
                    'create_par' => "$fields $engine",
                    'database' => false,
                    'table' => 'tbl',
                    'dbtb' => 'tbl',
                ],
                0,0
            ],

            [   //sql
                "CREATE TABLE `tbl` $fields $engine",
                //regpars
                [
                    'create_fn' => 'CREATE TABLE `tbl`',
                    'create_par' => "$fields $engine",
                    'database' => false,
                    'table' => 'tbl',
                    'dbtb' => '`tbl`',
                ],
                0,0
            ],

            [   //sql
                "CREATE TABLE `db.tbl` $fields $engine",
                //regpars
                [
                    'create_fn' => 'CREATE TABLE `db.tbl`',
                    'create_par' => "$fields $engine",
                    'database' => false,
                    'table' => 'db.tbl',
                    'dbtb' => '`db.tbl`',
                ],
                0,0
            ],

            [   //sql
                "CREATE TABLE `The database`.table $fields $engine",
                //regpars
                [
                    'create_fn' => 'CREATE TABLE `The database`.table',
                    'create_par' => "$fields $engine",
                    'database' => 'The database',
                    'table' => 'table',
                    'dbtb' => '`The database`.table',
                ],
                0,0
            ],

            [   //sql
                "CREATE TABLE db.`The table` $fields $engine",
                //regpars
                [
                    'create_fn' => 'CREATE TABLE db.`The table`',
                    'create_par' => "$fields $engine",
                    'database' => 'db',
                    'table' => 'The table',
                    'dbtb' => 'db.`The table`',
                ],
                0,0
            ],

            [   //sql
                "CREATE TABLE `the database`.`and The table` $fields $engine",
                //regpars
                [
                    'create_fn' => 'CREATE TABLE `the database`.`and The table`',
                    'create_par' => "$fields $engine",
                    'database' => 'the database',
                    'table' => 'and The table',
                    'dbtb' => '`the database`.`and The table`',
                ],
                0,0
            ],

            [   //sql
                "Create  temporarY  table  if  NOT  exists `the database`.`and The table` as `tbl`"
                . " On Cluster clust $fields $engine",
                //regpars
                [
                    'create_fn' => "Create  temporarY  table  if  NOT  exists `the database`.`and The table` as `tbl`"
                . " On Cluster clust",
                    'create_par' => "$fields $engine",
                    'database' => 'the database',
                    'table' => 'and The table',
                    'dbtb' => '`the database`.`and The table`',

                    'temporary' => true,
                    'if_not_exists' => true,
                    'as' => true,
                    'asdb' => false,
                    'astbl' => 'tbl',
                    'on_cluster' => true,
                    'cluster' => 'clust',
                ],
                0,0
            ],

            [   //sql
                'CREATE
                    TABLE
                        `d b`.`t b l`
                    '. $fields . ' ' . $engine,
                //regpars
                [
                    'create_fn' =>
                "CREATE
                    TABLE
                        `d b`.`t b l`",
                    'create_par' => "$fields $engine",
                    'database' => 'd b',
                    'table' => 't b l',
                    'dbtb' => '`d b`.`t b l`',
                ],
                0,0
            ],
        ];
    }

    /**
     * Provider of SQL-requests for create table
     *
     * @return array
     */
    public function createTableSQLRequestsProvider()
    {
        $fields = "(id UInt32, dt Date DEFAULT now(), s String)";
        $engine = "ENGINE = MergeTree(dt, (id, dt), 8192, ver)";
        $err = "Can't parse string as 'CREATE TABLE'-sql-request";
        return [
            [   //sql
                "CREATE TABLE db.x $fields $engine",
                //regpars
                [
                'create_fn' => 'CREATE TABLE db.x',
                'table' => 'x',
                'database' => 'db',
                'temporary' => false,
                'if_not_exists' => false,
                'asdb' => false,
                'astbl' => false,
                'cluster' => false,
                'create_fields' => 3,
                'names' => ['id', 'dt', 's'],
                'parse_fields' => 3,
                'create_engine' => 'ENGINE = MergeTree(dt, (id, dt), 8192, ver)',
                'engine' => 'MergeTree',
                'engine_param' => ['dt', '(id, dt)', 8192, 'ver'],
                'merge_tree_fam' => 1,
                'date_field' => 'dt',
                'sampl' => false,
                'primary_key' => ['id', 'dt'],
                'granul' => 8192,
                'ver' => 'ver'
                ]
            ],

            [
                "CREATE TOBLE db.x $fields $engine",
                $err
            ],

            [
                "CREATE TABLE db.x $fields Emgine = 123(456)",
                "Expected 'engine', unexpected 'Emgine '"
            ],

            [
                "CREATE TABLE db.x {a() Engine = 123(456)",
                $err
            ],

            [
                "CREATE TABLE db.x ( Engine = 123",
                $err
            ],

            [
                "CREATE TABLE `dv`.`partd` AS dx.`part` ENGINE = "
                . "Distributed(perftest_3shards_1replicas, default, part, rand());",
                [
                    'create_fn' => 'CREATE TABLE `dv`.`partd` AS dx.`part`',
                    'table' => 'partd',
                    'database' => 'dv',
                    'temporary' => false,
                    'if_not_exists' => false,
                    'asdb' => 'dx',
                    'astbl' => 'part',
                    'cluster' => false,
                    'create_fields' => 0,
                    'create_engine' => 'ENGINE = Distributed(perftest_3shards_1replicas, default, part, rand());',
                    'engine' => 'Distributed',
                    'engine_param' => [
                        'perftest_3shards_1replicas',
                        'default',
                        'part',
                        'rand()',
                        ],

                    'merge_tree_fam' => false,
                    'date_field' => false,
                    'sampl' => false,
                    'primary_key' => false,
                    'granul' => false,
                    'ver' => false,
                ]
            ],

            [
                "CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster (p Date, i Int32)"
                . " ENGINE = Distributed(cluster, default, hits)",
                [
                    'create_fn' => 'CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster',
                    'table' => 'all_hits',
                    'database' => false,
                    'temporary' => false,
                    'if_not_exists' => true,
                    'asdb' => false,
                    'astbl' => false,
                    'cluster' => 'cluster',
                    'names' => ['p', 'i'],
                    'create_fields' => 2,
                    'parse_fields' => 2,
                    'create_engine' => 'ENGINE = Distributed(cluster, default, hits)',
                    'engine' => 'Distributed',
                    'engine_param' => [ 'cluster', 'default', 'hits' ],

                    'merge_tree_fam' => false,
                    'date_field' => false,
                    'sampl' => false,
                    'primary_key' => false,
                    'granul' => false,
                    'ver' => false,
                ]
            ],

            [
                "CREATE TABLE rankings_tiny (
                    pageURL String,
                    pageRank UInt32,
                    avgDuration UInt32
                ) ENGINE = Log;",
                [
                    'create_fn' => 'CREATE TABLE rankings_tiny',
                    'table' => 'rankings_tiny',
                    'database' => false,
                    'temporary' => false,
                    'if_not_exists' => false,
                    'asdb' => false,
                    'astbl' => false,
                    'cluster' => false,
                    'create_fields' => 3,
                    'parse_fields' => 3,
                    'names' => ['pageURL', 'pageRank', 'avgDuration'],
                    'create_engine' => 'ENGINE = Log;',
                    'engine' => 'Log;',
                    'engine_param' => [''],
                    'merge_tree_fam' => false,
                    'date_field' => false,
                    'sampl' => false,
                    'primary_key' => false,
                    'granul' => false,
                    'ver' => false,
                ]
            ]
        ];
    }

    /**
     * @dataProvider createTableSQLRequestsProvider
     * @covers ierusalim\ClickHouse\ClickHouseSQLParser::parseCreateTableSql
     * @todo   Implement testParseCreateTableSql().
     */
    public function testParseCreateTableSql($sql, $results)
    {
        $ch = $this->object;
        $ans = $ch->parseCreateTableSql($sql);
        if (\is_array($results)) {
            foreach(['create_fields', 'parse_fields'] as $key) {
                if (isset($results[$key])) {
                    $this->assertEquals($results[$key], \count($ans[$key]));
                    unset($results[$key]);
                    unset($ans[$key]);
                }
            }
        }
        if ($results === true) {
            print_r($ans);
        } else {
            $this->assertEquals($results, $ans);
        }
//      print_r($ans);
//      $this->assertEquals($exparr, $ans);
    }

    /**
     * @dataProvider tableCreaterFnParserProvider
     * @covers ierusalim\ClickHouse\ClickHouseSQLParser::tableCreaterFnParser
     * @todo   Implement testTableCreaterFnParser().
     */
    public function testTableCreaterFnParser($sql, $regpars, $subfn, $exparr)
    {
        $ch = $this->object;
        $ans = $ch->tableCreaterFnParser($sql);
        foreach($regpars as $key => $val) {
            $this->assertEquals($val, $ans[$key]);
            unset($ans[$key]);
        }
        foreach($ans as $key => $val) {
            if ($val) {
                echo "\n[$key] => '$val'; // -- unexpected in ";
                print_r($ans);
                break;
            }
        }
    }

    /**
     * @return array
     */
    public function nameTypeDefProvider()
    {
        return [
            ['id', false],

            ['id UInt8', [
                'id',       //name
                'UInt8',    //type
                '',         //default
                'UInt8',    //type_explicit
                '']],       //default_explicit

            ['id uint8', [
                'id',
                'UInt8',
                '',
                'uint8',
                '']],

            ['`the id` uint8', [
                '`the id`',
                'UInt8',
                '',
                'uint8',
                '']],

            ['id 1', [
                'id',
                'UInt8',
                '1',
                '',
                '1']],

            ['dt now()', [
                'dt',
                'DateTime',
                'now()',
                '',
                'now()']],

            ['s toString(now())', [
                's',
                'String',
                'toString(now())',
                '',
                'toString(now())']],

            ['id toInt8(now()', false],

            ['dt Date now()', [
                'dt',
                'Date',
                'toDate(now())',
                'Date',
                'now()']],

            ['dt DateTime now()', [
                'dt',
                'DateTime',
                'now()',
                'DateTime',
                'now()']],

            ['x now()', [
                'x',
                'DateTime',
                'now()',
                '',
                'now()']],

            ['t d', [
                't',
                '',
                'd',
                '',
                'd']],

            ['t default d', [
                't',
                '',
                'd',
                '',
                'd']],

            ['id uint32 default 1', [
                'id',
                'UInt32',
                'toUInt32(1)',
                'uint32',
                '1']],

            ['id default 1', [
                'id',
                'UInt8',
                '1',
                '',
                '1']],

            ['`id test` uint32 default toUInt16(10)', [
                '`id test`',
                'UInt32',
                'toUInt32(toUInt16(10))',
                'uint32',
                'toUInt16(10)']],

            ["`The enum8 field` enum8('a'=1,'b'=2) 'a'", [
                "`The enum8 field`",
                "Enum8('a'=1,'b'=2)",
                "CAST('a' AS Enum8('a'=1,'b'=2))",
                "enum8('a'=1,'b'=2)",
                "'a'"]],

            ["`The enum8 field` default CAST('a' AS Enum8('a'=1,'b'=2))", [
                "`The enum8 field`",
                "Enum8('a'=1,'b'=2)",
                "CAST('a' AS Enum8('a'=1,'b'=2))",
                "",
                "CAST('a' AS Enum8('a'=1,'b'=2))"]],

            ["`The array field`  [1,2,3,4]", [
                "`The array field`",
                "Array(UInt8)",
                "[1,2,3,4]",
                "",
                "[1,2,3,4]"]],

            ["nulid Nullable(UInt8) [1,2,3,4]", [
                'nulid',
                "Nullable(UInt8)",
                "CAST([1,2,3,4] AS Nullable(UInt8))",
                "Nullable(UInt8)",
                "[1,2,3,4]"]],
        ];
    }

    /**
     * @dataProvider nameTypeDefProvider
     * @covers ierusalim\ClickHouse\ClickHouseSQLParser::parseNameTypeDefault
     * @todo   Implement testParseNameTypeDefault().
     */
    public function testParseNameTypeDefault($typedef_str, $typedef_arr)
    {
        $ch = $this->object;
        $ans = $ch->parseNameTypeDefault($typedef_str);
        if (\is_array($ans)) {
            \extract($ans);
            $repack = [$name, $type, $default, $type_explicit, $default_explicit];
            if ($typedef_arr != $repack) {
                print_r($ans);
            }
            $this->assertEquals($typedef_arr, $repack);
        } else {
            if ($typedef_arr === false) {
                $this->assertFalse($ans);
            } else {
                $this->assertFalse(1);
            }
        }
    }

    /**
     * Data provider for testMakeFieldCreater
     *
     * @return array of name, type, default, creater
     */
    public function makeFieldCreaterProvider()
    {
        return [
          ['id', 'UInt32', false, 'id UInt32'],
          ['', 'UInt32', false, false],
          ['dt', '', '', false],
          ['dt', '', 'now()', 'dt DEFAULT now()'],
          ['dt', '', 'how()', false],
        ];
    }

    /**
     * @dataProvider makeFieldCreaterProvider
     * @covers ierusalim\ClickHouse\ClickHouseSQLParser::makeFieldCreater
     * @todo   Implement testMakeFieldCreater().
     */
    public function testMakeFieldCreater($name, $type, $default, $creater)
    {
        $ch = $this->object;
        $ans = $ch->makeFieldCreater($name, $type, $default);
        $this->assertEquals($creater, $ans);
    }

    /**
     * Data provider for testIsValidName
     *
     * @return array of name, isValid, ret_name
     */
    public function isValidNameProvider()
    {
        return [
          ['id', true, false],
          ['id', 'id', true],
          ['', false, false],
        ];
    }

    /**
     * @dataProvider isValidNameProvider
     * @covers ierusalim\ClickHouse\ClickHouseSQLParser::isValidName
     * @todo   Implement testMakeFieldCreater().
     */
    public function testIsValidName($name, $isValid, $ret_name)
    {
        $ch = $this->object;
        $ans = $ch->isValidName($name, $ret_name);
        $this->assertEquals($isValid, $ans);
    }

    /**
     * Data provider for testParseCreateFields
     *
     * @return array of name, type, default, creater
     */
    public function parseCreateFieldsProvider()
    {
        return [
            ['id UInt32, dt Date',
            ['id','dt'], [
                'id' => [
                    'type' => 'UInt32',
                    'type_calc' => 'UInt32',
                    'default' => false,
                    'default_calc'=> false,
                    ],
                'dt' => [
                    'type' => 'Date',
                    'type_calc' => 'Date',
                    'default' => false,
                    'default_calc'=> false,
                ],
            ]],

            [['id UInt32', 'dt Date'],
            ['id','dt'], [
                'id' => [
                    'type' => 'UInt32',
                    'type_calc' => 'UInt32',
                    'default' => false,
                    'default_calc'=> false,
                    ],
                'dt' => [
                    'type' => 'Date',
                    'type_calc' => 'Date',
                    'default' => false,
                    'default_calc'=> false,
                ],
            ]],

            ['id UInt32, dt Date, id as Int16', "Already exists field 'id'", 0],

            ['id, dt Date', "All field-create strings must have format 'fieldname parameters'", 0],

            ['id, dt (', "Invalid parameter", 0],

        ];
    }

    /**
     * @dataProvider parseCreateFieldsProvider
     * @covers ierusalim\ClickHouse\ClickHouseSQLParser::parseCreateFields
     * @todo   Implement testParseCreateFields().
     */
    public function testParseCreateFields($create_fields, $names, $parse_fields)
    {
        $ch = $this->object;
        $ans = $ch->parseCreateFields($create_fields);
        if (\is_array($names)) {
            $this->assertEquals($names, $ans['names']);
            $this->assertEquals($parse_fields, $ans['parse_fields']);
        } else {
//            print_r($ans);
            $this->assertEquals($names, $ans);
        }
    }
}
