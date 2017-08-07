<?php

namespace ierusalim\ClickHouse;

/**
 * This class contains functions for simple operations with ClickHouse
 *
 * Functions:
 * - >createTableQuick($table, $fields_arr) - create table with specified fields
 * - >sendFileInsert($file, $table) - send TabSeparated-file into table
 * - >dropTable($table [, $sess]) - drop table
 * - >clearTable($table [, $sess]) - clear table (DROP and re-create)
 * - >renameTable($from_name_or_arr [, $to_name] [, $sess]) - rename tables
 * - >getTableFields($table [, $sess]) - returns [field_name=>field_type] array
 * - >getTableInfo($table [, $extended]) - returns array with info about table
 *
 * - >getTablesList([$db] [,$pattern]) - returns tables list by SHOW TABLES request
 *
 * - >createDatabase($db) - create new database with specified name
 * - >dropDatabase($db) - drop specified database and remove all tables inside
 * - >getDatabasesList() - returns array contained names of existing Databases
 * - >setCurrentDatabase($db [, $sess]) - set current database by 'USE db' request
 * - >getCurrentDatabase([$sess]) - return results of 'SELECT currentDatabase()'
 *
 * - >getVersion() - return version of ClickHouse server (function moved to ClickHouseAPI)
 * - >getUptime() - return server uptime in seconds
 * - >getSystemSettings() - get information from system.settings as array [name=>value]
 *
 *
 * PHP Version >=5.4
 *
 * @package    ierusalim\ClickHouseFunctions
 * @author     Alexander Jer <alex@ierusalim.com>
 * @copyright  2017, Ierusalim
 * @license    https://opensource.org/licenses/Apache-2.0 Apache-2.0
 */
class ClickHouseFunctions extends ClickHouseQuery
{
    /**
     * List of fixed-size data-types of ClickHouse
     * [key - type in Canonical-case] => count of bytes occupied
     * Not listed: FixedString, String, Array
     *
     * @var array
     */
    public $types_fix_size = [
        'Date' => 2,
        'DateTime' => 4,
        'Int8' => 1,
        'UInt8' => 1,
        'Int16' => 2,
        'UInt16' => 2,
        'Int32' => 4,
        'UInt32' => 4,
        'Int64' => 8,
        'UInt64' => 8,
        'Float32' => 4,
        'Float64' => 8,
        'Enum8' => 1,
        'Enum16' => 2
    ];

    /**
     * List of aliases [key must be lowercase] => value must be canonical case.
     * @var array
     */
    public $types_aliases = [
        'string' => 'String',
        'fixedstring' => 'FixedString',
        'array' => 'Array',
        'tinyint' => 'Int8',
        'bool' => 'Int8',
        'boolean' => 'Int8',
        'smallint' => 'Int16',
        'int' => 'Int32',
        'integer' => 'Int32',
        'bigint' => 'Int64',
        'float' => 'Float64',
        'double' => 'Float64',
        'blob' => 'String',
        'text' => 'String',
        'char' => 'FixedString',
        'varchar' => 'FixedString',
        'binary' => 'FixedString',
        'varbinary' => 'FixedString',
        'timestamp' => 'DateTime',
        'time' => 'DateTime',
        'enum' => 'Enum8',
    ];

    /**
     * Cached info about field names and types for known tables
     *
     * Set in function getTableFields($table)
     * array keys is table names
     *
     * @var array of $fields_arr about fields of tables
     */
    public $table_structure_cached = [];

    private $from = ' FROM '; // For stupid code analyzers

    /**
     * Add alias for data-type
     * Return false if $for_type is unknown data-type.
     * Return canonical-type (string) if successful.
     *
     * @param string $for_type
     * @param string $add_alias
     * @return boolean|string
     */
    public function addTypeAlias($for_type, $add_alias)
    {
        $canon = '';
        if ($this->parseType($for_type, $canon) === false || empty($add_alias)) {
            return false;
        }
        $this->types_aliases[\strtolower($add_alias)] = $canon;
        $this->changeIfIsAlias('');
        return $canon;
    }

    /**
     * Delete data-type alias
     * Return false if alias not exist
     * Return true if successful removed.
     *
     * @param string $alias
     * @return boolean
     */
    public function delTypeAlias($alias)
    {
        $key = strtolower($alias);
        if (isset($this->types_aliases[$key])) {
            unset($this->types_aliases[$key]);
            $this->changeIfIsAlias('');
            return true;
        }
        return false;
    }

    /**
     * Returns canonical-type for specified $type_src data-type if alias found
     * or return value without changes.
     *
     * @param string $type_src
     * @return string
     */
    public function changeIfIsAlias($type_src)
    {
        static $aliases_arr = [];
        $t_lower = \strtolower($type_src);
        if (empty($aliases_arr) || empty($type_src)) {
            $aliases_arr = $this->types_aliases;
            foreach ($this->types_fix_size as $canon => $v) {
                $aliases_arr[strtolower($canon)] = $canon;
            }
        }
        return isset($aliases_arr[$t_lower]) ? $aliases_arr[$t_lower] : $type_src;
    }

    /**
     * Return false if ClickHouse data-type is unknown.
     * Otherwise return length in bytes for specified data-type.
     * Return 0 if length is not fixed (for String and Array).
     *
     * @param string $type_full (by ref)
     * @param string|null $name (by ref)
     * @param array|null $to_conv (by ref)
     * @return false|int Return false for unknown types, or length in bytes for known.
     */
    public function parseType(&$type_full, &$name = null, &$to_conv = null)
    {
        $i = \strpos($type_full, '(');
        if ($i) {
            $name = \substr($type_full, 0, $i);
        } else {
            $name = $type_full;
        }
        $cano_name = $this->changeIfIsAlias($name);
        if ($cano_name != $name) {
            $name = $cano_name;
            if ($i) {
                $type_full = $cano_name . \substr($type_full, $i);
                $i = strlen($cano_name);
            } else {
                $type_full = $cano_name;
            }
        }

        // function for convertation, like String -> toString(...)
        $to_conv = ['to' . $name . '(', ')'];

        if (isset($this->types_fix_size[$name])) {
            if (substr($name, 0, 4) === 'Enum') {
                $to_conv = ['CAST(', ' AS ' . $type_full . ')'];
            }
            return $this->types_fix_size[$name];
        }

        switch ($name) {
            case 'Array':
                $to_conv = false;
                return 0;
            case 'String':
                return 0;
            case 'FixedString':
                $j = \strpos($type_full, ')', $i);
                $j = \substr($type_full, $i + 1, $j - $i - 1);
                if (\is_numeric($j)) {
                    $to_conv[1] = ',' . $j . ')';
                    return (int) $j;
                }
        }
        return false;
    }

    /**
     * Create Table with name $table_name (MergeTree or ReplacingMergeTree engine)
     *
     * $fields_arr: [fields names] => field types[ default].
     * - First key $fields_arr means as primary key
     * - Must contains 'Date' field
     *
     * Each of field type may be specified in this formats:
     * - 'Int16'      - String without spaces.
     * - 'Int16 1234' - String field_type[space default_value]
     * - ['field_type' [, 'default_value']] - array, for example
     * - ['Int16', '1234'] - is same 'Int16 1234'.
     *
     * Data-types case insensitive and may be specified via aliases.
     *
     * Default values may be specified with DEFAULT keyword,
     *  in this case expression used exactly as specified.
     *  Example: 'Int16 DEFAULT 123+5'
     *
     * @param string $table table name
     * @param array $fields_arr keys=field names => field_type[ def]
     * @param integer $if_exists If table exists: 2=drop old table, 1-do nothing, 0-ret error)
     * @param string $ver if null, table will create as MergeTree, otherwise ReplacingMergeTree
     * @return boolean|string
     */
    public function createTableQuick($table, $fields_arr, $if_exists = 0, $ver = null)
    {
        if ($if_exists == 2) {
            $ans = $this->queryFalse("DROP TABLE IF EXISTS $table");
            if ($ans !== false) {
                return $ans;
            }
        }
        $sql_arr = $this->sqlTableQuick($table, $fields_arr, $if_exists);

        // If $ver defined, then change db-engine to ReplacingMergeTree
        if (!\is_null($ver)) {
            $sql_arr[6] = 'ReplacingMergeTree';
            $sql_arr[14] .= (empty($ver) ? '' : ", $ver");
        }

        return $this->queryFalse(\implode($sql_arr));
    }

    /**
     * Make SQL-query for CREATE TABLE $table_name in ClickHouse format.
     *
     * Parameter $field_arr described in annotation of createTableQuick function.
     *
     * If $if_not_exist not empty, "IF NOT EXISTS" will be included in request.
     *
     * @param string $table_name Table name for make sql-request
     * @param array $fields_arr Array of fields for table
     * @param integer|boolean $if_not_exist set true for adding "IF NOT EXISTS"
     * @return string[] returned array need implode to string
     * @throws \Exception
     */
    public function sqlTableQuick($table_name, $fields_arr, $if_not_exist = 1)
    {
        if (!\is_array($fields_arr) || \count($fields_arr) < 2) {
            throw new \Exception("Table must contain as least 1 field");
        }

        $fields_arr = $this->parseFieldsArr($fields_arr);

        $primary_field = \key($fields_arr);
        foreach ($fields_arr as $field_name => $field_par) {
            if ($field_par['type_name'] === 'Date') {
                $date_field = $field_name;
                break;
            }
        }
        if (empty($date_field)) {
            throw new \Exception("Table must contain field 'Date' type");
        }

        return [0 => 'CREATE TABLE ' . ($if_not_exist ? 'IF NOT EXISTS ' : ''),
                1 => $table_name,
                2 => ' ( ',
                3 => implode(", ", \array_column($fields_arr, 'create')),
                4 => ' ) ',
                5 => 'ENGINE = ',
                6 => 'MergeTree',
                7 => '(',
                8 => $date_field,
                9 => ', (',
                10 => $primary_field,
                11 => ', ',
                12 => $date_field,
                13 => ')',
                14 => ', 8192',
                15 => ')'];
    }

    /**
     * Parse source $fields_arr from format [field_names => types[ defaults]]
     * to format with keys [create, type_full, type_name, type_src, default, bytes]
     *
     * @param array $fields_arr Array of elements [field_name]=>[field_type]
     * @return array Each element contains [create, type_name, default, ...]
     * @throws \Exception
     */
    public function parseFieldsArr($fields_arr)
    {
        foreach ($fields_arr as $field_name => $create) {
            if (!is_array($create)) {
                // Can parse as string only scalar types, not Enum or Array
                $i = \strpos($create, ' ');
                if ($i) {
                    $create = [\substr($create, 0, $i), \substr($create, $i + 1)];
                } else {
                    $create = [$create];
                }
            }

            // make type_full, default, create strings from $create array
            $type_full = $type_src = $create[0];
            // make type_full, type_name, to_conv, bytes (from type_full)
            $type_name = $to_conv = null;
            $bytes = $this->parseType($type_full, $type_name, $to_conv);
            if ($bytes === false) {
                throw new \Exception("Unrecognized data type '$type_full'");
            }

            if (isset($create[1])) {
                $default = $create[1];
                $create = $type_full . ' DEFAULT ' . $default;
            } else {
                $default = '';
                $create = $type_full;
            }

            if (strlen($default)) {
                if (\substr($default, 0, 8) !== 'DEFAULT ') {
                    if ($to_conv) {
                        $lp = $to_conv[0];
                        $rp = $to_conv[1];
                        if (\substr($default, 0, \strlen($lp)) != $lp) {
                            $default = $lp . $this->quotePar($default) . $rp;
                        }
                        $create = 'DEFAULT ' . $default;
                    } else {
                        $default = $this->quotePar($default);
                        $create = $type_full . ' DEFAULT ' . $default;
                    }
                } else {
                    $create = $type_full . ' ' . $default;
                    $default = \substr($default, 8);
                }
            }
            $create = $field_name . ' ' . $create;
            $fields_arr[$field_name] = compact(
                'create',
                'type_full',
                'type_name',
                'type_src',
                'default',
                'bytes'
            );
        }
        return $fields_arr;
    }

    /**
     * Return as Array [names=>values] data from system.settings table
     *
     * @return array|string Array with results or String with error description
     */
    public function getSystemSettings()
    {
        return $this->queryKeyValues('system.settings', 'name, value');
    }

    /**
     * Return server uptime in seconds
     *
     * @return string|false Integer of server uptime (seconds) or false if error
     */
    public function getUptime()
    {
        return $this->queryValue('SELECT uptime()');
    }

    /**
     * Get current database name.
     *
     * if option 'database' is not empty, return database from options.
     * Otherwise using SQL-query 'SELECT currentDatabase()' for current or specified session
     *
     * Keep in mind that current database can be set in two ways:
     *  - by option 'database', in this case '&database=...' is sent with each request
     *  - by SQL-request 'USE $db' - it only makes sense when the sessions supported
     *
     * @param string|null|true $sess session_id (or true for read only 'database' option)
     * @return string|false String with current db-name or false if error
     */
    public function getCurrentDatabase($sess = null)
    {
        $this->to_slot = false;
        $database = $this->getOption('database');
        if (!empty($database) || $sess === true) {
            return $database;
        }
        return $this->queryValue('SELECT currentDatabase()', null, $sess);
    }

    /**
     * Set current database by name for current or specified session.
     *
     * Function send SQL-query 'USE $db' if sessions supported
     *
     * If sessions not supported or parameter $sess is boolean true,
     *  then set current database by option ->setOption('database', $db)
     *
     * @param string $db Database name
     * @param string|null|true $sess session_id or true for use database-option
     * @return string|false false if ok, or string with error description
     */
    public function setCurrentDatabase($db, $sess = null)
    {
        $this->to_slot = false;
        if ($sess === true || !$this->isSupported('session_id')) {
            $this->setOption('database', $db);
            return false;
        } else {
            return $this->queryFalse("USE " . $db, [], $sess);
        }
    }

    /**
     * Drop database and remove all tables inside it.
     *
     * @param string $db_name database name for drop (delete, remove)
     * @param boolean $if_exists Add condition "IF EXISTS"
     * @return false|string false if ok, or string with error description
     */
    public function dropDatabase($db_name, $if_exists = false)
    {
        $sql = "DROP DATABASE " . ($if_exists ? 'IF EXISTS ' : '');
        return $this->queryFalse($sql . $db_name);
    }

    /**
     * Create new database
     *
     * @param string $db_name database name for create
     * @param boolean $if_not_exists Add condition "IF NOT EXISTS"
     * @return false|string false if ok, or string with error description
     */
    public function createDatabase($db_name, $if_not_exists = false)
    {
        $sql = "CREATE DATABASE " . ($if_not_exists ? 'IF NOT EXISTS ' : '');
        return $this->queryFalse($sql . $db_name);
    }

    /**
     * Return Array contained names of Databases existing on ClickHouse server
     *
     * @return array|string Array with results or String with error description
     */
    public function getDatabasesList()
    {
        return $this->queryStrings('SHOW DATABASES');
    }

    /**
     * Return names of tables from specified database and/or like pattern
     *
     * @param string|null $name Database name
     * @param string|null $like_pattern pattern for search table, example: d%
     * @param string|null $sess session_id
     * @return array|string Results in array or string with error description
     */
    public function getTablesList($name = null, $like_pattern = null, $sess = null)
    {
        return $this->queryStrings('SHOW TABLES ' .
                    (empty($name) ? '' : $this->from . $name) .
                    (empty($like_pattern) ? '' : " LIKE '$like_pattern'"),
                    false, $sess);
    }

    /**
     * Get results of request "SHOW PROCESSLIST"
     *
     * @return array|string Results in array or string with error description
     */
    public function getProcessList()
    {
        return $this->queryArray('SHOW PROCESSLIST');
    }

    /**
     * Return as Array information about specified table
     *
     * Result Array is [Keys - field names] => [Values - field types]
     *
     * @param string $table Table name
     * @param boolean $renew_cache True - send query on server, false - can return from cache
     * @param string|null $sess session_id
     * @return array|string Results in array or string with error description
     */
    public function getTableFields($table, $renew_cache = true, $sess = null)
    {
        $slot = $this->to_slot;

        if ($renew_cache || !isset($this->table_structure_cached[$table])) {
            $results = $this->queryKeyValues("DESCRIBE TABLE $table", null, $sess);
            if (empty($slot)) {
                return $this->table_structure_cached[$table] = $results;
            }
        }
        $this->to_slot = false;

        if (empty($slot)) {
            return $this->table_structure_cached[$table];
        }

        if (isset($this->table_structure_cached[$table]) && !$renew_cache) {
            $this->slotEmulateResults($slot, $this->table_structure_cached[$table]);
        } else {
            $yi = function($table) {
                $this->table_structure_cached[$table] = $results = yield;
                yield $results;
            };
            $this->last_yi = $yi = $yi($table);
            $this->slotHookPush($slot,
                ['mode' => 1, 'fn' => $yi, 'par' => 'getTableFields']);
        }
        return $this;
    }

    /**
     * Return array with numbers from ClickHouse server for tests
     *
     * @param integer $lim Limit of numbers
     * @param boolean $use_mt true for using table system.numbers_mt
     * @return array|string Results in array or string with error description
     */
    public function getNumbers($lim = 100, $use_mt = false)
    {
        return $this->queryStrings(
            'SELECT * FROM system.numbers' . ($use_mt ? '_mt' : '') .
            ' LIMIT ' . $lim);
    }

    /**
     * Return information about size of table row by fields definition.
     *
     * If parameter have string-type, its means as table name.
     *
     * If parameter is array, it is seen as $fileds_arr with fields definition.
     *
     * @param string|array $table_or_fields_arr (string)$table | (array)$fields_arr
     * @return array|string [fixed_bytes, fixed_fields, dynamic_fields, comment]
     */
    public function getTableRowSize($table_or_fields_arr)
    {
        if (!\is_array($table_or_fields_arr)) {
            $table_name = $table_or_fields_arr;
            $fields_arr = $this->getTableFields($table_name);
            if (!\is_array($fields_arr)) {
                return $fields_arr;
            }
        } else {
            $fields_arr = $table_or_fields_arr;
            $table_name = null;
        }
        $fixed_bytes = $this->countRowFixedSize($fields_arr);
        if (!\is_array($fixed_bytes)) {
            return $fixed_bytes;
        }
        \extract($fixed_bytes); // fixed_bytes, dynamic_fields
        $fixed_fields = \count($fields_arr) - $dynamic_fields;
        $comment = "$fixed_bytes bytes in $fixed_fields FIXED FIELDS, $dynamic_fields DYNAMIC FIELDS";
        return \compact('table_name', 'fixed_bytes', 'fixed_fields', 'dynamic_fields', 'comment');
    }

    /**
     * Count and return summary of fixed-size fields by $fields_arr
     *
     * Addition, in $dynamic_fields (by ref) return count of dynamic-fields.
     *
     * @param array $fields_arr Array [field_name]=>[field_type]
     * @param integer $dynamic_fields (by reference)
     * @return array|string Array of [fixed_bytes,dynamic_fields] or string with error description
     */
    public function countRowFixedSize($fields_arr)
    {
        if (!\is_array($fields_arr) || !\count($fields_arr)) {
            return "Need array";
        }
        try {
            // Parse fields array and check types
            $parsed_arr = $this->parseFieldsArr($fields_arr);
        } catch (\Exception $e) {
            return $e->getMessage();
        }
        $fixed_bytes = $dynamic_fields = 0;
        foreach (\array_column($parsed_arr, 'bytes') as $bytes) {
            if ($bytes) {
                $fixed_bytes += $bytes;
            } else {
                $dynamic_fields++;
            }
        }
        return compact('fixed_bytes', 'dynamic_fields');
    }

    /**
     * Return information about $table_name from ClickHosue server system tables.
     * - If $extended_info is 0, make 1 request (from system.columns table)
     * - If $extended_info is 1, make 2 requests (+[engine] field from system.tables)
     * - If $extended_info is 2, make 3 requests (+[create] field)
     * - If $extended_info is 3, make 4 requests (+[system.merge] field)
     * - If $extended_info is 4, make 5 requests (+[system.replicas] field)
     * - If $extended_info is 5, make 6 requests (+[system.parts] field)
     * - if -1 or >5 then as 5, grab all system information about table (6 req.)
     *
     * @param string $table Table name [db.]table
     * @param integer $extended if 0 make one sql-query, if 1 add [engine], if 2 full
     * @return array|string Results in array or string with error description
     */
    public function getTableInfo($table, $extended = 2)
    {
        $slot = $this->to_slot;

        $slot_prefix = substr(md5(microtime()), 0, 8) . $table . '_';

        if (empty($slot)) {
            $root_slot = $slot_prefix . 'col';
            $this->toSlot($root_slot);
        } else {
            $root_slot = $slot;
        }
        $slots = ['root' => $root_slot];

        $this->queryTableSubstract($table);

        $i = \strpos($table, '.');
        if ($i) {
            $db = \substr($table, 0, $i);
            $table = \substr($table, $i + 1);
        } else {
            $db = $this->getCurrentDatabase();
        }

        $dbtb = $db . '.' . $table;

        if ($ex = $extended) {
            $slots['tables'] = $slot_prefix . '_tab_'. $table;
            $this->toSlot($slots['tables']);
            $this->queryTableSys($dbtb, 'tables', ['d', 't', 'n']);
            if (--$ex) {
                $slots['create'] = $slot_prefix . 'cre';
                $this->toSlot($slots['create']);
                $this->queryValue("SHOW CREATE TABLE $dbtb");
            }
        }
        if ($ex) {
            foreach (['merges', 'replicas', 'parts'] as $sys) {
                if (!--$ex) {
                    break;
                }
                $slots[$sys] = $slot_prefix . $sys;
                $this->toSlot($slots[$sys]);
                $this->queryTableSys($dbtb, $sys, ['d', 't', 'n']);
            }
        }

        $yi = function ($slots, $table, $extended) {
            $columns_arr = yield; //results of queryTableSubstract($table);
            if (!is_array($columns_arr)) {
                yield $columns_arr;
            }
            \extract($columns_arr); //Keys is database, table, columns_arr

            $fields_arr = [];
            $sum_compressed_bytes = $sum_uncompressed_bytes = 0;
            foreach ($columns_arr as $col_name => $col_arr) {
                $sum_compressed_bytes += $col_arr['data_compressed_bytes'];
                $column_bytes = $col_arr['data_uncompressed_bytes'];
                $sum_uncompressed_bytes += $column_bytes;
                $fixed_bytes = $this->parseType($col_arr['type']);
                if (!empty($fixed_bytes)) {
                    $col_arr['fixed_bytes'] = $fixed_bytes;
                    $col_rows_cnt = $column_bytes / $fixed_bytes;
                    $rows_cnt = $col_rows_cnt;
                    $col_arr['rows_cnt'] = $col_rows_cnt;
                }
                $columns_arr[$col_name] = $col_arr;
                $fields_arr[$col_name] = $col_arr['type'];
            }
            $ret_arr = $this->getTableRowSize($fields_arr);
            $ret_arr['table_name'] = $database . '.' . $table;
            if ($extended) {
                //results of queryTableSys($dbtb, 'tables', ['d', 't', 'n']);
                $engine = $this->slotResults($slots['tables']);
                foreach ($engine as $col_name => $sys) {
                    $ret_arr[$col_name] = $sys;
                }
                if (--$extended) {
                     //results of queryValue("SHOW CREATE TABLE $dbtb");
                    $ret_arr['create'] = $this->slotResults($slots['create']);
                }
            }

            $ret_arr['uncompressed_bytes'] = $sum_uncompressed_bytes;
            $ret_arr['compressed_bytes'] = $sum_compressed_bytes;
            $ret_arr['rows_cnt'] = is_null($rows_cnt) ? "Unknown" : $rows_cnt;
            $ret_arr['columns_cnt'] = \count($columns_arr);
            $ret_arr['columns'] = $columns_arr;
            if ($extended) {
                foreach (['merges', 'replicas', 'parts'] as $sys) {
                    if (!--$extended) {
                        break;
                    }
                    // results of queryTableSys($dbtb, $sys, ['d', 't', 'n']);
                    $ret_arr['system.' . $sys] = $this->slotResults($slots[$sys]);
                }
            }

            foreach ($slots as $slot) {
                $this->eraseSlot($slot);
            }
            yield $ret_arr;
        };
        $this->last_yi = $yi = $yi($slots, $table, $extended);

        $this->slotHookPush($slots['root'],
            ['mode' => 1, 'fn' => $yi, 'par' => 'getTableInfo']);

        if (empty($slot)) {
            return $this->slotResults($slots['root']);
        }

        return $this;
    }

    /**
     * Drop table
     *
     * @param string $table [db.]table for drop
     * @param string|null $sess session_id
     * @return boolean|string Return false if ok, or string with error description
     */
    public function dropTable($table, $sess = null)
    {
        return $this->queryFalse("DROP TABLE $table", [], $sess);
    }

    /**
     * Clear table (DROP and CREATE by creation request)
     *
     * @param string $table [db.]table for drop and re-create
     * @param string|null $sess session_id
     * @return boolean|string Return false if ok, or string with error description
     */
    public function clearTable($table, $sess = null)
    {
        $slot = $this->to_slot;
        $yi = function ($table, $sess, $slot) {
            $create_request = (yield $this->queryValue("SHOW CREATE TABLE $table", null, $sess));
            if ($create_request === false) {
                yield "Can't clear '$table' because no information about its creation.";
            }
            // Not asynchronous for drop/create, because requests depend on each other
            $this->queryFalse("DROP TABLE IF EXISTS $table", [], $sess);
            yield $this->queryFalse($create_request, [], $sess);
        };
        $this->last_yi = $yi = $yi($table, $sess, $slot);
        $create_request = $yi->current();
        if (empty($slot)) {
            return $yi->send($create_request);
        }
        $this->slotHookPush($slot,
            ['mode' => 1, 'fn' => $yi, 'par' => 'clearTable']);

        return $this;
    }

    /**
     * Rename one table(from_name, to_name) or array of tables
     *
     * To rename many tables with one query, need to set an array, example:
     * - >renameTable(['from_name1' => 'to_name1', 'from_name2' => 'to_name2'])
     *
     * @param string|array $from_name_or_arr Old name or array [oldname=>newname,...]
     * @param string|null $to_name New name, or ignored if first parameter is array
     * @param string|null $sess session_id
     * @return boolean|string Return false if ok, or string with error description
     */
    public function renameTable($from_name_or_arr, $to_name = null, $sess = null)
    {
        if (!\is_array($from_name_or_arr)) {
            if (empty($to_name)) {
                return "table name not specified";
            } else {
                $from_name_or_arr = [$from_name_or_arr => $to_name];
            }
        }
        $sql = '';
        foreach ($from_name_or_arr as $from_name => $to_name) {
            if (empty($sql)) {
                $sql = "RENAME TABLE ";
            } else {
                $sql .= ", ";
            }
            $sql .= "`$from_name` TO `$to_name`";
        }
        return $this->queryFalse($sql, [], $sess);
    }

    /**
     * Send file to ClickHouse-server and insert into table
     *
     * File must have TabSeparated format and number of columns must match the table.
     *
     * @param string $file File with TabSeparated data
     * @param string $table Table for inserting data
     * @param boolean $only_return_structure Do not send file if true, only return par.
     * @return boolean|string|array Return false if ok, or string with error description
     */
    public function sendFileInsert($file, $table, $only_return_structure = false)
    {
        $fields_arr = $this->getTableFields($table, false);
        if (!\is_array($fields_arr)) {
            return $fields_arr;
        }
        // Read first line from file
        if (empty($file) || !\is_file($file) || !($f = \fopen($file, 'r'))) {
            return "Can't read file $file";
        } else {
            $fs = \fgets($f, 65535);
            \fclose($f);
            $fs = \explode("\t", \trim($fs));
            if (\count($fs) != \count($fields_arr)) {
                return "Can't autodetect format $file , " .
                    \count($fs) . ' col. found in first line of file, must have ' .
                    \count($fields_arr) . " tab-separated columns (as in $table)";
            }
        }
        $file_structure = [];
        $selector = [];
        $type_name = $to_conv = null;
        $n = 0;
        foreach ($fields_arr as $field_name => $to_type) {
            $this->parseType($to_type, $type_name, $to_conv);
            if (\strpos($type_name, 'Int') !== false && \is_numeric($fs[$n])) {
                $need_conv = false;
            } elseif ($type_name !== 'String') {
                $need_conv = \is_array($to_conv);
            } else {
                $need_conv = false;
            }
            if ($need_conv) {
                if (($type_name == 'Date' || $type_name == 'DateTime') &&
                    \is_numeric($fs[$n]) && $fs[$n] > 65536) {
                    $from_type = 'UInt32';
                } else {
                    $from_type = "String";
                }
                $sel = $to_conv[0] . $field_name . $to_conv[1];
            } else {
                $from_type = $to_type;
                $sel = $field_name;
            }
            $selector[] = $sel;
            $file_structure[] = $field_name . ' ' . $from_type;
            $n++;
        }
        $file_structure = \implode(", ", $file_structure);
        $selector = \implode(', ', $selector);
        if ($only_return_structure) {
            return \compact('file_structure', 'selector');
        }
        return $this->queryInsertFile($table, $file, $file_structure, $selector);
    }
}
