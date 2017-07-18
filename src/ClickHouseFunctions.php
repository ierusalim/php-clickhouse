<?php
namespace ierusalim\ClickHouse;

/**
 * This class contains functions for simple operations with ClickHouse
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
        static $aliases_arr = false;
        $t_lower = \strtolower($type_src);
        if (!$aliases_arr || empty($type_src)) {
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
     * @param string $type_full
     * @return boolean|int
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
                $to_conv = false;
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
     * Create Table with name $table_name (using MergeTree engine)
     *
     * $fields_arr: array with keys is fields names and values is field types.
     *
     * First field in array means as primary key
     * Fields must contain 'Date' type field
     *
     * Each of field type may be specified in 3 variants of format:
     *  - String without spaces. For example, 'Int16'
     *  - String field_type[space default_value], for example, 'Int16 1234'
     *  - Array ['field_type' [, 'default_value']]
     *  For example:
     *    ['Int16', '777'] - is same 'Int16 777'. Type "Int16", default=777
     *    ['String'] - is same 'String'. Type "String", no default value.
     *    ['Date', 'now()'] - type "Date", default value = now() function.
     *
     * Data-types may be specified as aliases and case insensitive.
     * See list of aliases in $this->type_aliases and function ->addTypeAlias.
     *
     * Default values may be specified with DEFAULT keyword, for example:
     *    field_type = 'Int16 DEFAULT 123+5' , or ['Int16', 'DEFAULT 123+5']
     * Keyword 'DEFAULT' blocking changes, expression used exactly as specified.
     * Otherwise, if keyword DEFAULT not specified, parser used fnQuote function
     *   to add quotes if necessary, and used type conversion if possible.
     *
     * if_exists: 2-drop old table if exists
     *            1-do nothing if table already exists
     *            0-return error if table already exists
     *
     * @param string  $table_name
     * @param array   $fields_arr
     * @param integer $if_exists
     * @return boolean
     */
    public function createTableQuick($table_name, $fields_arr, $if_exists = 0)
    {
        if ($if_exists == 2) {
            $this->queryGood("DROP TABLE IF EXISTS $table_name");
        }
        $sql = $this->sqlTableQuick($table_name, $fields_arr, !$if_exists);
        return $this->queryGood($sql);
    }

    /**
     * Make SQL-query for CREATE TABLE $table_name in ClickHouse format.
     *
     * Parameter $field_arr described in annotation of createTableQuick function.
     *
     * If $if_not_exist not empty, "IF NOT EXISTS" will be included in request.
     *
     * @param string          $table_name
     * @param array           $fields_arr
     * @param integer|boolean $if_not_exist
     * @return string
     * @throws \Exception
     */
    public function sqlTableQuick($table_name, $fields_arr, $if_not_exist = 1)
    {
        if (!is_array($fields_arr) || count($fields_arr) < 2) {
            throw new \Exception("Table must contain as least 1 field");
        }

        // Parse fields array and check types
        $fields_arr = $this->parseFieldsArr($fields_arr);

        // Search Date field
        foreach ($fields_arr as $field_name => $field_par) {
            if (empty($primary_field)) {
                $primary_field = $field_name;
            }
            if ($field_par['type_name'] === 'Date' && empty($date_field)) {
                $date_field = $field_name;
            }
        }
        if (empty($date_field)) {
            throw new \Exception("Table must contain field 'Date' type");
        }

        // If have field named "ver", then ReplacingMergeTree will be used
        if (isset($fields_arr['ver'])) {
            $engine = "ReplacingMergeTree";
            $last_engine_par = ", 8192 ,ver";
        } else {
            $engine = "MergeTree";
            $last_engine_par = ", 8192";
        }

        return
            "CREATE TABLE " . ($if_not_exist ? 'IF NOT EXIST ' : '') .
            $table_name . ' ( ' .
            implode(", ", \array_column($fields_arr, 'create')) .
            ' ) ENGINE = ' . $engine .
            "($date_field, ($primary_field, $date_field)$last_engine_par)";
    }

    /**
     * Parse source array from format [field_names => field_types_defaults]
     *
     * @param array $fields_arr
     * @return array
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

            // make $type_full, $default, $create strings from $create array
            $type_full = $type_src = $create[0];
            // make $type_full, $type_name, $to_conv, $bytes (from $type_full)
            $type_name = $to_conv = 0;
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
                    $default = $this->fnQuote($default);
                    if ($to_conv) {
                        $default = $to_conv[0] . $default . $to_conv[1];
                        $create = 'DEFAULT ' . $default;
                    } else {
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
     *  123  => 123    (No changes because is_numeric)
     * "aaa" => "aaa"  (No changes because have begin-final quotes)
     * 'aaa' => 'aaa'  (No changes because have begin-final quotes)
     * fn(x) => fn(x)  (No changes because have final ")" and "(" within)
     *  aaa  => "aaa"  (add $quote-quotes and slashes for [ ' \t \n \r ] )
     *
     * @param string $str
     * @return string
     */
    public function fnQuote($str, $quote = "'")
    {
        $fc = substr($str, 0, 1);
        $lc = substr($str, -1);
        return (is_numeric($str) ||
           (($fc === '"' || $fc === "'") && ($fc === $lc)) ||
           (($lc === ')' && strpos($str, '(') !== false))
        ) ? $str : $quote . addcslashes($str, "'\t\n\r") . $quote;
    }

    /**
     * Return as Array [names=>values] data from system.settings table
     *
     * @return array|string
     */
    public function getSystemSettings()
    {
        return $this->queryKeyValues('system.settings', 'name, value');
    }

    /**
     * Return as string version of ClickHouse server
     *
     * @return string
     */
    public function getVersion()
    {
        return $this->queryValue('SELECT version()');
    }

    /**
     * Return server uptime in seconds
     *
     * @return integer
     */
    public function getUptime()
    {
        return $this->queryValue('SELECT uptime()');
    }

    /**
     * Get current database name for current or specified session
     *
     * @param string|null $sess
     * @return string
     */
    public function getCurrentDatabase($sess = null)
    {
        return $this->queryValue('SELECT currentDatabase()', null, $sess);
    }

    /**
     * Set current database name for current or specified session
     *
     * @param string      $db
     * @param string|null $sess
     * @return boolean
     */
    public function setCurrentDatabase($db, $sess = null)
    {
        return $this->queryGood("USE $db", $sess);
    }

    /**
     * Return Array contained names of existing Databases
     *
     * @return array|string
     */
    public function getDatabasesList()
    {
        return $this->queryColumnTab('SHOW DATABASES');
    }

    /**
     * Return names of tables from specified database or all like pattern
     *
     * @param string|null $name
     * @param string|null $like_pattern
     * @return array|string
     */
    public function getTablesList($name = null, $like_pattern = null)
    {
        return $this->queryColumnTab('SHOW TABLES ' .
                    (empty($name) ? '' : $this->from . $name) .
                    (empty($like_pattern) ? '' : " LIKE '$like_pattern'"));
    }

    /**
     * Return results of request "SHOW PROCESSLIST"
     *
     * @return array|string
     */
    public function getProcessList()
    {
        return $this->queryArray('SHOW PROCESSLIST');
    }

    /**
     * Return as Array information about specified table
     * Array is [Keys - field names] => [Values - field types]
     *
     * @param string $table
     * @return array|string
     */
    public function getTableFields($table)
    {
        //DESCRIBE TABLE [db.]table
        return $this->queryKeyValues("DESCRIBE TABLE $table");
    }

    /**
     * Return array with numbers from ClickHouse server for tests
     *
     * @param integer $lim
     * @param boolean $use_mt
     * @return array
     */
    public function getNumbers($lim = 100, $use_mt = false)
    {
        return $this->queryColumnTab(
            'SELECT * FROM system.numbers' . ($use_mt ? '_mt' : '') .
            ' LIMIT ' . $lim);
    }

    /**
     * Return information about size of table row by fields definition.
     * If parameter have string-type, fields array try get from db-table.
     * If parameter is array, it is seen as $fileds_arr with fields definition.
     *
     * @param string|array $table_name_or_fields_arr
     * @return array with $fixed_bytes, $fixed_fields, $dynamic_fields, $comment
     */
    public function getTableRowSize($table_name_or_fields_arr)
    {
        if (!\is_array($table_name_or_fields_arr)) {
            $table_name = $table_name_or_fields_arr;
            $fields_arr = $this->getTableFields($table_name);
            if (!\is_array($fields_arr)) {
                return $fields_arr;
            }
        } else {
            $fields_arr = $table_name_or_fields_arr;
            $table_name = null;
        }
        $dynamic_fields = 0;
        $fixed_bytes = $comment = $this->countRowFixedSize($fields_arr, $dynamic_fields);
        if (!\is_numeric($fixed_bytes)) {
            return $fixed_bytes;
        }
        $fixed_fields = \count($fields_arr) - $dynamic_fields;
        $comment .= " bytes in $fixed_fields FIXED FIELDS, $dynamic_fields DYNAMIC FIELDS";
        return \compact('table_name', 'fixed_bytes', 'fixed_fields', 'dynamic_fields', 'comment');
    }

    /**
     * Count and return summary of fixed-size fields by $fields_arr
     * Addition, in $dynamic_fields (by ref) return count of dynamic-fields.
     *
     * @param array $fields_arr
     * @param integer $dynamic_fields (by reference)
     * @return integer|string
     */
    public function countRowFixedSize($fields_arr, &$dynamic_fields = 0)
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
        $sum = 0;
        foreach (\array_column($parsed_arr, 'bytes') as $bytes) {
            if ($bytes) {
                $sum += $bytes;
            } else {
                $dynamic_fields++;
            }
        }
        return $sum;
    }

    /**
     * Get information about $table_name from system.columns
     * return array with extra info like [rows_cnt], [uncompressed_bytes], etc.
     *
     * @param string $table_name
     * @return array|string
     */
    public function getTableInfo($table_name)
    {
        $i = \strpos($table_name, '.');
        if ($i) {
            $db = \substr($table_name, 0, $i);
            $table_name = substr($table_name, $i+1);
        } else {
            $db = "currentDatabase()";
        }
        $where = "WHERE table='$table_name' AND database=" . $this->fnQuote($db);
        $columnts_arr = $this->queryArr("SELECT * FROM system.columns $where");
        if (!is_array($columnts_arr)) {
            return $columnts_arr;
        }
        if (!count($columnts_arr)) {
            return "Not found info about table $table_name";
        }
        $field_names = \array_column($columnts_arr, 'name');
        $columnts_arr = \array_combine($field_names, $columnts_arr);
        $fields_arr = [];
        $sum_compressed_bytes = $sum_uncompressed_bytes = 0;
        foreach ($columnts_arr as $col_name => $col_arr) {
            $database = $col_arr['database'];
            $table = $col_arr['table'];
            unset($col_arr['database']);
            unset($col_arr['table']);
            unset($col_arr['name']);
            $sum_compressed_bytes += $col_arr['data_compressed_bytes'];
            $column_bytes = $col_arr['data_uncompressed_bytes'];
            $sum_uncompressed_bytes += $column_bytes;
            $fixed_bytes = $this->parseType($col_arr['type']);
            if ($fixed_bytes) {
                $col_arr['fixed_bytes'] = $fixed_bytes;
                $col_rows_cnt = $column_bytes / $fixed_bytes;
                $rows_cnt = $col_rows_cnt;
                $col_arr['rows_cnt'] = $col_rows_cnt;
            }
            $columnts_arr[$col_name] = $col_arr;
            $fields_arr[$col_name] = $col_arr['type'];
        }
        $ret_arr = $this->getTableRowSize($fields_arr);
        $ret_arr['table_name'] = $database . '.' . $table;
        $ret_arr['uncompressed_bytes'] = $sum_uncompressed_bytes;
        $ret_arr['compressed_bytes'] = $sum_compressed_bytes;
        $ret_arr['rows_cnt'] = is_null($rows_cnt) ? "Unknown" : $rows_cnt;
        $ret_arr['columns_cnt'] = \count($columnts_arr);
        $ret_arr['columns'] = $columnts_arr;
        return $ret_arr;
    }
}
