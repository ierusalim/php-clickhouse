<?php
namespace ierusalim\ClickHouse;

/**
 * Class ClickHouseQuery for make queries to ClickHouse database engine.
 *
 * Functions are a wrapper for ClickHouseAPI and allow to easily send
 * sql-queries to ClickHouse server and parsing answering data.
 *
 * Main query-functions for use:
 * - >queryFalse($sql, [post])- Return false only if NOT error, otherwise string with error.
 * - >queryTrue($sql, [post]) - Return false only if error, otherwise return true or data
 * - >queryValue($sql, [post]) - Send query and receive data in one string (false if error)
 * - >queryArray($sql) - for queries returning multi-rows data
 * - >queryKeyValues(see descr.) - for queries returning 2 columns key => value
 * - >queryInsertArray($table, $fields_names, $fields_set) - insert data into table
 * - >queryInsertFile($table, $file, $structure) - insert data from file into table
 *
 * PHP Version >= 5.4
 *
 * @package    ierusalim/php-clickhouse
 * @author     Alexander Jer <alex@ierusalim.com>
 * @copyright  2017, Ierusalim
 * @license    https://opensource.org/licenses/Apache-2.0 Apache-2.0
 *
 */
class ClickHouseQuery extends ClickHouseAPI
{
    /**
     * Using JSON-full format or JSON-compact format for transferring arrays.
     *
     * (In my tests JSONCompact always the best than JSON)
     *
     * @var boolean
     */
    public $json_compact = true;

    /**
     * Contains array with field-names from received meta-data
     *
     * Available after calling functions queryFullArray, queryArray, queryArr
     *
     * @var array|null
     */
    public $names;

    /**
     * Contains array with field-types from received meta-data
     *
     * Available after calling functions queryFullArray and queryArray
     *
     * @var array|null
     */
    public $types;

    /**
     * Stored [meta]-section from received data
     *
     * Available after calling functions queryFullArray and queryArray
     *
     * (not need because this data already have in $this-keys and $this-types)
     *
     * @var array|null
     */
    public $meta;

    /**
     * Stored [statistics]-section from received data
     *
     * Available after calling functions queryFullArray and queryArray
     *
     * @var array|null
     */
    public $statistics;

    /**
     * Stored [extremes]-section from received data.
     *
     * Available after calling functions queryFullArray, queryArray, queryArr
     *
     * For use extremes need set flag by $this->setOption('extremes', 1)
     *
     * @var array|null
     */
    public $extremes;

    /**
     * Stored [rows]-section from received data, contains data-rows count.
     *
     * @var integer|null
     */
    public $rows;

    /**
     * Sometimes returns with JSON | JSONCompact formats
     *
     * @var integer|null
     */
    public $rows_before_limit_at_least;

    /**
     * Set after calling queryFullArray, queryArry, queryArr when 'WITH TOTALS'
     *
     * @var array|null
     */
    public $totals;

    /**
     * Data remaining in the array after remove all known sections-keys
     *  Known keys is ['meta', 'statistics', 'extremes', 'rows']
     *
     * Available after calling functions queryArray, queryArr
     * (usually contains empty array)
     *
     * @var array
     */
    public $extra = [];

    /**
     * String contained last error which returned by CURL or server response
     *
     * Set by function queryValue
     *
     * @var string
     */
    public $last_error_str = '';

    /**
     * For queries that involve either no return value or one string value.
     * - Return true (or non-empty string) if ok
     * - Return false only if error
     *
     * queryTrue send POST-request by default for clear read_only-flag.
     *
     * @param string            $sql SQL-request
     * @param string|array|null $post_data any POST-data or null for GET-request
     * @param string|null       $sess session_id
     * @return boolean|string false if error | true or string with results if ok
     */
    public function queryTrue($sql, $post_data = [], $sess = null)
    {
        $slot = $this->to_slot;
        $ans = $this->queryValue($sql, $post_data, $sess);
        $fi = function($ans) {
            return ($ans !== false && empty($ans)) ?: $ans;
        };
        if (empty($slot)) {
            return $fi($ans);
        } else {
            $this->slotHookPush($slot,
                ['mode' => 2, 'fn' => $fi, 'par' => 'queryTrue']);
            return $this;
        }
    }

    /**
     * For queries that involve either no return value or any string data.
     * - Return false only if no errors.
     * - In case of an error, return string with error description
     *
     * queryFalse send POST-request by default for clear read_only-flag.
     *
     * @param string            $sql SQL-request
     * @param string|array|null $post_data any POST-data or null for GET-request
     * @param string|null       $sess session_id
     * @return string|false False if no errors | string if error with describe
     */
    public function queryFalse($sql, $post_data = [], $sess = null)
    {
        $slot = $this->to_slot;
        $ans = $this->queryValue($sql, $post_data, $sess);
        $fi = function ($ans) {
            return ($ans === false) ? $this->last_error_str : false;
        };
        if (empty($slot)) {
            return $fi($ans);
        } else {
            $this->slotHookPush($slot,
                ['mode' => 2, 'fn' => $fi, 'par' => 'queryFalse']);
            return $this;
        }
    }

    /**
     * Sends query and returns response data in one string (return false if error)
     *
     * Nuances:
     * - Error description available in $this->last_error_str (contains empty string if no error)
     * - To results are applied to the trim function (for removing \n from end)
     * - Function send POST-request if have post_data, send GET-request if no post_data
     *
     * @param string $sql SQL-request
     * @param array|string|null $post_data Post-data or Null for Get-request
     * @param string|null $sess session_id
     * @return false|string False if error | String with results if ok
     */
    public function queryValue($sql, $post_data = null, $sess = null)
    {
        $slot = $this->to_slot;

        $ans = $this->anyQuery($sql, $post_data, $sess);

        $fi = function($ans) {
            if (!empty($ans['curl_error'])) {
                $this->last_error_str = $ans['curl_error'];
                return false;
            }

            if ($ans['code'] == 200) {
                $this->last_error_str = '';
                return isset($ans['response']) ? trim($ans['response']) : false;
            } else {
                $this->last_error_str = $ans['response'];
                return false;
            }
        };

        if (empty($slot)) {
            return $fi($ans);
        }

        $this->slotHookPush($slot,
            ['mode' => 2, 'fn' => $fi, 'par' => 'queryValue']);

        return $this;
    }

    /**
     * Return strings array (using TabSeparated-formats for data transfer)
     * - If results have more of one column, strings need be explode by tab
     * - If $with_names_types is true, first 2 strings of results
     *   contains names and types of returned columns.
     *
     * Nuances:
     * - Returned data not unescaped
     * - If have option 'extreme' or 'WITH TOTALS' requires filtering extra lines
     *
     * @param string      $sql SQL-request
     * @param boolean     $with_names_types true for TabSeparatedWithNamesAndTypes
     * @param string|null $sess session_id
     * @return array|string Returns array if ok, or string with error description
     */
    public function queryStrings($sql, $with_names_types = false, $sess = null)
    {
        $slot = $this->to_slot;

        $arr = $this->queryValue($sql .
            ' FORMAT TabSeparated' . ($with_names_types ? 'WithNamesAndTypes' : ''),
            null, $sess);

        $fi = function($arr) {
            $arr = (!$arr) ?: \explode("\n", $arr);
            return \is_array($arr) ? $arr : $this->last_error_str;
        };

        if (empty($slot)) {
            return $fi($arr);
        }

        $this->slotHookPush($slot,
            ['mode' => 2, 'fn' => $fi, 'par' => 'queryStrings']);

        return $this;
    }

    /**
     * For queries returning exactly 2 columns, which can mean key => value
     *
     * Data of first column interpreted as array keys, the second column as values.
     * - If the result contains only 1 column, it returns as an array with numeric keys.
     * - If return contains more of 2 columns, extra data ignored, only 2 columns used.
     * - First column as keys must be unique
     * - if second parameter is null, first parameter must contain full sql-request
     * - if second parameter not null, first parameter means table name (that after FROM)
     * - Second parameter means field names (that between SELECT and FROM)
     * - Returns array only if not error. If error returns string with error description.
     *
     * Similar than queryKeyValArr, but using JSONCompact for data transferring.
     *
     * @param string $tbl_or_sql Table name (or SQL-request if next parameter is null)
     * @param string|null $key_and_value_fields field names, example: 'id,name'
     * @param string|null $sess session_id
     * @return array|string Returns array if ok, or string with error description
     */
    public function queryKeyValues(
        $tbl_or_sql,
        $key_and_value_fields = null,
        $sess = null
    ) {
        $slot = $this->to_slot;

        $yi = function ($tbl_or_sql, $key_and_value_fields, $sess) {
            if (is_null($key_and_value_fields)) {
                $sql = $tbl_or_sql;
            } else {
                $sql = "SELECT $key_and_value_fields FROM $tbl_or_sql";
            }
            $data = (yield $this->queryArray($sql, true, $sess));
            if (!\is_array($data) || !\count($data)) {
                yield $data;
            }
            if (\count($data[0]) == 1) {
                yield \array_column($data, 0);
            }
            yield \array_combine(\array_column($data, 0), \array_column($data, 1));
        };

        $yi = $yi($tbl_or_sql, $key_and_value_fields, $sess);

        $arr = $yi->current();

        if (empty($slot)) {
            return $yi->send($arr);
        }

        $this->slotHookPush($slot,
            ['mode' => 1, 'fn' => $yi, 'par' => 'queryKeyValues']);

        return $this;
    }

    /**
     * Return Array [keys => values], first column of results means keys.
     *
     * Similar than queryKeyValues, but using TabSeparated for data transferring.
     * Returned data not unescaped. Provides the best performance on powerful servers,
     *  but, on weak processors it runs slower than queryKeyValues
     * - If unescape is important, it's best to use queryKeyValues instead.
     * - If the result contains only 1 column, it returns as an array with numeric keys.
     * - If the results contain 2 columns or more, first column as keys must be unique
     * - If return contains more of 2 columns, values returned in tab-separated format.
     * - if second parameter is null, first parameter must contain full sql-request
     * - if second parameter not null, first parameter means table name (that after FROM)
     * - Second parameter means field names (that between SELECT and FROM), may be '*'
     * - Returns array only if not error. If error returns string with error description.
     *
     * @param string $tbl_or_sql Table name or full sql request
     * @param string|null $key_name_and_value_name fields like 'id, name'
     * @param string|null $sess session_id
     * @return array|string Return array if ok, or string with error description
     */
    public function queryKeyValArr(
        $tbl_or_sql,
        $key_name_and_value_name = null,
        $sess = null
    ) {
        $slot = $this->to_slot;

        $yi = function($tbl_or_sql, $key_name_and_value_name, $sess) {
            if (is_null($key_name_and_value_name)) {
                $sql = $tbl_or_sql;
            } else {
                $sql = "SELECT $key_name_and_value_name FROM $tbl_or_sql";
            }
            $data = (yield $this->queryStrings($sql, false, $sess));

            if (!\is_array($data) || !\count($data) || !\strpos($data[0], "\t")) {
                yield $data;
            }
            $ret = [];
            foreach ($data as $s) {
                if (empty($s)) {
                    break;
                }
                $i = strpos($s, "\t");
                $ret[substr($s, 0, $i)] = substr($s, $i + 1);
            }
            yield $ret;
        };
        $yi = $yi($tbl_or_sql, $key_name_and_value_name, $sess);

        $arr = $yi->current();

        if (empty($slot)) {
            return $yi->send($arr);
        }

        $this->slotHookPush($slot,
            ['mode' => 1, 'fn' => $yi, 'par' => 'queryKeyValArr']);

        return $this;

    }

    /**
     * Function for queries returning multi-row data in array
     *
     * Results array have numeric_keys if $numeric_keys = true,
     * otherwise keys named as field names.
     *
     * Additional,
     * - Information about field names available in $this->names
     * - Information about field types available in $this->types
     *
     * If error, return non-array data (usually string with error description)
     *
     * @param string $sql SQL-query
     * @param boolean $numeric_keys if true then array returning with numeric keys
     * @param string|null $sess session_id
     * @return array|string Returns array if ok, or string with error description
     */
    public function queryArray($sql, $numeric_keys = false, $sess = null)
    {
        $slot = $this->to_slot;

        $yi = function($sql, $numeric_keys, $sess) {
            $arr = (yield $this->queryFullArray($sql, $numeric_keys, $sess));

            if (!is_array($arr) || $numeric_keys) {
                yield $arr;
            }
            $data = $arr['data'];
            foreach ([
                'data',
                'meta',
                'statistics',
                'extremes',
                'rows',
                'rows_before_limit_at_least',
                'totals'
                ] as $key) {
                unset($arr[$key]);
            }
            $this->extra = $arr;
            yield $data;
        };

        $yi = $yi($sql, $numeric_keys, $sess);

        $arr = $yi->current();

        if (empty($slot)) {
            return $yi->send($arr);
        }

        $this->slotHookPush($slot,
            ['mode' => 1, 'fn' => $yi, 'par' => 'queryArray']);

        return $this;
    }

    /**
     * Similar as queryArray, but use TabSeparated format for data transferring.
     *
     * Provides better performance than queryArray, but needed unescape later.
     * If unescape is important, it's best to use queryArray instead queryArr.
     *
     * @param string      $sql SQL request
     * @param boolean     $numeric_keys if true field names set as keys of results array
     * @param string|null $sess session_id
     * @return array|string Return array if ok, or string with error description
     */
    public function queryArr($sql, $numeric_keys = false, $sess = null)
    {
        $slot = $this->to_slot;

        $yi = function($sql, $numeric_keys, $sess) {
            $this->extra = [];
            $this->totals = $this->meta = $this->types = null;

            $data = (yield $this->queryStrings($sql, !$numeric_keys, $sess));
            if (!\is_array($data)) {
                yield $data;
            }
            $found_extra = false;
            $ret = [];
            foreach ($data as $k => $s) {
                if (empty($s)) {
                    $found_extra = true;
                    break;
                }
                $x = explode("\t", $s);
                if ($numeric_keys) {
                    $ret[] = $x;
                } else {
                    if ($k < 2) {
                        if ($k) {
                            $this->types = $x;
                        } else {
                            $keys = $x;
                            $this->names = $x;
                        }
                    } else {
                        while(\count($x) < \count($keys)) $x[]='';
                        $ret[] = \array_combine($keys, $x);
                    }
                }
            }

            // Parsing extra data if found. Its may be if 'extremes' or 'with totals'.
            if ($found_extra) {
                $c = \count($data);
                for ($l = $c + 1; $k < $l; $k++) {
                    $s = ($k < $c) ? $data[$k] : '';
                    if (empty($s)) {
                        if (\count($this->extra) == 1) {
                            $this->totals = $this->extra[0];
                        }
                        if (\count($this->extra) == 2) {
                            $this->extremes = \array_combine(
                                ['min', 'max'],
                                $this->extra);
                        }
                        $this->extra = [];
                    } else {
                        $s = \explode("\t", $s);
                        if (!$numeric_keys) {
                            while(\count($s) < \count($keys)) {
                                $s[]='';
                            }
                            if (\count($s) == \count($keys)) {
                                $s = \array_combine($keys, $s);
                            }
                        }
                        $this->extra[] = $s;
                    }
                }
            }
            $this->rows = \count($ret);
            yield $ret;
        };

        $yi = $yi($sql, $numeric_keys, $sess);

        $data = $yi->current();

        if (empty($slot)) {
            return $yi->send($data);
        }

        $this->slotHookPush($slot,
            ['mode' => 1, 'fn' => $yi, 'par' => 'queryArr']);

        return $this;
    }

    /**
     * Try to decode json-string and return array if ok, or string $err if error
     *
     * @param mixed $json_raw String for decode
     * @param string $err Error string for answering if json_decode failed
     * @return array|string
     */
    public function jsonDecode($json_raw, $err = "No json")
    {
        if (!\is_string($json_raw)) {
            return $err;
        }
        return \json_decode($json_raw, true) ?: $err;
    }

    /**
     * Sends a SQL query and returns data in an array in the format ClickHouse.
     *
     * The requested data is transmitted through the JSON format or JSONCompact.
     * - If $data_only flag is false, return full array with [meta],[data], etc.
     * - If $data_only is true, return only [data]-section from received array.
     * - If error return false
     *
     * In $data_only=true then using JSONCompact and returning array with
     *  numeric keys (but field names available in array $this->names)
     *
     * @param string $sql SQL-query
     * @param boolean $data_only if false return full array, if true only data-key
     * @param string|null $sess session_id
     * @return array|string Returns array if ok, or string with error description
     */
    public function queryFullArray($sql, $data_only = false, $sess = null)
    {
        $slot = $this->to_slot;

        $yi = function($sql, $data_only, $sess) {
            $arr = (yield $this->queryValue($sql . ' FORMAT JSON' .
                (($this->json_compact || $data_only) ? 'Compact' : ''),
                null, $sess));

            $arr = $this->jsonDecode($arr, 'No json');

            if (!\is_array($arr)) {
                yield $this->last_error_str;
            }

            foreach (['meta', 'statistics', 'extremes', 'totals'] as $key) {
                $this->$key = (isset($arr[$key]) && \is_array($arr[$key])) ? $arr[$key] : null;
            }
            foreach (['rows_before_limit_at_least', 'rows'] as $key) {
                $this->$key = (isset($arr[$key]) && \is_numeric($arr[$key])) ? $arr[$key] : null;
            }
            $this->names = $names = isset($this->meta) ? \array_column($this->meta, 'name') : null;
            $this->types = \is_array($this->meta) ? \array_column($this->meta, 'type') : null;

            if ($this->json_compact && !empty($names)) {
                foreach (['data', 'extremes'] as $key) {
                    if ($key == 'data' && $data_only) {
                        continue;
                    }
                    if (!empty($arr[$key]) && \is_array($arr[$key])) {
                        foreach ($arr[$key] as $k => $ret) {
                            $arr[$key][$k] = \array_combine($names, $ret);
                        }
                        if ($key != 'data') {
                            $this->$key = $arr[$key];
                        }
                    }
                }
                if (!empty($this->totals) && \is_array($this->totals)) {
                    $this->totals = \array_combine($names, $this->totals);
                    $arr['totals'] = $this->totals;
                }
            }
            yield $data_only ? $arr['data'] : $arr;
        };

        $yi = $yi($sql, $data_only, $sess);

        $arr = $yi->current();

        if (empty($slot)) {
            return $yi->send($arr);
        }

        $this->slotHookPush($slot,
            ['mode' => 1, 'fn' => $yi, 'par' => 'queryFullArray']);

        return $this;
    }

    /**
     * Inserting data into table from file by one request.
     *
     * TabSeparated format supported only
     *
     * Low-level function, using undocumented features, be careful
     *
     * @param string $table Table for inserting data
     * @param string $file File for send
     * @param string $file_structure Structure of file columns
     * @param string $selector any SELECT expressions, field names, *, etc.
     * @param string|null $sess session_id
     * @return boolean|string Return false if ok, or string with error description
     * @throws \Exception
     */
    public function queryInsertFile(
        $table,
        $file,
        $file_structure = 'id UInt32, dt Date, s String',
        $selector = '*',
        $sess = null
    ) {
        if (empty($file_structure) || empty($table) || empty($file)) {
            throw new \Exception("Illegal parameter");
        }
        if (!is_file($file)) {
            throw new \Exception("File not found");
        }

        $slot = $this->to_slot;

        $fs = 'file_structure';
        $old_fs = $this->setOption($fs, $file_structure, true);
        $sql = "INSERT INTO $table SELECT $selector FROM file";
        $ans = $this->doQuery($sql, true, [], $sess, $file);
        $this->setOption($fs, $old_fs, true);

        $fi = function($ans) {
            // same queryFalse (Return string if curl_error or server answer is not 200)
            return $ans['curl_error'] ?: (($ans['code'] == 200) ? false : $ans['response']);
        };
        if (empty($slot)) {
            return $fi($ans);
        }

        $this->slotHookPush($slot,
            ['mode' => 2, 'fn' => $fi, 'par' => 'queryInsertFile']);

        return $this;
    }

    /**
     * Inserting data into table from specified gzip-compressed file
     *
     * If the file has .gz extension, it is sent as is.
     * Otherwise apply gzip-compression.
     *
     * $format may be JSONEachRow, TabSeparated, CSV, TabSeparatedWithNames, CSVWithNames
     * If used JSONEachRow format, each row must have field names in keys
     *
     * $file_colums may contains inserted fields like '(id, dt, x)' or be empty for JSONEachRow
     * if used *WithNames-formats, field names must contain the first row
     *
     * @param string $table Table for inserting data
     * @param string $file File from inserting data
     * @param string $format JSONEachRow (by default), TabSeparated, CSV [WithNames]
     * @param string $file_columns field names like '(id, dt, x)' or empty string
     * @param string|null $sess Session id
     * @return boolean|string|$this
     * @throws \Exception
     */
    public function queryInsertGzip(
        $table,
        $file,
        $format = 'JSONEachRow',
        $fields_str = '',
        $sess = null
    ) {
        if (!is_file($file)) {
            throw new \Exception("File not found");
        }

        $slot = $this->to_slot;

        $sql = "INSERT INTO $table $fields_str FORMAT $format";

        $ans = $this->doQuery($sql, true, [], $sess, $file, true);

        $fi = function($ans) {
            // same queryFalse (Return string if curl_error or server answer is not 200)
            return $ans['curl_error'] ?: (($ans['code'] == 200) ? false : $ans['response']);
        };
        if (empty($slot)) {
            return $fi($ans);
        }

        $this->slotHookPush($slot,
            ['mode' => 2, 'fn' => $fi, 'par' => 'queryInsertGzip']);

        return $this;
    }
    /**
     * Inserting data into table from array
     *
     * Array of inserting data may be in following variants of format:
     * For insert one row:
     * - [value1, value2, ...] - array without keys. Keys must define in $field_names
     * - [field1=>value1, field2=>value2 ...] - array with keys.
     * For insert many rows:
     * - [[row1value1, row1value2, ...] , [row2value1, row2value2, ...] ...]
     * - [[row1field1=>row1value1, row1field2=>row1value2, ...] , ...]
     * When inserting many rows, it is more efficient to pass an array without keys.
     *
     * @param string $table_name Table name for inserting data
     * @param array|null $fields_names Array with names of inserting fields
     * @param array $fields_set_arr Array with inserting data
     * @param string|null $sess session_id
     * @return boolean|string Return false if ok, or string with error description
     * @throws \Exception
     */
    public function queryInsertArray($table_name, $fields_names, $fields_set_arr, $sess = null)
    {
        if (!\is_array($fields_set_arr) || !\count($fields_set_arr)) {
            return "Illegal parameters";
        }

        // Resolve $fields_arr
        $use_json_each_row = false;
        if (isset($fields_set_arr[0])) {
            if (\is_array($fields_set_arr[0])) {
                if (!isset($fields_set_arr[0][0])) {
                    $in_arr_fields_arr = \array_keys($fields_set_arr[0]);
                    $use_json_each_row = true;
                }
            }
        } else {
            $in_arr_fields_arr = \array_keys($fields_set_arr);
            $use_json_each_row = true;
        }

        if (\is_null($fields_names)) {
            if (!isset($in_arr_fields_arr)) {
                $fields_names = $this->names;
            } else {
                $fields_names = $in_arr_fields_arr;
            }
        }
        if (\is_null($fields_names)) {
            throw new \Exception("Inserting fields undefined");
        }

        $sql = "INSERT INTO $table_name (" . implode(",", $fields_names) . ") " .
            "FORMAT " . ($use_json_each_row ? 'JSONEachRow' : 'TabSeparated');

        if (!isset($fields_set_arr[0])) {
            $post_data = [\json_encode($fields_set_arr)];
        } else {
            $post_data = [];
            if (!$use_json_each_row && !\is_array($fields_set_arr[0])) {
                $fields_set_arr = [$fields_set_arr];
            }
            foreach ($fields_set_arr as $row_arr) {
                if ($use_json_each_row) {
                    $post_data[] = \json_encode($row_arr);
                } else {
                    $row_arr = array_map(
                        function($s) {
                            return \addcslashes($s, "\t\\\n\0");
                        },
                        $row_arr);
                    $post_data[] = \implode("\t", $row_arr);
                }
            }
        }
        return $this->queryFalse($sql, \implode("\n", $post_data) . "\n", $sess);
    }


    /**
     * Add quotes and slashes if need for ClickHouse-SQL parameter
     *
     * Examples:
     * - ("123")   => 123    (No changes because is_numeric)
     * - ('"aaa"') => "aaa"  (No changes because have begin-final quotes)
     * - ("'aaa'") => 'aaa'  (No changes because have begin-final quotes)
     * - ("fn(x)") => fn(x)  (No changes because have final ")" and "(" within)
     * - ("aaa")   => 'aaa'  (add $quote-quotes and slashes for [ ` ' \t \n \r ] )
     *
     * @param string $str
     * @return string
     */
    public function quotePar($str, $quote = "'")
    {
        $fc = substr($str, 0, 1);
        $lc = substr($str, -1);
        return (is_numeric($str) ||
           (($fc === '"' || $fc === "'") && ($fc === $lc)) ||
           (($lc === ')' && strpos($str, '(') !== false)) ||
           (($fc === '[' && $lc === ']'))
        ) ? $str : $quote . addcslashes($str, "'\t\n\r\0`") . $quote;
    }

    /**
     * Binding variables to string pattern
     *
     * Example:
     * - ->bindPars("SELECT {n} FROM {t}", ['n'=>'name','t'=>'table'])
     * - Result: "SELECT name FROM table"
     *
     * @param string $pattern String pattern with places for binding variables
     * @param array $bindings Array of variables for binding [name]=>value
     * @param string|null $e_pre Pattern-prefix for binding var
     * @param string|null $e_pos Pattern-postfix for binding var
     * @return string
     */
    public function bindPars($pattern, $bindings, $e_pre = '{', $e_pos = '}')
    {
        $search_arr = \array_map(function($s) use ($e_pre, $e_pos) {
            return $e_pre . $s . $e_pos;
        }, \array_keys($bindings));
        $replace_arr = \array_values($bindings);
        return \str_replace($search_arr, $replace_arr, $pattern);
    }

    /**
     * Execute SQL by pattern with bindings and return array with filtered results
     *
     * For understanding, see the default values as example.
     *
     * By default returns information from system.columns about specified table.
     *
     * @param string $table Table name [db.]table (set into ['db','table','dbtb'] bindings)
     * @param string $sql_pattern Pattern for SQL-request
     * @param array $bindings Array with binding of values for patterns
     * @param string $not_found_pattern Pattern for return if request got empty results
     * @param array $columns_up Columns (from bindings) for move to up level of results
     * @param array $colums_del Columns (from bindings) for delete from results
     * @param string|null $keys_from_field null for set numeric keys in columns_arr
     * @param string|null $sess session_id
     * @return array|string Results in array or string with error description
     */
    public function queryTableSubstract(
        $table,
        $sql_pattern = "SELECT * FROM {s} WHERE {t}={table} AND {d}={db}",
        $bindings = [
            's' => 'system.columns',
            'd' => 'database',
            't' => 'table',
            'n' => 'name'
            ],
        $not_found_pattern = "No information about {t} {dbtb} in {s}",
        $columns_up = ['t', 'd'],
        $colums_del = ['t', 'd', 'n'],
        $keys_from_field = 'name',
        $sess = null
    ) {
        $slot = $this->to_slot;

        $yi = function ($table, $sql_pattern, $bindings, $not_found_pattern,
            $columns_up, $colums_del, $keys_from_field, $sess) {

            $i = \strpos($table, '.');
            if ($i) {
                $db = \substr($table, 0, $i);
                $table = \substr($table, $i + 1);
            } else {
                $db = "currentDatabase()";
            }

            $bindings['db'] = $this->quotePar($db);
            $bindings['table'] = $this->quotePar($table);
            $bindings['dbtb'] = ($i ? $db . '.' : '') . $table;

            $sql = $this->bindPars($sql_pattern, $bindings);

            $columns_arr = (yield $this->queryArr($sql, false, $sess));

            if (is_array($columns_arr)) {
                if (!\count($columns_arr)) {
                    yield $this->bindPars($not_found_pattern, $bindings);
                }
                $ret_arr = [];
                foreach ($columns_up as $b) {
                    $b = $bindings[$b];
                    $ret_arr[$b] = $columns_arr[0][$b];
                }
                $ret_arr['columns_arr'] = [];
                foreach ($columns_arr as $k => $col_arr) {
                    if (!empty($keys_from_field)) {
                        $k = $col_arr[$keys_from_field];
                    }
                    foreach ($colums_del as $b) {
                        unset($col_arr[$bindings[$b]]);
                    }
                    $ret_arr['columns_arr'][$k] = $col_arr;
                }
                $columns_arr = $ret_arr;
            }
            yield $columns_arr;
        };
        $yi = $yi($table, $sql_pattern, $bindings, $not_found_pattern,
                  $columns_up, $colums_del, $keys_from_field, $sess);

        $data = $yi->current();

        if (empty($slot)) {
            return $yi->send($data);
        }

        $this->slotHookPush($slot,
            ['mode' => 1, 'fn' => $yi, 'par' => 'queryTableSubstract']);

        return $this;
    }

    /**
     * Get all information from system.$sys about specified table.
     *
     * Known system.tables with information about [db.]tables is:
     * - system.columns
     * - system.tables
     * - system.merges
     * - system.parts
     * - system.replicas
     *
     * By default using 'system.tables' and result contains [engine], etc.
     *
     * @param string $table Table name [db.]table
     * @param string $sys Table in system database, by default 'system.tables'
     * @param array $columns_del Array with columns aliases to remove from results
     * @return array|string Return results array or string with error description.
     */
    public function queryTableSys($table, $sys = 'tables', $columns_del = ['n'])
    {
        $slot = $this->to_slot;

        switch ($sys) {
            case 'columns':
                return $this->queryTableSubstract($table);
            case 'tables':
                $sql = "SELECT {n} as {t}, * FROM {s} WHERE {n}={table} AND {d}={db}";
                break;
            default:
                $sql = "SELECT * FROM {s} WHERE {t}={table} AND {d}={db}";
        }

        $yi = function ($sql, $table, $sys, $columns_del) {
            $arr = (yield $this->queryTableSubstract($table, $sql, [
                's' => 'system.' . $sys,
                'd' => 'database',
                'n' => 'name',
                't' => 'table'
                ], "No information about {dbtb} in {s}", [], $columns_del, null
            ));
            if (\is_array($arr)) {
                if (\count($arr['columns_arr']) == 1) {
                    $arr = $arr['columns_arr'][0];
                }
            }
            yield $arr;
        };
        $yi = $yi($sql, $table, $sys, $columns_del);

        $data = $yi->current();

        if (empty($slot)) {
            return $yi->send($data);
        }

        $this->slotHookPush($slot,
            ['mode' => 1, 'fn' => $yi, 'par' => 'queryTableSys']);

        return $this;
    }
}
