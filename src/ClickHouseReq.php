<?php
namespace ierusalim\ClickHouse;

/**
 * Class ClickHouseReq for make queries to ClickHouse database engine
 *
 * PHP Version >= 5.4
 *
 * @package    ierusalim/php-clickhouse
 * @author     Alexander Jer <alex@ierusalim.com>
 * @copyright  2017, Ierusalim
 * @license    https://opensource.org/licenses/Apache-2.0 Apache-2.0
 */
class ClickHouseReq extends ClickHouseAPI
{
    /**
     * Using JSON-full format or JSON-compact format for transferring arrays.
     * In my tests JSONCompact always the best than JSON.
     *
     * @var boolean
     */
    public $json_compact = true;
    
    /**
     * Contains array with field-names from received meta-data
     * Available after calling functions queryFullArray, queryArray, queryArr
     *
     * @var array|null
     */
    public $keys;
    
    /**
     * Contains array with field-types from received meta-data
     * Available after calling functions queryFullArray and queryArray
     *
     * @var array|null
     */
    public $types;

    /**
     * Stored [meta]-section from received data
     * Available after calling functions queryFullArray and queryArray
     * (not need because this data already have in $this-keys and $this-types)
     *
     * @var array|null
     */
    public $meta;
    
    /**
     * Stored [statistics]-section from received data
     * Available after calling functions queryFullArray and queryArray
     *
     * @var array|null
     */
    public $statistics;
    
    /**
     * Stored [extremes]-section from received data
     * Available after calling functions queryFullArray, queryArray, queryArr
     * for use extremes need set flag by $this->setOption('extremes', 1)
     *
     * @var array|null
     */
    public $extremes;
    
    /**
     * Stored [rows]-section from received data, contains rows count.
     * (not need because count(array) is same)
     *
     * @var integer|null
     */
    public $rows;
    
    /**
     * Data remaining in the array after remove all known sections-keys
     *  Known keys is ['meta', 'statistics', 'extremes', 'rows']
     * Available after calling functions queryArray, queryArr
     * (usually contains empty array)
     *
     * @var array|null
     */
    public $extra;
    
    /**
     * Set after calling queryFullArray, queryArry, queryArr when 'WITH TOTALS'
     *
     * @var array|null
     */
    public $totals;
    
    /**
     * String contained last error which returned by CURL or in server response
     *
     * @var string
     */
    public $last_error_str = '';

    /**
     * Last string-response for request (after functions queryGood, queryValue)
     * without 'trim' executed, so usually contained "\n" in end
     *
     * @var string|null
     */
    public $last_raw_str;

    /**
     * For queries that involve either no return value or one string value.
     * Return true or non-empty string if ok
     * Return false only if error
     *
     * Very similar to the function queryValue, but return true for empty string
     * queryGood always send POST-queries for clear read_only-flag.
     *
     * @param string $sql
     * @param string|null $sess
     * @return boolean|string
     */
    public function queryGood($sql, $sess = null)
    {
        $ans = $this->queryValue($sql, [], $sess);
        if ($ans !== false && empty($ans)) {
            return true;
        } else {
            return $ans;
        }
    }
    
    /**
     * For queries that involve either no return value or one string value.
     * Return string if ok
     * Return false if error
     *
     * Send POST-request if have post_data, send GET-request if no post_data
     *
     * @param type $sql
     * @param type $post_data
     * @param type $sess
     * @return boolean|string
     */
    public function queryValue($sql, $post_data = null, $sess = null)
    {
        // Do query
        $ans = $this->anyQuery($sql, $post_data, $sess);
        
        // Return false if error
        if (!empty($ans['curl_error'])) {
            $this->last_error_str = $ans['curl_error'];
            return false;
        }
        
        $this->last_raw_str = isset($ans['response']) ? $ans['response'] : null;
        if ($ans['code'] == 200) {
            return trim($this->last_raw_str);
        } else {
            $this->last_error_str = $ans['response'];
            return false;
        }
    }

    /**
     * Return strings array (using TabSeparated-format for data transfer)
     * If return one column, result array no need transformations.
     * If return more of one column, array strings need be explode by tab
     *
     * If with_names is true, first string of results contain names of columns.
     *
     * @param string $sql
     * @param string|null $sess
     * @param boolean $with_names
     * @return array
     */
    public function queryColumn($sql, $sess = null, $with_names = false)
    {
        $data = $this->getQuery(
            $sql .
            ' FORMAT TabSeparated' . ($with_names ? 'WithNames':''),
            $sess);
        if ($data['code'] != 200) {
            return $data['response'];
        }
        $data = explode("\n", $data['response']);
        $c = count($data);
        if ($c && empty($data[$c-1])) {
            unset($data[$c-1]);
        }
        return $data;
    }

    /**
     * Return Array [keys => values]
     * Request and return data from table by 2 specified field-names,
     * or return results of any SQL-query with 2 columns in results.
     * Similar than queryKeyValues, but using JSONCompact for data transferring.
     *
     * @param string $tbl_or_sql
     * @param string|null $key_name_and_value_name
     * @param string|null $sess
     * @return array
     */
    public function queryKeyValues(
        $tbl_or_sql,
        $key_name_and_value_name = null,
        $sess = null
    ) {
        if (is_null($key_name_and_value_name)) {
            $sql = $tbl_or_sql;
        } else {
            $sql = "SELECT $key_name_and_value_name FROM $tbl_or_sql";
        }
        $data = $this->queryArray($sql, true, $sess);
        if (!\is_array($data)) {
            return $data;
        }
        return \array_combine(\array_column($data, 0), \array_column($data, 1));
    }
    
    /**
     * Return Array [keys => values]
     * Similar than queryKeyValues, but using TabSeparated for data transferring.
     * Provides the best performance on powerful servers,
     *  but, on weak processors it runs slower than queryKeyValues
     *
     * @param string $tbl_or_sql
     * @param string|null $key_name_and_value_name
     * @param string|null $sess
     * @return array
     */
    public function queryKeyValArr(
        $tbl_or_sql,
        $key_name_and_value_name = null,
        $sess = null
    ) {
        if (is_null($key_name_and_value_name)) {
            $sql = $tbl_or_sql;
        } else {
            $sql = "SELECT $key_name_and_value_name FROM $tbl_or_sql";
        }
        $data = $this->queryColumn($sql, $sess);
        if (!\is_array($data)) {
            return $data;
        }
        $ret = [];
        foreach ($data as $s) {
            $x = explode("\t", $s);
            $ret[$x[0]] = $x[1];
        }
        return $ret;
    }

    /**
     * Function for queries returning an array (like SELECT * ...)
     * Returned array have numeric_keys (if $numeric_keys = true),
     *  or have keys as returned field names (in $numeric_keys = false).
     * Additional,
     * Information about field names available in $this->keys
     * Information about field types available in $this->types
     *
     * If error, return non-array data (usually string of error description)
     *
     * @param string $sql
     * @param boolean $numeric_keys
     * @param string|null $sess
     * @return array
     */
    public function queryArray($sql, $numeric_keys = false, $sess = null)
    {
        $arr = $this->queryFullArray($sql, $numeric_keys, $sess);
        if (!is_array($arr) || $numeric_keys) {
            return $arr;
        }
        $data = $arr['data'];
        foreach ([
            'data',
            'meta',
            'statistics',
            'extremes',
            'rows',
            'totals'
            ] as $key) {
            unset($arr[$key]);
        }
        $this->extra = $arr;
        return $data;
    }
    
    /**
     * Similar as queryArray, but use TabSeparated format for data transferring.
     *
     * @param string $sql
     * @param boolean $numeric_keys
     * @param string|null $sess
     * @return array
     */
    
    public function queryArr($sql, $numeric_keys = false, $sess = null)
    {
        $data = $this->queryColumn($sql, $sess, !$numeric_keys);
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
                if (!$k) {
                    $keys = $x;
                    $this->keys = $x;
                } else {
                    $ret[] = \array_combine($keys, $x);
                }
            }
        }

        $this->extra = [];
        $this->totals = $this->meta = $this->types = null;

        // Parsing extra data if found. Its may be extremes and/or total.
        if ($found_extra) {
            $c = \count($data);
            for ($l = $c + 1; $k < $l; $k++) {
                $s = ($k < $c) ? $data[$k]: '';
                if (empty($s)) {
                    if (\count($this->extra) == 1) {
                        $this->totals = $this->extra[0];
                        $this->extra = [];
                    }
                    if (\count($this->extra) == 2) {
                        $this->extremes = array_combine(
                            ['min', 'max'],
                            $this->extra);
                        $this->extra = [];
                    }
                } else {
                    $s = \explode("\t", $s);
                    if (!$numeric_keys) {
                        $s = \array_combine($keys, $s);
                    }
                    $this->extra[] = $s;
                }
            }
        }
        $this->rows = \count($ret);
        return $ret;
    }

    /**
     * Function for queries returning an array (like SELECT * ...)
     * The requested data is transmitted through the JSON format or JSONCompact.
     * If $data_only flag is false, return full array with [meta],[data], etc.
     * If $data_only is true, return only [data]-section from received array.
     * If got error, return non-array data (usually string error description)
     *
     * In $data_only mode using only JSONCompact format and returning data
     *  array have numeric keys (but field names available in $this->keys array)
     *
     * When not $data_only mode, returning data-array have keys as field names.
     *
     * @param string $sql
     * @param boolean $data_only
     * @param string|null $sess
     * @return array|string
     */
    public function queryFullArray($sql, $data_only = false, $sess = null)
    {
        $data = $this->getQuery($sql . ' FORMAT ' .
            (($this->json_compact || $data_only) ? 'JSONCompact' : 'JSON'),
            $sess
            );

        $arr = ($data['code'] == 200) ? \json_decode($data['response'], true) : 0;

        if (!is_array($arr)) {
            return isset($data['response']) ? $data['response'] : false;
        }
        
        foreach (['meta', 'statistics', 'extremes', 'rows', 'totals'] as $key) {
            $this->$key = isset($arr[$key]) ? $arr[$key]:null;
        }
        $this->keys = $keys = (is_array($this->meta) && count($this->meta)) ?
            array_column($this->meta, 'name') : null;
        $this->types = is_array($keys) ?
            array_column($this->meta, 'type') : null;

        if ($this->json_compact && !empty($keys)) {
            foreach(['data', 'extremes'] as $key) {
                if ($key == 'data' && $data_only) {
                    continue;
                }
                if (!empty($arr[$key])) {
                    foreach ($arr[$key] as $k => $ret) {
                        $ret = \array_combine($keys, $ret);
                        $arr[$key][$k] = $ret;
                    }
                    if($key != 'data') {
                        $this->$key = $arr[$key];
                    }
                }
            }
            if(!empty($this->totals)) {
                $this->totals = \array_combine($keys, $this->totals);
                $arr['totals'] = $this->totals;
            }
        }

        if ($data_only) {
            return $arr['data'];
        } else {
            return $arr;
        }
    }
}
