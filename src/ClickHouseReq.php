<?php
namespace ierusalim\ClickHouse;

/**
 * Class ClickHouseReq for make queries to ClickHouse
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
    public $json_compact = true;
    
    public $meta;
    public $keys;
    public $types;
    public $statistics;
    public $rows;
    public $extra;
    
    /**
     * String contained last error which returned by CURL or in server response
     *
     * @var string
     */
    public $last_error_str = '';

    /**
     * Last response for plain requests like queryGood, queryValue
     * without 'trim' executed
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
     * Return strings array in "TabSeparated"-format
     * If return one column, result array no need transformations.
     * If return more of one column, array strings need be explode by tab
     *
     * @param string $sql
     * @param string|null $sess
     * @return array
     */
    public function queryColumn($sql, $sess = null)
    {
        $data = $this->getQuery($sql . ' FORMAT TabSeparated', $sess);
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
     * Return [keys => values] Array
     * From table by 2 specified names
     * or results of any SQL-query with 2 columns
     *
     * @param string $tbl_or_sql
     * @param string|null $key_name_and_value_name
     * @return array
     */
    public function queryKeyValues($tbl_or_sql, $key_name_and_value_name = null)
    {
        if (is_null($key_name_and_value_name)) {
            $sql = $tbl_or_sql;
        } else {
            $sql = "SELECT $key_name_and_value_name FROM $tbl_or_sql";
        }
        $data = $this->queryArray($sql, true);
        if (!\is_array($data)) {
            return $data;
        }
        $names = \array_column($data, 0);
        $values = \array_column($data, 1);
        $data = \array_combine($names, $values);
        return $data;
    }

    public function queryArray($sql, $numeric_keys = false, $sess = null)
    {
        $arr = $this->queryFullArray($sql, $numeric_keys, $sess);
        if (!is_array($arr) || $numeric_keys) {
            return $arr;
        }
        $data = $arr['data'];
        foreach (['data', 'meta', 'statistics', 'extremes', 'rows' ] as $key) {
            unset($arr[$key]);
        }
        $this->extra = $arr;
        return $data;
    }
    
    public function queryFullArray($sql, $data_only = false, $sess = null)
    {
        $data = $this->getQuery($sql . ' FORMAT ' .
            (($this->json_compact || $data_only) ? 'JSONCompact' : 'JSON'),
            $sess
            );

        if ($data['code'] != 200) {
            return $data['response'];
        }

        $arr = json_decode($data['response'], true);

        if (!is_array($arr)) {
            return $arr;
        }

        foreach (['meta', 'statistics', 'extremes', 'rows'] as $key) {
            $this->$key = isset($arr[$key]) ? $arr[$key]:null;
        }
        $this->keys = $keys = (is_array($this->meta) && count($this->meta)) ?
            array_column($this->meta, 'name') : null;
        $this->types = is_array($keys) ?
            array_column($this->meta, 'type') : null;

        if ($data_only) {
            return $arr['data'];
        }

        if ($this->json_compact && !empty($keys)) {
            if (!empty($arr['data'])) {
                foreach ($arr['data'] as $k => $ret) {
                    $ret = array_combine($keys, $ret);
                    $arr['data'][$k] = $ret;
                }
            }
            if (!empty($arr['extremes'])) {
                foreach ($arr['extremes'] as $k => $ret) {
                    $ret = array_combine($keys, $ret);
                    $arr['extremes'][$k] = $ret;
                }
            }
        }
        return $arr;
    }
    public function binding($pattern, $vars_arr)
    {
        $src_arr = [];
        $rep_arr = [];
        foreach ($vars_arr as $k => $v) {
            $src_arr[]=':'.$k;
            $rep_arr[]=$v;
        }
        return str_replace($src_arr, $rep_arr, $pattern);
    }
}
