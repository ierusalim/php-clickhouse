<?php
namespace ierusalim\ClickHouse;

/**
 * Class ClickHouseReq
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
     * @param string $db
     * @param string|null $sess
     * @return boolean
     */
    public function setCurrentDatabase($db, $sess = null)
    {
        return $this->queryGood("USE $db", $sess);
    }

    public function queryGood($sql, $sess = null)
    {
        $ans = $this->queryValue($sql, [], $sess);
        if ($ans !== false && empty($ans)) {
            return true;
        } else {
            return $ans;
        }
    }
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
     * Query return strings array in format "TabSeparated"
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
        if($c && empty($data[$c-1])) {
            unset($data[$c-1]);
        } 
        return $data;
    }

    public function queryKeyValues($tbl, $key_name, $value_name, $is_sql = 0)
    {
        if ($is_sql) {
            $sql = $tbl;
        } else {
            $sql = "SELECT $key_name, $value_name FROM $tbl";
        }
        $data = $this->queryArray($sql);
        if (!\is_array($data)) {
            return $data;
        }
        $names = \array_column($data, $key_name);
        $values = \array_column($data, $value_name);
        $data = \array_combine($names, $values);
        return $data;
    }

    public function queryArray($sql, $numeric_keys = false, $sess = null)
    {
        $arr = $this->queryFullArray($sql, $numeric_keys, $sess);
        if (!is_array($arr) || $numeric_keys) {
            return $arr;
        }
        if (!isset($arr['data'])) {
            return "No [data] in server answer";
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
    
    /**
     * Return Array contained names of existing Databases
     *
     * @return array|string
     */
    public function getDatabasesList()
    {
        return $this->queryColumn("SHOW DATABASES");
    }

    /**
     * Return names of tables from specified database or all like pattern
     *
     * @param string|null $db_name
     * @param string|null $like_pattern
     * @return array|string
     */
    public function getTablesList($db_name = null, $like_pattern = null)
    {
        //SHOW TABLES [FROM db] [LIKE 'pattern']
        return $this->queryColumn("SHOW TABLES"
            . (empty($db_name) ? '' : ' FROM ' . $db_name)
            . (empty($like_pattern) ? '' : " LIKE '$like_pattern'"), true);
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
     * Array is [Keys => field names] => [Values - field types]
     *
     * @param string $table
     * @return array|string
     */
    public function getTableFields($table)
    {
        //DESCRIBE TABLE [db.]table
        return $this->queryKeyValues("DESCRIBE TABLE $table", 'name', 'type', 1);
    }
    
    /**
     * Return as Array [names=>values] data from system.settings table
     *
     * @return array|string
     */
    public function getSystemSettings()
    {
        return $this->queryKeyValues('system.settings', 'name', 'value');
    }
    public function getVersion()
    {
        return $this->queryValue('SELECT version()');
    }
    public function getUptime()
    {
        return $this->queryValue('SELECT uptime()');
    }
    
    public function getNumbers($lim = 100, $use_mt = false)
    {
        return $this->queryColumn(
            'SELECT * FROM system.numbers' . ( $use_mt ? '_mt': '') .
            ' LIMIT '. $lim);
    }
}
