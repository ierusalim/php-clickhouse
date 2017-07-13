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
    
    public function setCurrentDatabase($db, $sess = null)
    {
        return $this->queryGood("USE $db", $sess);
    }
    public function getCurrentDatabase($sess = null) {
        return $this->queryValue('SELECT currentDatabase()', null, $sess);
    }
    
    public function queryGood($sql, $sess = null) {
        $ans = $this->queryValue($sql, [], $sess);
        if ($ans !== false && empty($ans)) {
            return true;
        } else {
            return $ans;
        }
    }
    public function queryValue($sql, $post_data = null, $sess = null) {

        // Do query
        $ans = $this->anyQuery($sql, $post_data, $sess);
        
        // Return string response or false if error
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
    public function queryFullArray($sql, $only_data = false, $sess = null)
    {
        $data = $this->getQuery($sql . ' FORMAT '. 
            (($this->json_compact || $only_data) ? 'JSONCompact' : 'JSON')
            , $sess
            );
        if($data['code'] != 200) {
            return $data['response'];
        }

        $arr = json_decode($data['response'], true);

        if(!is_array($arr)) return $arr;
        
        foreach(['meta', 'statistics', 'extremes', 'rows'] as $key) {
            $this->$key = isset($arr[$key]) ? $arr[$key]:null;
        }
        $this->keys = $keys = (is_array($this->meta) && count($this->meta)) ?
            array_column($this->meta, 'name') : null;
        $this->types = is_array($keys) ? 
            array_column($this->meta, 'type') : null;

        if($only_data) {
            return $arr['data'];
        }

        if($this->json_compact && !empty($keys)) {
            if(!empty($arr['data'])) {
                foreach($arr['data'] as $k=>$ret) {
                    $ret = array_combine($keys, $ret);
                    $arr['data'][$k] = $ret;
                }
            }
            if(!empty($arr['extremes'])) {
                foreach($arr['extremes'] as $k=>$ret) {
                    $ret = array_combine($keys, $ret);
                    $arr['extremes'][$k] = $ret;
                }
            }
        }
        return $arr;
    }

    public function queryData($sql, $only_data = false) {
        $arr = $this->queryFullArray($sql, $only_data);
        if (!is_array($arr) || $only_data) {
            return $arr;
        }
        if(!isset($arr['data'])) {
            return "No [data] in server answer";
        }
        $data = $arr['data'];
        foreach(['data', 'meta', 'statistics', 'extremes', 'rows' ] as $key) {
            unset($arr[$key]);
        }
        $this->extra = $arr;
        return $data;
    }

    
    /**
     * For requests like SHOW DATABASES, SHOW PROCESSLIST, etc.
     * Return array of returned names.
     * If error return string with error description.
     * 
     * @param string $about
     * @return array|string
     */
    public function showAbout($about, $first_column_only = false) {
        $data = $this->queryData('SHOW ' . $about, true);
        if (!\count($data)) {
            return [];
        }
        if($first_column_only) {
            return \array_column($data, \key($data[0]));
        } else {
            return $data;
        }
    }
    
    /**
     * Return Array contained names of existing Databases
     * 
     * @return array|string
     */
    public function showDataBases()
    {
        return $this->showAbout("DATABASES", true);
    }
    public function showProcessList()
    {
        return $this->showAbout('PROCESSLIST', false);
    }
    
    /**
     * Return names of tables from specified database or all like pattern
     * 
     * @param string|null $db
     * @param string|null $like_pattern
     * @return array|string
     */
    public function showTables($db = null, $like_pattern = null)
    {
        //SHOW TABLES [FROM db] [LIKE 'pattern']
        return $this->showAbout("TABLES"
            . (empty($db) ? '' : ' FROM ' . $db) 
            . (empty($like_pattern) ? '' : " LIKE '$like_pattern'") 
        ,true);
    }
    
    /**
     * Return description of table as Array in following format:
     * [Keys - field names] => [Values - field types]
     * 
     * @param string $table
     * @return array|string
     */
    public function describeTable($table) {
        //DESCRIBE TABLE [db.]table
        $data = $this->queryData("DESCRIBE TABLE $table");
        if (!\is_array($data)) {
            return $data;
        }
        $names = \array_column($data, 'name');
        $types = \array_column($data, 'type');
        $data = \array_combine($names, $types);
        return $data;
    }

}
