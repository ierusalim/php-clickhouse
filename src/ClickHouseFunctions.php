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
class ClickHouseFunctions extends ClickHouseReq
{
    private $from = ' FROM '; // For stupid code analyzers

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
     * Array is [Keys => field names] => [Values - field types]
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
     * Return array with numbers from ClickHouse server for tests
     *
     * @param integer $lim
     * @param boolean $use_mt
     * @return array
     */
    public function getNumbers($lim = 100, $use_mt = false)
    {
        return $this->queryColumnTab(
            'SELECT * FROM system.numbers' . ( $use_mt ? '_mt': '') .
            ' LIMIT '. $lim);
    }
}
