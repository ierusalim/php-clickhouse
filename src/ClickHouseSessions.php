<?php
namespace ierusalim\ClickHouse;

/**
 * This trait contains ClickHouseSessions
 *
 * PHP Version >=5.4
 *
 * @package    ierusalim\ClickHouseSessions
 * @author     Alexander Jer <alex@ierusalim.com>
 * @copyright  2017, Ierusalim
 * @license    https://opensource.org/licenses/Apache-2.0 Apache-2.0
 */
trait ClickHouseSessions
{
    public $default_db_name = 'default';

    private $session_data = [];

    /**
     * Auto-create session_id and send it with each request
     *
     * @var boolean
     */
    public $session_autocreate = false;


    /**
     * Set session_id for http-request options
     *
     * if session_id not specified (or specified as false) than generate and set new random id.
     * if session_id is null - clear option 'session_id' (clear current session_id)
     *
     * @param string|false|null $session_id session_id for set or false for generate new id
     * @param boolean $overwrite false = set only if session not defined, true = always
     * @return string|null Return old value of session_id option
     */
    public function setSession($session_id = false, $overwrite = true)
    {
        if ($session_id === false) {
            $session_id = \md5(\uniqid(\mt_rand(0, \PHP_INT_MAX), true));
            $this->session_data[$session_id . $this->server_url] = ['db' => $this->default_db_name];
        }
        return $this->setOption('session_id', $session_id, $overwrite);
    }

    /**
     * Return current session_id from http-options. Return null if not exists.
     *
     * @return string|null
     */
    public function getSession()
    {
        return $this->getOption('session_id');
    }

    /**
     * Get ClickHouse-session data from memory by key
     *
     * Returns session-data by specified key, or all data (array) if key===false
     * If there is no data for specified session - return false
     * If there is no data for specified key, but there is a session data - return null
     *
     * @param string|false $key Get specific parameter or false to get all parameters
     * @param string|null $sess session_id
     * @return mixed
     */
    public function getSessionData($key = 'data', $sess = null)
    {
        $su = $this->server_url;

        if (empty($sess)) {
            $sess = $this->getOption('session_id');
        }
        if (\is_string($sess) && !empty($sess)) {
            if (\is_string($key) && isset($this->session_data[$sess . $su])) {
                if (isset($this->session_data[$sess . $su][$key])) {
                    return $this->session_data[$sess . $su][$key];
                } else {
                    return null;
                }
            } elseif ($key === false && isset($this->session_data[$sess . $su])) {
                return $this->session_data[$sess . $su];
            }
        }
        return false;
    }

    /**
     * Set ClickHouse-session data to memory by key
     *
     * Returns old value if replaced, or null if there was no previous value,
     *  or false if specified session not exist)
     * If $key is false and $data is array, set this array as new session-data
     * if $key is not false and there is no session-data, returns false (not set)
     *
     * @param mixed $data Data for assignment
     * @param string $key Set specific session parameter (or set all as array if false)
     * @param string|null $sess session_id
     * @return mixed
     */
    public function setSessionData($data, $key = 'data', $sess = null)
    {
        $su = $this->server_url;

        if (empty($sess)) {
            $sess = $this->getOption('session_id');
        }

        $old_data = false;
        if (\is_string($sess) && !empty($sess)) {
            if ($key === false && \is_array($data)) {
                if (isset($this->session_data[$sess . $su])) {
                    $old_data = $this->session_data[$sess . $su];
                }
                $this->session_data[$sess . $su] = $data;
                return $old_data;
            } elseif (isset($this->session_data[$sess . $su])) {
                $old_data = null;
                if (\is_string($key)) {
                    if (isset($this->session_data[$sess . $su][$key])) {
                        $old_data = $this->session_data[$sess . $su][$key];
                    }
                    $this->session_data[$sess . $su][$key] = $data;
                }
            }
        }
        return $old_data;
    }

    /**
     * Quick get current database name.
     *
     * if session specified, return db-name from session-cache $this->session_data[]
     * if no session specified and option 'database' is not empty, return database from options.
     * if session not specified, but current session is set, try return from cache.
     * Otherwise, SQL-query 'SELECT currentDatabase()' by getCurrentDBreq
     *
     * Keep in mind that current database can be set in two ways:
     *  - by option 'database', in this case '&database=...' is sent with each request
     *  - by SQL-request 'USE $db' - this request only makes sense if sessions specified
     *
     * @param string|null|true $sess session_id (or true for read only 'database' option)
     * @return string|false String with current db-name or false if error
     */
    public function getCurrentDatabase($sess = null)
    {
        $this->to_slot = false;

        $su = $this->server_url;

        if (!empty($sess) && \is_string($sess) && isset($this->session_data[$sess . $su]['db'])) {
            return $this->session_data[$sess . $su]['db'];
        }

        $database = $this->getOption('database');
        if (!empty($database) || $sess === true) {
            return $database;
        }

        if (empty($sess)) {
            $sess = $this->getOption('session_id');
            if (!empty($sess) && isset($this->session_data[$sess . $su]['db'])) {
                return $this->session_data[$sess . $su]['db'];
            }
        }

        $ans = $this->getQuery('SELECT currentDatabase()', $sess);
        if ($ans ['code'] == 200) {
            $db = \trim($ans['response']);
            if (!empty($sess) && \is_string($sess)) {
                $this->session_data[$sess . $su]['db'] = $db;
            }
            return $db;
        }
        return false;
    }

    /**
     * Send request 'SELECT currentDatabase()' and return database name.
     *
     * @param string|null $sess
     * @return false|string Return string of database name or false if error
     */
    public function currentDBrequest($sess = null) {
        $ans = $this->getQuery('SELECT currentDatabase()', $sess);
        if ($ans ['code'] == 200) {
            $db = trim($ans['response']);
            if (\is_string($sess) && !empty($sess)) {
                $this->session_data[$sess . $this->server_url]['db'] = $db;
            }
            return $db;
        }
        return false;
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

        $su = $this->server_url;

        if (\is_string($sess) &&
            isset($this->session_data[$sess . $su]['db']) &&
            ($this->session_data[$sess . $su]['db'] === $db)
        ) {
            return false;
        }

        if ($sess === true || !$this->isSupported('session_id')) {
            $this->setOption('database', $db);
        } else {
            $ans = $this->postQuery("USE $db", [], $sess);
            if (!empty($ans['curl_error'])) {
                return $ans['curl_error'];
            } elseif ($ans['code'] != 200) {
                return $ans['response'];
            }
            if (\is_string($sess)) {
                $this->session_data[$sess . $su]['db'] = $db;
            } else {
                $this->session_data[$this->getOption('session_id') . $su]['db'] = $db;
            }
        }
        return false;
    }
}
