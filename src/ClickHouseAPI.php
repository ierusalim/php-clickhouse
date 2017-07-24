<?php

namespace ierusalim\ClickHouse;

/**
 * This class contains simple http/https connector for ClickHouse db-server
 *
 * No dependency, nothing extra.
 *
 * API functions:
 * - >setServerUrl($url) - set ClickHouse server parameters by url (host, port, etc.)
 * - >getQuery($h_query [, $sess]) - send GET request
 * - >postQuery($h_query, $post_data [, $sess]) - send POST request
 * Sessions functions:
 * - >getSession() - get current session_id from options
 * - >setSession([$sess]) - set session_id or generate new session_id
 * Options functions:
 * - >setOption($key, $value) - set http-option for requests
 * - >getOption($key) - get http-option value
 * - >delOption($key) - delete http-option (same ->setOption($key, null)
 *
 * PHP Version >= 5.4
 *
 * This file is a part of packet php-clickhouse, but it may be used independently.
 *
 * @package    ierusalim/php-clickhouse
 * @author     Alexander Jer <alex@ierusalim.com>
 * @copyright  2017, Ierusalim
 * @license    https://opensource.org/licenses/Apache-2.0 Apache-2.0
 *
 * Example of independent use:
 *
 *  require "ClickHouseAPI.php";
 *  $ch = new ClickHouseAPI("http://127.0.0.1:8123");
 *  $response = $ch->getQuery("SELECT 1");
 *  print_r($response);
 *  $ch->postQuery("CREATE TABLE t (a UInt8) ENGINE = Memory");
 *  $ch->postQuery('INSERT INTO t VALUES (1),(2),(3)');
 *  $data = $ch->getQuery('SELECT * FROM t FORMAT JSONCompact');
 *  $data = json_decode($data['response']);
 *  $ch->postQuery("DROP TABLE t");
 *  print_r($data);
 */
class ClickHouseAPI
{
    /**
     * Protocol for access to ClickHouse server
     *
     * @var string http or https
     */
    public $scheme = 'http';

    /**
     * ClickHouse server IP or host name
     *
     * @var string
     */
    public $host = '127.0.0.1';

    /**
     * ClickHouse server TCP/IP-port
     *
     * @var integer
     */
    public $port = 8123;

    /**
     * Base path for server request
     *
     * @var string
     */
    public $path = '/';

    /**
     * Username if need authorization on ClickHouse server
     *
     * @var string|null
     */
    private $user;

    /**
     * Password if need authorization on ClickHouse server
     *
     * @var string|null
     */
    private $pass;

    /**
     * Server URL as scheme://host:port/
     *
     * @var string|null
     */
    private $server_url;

    /**
     * CURL option for do not verify hostname in SSL certificate
     *
     * @var integer 0 by default. Set 2 if server have valid ssl-sertificate
     */
    public $ssl_verify_host = 0;

    /**
     * CURL option CURLOPT_CONNECTTIMEOUT
     *
     * @var integer 2 sec. by default
     */
    public $curl_conn_timeout = 3;

    /**
     * CURL option CURLOPT_TIMEOUT
     *
     * @var integer 60 sec. by default
     */
    public $curl_timeout = 60;

    /**
     * Last error reported by CURL or empty string if none
     *
     * @var string
     */
    public $last_curl_error_str = '';

    /**
     * HTTP-Code from last server response
     *
     * @var mixed
     */
    public $last_code;

    /**
     * Options for http-request
     *
     * @var array
     */
    public $options = [];

    /**
     * Set true for show sending request and server answers
     *
     * @var boolean
     */
    public $debug = false;

    /**
     * Last sent query or default query when not specified
     *
     * @var string
     */
    public $query = 'SELECT 1';

    /**
     * Hook on doApiCall executing, before send request.
     *
     * @var callable|false
     */
    public $hook_before_api_call = false;

    /**
     * Version of ClickHouse server
     *
     * @var string|null
     */
    public $server_version;

    /**
     * list of support features array
     *
     * @var array
     */
    public $support_fe = [];

    /**
     * Auto-create session_id and send it with each request
     *
     * @var boolean
     */
    public $session_autocreate = true;

    /**
     * Last used session_id
     *
     * @var string|null
     */
    public $last_used_session_id;

    /**
     * Two formats supported for set server parameters when creating object:
     *
     *  $h = new ClickHouseAPI("http://127.0.0.1:8123/" [, $user, $pass]);
     *
     *  $h = new ClickHouseAPI("127.0.0.1", 8321 [, $user, $pass]);
     *
     * Also, server parameters may be set late via setServerUrl($url)
     *
     * Example:
     *  $h = new ClickHouseAPI;
     *  $h->setServerUrl("https://user:pass@127.0.0.1:8443/");
     *
     * @param string|null $host_or_full_url Host name or Full server URL
     * @param integer|null $port TCP-IP port of ClickHouse server
     * @param string|null $user user for authorization (if need)
     * @param string|null $pass password for authorization (if need)
     */
    public function __construct(
        $host_or_full_url = null,
        $port = null,
        $user = null,
        $pass = null
    ) {
        if (!empty($host_or_full_url)) {
            if (strpos($host_or_full_url, '/')) {
                $this->setServerUrl($host_or_full_url);
            } else {
                $this->host = $host_or_full_url;
            }
        }
        if (!empty($port)) {
            $this->port = $port;
        }
        if (!is_null($user)) {
            $this->user = $user;
        }
        if (!is_null($pass)) {
            $this->pass = $pass;
        }
        $this->setServerUrl();
    }

    /**
     * Set server connection parameters from url
     *
     * Example:
     * - Set scheme=http, host=127.0.0.1, port=8123, user=default, pass=[empty]
     * - ->setServerUrl("http://default:@127.0.0.1:8123/");
     *
     * @param string|null $full_server_url Full server URL
     * @throws \Exception
     */
    public function setServerUrl($full_server_url = null)
    {
        if (!empty($full_server_url)) {
            $p_arr = \parse_url($full_server_url);
            foreach (['scheme', 'host', 'port', 'user', 'pass', 'path'] as $p) {
                if (!empty($p_arr[$p])) {
                    $this->$p = $p_arr[$p];
                }
            }
        }
        if (empty($this->scheme) ||
            empty($this->host) ||
            empty($this->port) ||
            !in_array($this->scheme, ['http', 'https'])
        ) {
            throw new \Exception("Illegal server parameters");
        }
        $this->server_url = $this->scheme . '://' . $this->host . ':' . $this->port
            . (empty($this->path) ? '/' : $this->path);
    }

    /**
     * Send Get query if $post_data is empty, otherwise send Post query
     *
     * @param string            $h_query Parameters send in http-request after "?"
     * @param array|string|null $post_data Parameters send in request body
     * @param string|null       $sess session_id
     * @return array
     */
    public function anyQuery($h_query, $post_data = null, $sess = null)
    {
        return
            \is_null($post_data) ?
            $this->getQuery($h_query, $sess) : $this->postQuery($h_query, $post_data, $sess)
        ;
    }

    /**
     * Send Get API-query
     *
     * @param string|null $h_query
     * @param string|null $sess
     * @return array
     */
    public function getQuery($h_query = null, $sess = null)
    {
        return $this->doQuery($h_query, false, null, $sess);
    }

    /**
     * Send Post API-query
     *
     * @param string|null $h_query
     * @param array|string|null $post_data
     * @param string|null $sess
     * @param string|null $file
     * @return array
     */
    public function postQuery(
        $h_query = null,
        $post_data = null,
        $sess = null,
        $file = null
    ) {
        return $this->doQuery($h_query, true, $post_data, $sess, $file);
    }

    /**
     * Send Get or Post API-query depends of $is_post parameter
     *
     * @param string $query SQL-query for send to ClickHouse server
     * @param boolean $is_post true for send POST-request, false for GET
     * @param array|string|null $post_data for send in POST-request body
     * @param string|null $session_id session_id
     * @param string|null $file file name (full name with path) for send
     * @return array
     */
    public function doQuery(
        $query = null,
        $is_post = false,
        $post_data = null,
        $session_id = null,
        $file = null
    ) {
        if (is_null($query)) {
            $query = $this->query;
        } else {
            $this->query = $query;
        }

        $user = $this->user;
        $password = $this->pass;

        // Set session if need
        if (!empty($session_id) && $this->getSession() != $session_id) {
            $old_session = $this->setSession($session_id);
        } else {
            if ($this->session_autocreate && $this->getSession() === null) {
                $this->setSession();
            }
        }

        $h_parameters = \array_merge(
            \compact('user', 'password', 'query'),
            $this->options
        );

        if (isset($h_parameters['session_id']) && !$this->isSupported('session_id')) {
            unset($h_parameters['session_id']);
        }


        $this->last_used_session_id = isset($h_parameters['session_id']) ?
            $h_parameters['session_id'] : null;

        $response_data = $this->doApiCall(
            $this->server_url,
            $h_parameters,
            $is_post,
            $post_data,
            $file
        );

        // Restore old session if need
        if (!empty($old_session)) {
            $this->setSession($old_session);
        }

        return $response_data;
    }

    /**
     * Send API query on server and get answer
     *
     * @param string $api_url Full URL of server
     * @param array $h_params Parameter after "?"
     * @param boolean $post_mode true for POST request, false for GET request
     * @param array|string|null $post_data Data for send in body of POST-request
     * @param string|null $file file name (full name with path) for send
     * @return array
     */
    public function doApiCall(
        $api_url,
        $h_params,
        $post_mode = false,
        $post_data = null,
        $file = null
    ) {
        $api_url .= "?" . \http_build_query($h_params);

        if ($this->hook_before_api_call) {
            $api_url = call_user_func($this->hook_before_api_call, $api_url, $this);
        }

        if ($this->debug) {
            echo ($post_mode ? 'POST' : 'GET') . "->$api_url\n" . $file;
        }

        $ch = curl_init($api_url);

        if ($post_mode) {
            if (empty($post_data)) {
                $post_data = array();
            }

            if (!empty($file) && \file_exists($file)) {
                if (\function_exists('\curl_file_create')) {
                    $post_data['file'] = \curl_file_create($file);
                } else {
                    $post_data['file'] = "@$file;filename=" . \basename($file);
                }
            }
            \curl_setopt($ch, \CURLOPT_POST, true);
            \curl_setopt($ch, \CURLOPT_POSTFIELDS, $post_data);
        }

        \curl_setopt($ch, \CURLOPT_SSL_VERIFYPEER, false);
        \curl_setopt($ch, \CURLOPT_SSL_VERIFYHOST, $this->ssl_verify_host);

        \curl_setopt($ch, \CURLOPT_CONNECTTIMEOUT, $this->curl_conn_timeout);
        \curl_setopt($ch, \CURLOPT_TIMEOUT, $this->curl_timeout);
        \curl_setopt($ch, \CURLOPT_RETURNTRANSFER, true);
        \curl_setopt($ch, \CURLOPT_USERAGENT, "PHP-ClickHouse");

        $response = \curl_exec($ch);

        $this->last_curl_error_str = $curl_error = \curl_error($ch);

        $this->last_code = $code = \curl_getinfo($ch, \CURLINFO_HTTP_CODE);

        \curl_close($ch);

        if ($this->debug) {
            echo "HTTP $code $curl_error \n{\n$response\n}\n";
        }
        return \compact('code', 'curl_error', 'response');
    }

    /**
     * Set addition http-request parameter.
     *
     * @param string $key option name
     * @param string|null $value option value
     * @param boolean $overwrite true = set always, false = set only if not defined
     * @return string|null Return old value (or null if old value undefined)
     */
    public function setOption($key, $value, $overwrite = true)
    {
        $old_value = isset($this->options[$key]) ? $this->options[$key] : null;
        if (is_null($old_value) || $overwrite) {
            if (is_null($value)) {
                unset($this->options[$key]);
            } else {
                $this->options[$key] = $value;
            }
        }
        return $old_value;
    }

    /**
     * Get http-request parameter that was set via setOption
     *
     * @param string $key option name
     * @return string|null Return option value or null if option not defined
     */
    public function getOption($key)
    {
        return isset($this->options[$key]) ? $this->options[$key] : null;
    }

    /**
     * Set session_id into http-request options
     * if session_id not specified (or specified as null) create and set random.
     *
     * @param string|null $session_id session_id or null for generate new id
     * @param boolean $overwrite true = set only if session not defined, false = always
     * @return string|null Return old value of session_id option
     */
    public function setSession($session_id = null, $overwrite = true)
    {
        if (\is_null($session_id)) {
            $session_id = \md5(\uniqid(\mt_rand(0, \PHP_INT_MAX), true));
        }
        return $this->setOption('session_id', $session_id, $overwrite);
    }

    /**
     * Return current session_id from http-req options (or null if not exists)
     *
     * @return string|null
     */
    public function getSession()
    {
        return $this->getOption('session_id');
    }

    /**
     * Delete http-request option by specified key
     *
     * @param string $key Option name
     * @return string|null Return old value of deleted option
     */
    public function delOption($key)
    {
        $old_value = $this->getOption($key);
        unset($this->options[$key]);
        return $old_value;
    }

    /**
     * Check supported feature by string name
     *
     * @param string $fe_key feature name
     * @param boolean $re_check set true for check again
     * @return boolean true = supported, false = unsupported
     */
    public function isSupported($fe_key = 'session_id', $re_check = false)
    {
        if (!isset($this->support_fe[$fe_key]) || $re_check) {
            if ($fe_key == 'query' || $fe_key == 'session_id') {
                $this->getVersion($re_check);
            } else {
                return false;
            }
        }
        return $this->support_fe[$fe_key];
    }

    /**
     * Return version of ClickHouse server by function SELECT version()
     *
     * Do SELECT version() + session_id to see if a session is supported or not
     * if session_id not supported, request is send again, but without session_id.
     * - Depending on result, the isSupported('session_id') is set true or false.
     * - If server version response unrecognized, isSupported('query') set false.
     *
     * @param boolean $re_check Set true for re-send query to server
     * @return string|boolean String version or false if error
     */
    public function getVersion($re_check = false)
    {
        if (\is_null($this->server_version) || $re_check) {
            $old_sess = $this->setSession(null, false);
            $query = 'SELECT version()';
            $ans = $this->doGet($query, ['session_id' => $this->getSession()]);
            if (!($this->support_fe['session_id'] = $ans['code'] != 404)) {
                $ans = $this->doGet($query);
            }
            $ver = explode("\n", $ans['response']);
            $ver = (count($ver) == 2 && strlen($ver[0]) < 32) ? $ver[0] : "Unknown";
            $this->support_fe['query'] = \is_string($ver) && (\count(\explode(".", $ver)) > 2);
            if (!$this->support_fe['query']) {
                $this->support_fe['session_id'] = false;
            }
            if (!$this->support_fe['session_id']) {
                $this->session_autocreate = false;
            }
            $this->server_version = $ver;
            $this->setOption('session_id', $old_sess);
        }
        return $this->server_version;
    }

    /**
     * Do GET-request with base options (query, user, password) + specified options
     *
     * The result array is the same as for the function doApiCall
     *
     * @param string $query will be set as an option ?query=...
     * @param array $h_opt Other in-url-options for make GET-request
     * @return array
     */
    public function doGet($query, $h_opt = [])
    {
        $user = $this->user;
        $password = $this->pass;
        $database = $this->getOption('database');
        $h_opt_arr = \array_merge(\compact('query', 'user', 'password', 'database'), $h_opt);
        return $this->doApiCall($this->server_url, $h_opt_arr);
    }
}
