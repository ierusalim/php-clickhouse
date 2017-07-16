<?php

namespace ierusalim\ClickHouse;

/**
 * This class contains simple http/https connector for ClickHouse db-server
 * No dependency, nothing extra.
 *
 * PHP Version >= 5.4
 *
 * This file can be used independently.
 *
 * Example of independent use:
 *  require "ClickHouseAPI.php";
 *  $ch = new ClickHouseAPI(); // connect http://127.0.0.1:8123 by default
 *  $response = $ch->getQuery("SELECT 1");
 *  print_r($response);
 *  $ch->postQuery("CREATE TABLE t (a UInt8) ENGINE = Memory");
 *  $ch->postQuery('INSERT INTO t VALUES (1),(2),(3)');
 *  $data = $ch->getQuery('SELECT * FROM t FORMAT JSONCompact');
 *  $data = json_decode($data['response']);
 *  $ch->postQuery("DROP TABLE t");
 *  print_r($data);
 *
 * @package    ierusalim/php-clickhouse
 * @author     Alexander Jer <alex@ierusalim.com>
 * @copyright  2017, Ierusalim
 * @license    https://opensource.org/licenses/Apache-2.0 Apache-2.0
 */
class ClickHouseAPI
{
    /**
     * http or https
     *
     * @var string
     */
    public $scheme = 'http';
    
    /**
     * Server IP or host name
     *
     * @var string
     */
    public $host = '127.0.0.1';
    
    /**
     * Server TCP/IP-port
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
     * Username if need authorization
     *
     * @var string|null
     */
    private $user;
    
    /**
     * Password if need authorization
     *
     * @var string|null
     */
    private $pass;

    /**
     * Server URL as scheme://host:port/
     *
     * @var type string
     */
    private $server_url;
    
    /**
     * CURL option for do not verify hostname in SSL certificate
     *
     * @var integer
     */
    public $ssl_verify_host = 0; //set 2 if server have valid ssl-sertificate
    
    /**
     * CURL option CURLOPT_TIMEOUT
     *
     * @var integer
     */
    public $curl_timeout = 30;
    
    /**
     * Last error reported by CURL or empty string if none
     *
     * @var string
     */
    public $last_curl_error_str = '';
    
    /**
     * HTTP-Code from last server response
     *
     * @var integer
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
     * @var callable|boolean
     */
    public $hook_before_api_call = false;
    
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
     *  $h = new ClickHouseAPI("http://127.0.0.1:8123/" [, $user, $pass]);
     * or
     *  $h = new ClickHouseAPI("127.0.0.1", 8321 [, $user, $pass]);
     *
     * Also, server parameters may be set late via setServerUrl($url)
     * Example:
     *  $h = new ClickHouseAPI;
     *  $h->setServerUrl("https://user:pass@127.0.0.1:8443/");
     *
     * @param string|null $host_or_full_url
     * @param string|null $port
     * @param string|null $user
     * @param string|null $pass
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
     * Example:
     * Set scheme=http, host=127.0.0.1, port=8123, user=default, pass=[empty]
     *  $h->setServerUrl("http://default:@127.0.0.1:8123/");
     *
     * @param string|null $full_server_url
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
        $this->server_url = $this->scheme . '://' . $this->host
            . ':' . $this->port
            . (empty($this->path) ? '/' : $this->path);
    }
    
    /**
     * Send Get query if $post_data is empty, otherwise send Post query
     *
     * @param string      $h_query
     * @param array|null  $post_data
     * @param string|null $sess
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
     * @param string|null $post_fields
     * @param string|null $sess
     * @param string|null $file
     * @return array
     */
    public function postQuery(
        $h_query = null,
        $post_fields = null,
        $sess = null,
        $file = null
    ) {
        return $this->doQuery($h_query, true, $post_fields, $sess, $file);
    }
    
    /**
     * Send Get or Post API-query depends of $is_post value
     *
     * @param string $query
     * @param boolean $is_post
     * @param stirng|null $post_fields
     * @param string|null $session_id
     * @param string|null $file
     * @return array
     */
    public function doQuery(
        $query = null,
        $is_post = false,
        $post_fields = null,
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
            if ($this->session_autocreate && !$this->getSession()) {
                $this->setSession();
            }
        }
        
        $h_parameters = \array_merge(
            \compact('user', 'password', 'query'),
            $this->options
        );
        
        $this->last_used_session_id = isset($h_parameters['session_id']) ?
            $h_parameters['session_id'] : null;
        
        $response_data = $this->doApiCall(
            $this->server_url,
            $h_parameters,
            $is_post,
            $post_fields,
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
     * @param string $api_url
     * @param array $get_params
     * @param boolean $post_mode
     * @param array|null $post_fields
     * @param string|null $file
     * @return array
     */
    public function doApiCall(
        $api_url,
        $get_params,
        $post_mode = false,
        $post_fields = null,
        $file = null
    ) {
        $api_url .= "?" . \http_build_query($get_params);

        if ($this->hook_before_api_call) {
            $api_url = call_user_func($this->hook_before_api_call, $api_url);
        }
        
        if ($this->debug) {
            echo ($post_mode ? 'POST' : 'GET') . "->$api_url\n" . $file;
        }
        
        $ch = curl_init($api_url);

        if ($post_mode) {
            if (empty($post_fields)) {
                $post_fields = array();
            }

            if (!empty($file) && \file_exists($file)) {
                if (function_exists('\curl_file_create')) {
                    $post_fields['file'] = \curl_file_create($file);
                } else {
                    $post_fields['file'] = "@$file;filename=" . basename($file);
                }
            }
            \curl_setopt($ch, \CURLOPT_POST, true);
            \curl_setopt($ch, \CURLOPT_POSTFIELDS, $post_fields);
        }

        \curl_setopt($ch, \CURLOPT_SSL_VERIFYPEER, false);
        \curl_setopt($ch, \CURLOPT_SSL_VERIFYHOST, $this->ssl_verify_host);

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
        return compact('code', 'curl_error', 'response');
    }

    /**
     * Set addition http-request parameter.
     *
     * @param string $key
     * @param string|null $value
     * @param boolean $overwrite
     * @return string|null
     */
    public function setOption($key, $value, $overwrite = true)
    {
        $old_value = isset($this->options[$key]) ? $this->options[$key] : null;
        if (is_null($old_value) || $overwrite) {
            $this->options[$key] = $value;
        }
        return $old_value;
    }
    
    /**
     * Get http-request parameter that was set via setOption
     *
     * @param string $key
     * @return string|null
     */
    public function getOption($key)
    {
        return isset($this->options[$key]) ? $this->options[$key] : null;
    }
    
    /**
     * Set session_id into http-request options
     * if session_id not specified (or specified as null) create and set random.
     *
     * @param string|null $session_id
     * @param boolean $overwrite
     * @return string|null
     */
    public function setSession($session_id = null, $overwrite = true)
    {
        if (is_null($session_id)) {
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
     * @param string $key
     * @return string|null
     */
    public function delOption($key)
    {
        $old_value = $this->getOption($key);
        unset($this->options[$key]);
        return $old_value;
    }
}
