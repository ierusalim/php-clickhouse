<?php

namespace Ierusalim\ClickHouse;

/**
 * This is a simple http/https connector for ClickHouse database server
 * No dependency, nothing extra.
 *
 * PHP Version >= 5.4
 *
 * Example of use:
 *  $ch = new ClickHouseAPI("http://127.0.0.1:8123");
 *  $response = $ch->getQuery("SELECT 1");
 *  if ($response['code'] != 200 || $response['response'] != 1) {
 *     die("The server does not work");
 *  }
 *  $ch->postQuery("CREATE TABLE t (a UInt8) ENGINE = Memory");
 *  $ch->postQuery('INSERT INTO t VALUES (1),(2),(3)');
 *  $data = $ch->getQuery('SELECT * FROM t FORMAT JSONCompact');
 *  $data = json_decode($data['response']);
 *  $ch->postQuery("DROP TABLE t");
 *  print_r($data);
 *
 * @package    ierusalim\ClickHouse
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
    private $scheme = 'http';
    
    /**
     * Server IP or host name
     *
     * @var string
     */
    private $host = '127.0.0.1';
    
    /**
     * Server TCP/IP-port
     *
     * @var integer
     */
    private $port = 8123;
    
    /**
     * Base path for server request
     *
     * @var string
     */
    private $path = '/';
    
    /**
     * Username if need authorization
     *
     * @var string|null
     */
    private $user;
    
    /**
     * Password if need authorisation
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
     * CURL option for do not verify hostname in SSL sertificate
     *
     * @var integer
     */
    public $ssl_verify_host = 0; //set 2 if server have valid ssl-sertificate

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
     * @param string|null $host_or_url
     * @param string|null $port
     * @param string|null $user
     * @param string|null $pass
     */
    public function __construct(
        $host_or_url = null,
        $port = null,
        $user = null,
        $pass = null
    ) {
        if (!empty($host_or_url)) {
            if (strpos($host_or_url, '/')) {
                $this->setServerUrl($host_or_url);
            } else {
                $this->host = $host_or_url;
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
     * Set server connetion parameters from url, for example:
     * set scheme=http, host=127.0.0.1, port=8123, user=default, pass=[empty]
     *  $h->setServerUrl("http://default:@127.0.0.1:8123/");
     *
     * @param string|null $server_url
     * @throws \Exception
     */
    public function setServerUrl($server_url = null)
    {
        if (!empty($server_url)) {
            $p_arr = \parse_url($server_url);
            foreach (['scheme', 'host', 'port', 'user', 'pass', 'path'] as $p) {
                if (!empty($p_arr[$p])) {
                    $this->$p = $p_arr[$p];
                }
            }
        }
        if (empty($this->scheme) || empty($this->host) || empty($this->port)) {
            throw new \Exception("Illegal server parameters");
        }
        $this->server_url = $this->scheme . '://' . $this->host
            . ':' . $this->port
            . (empty($this->path) ? '/' : $this->path);
    }
    
    /**
     * Send Get query if $post_data is empty, otherwise send Post query
     *
     * @param string     $get_query
     * @param array|null $post_data
     * @return array
     */
    public function query($get_query, $post_data = null)
    {
        return
            empty($post_data) ?
            $this->getQuery($get_query) :
            $this->postQuery($get_query, $post_data)
        ;
    }
    
    /**
     * Send Get API-query
     *
     * @param string| null $h_query
     * @return array
     */
    public function getQuery($h_query = null)
    {
        return $this->doQuery($h_query, false);
    }
    
    /**
     * Send Post API-query
     *
     * @param string|null $h_query
     * @param string|null $post_fields
     * @return array
     */
    public function postQuery($h_query = null, $post_fields = null)
    {
        return $this->doQuery($h_query, true, $post_fields);
    }
    
    /**
     * Send Get or Post API-query depends of $is_post value
     *
     * @param string $query
     * @param boolean $is_post
     * @param stirng|null $post_fields
     * @return array
     */
    public function doQuery($query = null, $is_post = false, $post_fields = null)
    {
        if (is_null($query)) {
            $query = $this->query;
        } else {
            $this->query = $query;
        }

        $user = $this->user;
        $password = $this->pass;

        $get_parameters = \array_merge(
            \compact('user', 'password', 'query'),
            $this->options
        );
        
        $response_data = $this->doApiCall(
            $this->server_url,
            $get_parameters,
            $is_post,
            $post_fields
        );
        
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

        if ($this->debug) {
            echo "Send api request: $api_url\n";
        }
        
        $ch = curl_init($api_url);

        if (!is_array($post_fields)) {
            $post_fields = array();
        }

        if ($post_mode) {
            if (!empty($file) && file_exists($file)) {
                if (function_exists('\curl_file_create')) {
                    $post_fields['file'] = \curl_file_create($file);
                } else {
                    $post_fields['file'] = "@$filename;filename="
                        . basename($filename);
                }
            }

            \curl_setopt($ch, \CURLOPT_POST, true);
            \curl_setopt($ch, \CURLOPT_POSTFIELDS, $post_fields);
        }

        \curl_setopt($ch, \CURLOPT_SSL_VERIFYPEER, false);
        \curl_setopt($ch, \CURLOPT_SSL_VERIFYHOST, $this->ssl_verify_host);

        \curl_setopt($ch, \CURLOPT_TIMEOUT, 30);
        \curl_setopt($ch, \CURLOPT_RETURNTRANSFER, true);
        \curl_setopt($ch, \CURLOPT_USERAGENT, "PHP-API");

        $response = \curl_exec($ch);

        $curl_error = \curl_error($ch);

        $code = \curl_getinfo($ch, \CURLINFO_HTTP_CODE);

        \curl_close($ch);

        if ($this->debug) {
            echo "Serever answered: ";
            print_r(compact('code', 'curl_error', 'response'));
        }
        return compact('code', 'curl_error', 'response');
    }
}
