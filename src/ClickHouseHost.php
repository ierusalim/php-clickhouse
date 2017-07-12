<?php

namespace Ierusalim\ClickHouse;

/**
 * This class contains ClickHouse simple PHP-api-client
 *
 * PHP Version >= 5.4
 *
 * @package    ierusalim\ClickHouse
 * @author     Alexander Jer <alex@ierusalim.com>
 * @copyright  2017, Ierusalim
 * @license    https://opensource.org/licenses/Apache-2.0 Apache-2.0
 */
class ClickHouseHost
{
    private $scheme = 'http';
    private $host = '127.0.0.1';
    private $port = 8123;
    private $path = '/';
    private $user;
    private $pass;

    private $server_url;
    public $ssl_verify_host = 0; //set 2 if server have valid ssl-sertificate

    public $options = [];
    
    public $query = 'SELECT 1';

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
    
    public function query($get_query, $post_data = null)
    {
        return
            empty($post_data) ?
            $this->getQuery($get_query) :
            $this->postQuery($get_query, $post_data)
        ;
    }
    
    public function getQuery($h_query = null)
    {
        return $this->doQuery($h_query, false);
    }
    
    public function postQuery($h_query = null, $post_fields = null)
    {
        return $this->doQuery($h_query, true, $post_fields);
    }
    
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
    }
    
    public function doApiCall(
        $api_url,
        $get_params,
        $post_mode = false,
        $post_fields = null,
        $file = null
    ) {
        $api_url .= "?" . \http_build_query($get_params);

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

        return compact('code', 'curl_error', 'response');
    }
}
