<?php

namespace ierusalim\ClickHouse;

/**
 * This class contains simple http/https connector for ClickHouse db-server
 *
 * API simple requests functions:
 * - query($sql [,$post_data]) - object-oriented style SQL-query (return $this)
 * - getQuery($query [, $sess]) - function. Send GET request and get raw-response
 * - postQuery($query, $post_data [, $sess]) - send POST request and get raw-response
 *
 * Async (parallel) requests:
 * - Set toSlot(name) before any request, and the request will be launched asynchronously.
 * For example:
 * - toSlot("name")->query($sql) - start async-query, results will be written to slot "name"
 * Get results from this slot may at any time later:
 * - slotResults("name") - get results from slot "name".
 *
 * Server-state functions:
 * - setServerUrl($url) - set ClickHouse server parameters from url (host, port, etc.)
 * - getVersion() - return version of ClickHouse server. Side effect: detect server features.
 * - isSupported(feature-name) - returns server-dependent variables about supported features.
 *
 * Options:
 * - setOption($key, $value) - set http-option for all next requests
 * - getOption($key) - get current http-option value for specified $key
 * - delOption($key) - delete http-option (same ->setOption($key, null)
 *
 * Sessions:
 *  Check isSupported('session_id'). Relevant only for new ClickHouse versions.
 * - getSession() - get current session_id from options
 * - setSession([$sess]) - set specified session_id or generate new session_id
 *
 *
 * PHP Version >= 5.5
 *
 * This file is a part of packet php-clickhouse, but it may be used independently.
 *
 * @package    ierusalim/php-clickhouse
 * @author     Alexander Jer <alex@ierusalim.com>
 * @copyright  2017, Ierusalim
 * @license    https://opensource.org/licenses/Apache-2.0 Apache-2.0
 */
class ClickHouseAPI
{
    /*
     * Trait for async requests
     */
    use ClickHouseSlots;
    /*
     * Trait for currentDatabase() and setCurrentDatabase();
     */
    use ClickHouseSessions;

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
     * Options for http-request. Array [option => value]
     *
     * @var array
     */
    public $options = [];

    /**
     * CURL options for each request
     *
     * @var array
     */
    public $curl_options = [
        \CURLOPT_RETURNTRANSFER => true,
        \CURLOPT_USERAGENT => "PHP-ClickHouse",

        //Set 2 if server have valid ssl-sertificate
        \CURLOPT_SSL_VERIFYHOST => 0,
        \CURLOPT_SSL_VERIFYPEER => false,

        // time limits in seconds
        \CURLOPT_CONNECTTIMEOUT => 0,
//        \CURLOPT_TIMEOUT => 99,
        \CURLOPT_TIMEOUT_MS => 99999,

        // its recommended for resolve "Timeout was reached"-problem, but dont resolve.
 //       \CURLOPT_NOSIGNAL => 1,

        \CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_1,

        // optional, if the request-headers sent is interesting
        \CURLINFO_HEADER_OUT => true,
    ];

    /**
     * Parameters, interesting for get by function curl_getinfo after request.
     *
     * @var array|false
     */
    public $curl_info = [
        // must be present
        \CURLINFO_HTTP_CODE => 0,
        \CURLINFO_HEADER_SIZE =>0,

        \CURLINFO_CONTENT_TYPE => 0,

        // optional: size of compressed response (or full size if compression is off)
        \CURLINFO_SIZE_DOWNLOAD => 0,

        // optional: size of body-data sent
        \CURLINFO_SIZE_UPLOAD => 0,

        // optional, if the request-headers sent is interesting
        \CURLINFO_HEADER_OUT => 0,
    ];

    /**
     * Last error reported by CURL or empty string if no errors
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
     * Set true for show sending requests and server answers
     *
     * @var boolean
     */
    public $debug = false;

    /**
     * Last sent query or query for sending if when explicitly not specified
     *
     * @var string
     */
    public $query = 'SELECT 1';

    /**
     * Hook on doApiCall executing (before send request, can modify url)
     *
     * @var callable|false
     */
    public $hook_before_api_call = false;

    /**
     * True if running under windows, false otherwise
     *
     * @var boolean
     */
    public $is_windows;

    /**
     * File handler
     *
     * @var resource|null
     */
    public $fh;

    /**
     * Results of last ->query(sql) request
     * Contains string with error description or data non-empty server response.
     * If no errors and server response is empty, this value not changed.
     *
     * @var string
     */
    public $results;

    /**
     * Two formats are supported for set server parameters:
     *
     *  1) new ClickHouseAPI($server_url [, $user, $pass]);
     *
     *  2) new ClickHouseAPI($host, $port [, $user, $pass]);
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
        $this->is_windows = \DIRECTORY_SEPARATOR !== '/';

        if (!empty($host_or_full_url)) {
            if (\strpos($host_or_full_url, '/')) {
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

        $this->setCompression(true);

        $this->setServerUrl();
    }

    /**
     * Set server connection parameters from url
     *
     * Object-oriented style, return $this if ok, throw \Exception on errors
     *
     * Example:
     * - Set scheme=http, host=127.0.0.1, port=8123, user=default, pass=[empty]
     * - setServerUrl("http://default:@127.0.0.1:8123/");
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

        if ($this->host) {
            // If host is set, we assume that the server accepts requests
            $this->isSupported('query', false, true);
        }

        return $this;
    }

    /**
     * Object-oriented style ->query($sql [,$post_data])->query(...)
     *
     * Sends SQL-query to server (always in POST-mode)
     * - If server response not empty, places results to $this->results.
     * - Note that there is an empty string at the end of the response line \n
     * - Note that if server return an empty result and the value $this->results does not change
     * - Note that requests are sent only if isSupported('query') is true
     *
     * Throws an exception if there is an error. Return $this-object if not error.
     *
     * @param string $sql SQL-query
     * @param array|string|null $post_data Parameters send in request body
     * @param string|null $sess session_id
     * @return $this
     * @throws \Exception
     */
    public function query($sql, $post_data = null, $sess = null)
    {
        if (!$this->isSupported('query')) {
            throw new \Exception("Server does not accept ClickHouse-requests");
        }
        $to_slot = $this->to_slot;
        $ans = $this->postQuery($sql, $post_data, $sess);
        if (empty($to_slot)) {
            if (!empty($ans['curl_error'])) {
                $this->results = $ans['curl_error'];
                throw new \Exception($this->results);
            }
            if (!empty($ans['response'])) {
                $this->results = $ans['response'];
            }
            if ($ans['code'] != 200) {
                throw new \Exception(\substr($ans['response'], 0, 2048));
            }
        }
        return $this;
    }

    /**
     * Send Get query if $post_data is empty, otherwise send Post query
     * This is a multiplexor for functions getQuery|postQuery
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
     * Function is envelope for doQuery
     *
     * @param string|null $query
     * @param string|null $sess
     * @return array
     */
    public function getQuery($query = null, $sess = null)
    {
        return $this->doQuery($query, false, null, $sess);
    }

    /**
     * Send Post API-query
     *
     * Function is envelope for doQuery
     *
     * @param string|null $query
     * @param array|string|null $post_data
     * @param string|null $sess
     * @param string|null $file
     * @return array
     */
    public function postQuery(
        $query = null,
        $post_data = null,
        $sess = null,
        $file = null
    ) {
        return $this->doQuery($query, true, $post_data, $sess, $file);
    }

    /**
     * Send Get or Post API-query depends of $is_post parameter
     *
     * Function is envelope for doApiCall
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
        $file = null,
        $put = false
    ) {
        if (\is_null($query)) {
            $query = $this->query;
        } else {
            $this->query = $query;
        }

        $user = $this->user;
        $password = $this->pass;

        // Set session if need
        if (!empty($session_id) && ($this->getSession() !== $session_id)) {
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

        $response_data = $this->doApiCall(
            $this->server_url, $h_parameters, $is_post, $post_data, $file, $put
        );

        // Restore old session if need
        if (!empty($old_session)) {
            $this->setSession($old_session);
        }

        return ($response_data['code'] === 102) ? $this : $response_data;
    }

    /**
     * Function for send API query to server and get answer
     *
     * @param string|false $api_url Full URL of server API (false => $this->server_url)
     * @param array $h_params Parameters for adding after "?"
     * @param boolean $post_mode true for POST request, false for GET request
     * @param array|string|null $post_data Data for send in body of POST-request
     * @param string|null $file file name (full name with path) for send
     * @return array|resource
     */
    public function doApiCall($api_url,
        $h_params,
        $post_mode = false,
        $post_data = null,
        $file = null,
        $put = false
    ) {
        $yi = $this->yiDoApiCall($api_url, $h_params, $post_mode, $post_data, $file, $put);
        $curl_h = $yi->current();
        if (empty($this->to_slot)) {
            $response = \curl_exec($curl_h);
            $curl_error = \curl_error($curl_h);
            $curl_info = [];
            foreach (\array_keys($this->curl_info) as $key) {
                $curl_info[$key] = \curl_getinfo($curl_h, $key);
            }
            $this->curl_info = $curl_info;
            \curl_close($curl_h);
            $response_arr = $yi->send(\compact('response', 'curl_error', 'curl_info'));
            if ($this->debug) {
                $yi->next();
            }
        } else {
            $response_arr = $this->slotStart($this->to_slot, $curl_h,
                ['mode' => 1, 'fn' => $yi, 'par' => 'doApiCall']);
        }
        return $response_arr;
    }

    public function yiDoApiCall($api_url,
        $h_params,
        $post_mode = false,
        $post_data = null,
        $file = null,
        $put_mode = false
    ) {
        if (empty($api_url)) {
            $api_url = $this->server_url;
        }
        $api_url .= "?" . \http_build_query($h_params);

        if ($this->hook_before_api_call) {
            $api_url = call_user_func($this->hook_before_api_call, $api_url, $this);
        }

        if ($this->debug) {
            echo ($post_mode ? 'POST' : 'GET') . "->$api_url\n" . $file;
        }

        $curl_h = curl_init($api_url);

        if ($post_mode) {
            if (empty($post_data)) {
                $post_data = array();
            }

            if (!empty($file) && \file_exists($file)) {
                if ($put_mode) {
                    $this->fh = \fopen($file, 'rb');
                    if (substr($file, -3) !== '.gz') {
                        if ($this->is_windows) {
                            // if Windows, compress file before sending
                            $dest = $file . '.gz';
                            if ($fp_out = \gzopen($dest, 'wb')) {
                                while (!\feof($this->fh)) {
                                    \gzwrite($fp_out, fread($this->fh, 524288));
                                }
                                \fclose($this->fh);
                                \gzclose($fp_out);
                                $this->fh = \fopen($dest, 'rb');
                            } else {
                                throw new \Exception("Can't read source file $file");
                            }
                        } else {
                            // if not Windows - can gzip on the fly
                            \stream_filter_append($this->fh, 'zlib.deflate',
                                \STREAM_FILTER_READ, ["window" => 30]);
                        }
                    }

                    \curl_setopt($curl_h, \CURLOPT_HTTPHEADER, [
                        'Content-Type: application/x-www-form-urlencoded',
                        'Content-Encoding: gzip',
                        'Expect:',
                        ]);

                    \curl_setopt($curl_h, \CURLOPT_SAFE_UPLOAD, 1);
                    \curl_setopt($curl_h, \CURLOPT_PUT, true);
                    \curl_setopt($curl_h, \CURLOPT_INFILE, $this->fh);
                } else {
                    $post_data['file'] = \curl_file_create($file);
                }
            }
            \curl_setopt($curl_h, \CURLOPT_POST, true);
            \curl_setopt($curl_h, \CURLOPT_POSTFIELDS, $post_data);
        }
        \curl_setopt_array($curl_h, $this->curl_options);

        $response_arr = (yield $curl_h);
        \extract($response_arr); // $response, $curl_error, $curl_info

        $this->last_curl_error_str = $curl_error;
        $this->last_code = $code = $curl_info[\CURLINFO_HTTP_CODE];

        yield \compact('code', 'curl_error', 'response');
        echo "HTTP $code $curl_error \n\n$response\n}\n";
    }
    /**
     * Set http-compression mode on/off
     *
     * @param boolean $true_or_false
     */
    public function setCompression($true_or_false)
    {
        if ($true_or_false) {
            $this->setOption('enable_http_compression', 1);
            $this->curl_options[\CURLOPT_ENCODING] = 'gzip';
        } else {
            $this->setOption('enable_http_compression', null);
            unset($this->curl_options[\CURLOPT_ENCODING]);
        }
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
        if (\is_null($old_value) || $overwrite) {
            if (\is_null($value)) {
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
     * Delete http-option by specified key
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
     * Check feature support by name
     *
     * @param string $fe_key feature name
     * @param boolean $re_check set true for check again
     * @param boolean|null $set for set is_feature value
     * @return boolean|string false = unsupported, true or string = supported,
     */
    public function isSupported($fe_key = 'session_id', $re_check = false, $set = null)
    {
        static $support_fe = [];
        $s_key = $this->server_url;
        if (!isset($support_fe[$s_key])) {
            // First connect of server $s_key
            $support_fe[$s_key] = [];
        }
        if (\is_null($set)) {
            if ($re_check && $fe_key === 'version') {
                return $this->getVersion($re_check);
            }
            if (!isset($support_fe[$s_key][$fe_key]) || $re_check) {
                if ($fe_key == 'session_id' || ($fe_key === 'query' && $re_check)) {
                    $this->getVersion($re_check);
                } else {
                    return false;
                }
            }
        } else {
            $support_fe[$s_key][$fe_key] = $set;
        }
        return isset($support_fe[$s_key][$fe_key]) ? $support_fe[$s_key][$fe_key] : false;
    }

    /**
     * Return version of ClickHouse server by function SELECT version()
     *
     * send 2 requests 'SELECT version()' with and without session_id.
     * - Depending on results, the isSupported('session_id') is set true or false.
     * - If server version response unrecognized, isSupported('query') set false.
     * - for get cached value of 'version' may use function isSupported('version')
     *
     * @param boolean $re_check Set true for re-send query to server
     * @return string|boolean String version or false if error
     */
    public function getVersion($re_check = false)
    {
        $ver = $this->isSupported('version');
        if (empty($ver) || $re_check) {
            $slot1 = '_vs_' . $this->server_url;
            $slot2 = '_vq_' . $this->server_url;
            $status = $this->slotCheck($slot1);
            if ($status === false) {
                $this->versionSendQuery();
            } elseif ($status && $re_check) {
                $this->eraseSlot($slot1);
                $this->eraseSlot($slot2);
                $this->versionSendQuery();
            }
            $this->slotWaitReady($slot1);
            $this->slotWaitReady($slot2);
            $ver = $this->isSupported('version');
            if (empty($ver)) {
                $ver = 'Unknown';
                $this->isSupported('version', false, $ver);
            }
        }
        return $ver;
    }

    /**
     * Send background async-requests about server version and supported features
     *
     * @return null
     */
    public function versionSendQuery()
    {
        // save to_slot and old session_id for restore it after complete
        $old_to_slot = $this->to_slot;
        $old_sess = $this->setSession(false, false);

        $query = 'SELECT version()';

        // hook-function for each requests execute when request is finished
        $fn = function($obj, $slot_low, $par) {
            // at first, checking curl-error
            $ver = $this->slot_results[$slot_low]['curl_error'];
            if (empty($ver)) {
                // if no curl error, checking response
                $ver = \explode("\n", $this->slot_results[$slot_low]['response']);
                $ver = (\count($ver) == 2 && \strlen($ver[0]) < 32) ? $ver[0] : "Unknown";
            }
            $is_supported = \is_numeric(\substr($ver,0,1)) && (\count(\explode(".", $ver)) > 2);
            if (!$is_supported) {
                $this->session_autocreate = false;
            } elseif ($par == 'query') {
                $this->isSupported('version', false, $ver);
            }
            $this->isSupported($par, false, $is_supported);
        };

        // set hook and send async-request without session_id
        $this->to_slot = '_vq_' . $this->server_url;
        $this->slotHookPush($this->to_slot,
            ['mode' => 0, 'fn' => $fn, 'par' => 'query']);
        $ans1 = $this->doGet($query);

        // set hook and send async-request with session_id
        $this->to_slot = '_vs_' . $this->server_url;
        $this->slotHookPush($this->to_slot,
            ['mode' => 0, 'fn' => $fn, 'par' => 'session_id']);
        $ans2 = $this->doGet($query, ['session_id' => $this->getSession()]);

        // restore old session_id and old to_slot
        $this->setOption('session_id', $old_sess);
        $this->to_slot = $old_to_slot;
        return \compact('ans1', 'ans2');
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
        $enable_http_compression = $this->getOption('enable_http_compression');
        $h_opt_arr = \array_merge(\compact(
            'query',
            'user',
            'password',
            'database',
            'enable_http_compression'
            ), $h_opt);
        return $this->doApiCall($this->server_url, $h_opt_arr);
    }
}
