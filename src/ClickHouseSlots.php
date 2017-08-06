<?php
namespace ierusalim\ClickHouse;

/**
 * Multi-curl slots trait for ClickHouseAPI
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
trait ClickHouseSlots
{

    /**
     * Slot name for save results
     *
     * @var string|false
     */
    public $to_slot = false;

    /**
     * Slot name for get results
     *
     * @var string|false
     */
    public $from_slot = false;

    /**
     * Results of slots for finishing conveyors
     *
     * Each element contains false or [code, response, curl_error, curl_info, headers]
     *
     * @var array
     */
    public $slot_results = [];

    /**
     * Hooks array [[$slot] => hook]
     *
     * Format of hooks:
     * - false - no hook (do nothing)
     * - [ 0 => int mode, 1 => callable fn, 2 => mixed parameters ]
     *
     * hook mode:
     * - =0 - call from slotContentToResults, parameters: ($this, $slot_low, $param)
     * - =1 - call from slotResults()
     *
     * @var array
     */
    public $slot_hooks = [];

    /**
     * Last used yi-finisher
     *
     * @var generator
     */
    public $last_yi;

    /**
     * Raw-Data server responses
     *
     * @var array
     */
    public $slot_data = [];

    /**
     * Multi-curl header
     *
     * @var resource
     */
    public $multi_h;

    /**
     * Flag to tell whether the operations are still running.
     *
     * It is set by function curl_multi_exec
     *
     * @var integer
     */
    public $multi_still_run;

    /**
     * Array of curl-headers, used in multi-curl
     *
     * @var curl[]
     */
    public $multi_ch;

    /**
     * Array of multicurl-slot statuses
     *
     * Each slot have those status:
     * 0 - slot added and started
     * true - slot completed successfully (when got results \CURLE_OK)
     * int - error CURLE_* (see [result] of curl_multi_info_read )
     *
     * @var array of boolean|integer
     */
    public $multi_status = [];

    /**
     * http-like envelope for slotAdd
     *
     * @param string $slot Slot name for init
     * @param resource $curl_h CURL handler for this slot
     * @param false|array $slot_hook optional hook
     * @return array Returns [code, curl_error, response, curl_h]
     */
    public function slotStart($slot, $curl_h, $slot_hook = false)
    {
        \curl_setopt($curl_h, \CURLOPT_HEADER, true);
        if ($response = $this->slotAdd($slot, $curl_h, $slot_hook)) {
            $code = 417;
            $curl_error = $response;
        } else {
            $this->to_slot = false;
            $code = 102;
            $curl_error = '';
        }
        return \compact('code', 'curl_error', 'response', 'curl_h', 'slot');
    }

    /**
     * Push hook for specified slot name
     *
     * Hook must be array with keys [mode, fn, par]
     *
     * Mode 0 = call when slot complete, parameters $fn($this, $slot_low, $par)
     * Mode 1 = yi-finisher calling from slotResults, $fn = generator $yi
     * Mode 2 = fi-finisher calling from slotResults, $fn = function $fi
     *
     * @param string $slot Slot name
     * @param array|false $slot_hook
     * @return boolean|string
     */
    public function slotHookPush($slot, $slot_hook)
    {
        if (\is_array($slot_hook)) {
            if (!isset($slot_hook[0]) && isset($slot_hook['mode'])) {
                $slot_hook = [$slot_hook];
            }
            $n = 0;
            foreach($slot_hook as $k => $hook) {
                if (($k != $n++ ) || !isset($hook['mode']) || !isset($hook['fn'])) {
                    return "Hook array have incompatible format. Must have [mode, fn] keys";
                }
            }
        } elseif ($slot_hook !== false) {
            return "Hook must be false or array with keys [mode, fn, [,par]]";
        } else {
            $slot_hook = [];
        }
        $slot_low = strtolower($slot);
        if (empty($this->slot_hooks[$slot_low])) {
            $this->slot_hooks[$slot_low] = $slot_hook;
        } else {
            $this->slot_hooks[$slot_low] = \array_merge($this->slot_hooks[$slot_low], $slot_hook);
        }
        return false;
    }
    /**
     * Adding specified curl-handler to work multi-curl and start
     *
     * Return false if ok, or string with error description
     *
     * @param string $slot Slot name for adding
     * @param curl $curl_h CURL handler for this slot
     * @param false|array $slot_hook optional hook
     * @return string|false Return string if error, false if ok
     */
    public function slotAdd($slot, $curl_h, $slot_hook = false)
    {
        $slot = \strtolower($slot);
        if (isset($this->multi_status[$slot])) {
            return "Slot $slot already exists";
        }

        if ($ans = $this->slotHookPush($slot, $slot_hook)) {
            return $ans;
        }

        if (!$this->multi_h) {
            $this->multi_h = \curl_multi_init();
            curl_multi_setopt($this->multi_h, \CURLMOPT_MAXCONNECTS, 1023);
            curl_multi_setopt($this->multi_h, \CURLMOPT_MAX_TOTAL_CONNECTIONS, 1023);
            curl_multi_setopt($this->multi_h, \CURLMOPT_MAX_HOST_CONNECTIONS, 1023);
        }
        $ans = \curl_multi_add_handle($this->multi_h, $curl_h);
        if (!$this->multi_h || ($ans !== 0)) {
            return "Multi-curl error: " . ($ans ?: "can't init" );
        }

        $this->multi_status[$slot] = 0;
        $this->slot_results[$slot] = false;
        $this->slot_data[$slot] = null;
        $this->multi_ch[$slot] = $curl_h;

        while (\CURLM_CALL_MULTI_PERFORM === ($ans = \curl_multi_exec($this->multi_h,
        $this->multi_still_run)))  \usleep(1000);

        return ($ans != \CURLM_OK) ? \curl_strerror($ans) : false;
    }
    /**
     * Create slot and set results, that can be read by slotResults($slot)
     * Erase old slot with same name, if exists
     *
     * @param string $slot
     * @param string $response
     * @param array|false $slot_data
     * @return $this
     */
    public function slotEmulateResults($slot, $response, $slot_data = false)
    {
        $slot = \strtolower($slot);

        // erase slot if exists
        $this->eraseSlot($slot, true);

        if (\is_array($slot_data)) {
            \extract($slot_data);
        } else {
            $slot_data = [];
        }
        if (!isset($code)) {
            $code = 200;
        }
        if (empty($curl_error)) {
            $curl_error = '';
        }
        if (empty($curl_info)) {
            $curl_info = [\CURLINFO_HTTP_CODE => $code];
        }
        if (!isset($status)) {
            $status = true;
        }
        $this->slot_data[$slot] = \array_merge($slot_data,
            \compact('code', 'response', 'curl_error', 'curl_info', 'headers', 'status')
        );
        $this->multi_status[$slot] = $status;
        $this->slot_results[$slot] = $response;
        $this->multi_ch[$slot] = 1;
        $this->slot_hooks[$slot] = [];

        return $this;
    }

    /**
     * Remove specified slot from multi-curl array and close curl-handler
     * After this function, the specified slot can be used again.
     *
     * @param string $slot Slot name
     * @return false|string
     */

    public function slotFree($slot)
    {
        $slot = strtolower($slot);
        if (!isset($this->multi_ch[$slot])) {
            return "Slot unknown: $slot";
        }
        $curl_h = $this->multi_ch[$slot];
        // resource may be already closed before
        if (\is_resource($curl_h)) {
            \curl_multi_remove_handle($this->multi_h, $curl_h);
            \curl_close($curl_h);
        }

        unset($this->multi_status[$slot]);
        unset($this->multi_ch[$slot]);

        return false;
    }

    /**
     * Erase all data about specified slot
     *
     * @param string $slot Slot name
     * @param boolean $if_exists true for block 'Slot unknown' exception
     * @return $this
     * @throws \Exception
     */
    public function eraseSlot($slot, $if_exists = false)
    {
        $slot = strtolower($slot);
        if (isset($this->slot_results[$slot])) {
            $this->slotFree($slot);
        } elseif (!$if_exists) {
            throw new \Exception("Slot unknown: $slot");
        }
        unset($this->slot_results[$slot]);
        unset($this->slot_data[$slot]);
        unset($this->slot_hooks[$slot]);

        return $this;
    }

    /**
     * Return true or integer>0 if curl-request complete
     * Return 0 if request started but not complete
     * Return false if specified slot is unknown
     *
     * @param string $slot Slot name
     * @return boolean|integer
     */
    public function slotCheck($slot)
    {
        return isset($this->multi_status[$slot]) ? $this->multi_status[$slot] : false;
    }

    /**
     * Waiting for the slot to be ready
     *
     * Return -1 if no active slots (or multi-curl not initialized)
     *
     * if slot not specified, returns array have changed_slots names
     *
     * if slot specified (is_string), returns false if slot is unknown, or
     * true if slot complete successful, or integer of error code if slot got error
     *
     * @param string|false $slot Slot name (or false to wait for any finished slot)
     * @return integer|boolean|array
     */
    public function slotWaitReady($slot = false)
    {
        for ($i=1; $i<3; $i++) {
            if (\is_string($slot)) {
                $slot = \strtolower($slot);
                $status = $this->slotCheck($slot);
                // may be slot has already got status, return it, dont wait
                if ($status !== 0) {
                    return $status;
                }
            }
            if (empty($this->multi_h)) {
                break;
            }
            do {
                $changed_slots = $this->slotsRefresh();
                if (!\is_string($slot) && \count($changed_slots)) {
                    return $changed_slots;
                } elseif (\is_array($changed_slots) && \in_array($slot, $changed_slots)) {
                    return $this->slotCheck($slot);
                }
            } while (\curl_multi_select($this->multi_h, 0.1) >= 0);
        }
        // no active slots
        return -1;
    }

    /**
     * Refresh statuses of multi-curl slots
     *
     * Returns array of changed slots
     *
     * @return array
     */
    public function slotsRefresh()
    {
        if (empty($this->multi_h)) {
            return [];
        }

        // time limit 2 sec
        $_tl = \microtime(true) + 2;

        $msgs_in_queue = 1;
        $changed_slots = [];
        while ($msgs_in_queue && (\microtime(true) < $_tl)) {
            $arr = \curl_multi_info_read($this->multi_h, $msgs_in_queue);
            if (\is_array($arr)) {
                \extract($arr); // handle, result, msg
                $slot = \array_search($handle, $this->multi_ch);
                if ($slot !== false) {
                    $changed_slots[] = $slot;
                    $this->multi_status[$slot] = ($result === \CURLE_OK) ? true : $result;
                    $this->slotContentToResults($slot);
                }
                if (!$msgs_in_queue) {
                    break;
                }
            }
            if (!$this->multi_still_run) {
                break;
            }
            $ans = \curl_multi_exec($this->multi_h, $this->multi_still_run);
        }
        foreach ($changed_slots as $slot) {
            $curl_h = $this->multi_ch[$slot];
            \curl_multi_remove_handle($this->multi_h, $curl_h);
            \curl_close($curl_h);
        }
        return $changed_slots;
    }
    /**
     * Get slot results from multi-curl and save to $this->slot_results[$slot]
     *
     * Call hook mode 0 if presents
     *
     * @param string $slot_low Slot name in lowercase
     */
    public function slotContentToResults($slot_low)
    {
        $curl_h = $this->multi_ch[$slot_low];

        $status = $this->multi_status[$slot_low];

        $curl_info = [];
        if ($status === true) {
            foreach (\array_keys($this->curl_info) as $key) {
                $curl_info[$key] = \curl_getinfo($curl_h, $key);
            }
            $curl_error = '';
        } else {
            foreach (\array_keys($this->curl_info) as $key) {
                $curl_info[$key] = 0;
            }
            $curl_error = \curl_strerror($status);
        }
        $code = $curl_info[\CURLINFO_HTTP_CODE];


        $headers = '';
        $response = \curl_multi_getcontent($curl_h);

        if (\is_string($response)) {
            if (empty($curl_info[\CURLINFO_HEADER_SIZE])) {
                if ($header_size = \strpos($response,"\n\r\n")) {
                    $header_size +=4;
                }
            } else {
                $header_size = $curl_info[\CURLINFO_HEADER_SIZE];
            }
            $headers = \explode("\n", \trim(\substr($response, 0, $header_size)));
            $response = \substr($response, $header_size);
        }

        $this->slot_results[$slot_low] = $this->slot_data[$slot_low] = \compact('code',
            'response', 'curl_error', 'curl_info', 'headers', 'status');

        if (isset($this->slot_hooks[$slot_low][0])) {
            foreach ($this->slot_hooks[$slot_low] as $k => $hook) {
                if ($hook['mode'] == 0) {
                    $ret = \call_user_func(
                        $hook['fn'], $this, $slot_low,
                        isset($hook['par']) ? $hook['par'] : null
                    );
                    unset($this->slot_hooks[$slot_low][$k]);
                }
            }
        }
    }

    /**
     * Get results from slot with final conveyor execute
     *
     * @param string|false $slot
     * @return $this
     * @throws \Exception
     */
    public function slotResults($slot = false)
    {
        if (empty($slot)) {
            $slot = $this->from_slot;
        }
        if (!\is_string($slot)) {
            throw new \Exception("Illegal slot name");
        }
        $slot_low = strtolower($slot);
        if (($status = $this->slotWaitReady($slot_low)) && ($status !== -1)) {
            // true - successful | int >0 - error
            if (!isset($this->slot_data[$slot_low]['curl_error'])) {
                throw new \Exception("Slot '$slot' damaged");
            }
        } else {
            // response is false if specified slot is unknown | -1 if multi-curl not initialized
            throw new \Exception("Unknown slot '$slot'");
        }

        if (isset($this->slot_hooks[$slot_low][0])) {
            foreach ($this->slot_hooks[$slot_low] as $k => $hook) {
                if ($hook['mode'] == 1) {
                    $this->last_yi = $yi =  $hook['fn'];
                    $this->slot_results[$slot_low] = $yi->send($this->slot_results[$slot_low]);
                    unset($this->slot_hooks[$slot_low][$k]);
                } elseif ($hook['mode'] == 2) {
                    $this->slot_results[$slot_low] = call_user_func($hook['fn'],
                        $this->slot_results[$slot_low]);
                    unset($this->slot_hooks[$slot_low][$k]);
                }
            }
        }

        return $this->slot_results[$slot_low];
    }

    /**
     * Wait data from specified slot and actualizing to $this-variables
     *
     * @param string $slot Slot name
     * @return $this
     * @throws \Exception
     */
    public function fromSlot($slot)
    {
        $status = $this->slotWaitReady($slot);
        if ($status < 0 || $status === false) {
            throw new \Exception("Slot '$slot' not found" . $status);
        }

        $slot = strtolower($slot);
        $this->from_slot = $slot;

        // slot_results[$slot] = code, response, curl_error, curl_info, headers
        $this->last_code = $this->slot_data[$slot]['code'];
        $this->last_curl_error_str = $this->slot_data[$slot]['curl_error'];
        $this->curl_info = $this->slot_data[$slot]['curl_info'];
        $this->results = $this->slot_data[$slot]['response'];
        return $this;
    }

    /**
     * Set slot for prepare async request
     *
     * @param string $slot Slot name
     * @return $this
     * @throws \Exception
     */
    public function toSlot($slot)
    {
        $slot_low = strtolower($slot);
        if (isset($this->multi_ch[$slot_low])) {
            throw new \Exception("Slot '$slot' is busy");
        }
        $this->to_slot = $slot_low;
        return $this;
    }

    /**
     * Generator for walking finished slot by 'foreach' (waiting for each complete)
     *
     * Bypasses all running slots and waits for completion.
     * Passes parameters as key=>value, where key = slot name, value = array of slot results.
     *
     * Parameter (optional):
     * - (default) = 1 - take results by execute slotResults function (exec 0,1,2-mode hooks)
     * - 0 - get raw-data from slot (without slotResults, but 0-mode hooks will be executed)
     *
     * @param integer $get_res 0 = get raw data from slot, 1 (by default) = slotResults
     */
    public function slotFinished($get_res = 1)
    {
        while ($this->multi_still_run) {
            while (($arr = $this->slotWaitReady()) != -1) {
                foreach ($arr as $slot) {
                    if (isset($this->slot_data[$slot])) {
                        yield $slot => $get_res ? $this->slotResults($slot) : $this->slot_data[$slot];
                    }
                }
            }
        }
    }

    /**
     * Generator for walking all existing slots
     * Passes parameters as key=>value, where key = slot name, value = slot status
     */
    public function slotExisting()
    {
        foreach($this->slot_hooks as $slot => $hooks) {
            $status = $this->slotCheck($slot);
            yield $slot => $status;
        }
    }

    /**
     * Erase all existing slots (if true) or erase all completed slots (by default)
     *
     * @param boolean $true_for_erase_all_false_for_completed_only false by default
     * @return array Returns array with names of erased slots
     */
    public function eraseAllSlots($true_for_erase_all_false_for_completed_only = false)
    {
        $erased_slots = [];
        foreach ($this->slotExisting() as $slot => $status) {
            if ($true_for_erase_all_false_for_completed_only ||
                ($status && (!isset($this->multi_ch[$slot]) || !\is_resource($this->multi_ch[$slot])))
            ) {
                $this->eraseSlot($slot);
                $erased_slots[] = $slot;
            }
        }
        return $erased_slots;
    }
}
