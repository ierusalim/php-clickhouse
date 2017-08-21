<?php
namespace ierusalim\ClickHouse;

/**
 * This trait contains ClickHouseSQLParser functions
 *
 * PHP Version >=5.4
 *
 * @package    ierusalim\ClickHouseSQLParser
 * @author     Alexander Jer <alex@ierusalim.com>
 * @copyright  2017, Ierusalim
 * @license    https://opensource.org/licenses/Apache-2.0 Apache-2.0
 */
class ClickHouseSQLParser
{
    /**
     * Check string may interpreted as name of ClickHouse data-type
     * - return false if string not recognized as data-type-name
     * - return string in the CanonicalCase if type recognized
     * - call without parameter will returned array of all known canonical-types
     * - Null type supported
     *
     * @staticvar array $canon_types
     * @param mixed $type String for checking, or non-string for get array of all names
     * @return string
     */
    public function typeCanonicName($type = false)
    {
        static $canon_types = [];
        if (empty($canon_types)) {
            foreach ([
            'Null',
            'Date',
            'DateTime',
            'UInt8',
            'Int8',
            'UInt16',
            'Int16',
            'UInt32',
            'Int32',
            'UInt64',
            'Int64',
            'Float32',
            'Float64',
            'FixedString',
            'String',
            'Array',
            'Enum8',
            'Enum16',
            'Nullable',
            ] as $canon) {
                $canon_types[\strtolower($canon)] = $canon;
            }
        }
        if (!\is_string($type)) {
            return $canon_types;
        }
        $t = strtolower($type);
        return isset($canon_types[$t]) ? $canon_types[$t] : false;
    }

    /**
     * Return string if it recognized as ClickHouse data-type
     * Returned string in canonical-case.
     *
     * @param string $type String for checking
     * @param boolean $ignore_case Set true for ignore case
     * @return string|false
     */
    public function isClickHouseDataType($type, $ignore_case = false)
    {
        if ($i = \strpos($type, '(')) {
            $fn = \trim(\substr($type, 0, $i));
            if ($ignore_case) {
                $fn = $this->typeCanonicName($fn);
            }
            $in = \substr($type, $i+1, 8192);
            if ($i = \strrpos($in, ')')) {
                $in = \substr($in, 0, $i);
                if ($fn === 'Nullable') {
                    if (false === \strpos(\strtolower($in), 'nullable')) {
                        $ans = $this->isClickHouseDataType($in, $ignore_case);
                        return $ans ? $fn . '(' . $ans . ')' : false;
                    }
                } elseif ($fn === 'Array') {
                    $ans = $this->isClickHouseDataType($in, $ignore_case);
                    return $ans ? $fn . '(' . $ans . ')' : false;
                } elseif ($fn === 'FixedString' && \is_numeric($in)) {
                    return $fn . '(' . $in . ')';
                } elseif ($fn === 'Enum8' || $fn === 'Enum16') {
                    $param = $this->divParts($in);
                    if (isset($param[0]) && \count($param[0])) {
                        $lf = $rg = [];
                        foreach ($param[0] as $par) {
                            $par = $this->cutNextPart($par, '=');
                            if (!isset($par[2]) || $par[2] != '=') {
                                return false;
                            }
                            $lf[] = $par[0];
                            $rg[] = \trim($par[1]);
                        }
                        $lf = $this->typeDetectFromArray($lf);
                        if ($lf !== 'String') {
                            return false;
                        }
                        $rg = $this->typeDetectFromArray($rg);
                        if (($rg === 'UInt8') || (($fn === 'Enum16') && ($rg === 'UInt16'))) {
                            return $fn . '(' . $in . ')';
                        }
                    }
                }
            }
            return false;
        } else {
            if ($ignore_case) {
                return $this->typeCanonicName($type);
            } else {
                return ($type === $this->typeCanonicName($type)) ? $type : false;
            }
        }
    }

    /**
     * Attempt to detect the type of results of specified ClickHouse-SQL-expression.
     * Like ClickHouse function 'toTypeName', but only for simple cases.
     * For 'null' (in anyCase) return 'Null'.
     *
     * @param string $exp String expression for attempt to detect data-type
     * @return boolean|string Return string of data-type-name or false if unrecognized
     */
    public function typeDetectFromExpression($exp)
    {
        if (!empty($exp) || \is_numeric($exp)) {
            $fc = \substr($exp, 0, 1);
            $lc = \substr($exp, -1);
            if ($fc == "'" && $lc = $fc) {
                return 'String';
            }
            if ($fc === '[' && $lc === ']') {
                $param = $this->divParts(substr($exp, 1, -1));
                if (!\is_array($param)) {
                    return false;
                }
                $param = $this->typeDetectFromArray($param[0]);
                return $param ? 'Array(' . $param . ')' : false;
            }
            if (\is_numeric($exp)) {
                if ($exp >= 0) {
                    if ($exp < 256) {
                        return 'UInt8';
                    } elseif ($exp < 65536) {
                        return 'UInt16';
                    } elseif ($exp < 4294967296) {
                        return 'UInt32';
                    } elseif ($exp <= 18446744073709551615) {
                        return 'UInt64';
                    }
                } else {
                    if ($exp >= -128) {
                        return 'Int8';
                    } elseif ($exp >= -32768) {
                        return 'Int16';
                    } elseif ($exp >= -2147483648) {
                        return 'Int32';
                    } elseif ($exp >= -9223372036854775808) {
                        return 'Int64';
                    }
                }
                return 'Float64';
            }
            if (\strpos($exp, '(')) {
                return $this->typeDetectFromFunction($exp);
            }
            if (\strtolower($exp) === 'null') {
                return $this->typeCanonicName($exp);
            }
        }
        return false;
    }

    /**
     * Attempt to detect the type of an array by its elements
     *
     * @param array $arr
     * @return string|false
     */
    public function typeDetectFromArray($arr)
    {
        if (!\is_array($arr) || !\count($arr)) {
            return false;
        }
        $max = -9223372036854775808;
        $min = -$max;
        foreach ($arr as $par) {
            if (!\is_numeric($par)) {
                $min = false;
                break;
            }
            if ($par < $min) {
                $min = $par;
            }
            if ($par > $max) {
                $max = $par;
            }
        }
        if ($min !== false) {
            if ($min >= 0) {
                if ($max < 256) {
                    return 'UInt8';
                } elseif ($max < 65536) {
                    return 'UInt16';
                } elseif ($max < 4294967296) {
                    return 'UInt32';
                } elseif ($max <= 18446744073709551615) {
                    return 'UInt64';
                }
            } else {
                $oldmax = $max;
                if (-$min > $max) {
                    $max = -++$min;
                }
                if ($max < 128) {
                    return ($oldmax >= 0) ? 'Int16': 'Int8';
                } elseif ($max < 32768) {
                    return ($oldmax >= 256) ? 'Int32' : 'Int16';
                } elseif ($max < 2147483648) {
                    return ($oldmax >= 65536) ? 'Int64' : 'Int32';
                } elseif ($max < 9223372036854775808) {
                    return 'Int64';
                }
            }
            return 'Float64';
        }
        foreach ($arr as $par) {
            $type = $this->typeDetectFromExpression($par);
            if ($min && ($min !== $type)) {
                return false;
            }
            $min = $type;
        }
        return $type;
    }

    /**
     * Attempt to suggest the type that the function returns
     *
     * @param string $fn
     * @return string|false
     */
    public function typeDetectFromFunction($fn)
    {
        $arr = $this->parseSqlFunction($fn);
        if (!\is_array($arr)) {
            return false;
        }
        extract($arr); //function, param, tail

        if ($function === 'toNullable') {
            if (\count($param) == 1) {
                $type = 'Nullable(' . $this->typeDetectFromExpression($param[0]) . ')';
                return $this->isClickHouseDataType($type) ? $type : false;
            } else {
                return false;
            }
        }

        if ($fn = $this->staticFnTypes($function)) {
            return $fn;
        }

        $fn = strtolower($function);

        // toT
        if (strlen($fn)>5 && substr($fn, 0, 2)=='to' && ($type = $this->typeCanonicName(substr($fn, 2)))) {
            if ($type === 'FixedString') {
                if (count($param)==2 && is_numeric($param[1])) {
                    $type.= '(' . $param[1] . ')';
                } else {
                    return false;
                }
            }
        } elseif ($fn === 'cast') {
            if (count($param) == 2) {
                $type = substr($param[1], 1, -1);
                return $this->isClickHouseDataType($type) ? $type : false;
            } elseif (count($param) == 1) {
                $param = $param[0];
                while (strlen($param)>2) {
                    $param = $this->cutNextPart($param, ' ');
                    if (strlen($param[0]) ==2 && strtolower($param[0]) == 'as') {
                        $type = $param[1];
                        if (strpos($type, '(')) {
                            return $this->isClickHouseDataType($type) ? $type : false;
                        }
                        break;
                    }
                    $param = $param[1];
                }
            } else {
                return false;
            }
        } elseif ($fn === 'array') {
            $in = $this->typeDetectFromArray($param);
            return $in ? 'Array(' . $in . ')' : false;
        } elseif (strlen($this->typeCanonicName($fn))) {
            return false;
        } else {
            // xxxT, emptyArrayT
            foreach ($this->typeCanonicName() as $low => $type) {
                if (\substr($fn, -strlen($type)) === $low) {
                    if (\strpos($fn, 'mptyarray')) {
                        $type = 'Array(' . $type . ')';
                    }
                    return $type;
                }
            }
            return false;
        }
        return $type;
    }

    public function staticFnTypes($fn_name = false, $set_type = false)
    {
        static $ch_fn_type = [
        'toTypeName' => 'String',
        'arrayStringConcat' => 'String',
        'lowerUTF8' => 'String',
        'reverseUTF8' => 'String',
        'toStringCutToZero' => 'String',
        'alphaTokens' => 'Array(String)',
        'splitByChar' => 'Array(String)',
        'splitByString' => 'Array(String)',
        'splitByRegexp' => 'Array(String)',
        'extractAll' => 'Array(String)',
        'arrayCount' => 'UInt32',
        'uptime' => 'UInt32',

        'now' => 'DateTime',
        'hex' => 'String',
        'unhex' => 'String',
        'MD5' => 'FixedString(16)',
        'SHA1' => 'FixedString(20)',
        'SHA224' => 'FixedString(28)',
        'SHA256' => 'FixedString(32)',
        'cityHash64' => 'UInt64',
        'sipHash64' => 'UInt64',
        'sipHash128' => 'FixedString(16)',
        'intHash32' => 'UInt32',
        'intHash64' => 'UInt64',
        'URLHash' => 'UInt64',
        'metroHash64' => 'UInt64',
        'farmHash64' => 'UInt64',
        'halfMD5' => 'UInt64',
        'rand64' => 'UInt64',
        'randConstant' => 'UInt32',

        'toUnixTimestamp' => 'UInt32',
        'bitmaskToList' => 'String',
        'bitmaskToArray' => 'Array(UInt64)',
        'IPv4NumToStringClassC' => 'String',
        'IPv6StringToNum' => 'FixedString(16)',
        'IPv4ToIPv6' => 'FixedString(16)',
        'UUIDStringToNum' => 'FixedString(16)',
        'IPv4StringToNum' => 'UInt32',
        'MACStringToNum' => 'UInt64',
        'MACStringToOUI' => 'UInt64',
        'MACNumToString' => 'String',

        'today' => 'Date',
        'yesterday' => 'Date',
        'toYear' => 'UInt16',
        'toMonth' => 'UInt8',
        'toDayOfMonth' => 'UInt8',
        'toDayOfWeek' => 'UInt8',
        'toHour' => 'UInt8',
        'toMinute' => 'UInt8',
        'toSecond' => 'UInt8',
        'toStartOfDay' => 'DateTime',
        'toMonday' => 'Date',
        'toStartOfMonth' => 'Date',
        'toStartOfQuarter' => 'Date',
        'toStartOfYear' => 'Date',
        'toStartOfMinute' => 'DateTime',
        'toStartOfFiveMinute' => 'DateTime',
        'toStartOfHour' => 'DateTime',
        'toTime' => 'DateTime',

        'toRelativeYearNum' => 'UInt16',
        'toRelativeMonthNum' => 'UInt32',
        'toRelativeWeekNum' => 'UInt32',
        'toRelativeDayNum' => 'UInt32',
        'toRelativeHourNum' => 'UInt32',
        'toRelativeMinuteNum' => 'UInt32',
        'toRelativeSecondNum' => 'UInt32',

        'toYYYYMM' => 'UInt32',
        'toYYYYMMDD' => 'UInt32',
        'toYYYYMMDDhhmmss' => 'UInt64',

        'toUInt8OrZero' => 'UInt8',
        'toUInt16OrZero' => 'UInt16',
        'toUInt32OrZero' => 'UInt32',
        'toUInt64OrZero' => 'UInt64',
        'toInt8OrZero' => 'Int8',
        'toInt16OrZero' => 'Int16',
        'toInt32OrZero' => 'Int32',
        'toInt64OrZero' => 'Int64',
        'toFloat32OrZero' => 'Float32',
        'toFloat64OrZero' => 'Float64',

        'dictGetUInt8OrDefault' => 'UInt8',
        'dictGetUInt16OrDefault' => 'UInt16',
        'dictGetUInt32OrDefault' => 'UInt32',
        'dictGetUInt64OrDefault' => 'UInt64',
        'dictGetInt8OrDefault' => 'Int8',
        'dictGetInt16OrDefault' => 'Int16',
        'dictGetInt32OrDefault' => 'Int32',
        'dictGetInt64OrDefault' => 'Int64',
        'dictGetFloat32OrDefault' => 'Float32',
        'dictGetFloat64OrDefault' => 'Float64',
        'dictGetDateOrDefault' => 'Date',
        'dictGetDateTimeOrDefault' => 'DateTime',

        'domain' => 'String',
        'domainWithoutWWW' => 'String',
        'topLevelDomain' => 'String',
        'protocol' => 'String',
        'path' => 'String',
        'queryString' => 'String',
        'fragment' => 'String',
        'queryStringAndFragment' => 'String',
        'cutWWW' => 'String',
        'cutFragment' => 'String',
        'cutQueryString' => 'String',
        'cutQueryStringAndFragment' => 'String',
        'extractURLParameter' => 'String',
        'extractURLParameters' => 'Array(String)',
        'extractURLParameterNames' => 'Array(String)',
        'cutURLParameter' => 'String',
        'URLHierarchy' => 'Array(String)',
        ];
        if ($fn_name === false) {
            return $ch_fn_type;
        } elseif ($set_type !== false) {
            if (\is_null($set_type)) {
                unset($ch_fn_type[$fn_name]);
            } else {
                $ch_fn_type[$fn_name] = $set_type;
            }
        }
        return isset($ch_fn_type[$fn_name]) ? $ch_fn_type[$fn_name] : false;
    }

    /**
     * Parse SQL function to array
     *
     * @param string $sql function, for example: "Fn(Par1,par2)Tail"
     * @return array|false false if can't parse, or array(function, param, tail)
     */
    public function parseSqlFunction($sql)
    {
        if (false === ($i = \strpos($sql, '('))) {
            return false;
        }
        $function = \trim(\substr($sql, 0, $i));
        if (false === \strpos($sql, ')', $i)) {
            return false;
        }
        $tail = $this->divParts(substr($sql, $i+1));
        $param = $tail[0];
        $tail = \trim($tail[1]);
        return \compact('function', 'param', 'tail');
    }

    /**
     * Divides SQL-query parameters by ',' separator to the array
     *
     * @param string $str SQL parameters through ',' divider
     * @return array Return results in array[0], or false if can't parse
     */
    public function divParts($str)
    {
        if (!\is_string($str) && !\is_numeric($str)) {
            return false;
        }
        $parts = [];
        while (\strlen($str)) {
            if (!($par = $this->cutNextPart($str, ','))) {
                return false;
            }
            $str = $par[1];
            $parts[] = $par[0];
        }
        return [$parts, isset($par[2]) ? $par[2] : ''];
    }

    /**
     * Cut the next parameter from the SQL-parameters string
     *
     * Returns: false if can't parse (syntax error in source string)
     * if successfully parsed, returns array contains 2 or 3 elements:
     * [0] - cut-out part,
     * [1] - remainder if present, or empty string if there is no.
     * [2] - divider if present, or element [2] does not exist if end of the string is reached.
     *
     * @param string $str SQL parameters by divider
     * @param string $divc Parameters divider (',' by default)
     * @return array|false false=err, Array: [0] = cut-out part, [1] = tail [,[2] - divider]
     */
    public function cutNextPart($str, $divc = ',')
    {
        if (!\is_array($divc)) {
            $divc = [$divc];
        }
        $b = 0;
        $ob = '([{';
        $cb = ')]}';
        $z = [];
        $q = '';
        $l = \strlen($str);
        for ($i=0; $i < $l; $i++) {
            $c = $str[$i];
            if (!empty($q)) {
                if ($c === '\\') {
                    $i++;
                }
                if ($c === $q) {
                    $q = '';
                }
                continue;
            } elseif ($c === '"' || $c === "'" || $c === "`") {
                $q = $c;
                continue;
            }

            if (false !== ($j = \strpos($ob, $c))) {
                \array_unshift($z, \substr($cb,$j,1));
                $b++;
                continue;
            } elseif (strpos($cb, $c) !== false) {
                if (--$b < 0) {
                    return [\trim(\substr($str, 0, $i)), '', \substr($str, $i+1)];
                } else {
                    if ($c !== \array_shift($z)) {
                        return false;
                    }

                }
            }
            if (\in_array($c, $divc) && !$b) {
                return [\trim(\substr($str, 0, $i)), \substr($str, $i+1), $c];
            }
        }
        if (!empty($q) || $b) {
            return false;
        }
        return [\trim($str), ''];
    }

    /**
     * Prepare ClickHouse-SQL function for conversion to specified data-type
     *
     * Return false if data-type incorrect or not recognized
     *
     * If $exp specified, return string with data-type conversion function.
     * Examples:
     * - ('UInt8', 99) returns is 'toUInt8(99)'
     * - ('FixedString(5)', 99) ==> 'toFixedString(99, 5)'
     *
     * If $exp not specified, returns is array with conversion prefix and postfix,
     * Examples: ('UInt8') ==> ['toUInt8(', ')']
     *  ('FixedString(5)') ==> ['toFixedString(', ',5)']
     *
     * @param string $type_full Type specification
     * @param string|false $exp SQL-Expression for convert data-type
     * @return array|string|false
     */
    public function typeConvert($type_full, $exp = false)
    {
        if ($i = \strpos($type_full, '(')) {
            $type_name = \substr($type_full, 0, $i);
            $param = \trim(\substr($type_full, $i));
            if (\substr($param, -1) !== ')') {
                return false;
            }
        } else {
            $type_name = $type_full;
        }
        $type_name = $this->typeCanonicName($type_name);
        switch ($type_name) {
            case 'Nullable':
                if (!$i) {
                    $to_conv = ['toNullable(', ')'];
                    break;
                }
                // no break
            case 'Array':
            case 'Enum8':
            case 'Enum16':
                $in = $this->isClickHouseDataType($type_name . $param, true);
                if ($in) {
                    $to_conv = ['CAST(', ' AS ' . $in . ')'];
                }
                break;
            case 'FixedString':
                if (\is_numeric(\substr($param, 1, -1))) {
                    $to_conv = ['to' . $type_name . '(', ', ' . \substr($param, 1)];
                }
                break;
            case '':
                return false;
            default:
                $to_conv = ['to' . $type_name . '(', ')'];
        }
        if (empty($to_conv)) {
            return false;
        }
        return ($exp === false) ? $to_conv : $to_conv[0] . $exp . $to_conv[1];
    }

    /**
     * Compare the type of the expression $exp with $type_full and converts if different.
     *
     * @param string $type_full Target type
     * @param string $exp Expression for converts
     * @return string|false
     */
    public function typeConvertIfNeed($type_full, $exp)
    {
        $type = $this->typeDetectFromExpression($exp);
        return ($type === $type_full) ? $exp : $this->typeConvert($type_full, $exp);
    }

    /**
     * Get type and optional default value from string like 'UInt8 DEFAULT 123'
     *
     * @param string $td String definer type and/or default value
     * @return array|false
     */
    public function parseTypeDefault($td)
    {
        $pars = $this->cutNextPart($td, ' ');
        if (!\is_array($pars)) {
            return false;
        }
        $default = $type_explicit = $default_explicit = '';
        if (\count($pars) === 3) {
            // case 'T D' or 'default D' or 'T default D'
            $default = $pars[1];
            $type = $pars[0];
            if (\strtolower($type) === 'default') {
                // case 'default D'
                $default_explicit = $default;
                $type = $this->typeDetectFromExpression($default);
            } else {
                // csae 'T D' or 'T default D'
                $type_explicit = $type;
                if (\strtolower(\substr($default, 0, 8)) === 'default ') {
                    // case 'T default D'
                    $default = \trim(\substr($default, 8));
                }
                $default_explicit = $default;
                if ($cano_type = $this->isClickHouseDataType($type, true)) {
                    $type = $cano_type;
                    $default = $this->typeConvertIfNeed($type, $default);
                }
            }
        } else {
            // case 'T' or 'D'
            if ($cano_type = $this->isClickHouseDataType($pars[0], true)) {
                // case 'T'
                $type_explicit = $pars[0];
                $type = $cano_type;
            } else {
                // case 'D'
                $default= $default_explicit = $pars[0];
                $type = $this->typeDetectFromExpression($default);
            }
        }
        return compact('type', 'default', 'type_explicit', 'default_explicit');
    }

    /**
     * Parse first part of create-table-sql request by this pattern:
     * CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [db.]name[ AS id] [ON CLUSTER cluster]*
     *
     * Return array with keys defined in $interesting_vars
     *
     * @param string $sql
     * @return array
     */
    public function tableCreaterFnParser($sql) {
        \extract($interesting_vars = [
            'crefn' => 'create_fn',
            'crepar' => 'create_par',
            'temp' => 'temporary',
            'ifnx' => 'if_not_exists',
            'db' => 'database',
            'table' => 'table',
            'dbtb' => 'dbtb',
            'as' => 'as',
            'asid' => 'asid',
            'oncl' =>'on_cluster',
            'clus' => 'cluster',
        ]);
        $idpat = "[a-z_][0-9a-z_]*";
        $idpax = function($n, $name) use ($idpat) {
            return "(`)?(?<$name>(?($n)[^`]+|$idpat))(?($n)`)";
        };
        $tmpat = "(?<$temp>TEMPORARY(\s)+)?";
        $ifnxpat = "(?<$ifnx>IF(\s)+NOT(\s)+EXISTS(\s)+)?";
        $dbpat = "({$idpax(12, $db)}\.)?";
        $tbpat = "({$idpax(15, $table)})";
        $pdbtb = "(?<$dbtb>{$dbpat}{$tbpat})\s";
        $aspat = "(?<$as>AS\s{$idpax(18, $asid)}\s)?";
        $oncpat = "(?<$oncl>ON\sCLUSTER\s{$idpax(21, $clus)}\s)?";

        $sumpat = "#(?<$crefn>^CREATE(\s+){$tmpat}TABLE(\s+){$ifnxpat}"
            . "{$pdbtb}{$aspat}{$oncpat})(?<{$crepar}>.*)#is";

        $matches = $ret = [];
        \preg_match_all($sumpat, $sql, $matches);
        if (!empty($matches[$crefn]) && !empty($matches[$crepar])) {
            foreach ($interesting_vars as $key) {
                $ret[$key] = \trim($matches[$key][0]);
            }
            //booleanize
            foreach ([$temp, $ifnx, $as, $oncl] as $key) {
                $ret[$key] = !empty($ret[$key]);
            }
        }
        return $ret;
    }

    /**
     * Parse last part of create-table-sql request like "ENGINE = MergeTree(...)"
     *
     * @param string $create_engine
     * @return array|string
     */
    public function parseCreateEngineSql($create_engine)
    {
        // if engine have not parameters, for example, ENGINE = Log
        if (false === \strpos($create_engine, '(')) {
            $create_engine .= '()';
        }

        $exp = 'engine';
        if (!($eng_parr = $this->parseSqlFunction($create_engine))) {
            return "Illegal '$exp' part";
        }
        $engine_param = $eng_parr['param'];
        $engine = explode('=', $eng_parr['function']);
        if (\strtolower(\trim($engine[0])) !== $exp) {
            return "Expected '$exp'" . (empty($engine[0]) ? '' : ", unexpected '{$engine[0]}'");
        }
        $engine = \trim($engine[1]);

        // parse *MergeTree engine parameters
        $date_field = $sampl = $primary_key = $granul = $ver = '';
        if ($merge_tree_fam = (\strtolower(\substr($engine, -9)) === 'mergetree')) {
            // for *MergeTree engines seek granul and peek other parameters by it place
            foreach ($engine_param as $k => $par) {
                if (\is_numeric($par)) {
                    $granul = $par;
                    // after granul can be ver
                    if (isset($engine_param[$k+1])) {
                        $ver = $engine_param[$k+1];
                    }
                    if ($k) {
                        $date_field = $engine_param[0];
                        if ($k>2) {
                            // sampl can be after date and before primary key
                            $sampl = $engine_param[1];
                            $primary_key = $engine_param[2];
                        } else {
                            $primary_key = $engine_param[1];
                        }
                    }
                    break;
                }
            }
            if (empty($primary_key) ||
                (\substr($primary_key, 0, 1) !=='(')  ||
                (\substr($primary_key, -1) !==')')) {
                    return "Primary key undefined or illegal";
            }
            $primary_key = $this->divParts(\substr($primary_key, 1, -1));
            if (!\is_array($primary_key) || empty($primary_key[0])) {
                return "Can't parse primary key";
            }
            $primary_key = $primary_key[0];
        }
        return \compact(
            'engine',
            'engine_param',
            'merge_tree_fam',
            'date_field',
            'sampl',
            'primary_key',
            'granul',
            'ver'
            );
    }

    /**
     * Parse ClickHouse CREATE TABLE SQL-request
     *
     * - Return string with error description if can't parse, or
     * - Return array of table parameters
     *
     * @param string $create_full Full-SQL of CREATE .. TABLE ...(all parameters)
     * @return array|string
     */
    public function parseCreateTableSql($create_full)
    {
        $err = "Can't parse string as 'CREATE TABLE'-sql-request";

        // left part parsing
        $cre_arr = $this->tableCreaterFnParser($create_full);
        if (!\count($cre_arr)) {
            return $err;
        }

        \extract($cre_arr);
        // $create_fn, $create_par
        // $temporary (bool), $if_not_exists (bool)
        // $database (str|false), $table (str), $dbtb (str),
        // $as (bool), $asid (str)
        // $on_cluster (bool), $cluster (str)

        if (\strtolower(\substr($create_par, 0, 6)) == 'engine') {
            $create_fields = [];
            $create_engine = $create_par;
        } elseif (\substr($create_par, 0, 1) == '(') {
            $par_arr = $this->parseSqlFunction("CREATE$create_par");
            if (!$par_arr) {
                return $err;
            }
            $create_fields = $par_arr['param'];
            $create_engine = $par_arr['tail'];
        } else {
            return $err;
        }

        $eng_arr = $this->parseCreateEngineSql($create_engine);
        if (!\is_array($eng_arr)) {
            return $eng_arr;
        }

        if (!empty($create_fields)) {
            $fields_arr = $this->parseCreateFields($create_fields);
            if (\is_array($fields_arr)) {
                \extract($fields_arr);
            }
        }
        return \array_merge(\compact(
            'create_fn',     //str First part of sql request (CREATE ...)
            'table',         //str
            'database',      //false|str
            'temporary',     //false|true
            'if_not_exists', //false|true
            'asid',          //false|str
            'cluster',       //false|str

            'create_fields', //str Second part of sql request, optional
            'names',         //array
            'parse_fields',  //array

            'create_engine' //str Last part of sql request, (ENGINE = ...)
            // 'engine', 'engine_param', 'merge_tree_fam', 'date_field'
            // 'sampl', 'primary_key', 'granul', 'ver'
        ), $eng_arr);
    }


    /**
     * Parsing field-definer string like "field_name field_type [default]"
     *
     * Returns array: [name, type, default, type_explicit, default_explicit]
     *
     * @param string $ntd String of "field_name field_type default"
     * @return array|false false if error, otherwise array ['name', 'type', 'default']
     */
    public function parseNameTypeDefault($ntd)
    {
        $pars = $this->cutNextPart($ntd, ' ');
        if (!empty($pars[1])) {
            $arr = $this->parseTypeDefault(\trim($pars[1]));
            if (\is_array($arr)) {
                return \array_merge(['name' => $pars[0]], $arr);
            }
        }
        return false;
    }

    /**
     * Parse fields-create strings, recognizes source-data in 2 formats:
     * - all fields in one string by ',' divider (like in SQL-create request)
     * - array with numeric keys and values like 'fieldname type[default]'
     *
     * @param array|string $create_fields
     * @return array|string
     */
    public function parseCreateFields($create_fields)
    {
        if (!\is_array($create_fields)) {
            if (\is_string($create_fields)) {
                $create_fields = $this->divParts($create_fields)[0];
            }
            if (!\is_array($create_fields)) {
                return "Invalid parameter";
            }
        }
        $names = $parse_fields = [];
        foreach ($create_fields as $ntd) {
            $arr = $this->parseNameTypeDefault($ntd);
            if (!\is_array($arr)) {
                return "All field-create strings must have format 'fieldname parameters'";
            }

            \extract($arr); // name, type, default, type_explicit, default_explicit

            if (isset($parse_fields[$name])) {
                return "Already exists field '$name'";
            }

            $names[] = $name;
            $parse_fields[$name] = [
                'type' => $type_explicit,
                'default' => $default_explicit,
                'type_calc' => $type,
                'default_calc' => $default,
                ];
        }
        return \compact('names', 'parse_fields');
    }

    /**
     * Make field-create-sql-part from field name, type and default
     *
     * @param string $name
     * @param string|false $type
     * @param string|falsse $default
     * @return boolean|string
     */
    public function makeFieldCreater($name, $type, $default)
    {
        if (empty($name) || (empty($type) && empty($default))) {
            return false;
        }
        $creater = $name . ' ';
        if (empty($default)) {
            $creater .= $type;
        } else {
            if (empty($type)) {
                $type = $this->typeDetectFromExpression($default);
                if (empty($type)) {
                    return false;
                }
            }
            $creater .= 'DEFAULT ' . $this->typeConvertIfNeed($type, $default);
        }
        return $creater;
    }
}
