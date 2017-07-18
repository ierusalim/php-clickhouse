# php-clickhouse
[![Build Status](https://api.travis-ci.org/ierusalim/php-clickhouse.svg?branch=master)](https://www.travis-ci.org/ierusalim/php-clickhouse)
[![codecov](https://codecov.io/gh/ierusalim/php-clickhouse/branch/master/graph/badge.svg)](https://codecov.io/gh/ierusalim/php-clickhouse)
## ClickHouse simple access library (http, https)

* Class **ClickHouseAPI** contains simple http/https connector for ClickHouse server
and have not dependencies (may be used independently, file src/ClichHouseAPI.php).

* Class **ClickHouseQuery**  contains wrapper for ClickHouseAPI and allow to easily
send queries to ClickHouse server and parsing answering data.

* Class **ClickHouseFunctions** based on ClickHouseQuery and ClickHouseAPI and
contains functions for simple operations with ClickHouse.

### Example:
```php
<?php
    require "vendor/autoload.php";
    $ch = new ClickHouseFunctions("http://127.0.0.1:8123/");
    echo "ClickHouse version: " . $ch->getVersion();
    echo " Server uptime: " . $ch->getUptime();
    
    echo "\n\nDatabases: ";
    print_r($ch->getDatabasesList());
    $ch->setCurrentDatabase("system");
    echo "Tables in '" . $ch->getCurrentDatabase() ."' database:\n";
    print_r($ch->getTablesList());

    $ch->setCurrentDatabase("default");
    $ch->createTableQuick("temp", [
        'temp_id'   => 'integer',
        'temp_date' => 'date now()',
        'temp_name' => 'char(32) "Unknown name"',
        'temp_str'  => 'string'
    ]);
    print_r($ch->getTableRowSize("temp"));
 ```
