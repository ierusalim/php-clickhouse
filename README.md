# php-clickhouse
[![Build Status](https://api.travis-ci.org/ierusalim/php-clickhouse.svg?branch=master)](https://www.travis-ci.org/ierusalim/php-clickhouse)
[![codecov](https://codecov.io/gh/ierusalim/php-clickhouse/branch/master/graph/badge.svg)](https://codecov.io/gh/ierusalim/php-clickhouse)
[![SensioLabsInsight](https://insight.sensiolabs.com/projects/11c45a2c-1214-4b6e-909d-0e6ce4ad046c/mini.png)](https://insight.sensiolabs.com/projects/11c45a2c-1214-4b6e-909d-0e6ce4ad046c)
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

    $ch->createTableQuick("temptab", [
        'id'   => 'integer',
        'dt' => 'date now()',
        'name' => "char(32) 'example'",
        'email'  => 'string'
    ]);
    
    $ch->queryInsert("temptab", null, [
        'id' => 1,
        'email' => 'noreply@github.com'
    ]);
    
    $ch->queryInsert("temptab", ['id', 'email', 'name'], [
        [2, 'reply@github.com', 'Andy'],
        [3, null , 'Donald'],
    ]);

    $ch->queryInsert("temptab", null, [
        ['id'=>4, 'name'=>'Ronald', 'email'=>'no'],
        ['id'=>5, 'name'=>'', 'email'=>'yes'],
    ]);
    
    $rows = $ch->queryArray("SELECT * FROM temptab");
    print_r($rows);
    
    $name_emails_arr = $ch->queryKeyValues('temptab', 'name, email');
    print_r($name_emails_arr);
    
    print_r($ch->getTableInfo("temptab"));
 ```
