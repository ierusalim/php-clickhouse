# php-clickhouse
[![Build Status](https://api.travis-ci.org/ierusalim/php-clickhouse.svg?branch=master)](https://www.travis-ci.org/ierusalim/php-clickhouse)
[![codecov](https://codecov.io/gh/ierusalim/php-clickhouse/branch/master/graph/badge.svg)](https://codecov.io/gh/ierusalim/php-clickhouse)
## ClickHouse simple access library (http, https)

### Class **ClickHouseAPI**
Class **ClickHouseAPI** contains simple http/https connector for ClickHouse server
and have not dependencies (may be used independently, file src/ClichHouseAPI.php).
#### http/https connector functions:
* **setServerUrl**($url) - set ClickHouse server parameters by url (host, port, etc.)
* **getQuery**($h_query [, $sess]) - send GET request
* **postQuery**($h_query, $post_data [, $sess]) - send POST request
#### Special functions
* **getVersion**() - return version of ClickHouse server (side effect - detect server features)
* **isSupported**(feature-name) - true or false depending on the server support features.
##### If isSupported('session_id'):
* **getSession**() - get current session_id from options
* **setSession**([$sess]) - set session_id or generate new session_id and set it
##### Options:
* **setOption**($key, $value) - set http-option for all next requests
* **getOption**($key) - get current http-option value
* **delOption**($key) - delete http-option (same ->setOption($key, null)

### Class **ClickHouseQuery**
Class **ClickHouseQuery** contains wrapper for ClickHouseAPI and allow to easily
send queries to ClickHouse server and parsing answering data.

#### Main query-functions for use:
* **queryFalse**($sql, [post])- for queries that should not return anything. False if ok, or error string.
* **queryTrue**($sql, [post]) - return false only if error, otherwise return true or response data.
* **queryValue**($sql, [post]) - send any query and receive all data in one string (false if error)
* **queryArray**($sql) - for queries returning structured data (usually one or more table rows)
* **queryKeyValues**(see descr.) - for queries returning 2 columns, first means as key, second as value
* **queryInsertArray**($table, $fields_names, $fields_set) - insert data into table from array
* **queryInsertFile**($table, $file, $structure) - insert data from file into table

### Class **ClickHouseFunctions**
Class **ClickHouseFunctions** based on ClickHouseQuery and ClickHouseAPI and
contains functions for simple operations with ClickHouse.
#### Functions:
* **createTableQuick**($table, $fields_arr) - create table with specified fields
* **sendFileInsert**($file, $table) - send TabSeparated-file into table (structure autodetect)
* **clearTable**($table [, $sess]) - clear table (DROP and re-create)
* **dropTable**($table [, $sess]) - drop specified table
* **renameTable**($from_name_or_arr [, $to_name] [, $sess]) - rename tables
* **getTableFields**($table, ...) - returns [field_name=>field_type] array
* **getTableInfo**($table [, $extended]) - returns array with info about table
* **getTablesList**([$db] [,$pattern]) - returns tables list by SHOW TABLES request
* **createDatabase**($db) - create new database with specified name
* **dropDatabase**($db) - drop specified database and remove all tables inside
* **getDatabasesList**() - returns array contained names of existing Databases
* **setCurrentDatabase**($db [, $sess]) - set current database by 'USE db' request
* **getCurrentDatabase**([$sess]) - return results of 'SELECT currentDatabase()'
* **getUptime**() - return server uptime in seconds
* **getSystemSettings**() - get information from system.settings as array [name=>value]

### Example:
```php
<?php
    namespace ierusalim\ClickHouse;

    require "vendor/autoload.php";

    $ch = new ClickHouseFunctions("http://127.0.0.1:8123/");

    echo "ClickHouse version: " . $ch->getVersion();
    if (!$ch->isSupported('query')) {
        die(" Server not ready");
    }
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
    
    $ch->queryInsertArray("temptab", null, [
        'id' => 1,
        'email' => 'noreply@github.com'
    ]);
    
    $ch->queryInsertArray("temptab", ['id', 'email', 'name'], [
        [2, 'reply@github.com', 'Andy'],
        [3, null , 'Donald'],
    ]);

    $ch->queryInsertArray("temptab", null, [
        ['id'=>4, 'name'=>'Ronald', 'email'=>'no'],
        ['id'=>5, 'name'=>'', 'email'=>'yes'],
    ]);
    
    $rows = $ch->queryArray("SELECT * FROM temptab");
    print_r($rows);
    
    $name_emails_arr = $ch->queryKeyValues('temptab', 'name, email');
    print_r($name_emails_arr);
    
    print_r($ch->getTableInfo("temptab"));
 ```
