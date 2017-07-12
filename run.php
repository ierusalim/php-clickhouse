<?php

namespace Ierusalim\ClickHouse;

require_once "src/ClickHouseAPI.php";

$ch = new ClickHouseAPI();

$ch->debug = true;

$response = $ch->getQuery("SELECT 1");
if ($response['code'] != 200 || $response['response'] != 1) {
   die("The server does not work");
}
$ch->postQuery("CREATE TABLE t (a UInt8) ENGINE = Memory");
$ch->postQuery('INSERT INTO t VALUES (1),(2),(3)');
$data = $ch->getQuery('SELECT * FROM t FORMAT JSONCompact');
$data = json_decode($data['response']);
$ch->postQuery("DROP TABLE t");
print_r($data);

return 0;
