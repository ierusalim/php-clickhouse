<?php

namespace Ierusalim\ClickHouse;

require_once "src/ClickHouseAPI.php";

$ch = new ClickHouseAPI();

$response = $ch->getQuery("SELECT 1");

echo "Server response:";
print_r($response);
echo "\n";

return 0;
