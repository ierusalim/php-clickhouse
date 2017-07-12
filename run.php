<?php

namespace Ierusalim\ClickHouse;

require_once "src/ClickHouseHost.php";

$ch = new ClickHouseHost();

$response = $ch->getQuery("SELECT 1");

echo "Server response: $response\n";

return 0;
