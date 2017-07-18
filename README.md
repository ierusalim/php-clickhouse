# php-clickhouse
[![Build Status](https://api.travis-ci.org/ierusalim/php-clickhouse.svg?branch=master)](https://www.travis-ci.org/ierusalim/php-clickhouse)
[![codecov](https://codecov.io/gh/ierusalim/php-clickhouse/branch/master/graph/badge.svg)](https://codecov.io/gh/ierusalim/php-clickhouse)
## ClickHouse simple access library (http, https)

#### Class ClickHouseAPI
contains simple http/https connector for ClickHouse db-server
and have not dependencies (may be used independently, file src/ClichHouseAPI.php).

#### Class ClickHouseQuery
contains wrapper for ClickHouseAPI and allow to easily
send queries to ClickHouse server and parsing answering data.

#### Class ClickHouseFunctions
based on ClickHouseQuery and ClickHouseAPI and contains functions for
simple operations with ClickHouse.

