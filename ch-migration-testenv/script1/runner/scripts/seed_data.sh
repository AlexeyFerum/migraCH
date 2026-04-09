#!/bin/bash
# =============================================================================
# seed_data.sh  –  Populate the OLD cluster with a realistic test schema.
#
# Schema overview
# ---------------
# Database: analytics
#   events_local        MergeTree          – partitioned by month
#   events_dist         Distributed        – over events_local
#   metrics_local       SummingMergeTree   – tests variant engine conversion
#   metrics_dist        Distributed        – over metrics_local
#   user_sessions       AggregatingMergeTree
#   page_views          ReplacingMergeTree – tests version-column preservation
#   audit_log           MergeTree          – append-only log
#   v_daily_summary     View               – depends on events_dist
#
# Database: inventory
#   products            MergeTree
#   stock_movements     CollapsingMergeTree – tests sign-column preservation
#   products_dist       Distributed
# =============================================================================

set -euo pipefail

OLD_SHARD1="${OLD_SHARD1_HOST:-old-shard1}"
OLD_SHARD2="${OLD_SHARD2_HOST:-old-shard2}"

q1() { clickhouse client --host="$OLD_SHARD1" --port=9000 --multiquery -q "$1"; }
q2() { clickhouse client --host="$OLD_SHARD2" --port=9000 --multiquery -q "$1"; }
# Run the same DDL on both shards
qboth() { q1 "$1"; q2 "$1"; }

echo "=== Creating databases ==="
qboth "CREATE DATABASE IF NOT EXISTS analytics;"
qboth "CREATE DATABASE IF NOT EXISTS inventory;"

echo "=== analytics.events_local (MergeTree, partitioned by month) ==="
qboth "
CREATE TABLE IF NOT EXISTS analytics.events_local (
    event_date   Date,
    event_time   DateTime,
    user_id      UInt64,
    event_type   LowCardinality(String),
    page         String,
    duration_ms  UInt32,
    country      LowCardinality(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id, event_type);
"

echo "=== analytics.events_dist (Distributed over events_local) ==="
qboth "
CREATE TABLE IF NOT EXISTS analytics.events_dist (
    event_date   Date,
    event_time   DateTime,
    user_id      UInt64,
    event_type   LowCardinality(String),
    page         String,
    duration_ms  UInt32,
    country      LowCardinality(String)
) ENGINE = Distributed('epm_cluster', 'analytics', 'events_local', user_id);
"

echo "=== analytics.metrics_local (SummingMergeTree) ==="
qboth "
CREATE TABLE IF NOT EXISTS analytics.metrics_local (
    metric_date  Date,
    service      LowCardinality(String),
    endpoint     String,
    request_count UInt64,
    error_count   UInt64,
    total_ms      UInt64
) ENGINE = SummingMergeTree(request_count, error_count, total_ms)
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_date, service, endpoint);
"

qboth "
CREATE TABLE IF NOT EXISTS analytics.metrics_dist (
    metric_date  Date,
    service      LowCardinality(String),
    endpoint     String,
    request_count UInt64,
    error_count   UInt64,
    total_ms      UInt64
) ENGINE = Distributed('epm_cluster', 'analytics', 'metrics_local', cityHash64(service, endpoint));
"

echo "=== analytics.user_sessions (AggregatingMergeTree) ==="
qboth "
CREATE TABLE IF NOT EXISTS analytics.user_sessions (
    session_date Date,
    user_id      UInt64,
    session_count AggregateFunction(count, UInt8),
    total_duration AggregateFunction(sum, UInt32)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(session_date)
ORDER BY (session_date, user_id);
"

echo "=== analytics.page_views (ReplacingMergeTree with version column) ==="
qboth "
CREATE TABLE IF NOT EXISTS analytics.page_views (
    view_date    Date,
    page_id      UInt64,
    title        String,
    view_count   UInt64,
    updated_at   DateTime
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(view_date)
ORDER BY (view_date, page_id);
"

echo "=== analytics.audit_log (MergeTree, append-only) ==="
qboth "
CREATE TABLE IF NOT EXISTS analytics.audit_log (
    log_time     DateTime,
    actor        String,
    action       LowCardinality(String),
    resource     String,
    result       LowCardinality(String),
    details      String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(log_time)
ORDER BY (log_time, actor, action);
"

echo "=== analytics.v_daily_summary (View) ==="
qboth "
CREATE VIEW IF NOT EXISTS analytics.v_daily_summary AS
SELECT
    event_date,
    event_type,
    country,
    count()          AS event_count,
    avg(duration_ms) AS avg_duration
FROM analytics.events_dist
GROUP BY event_date, event_type, country;
"

echo "=== inventory.products (MergeTree) ==="
qboth "
CREATE TABLE IF NOT EXISTS inventory.products (
    product_id   UInt32,
    sku          String,
    name         String,
    category     LowCardinality(String),
    price        Decimal(10,2),
    created_at   DateTime
) ENGINE = MergeTree()
ORDER BY (category, product_id);
"

echo "=== inventory.stock_movements (CollapsingMergeTree with sign column) ==="
qboth "
CREATE TABLE IF NOT EXISTS inventory.stock_movements (
    movement_date Date,
    product_id    UInt32,
    warehouse_id  UInt16,
    quantity      Int32,
    sign          Int8
) ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(movement_date)
ORDER BY (movement_date, product_id, warehouse_id);
"

qboth "
CREATE TABLE IF NOT EXISTS inventory.products_dist (
    product_id   UInt32,
    sku          String,
    name         String,
    category     LowCardinality(String),
    price        Decimal(10,2),
    created_at   DateTime
) ENGINE = Distributed('epm_cluster', 'inventory', 'products', product_id);
"

echo ""
echo "=== Inserting sample data ==="

# events: 500 rows spread across 2 months, inserted via distributed table
# (ClickHouse will route rows to shards by user_id hash)
q1 "
INSERT INTO analytics.events_dist
SELECT
    toDate('2024-01-01') + (number % 31)              AS event_date,
    toDateTime('2024-01-01') + (number * 60)          AS event_time,
    (number % 100) + 1                                AS user_id,
    ['pageview','click','purchase','signup'][1 + number % 4] AS event_type,
    concat('/page/', toString(number % 20))            AS page,
    50 + (number % 500)                               AS duration_ms,
    ['US','DE','FR','GB','JP'][1 + number % 5]         AS country
FROM numbers(250);
"

q1 "
INSERT INTO analytics.events_dist
SELECT
    toDate('2024-02-01') + (number % 28)              AS event_date,
    toDateTime('2024-02-01') + (number * 60)          AS event_time,
    (number % 100) + 101                              AS user_id,
    ['pageview','click','purchase','signup'][1 + number % 4] AS event_type,
    concat('/page/', toString(number % 20))            AS page,
    100 + (number % 800)                              AS duration_ms,
    ['US','DE','FR','GB','JP'][1 + number % 5]         AS country
FROM numbers(250);
"

# metrics: directly into local tables on each shard
q1 "
INSERT INTO analytics.metrics_local
SELECT
    toDate('2024-01-01') + (number % 31) AS metric_date,
    ['api','web','worker'][1 + number % 3] AS service,
    concat('/endpoint/', toString(number % 10)) AS endpoint,
    100 + (number % 900) AS request_count,
    number % 10          AS error_count,
    (100 + number % 900) * (10 + number % 50) AS total_ms
FROM numbers(200);
"

q2 "
INSERT INTO analytics.metrics_local
SELECT
    toDate('2024-01-01') + (number % 31) AS metric_date,
    ['api','web','worker'][1 + number % 3] AS service,
    concat('/endpoint/', toString(number % 10)) AS endpoint,
    200 + (number % 800) AS request_count,
    number % 15          AS error_count,
    (200 + number % 800) * (15 + number % 40) AS total_ms
FROM numbers(200);
"

# page_views: 50 rows per shard
q1 "
INSERT INTO analytics.page_views
SELECT
    toDate('2024-01-15')          AS view_date,
    number + 1                    AS page_id,
    concat('Page ', toString(number + 1)) AS title,
    (number * 7) % 1000           AS view_count,
    now()                         AS updated_at
FROM numbers(50);
"

q2 "
INSERT INTO analytics.page_views
SELECT
    toDate('2024-01-15')          AS view_date,
    number + 51                   AS page_id,
    concat('Page ', toString(number + 51)) AS title,
    (number * 13) % 2000          AS view_count,
    now()                         AS updated_at
FROM numbers(50);
"

# audit_log: 100 rows per shard
q1 "
INSERT INTO analytics.audit_log
SELECT
    now() - (number * 600)       AS log_time,
    concat('user_', toString(number % 10)) AS actor,
    ['login','logout','update','delete'][1 + number % 4] AS action,
    concat('resource_', toString(number % 20)) AS resource,
    ['success','failure'][1 + number % 2] AS result,
    concat('detail_', toString(number))  AS details
FROM numbers(100);
"

q2 "
INSERT INTO analytics.audit_log
SELECT
    now() - (number * 300)       AS log_time,
    concat('admin_', toString(number % 5)) AS actor,
    ['read','write','execute'][1 + number % 3] AS action,
    concat('system_', toString(number % 10)) AS resource,
    'success'                    AS result,
    concat('sys_detail_', toString(number)) AS details
FROM numbers(100);
"

# products: 100 rows per shard
q1 "
INSERT INTO inventory.products
SELECT
    number + 1                   AS product_id,
    concat('SKU-', toString(number + 1)) AS sku,
    concat('Product ', toString(number + 1)) AS name,
    ['Electronics','Clothing','Food','Sports'][1 + number % 4] AS category,
    round(9.99 + (number % 100) * 1.5, 2) AS price,
    now() - (number * 3600)     AS created_at
FROM numbers(100);
"

q2 "
INSERT INTO inventory.products
SELECT
    number + 101                 AS product_id,
    concat('SKU-', toString(number + 101)) AS sku,
    concat('Product ', toString(number + 101)) AS name,
    ['Electronics','Clothing','Food','Sports'][1 + number % 4] AS category,
    round(4.99 + (number % 200) * 0.75, 2) AS price,
    now() - (number * 1800)     AS created_at
FROM numbers(100);
"

# stock_movements: CollapsingMergeTree – insert rows with sign=1
q1 "
INSERT INTO inventory.stock_movements
SELECT
    toDate('2024-01-01') + (number % 31) AS movement_date,
    (number % 100) + 1   AS product_id,
    (number % 5)  + 1    AS warehouse_id,
    10 + (number % 90)   AS quantity,
    1                    AS sign
FROM numbers(150);
"

q2 "
INSERT INTO inventory.stock_movements
SELECT
    toDate('2024-01-01') + (number % 31) AS movement_date,
    (number % 100) + 101 AS product_id,
    (number % 3)  + 1    AS warehouse_id,
    5 + (number % 50)    AS quantity,
    1                    AS sign
FROM numbers(150);
"

echo ""
echo "=== Row count summary on OLD cluster ==="
for db_table in \
    analytics.events_local \
    analytics.metrics_local \
    analytics.page_views \
    analytics.audit_log \
    inventory.products \
    inventory.stock_movements
do
    c1=$(clickhouse client --host="$OLD_SHARD1" --port=9000 -q "SELECT count() FROM $db_table" 2>/dev/null)
    c2=$(clickhouse client --host="$OLD_SHARD2" --port=9000 -q "SELECT count() FROM $db_table" 2>/dev/null)
    echo "  $db_table  shard1=$c1  shard2=$c2  total=$((c1 + c2))"
done

echo ""
echo "Seed data loaded successfully."
