# ClickHouse Migration – Test Environments

Two self-contained Docker Compose environments, one for each migration script.

```
ch-migration-testenv/
├── script1/          # ch_migration_asis.sh   (cross-cluster, plain TCP)
└── script2/          # clickhouse_migration.sh (in-cluster topology, TLS)
```

---

## Requirements

| Tool | Min version |
|------|-------------|
| Docker Engine | 24.x |
| Docker Compose plugin (`docker compose`) | 2.x |
| openssl | any (for script2 cert generation) |

---

## Script 1 – Cross-cluster migration (`ch_migration_asis.sh`)

### Topology

```
OLD cluster (plain TCP)          NEW cluster (plain TCP)
────────────────────────         ──────────────────────────────────────
old-shard1  :19000  shard 1      new-s1-r1  :29000  shard 1 replica 1
old-shard2  :19001  shard 2      new-s1-r2  :29001  shard 1 replica 2
                                 new-s2-r1  :29002  shard 2 replica 1
                                 new-s2-r2  :29003  shard 2 replica 2
```

Engine conversion:  `MergeTree` → `ReplicatedMergeTree`, all variants.

### Run

```bash
cd script1
docker compose up --build
```

Watch the runner:
```bash
docker compose logs -f runner
```

The runner container automatically:
1. Seeds the old cluster with ~1 500 rows across multiple databases and engine types
2. Executes `ch_migration_asis.sh`
3. Runs post-migration verification (engine check, row counts, replica health)

### Interact manually

```bash
# Connect to old cluster shard 1
clickhouse-client --host=localhost --port=19000

# Connect to new cluster entry point
clickhouse-client --host=localhost --port=29000

# Shell into runner for ad-hoc queries
docker compose exec runner bash

# Re-run just the verification
docker compose exec runner bash /scripts/verify_result.sh
```

### Reset

```bash
docker compose down -v   # removes all volumes (data)
docker compose up --build
```

---

## Script 2 – In-cluster topology migration (`clickhouse_migration.sh`)

### Topology

```
OLD cluster (TLS :9440)          NEW cluster (TLS :9440 internally)
────────────────────────         ──────────────────────────────────────
old-shard1  :9440   shard 1      new-s1-r1  :9442  shard 1 replica 1
old-shard2  :9441   shard 2      new-s1-r2  :9443  shard 1 replica 2
                                 new-s2-r1  :9444  shard 2 replica 1
                                 new-s2-r2  :9445  shard 2 replica 2
```

Engine conversion:  `MergeTree` → `ReplicatedMergeTree`, all variants.  
Connections use TLS with a self-signed test CA (`tls/ca.crt`).

### Step 1 – Generate TLS certificates (once only)

```bash
cd script2
bash tls/gen_certs.sh
```

This creates `tls/ca.crt`, one cert+key pair per node, and `tls/dhparam.pem`.
Commit the `tls/` directory or re-run the script on every fresh checkout.

> **Note:** `gen_certs.sh` uses `verificationMode=none` on the server side
> and `--no-verify` / `--insecure` on the client side, so any self-signed
> cert is accepted.  For production, replace with properly signed certs and
> remove those flags.

### Step 2 – Run

```bash
cd script2
docker compose up --build
```

```bash
docker compose logs -f runner2
```

The runner automatically:
1. Seeds the old cluster (TLS) with the same schema as script1
2. Executes `clickhouse_migration.sh`
3. Runs post-migration verification (engine check, ZK paths, replica health,
   ON CLUSTER presence in DDL, and a dry-run smoke test)

### Interact manually

```bash
# Connect to old cluster shard 1 (TLS, self-signed)
clickhouse-client --host=localhost --port=9440 --secure --no-verify

# Connect to new cluster entry point
clickhouse-client --host=localhost --port=9442 --secure --no-verify

# Shell into runner
docker compose exec runner2 bash

# Re-run dry-run only
docker compose exec runner2 bash -c "
  cp /scripts/clickhouse_migration.sh /tmp/dr.sh
  chmod +x /tmp/dr.sh
  /tmp/dr.sh \
    --old-host old-shard1 --new-host new-s1-r1 \
    --disable-tls --dry-run
"
```

### Reset

```bash
docker compose down -v
docker compose up --build
```

---

## Shared test schema

Both environments create the same databases and tables:

| Table | Engine (old) | Engine (new) | Notes |
|-------|-------------|-------------|-------|
| `analytics.events_local` | `MergeTree` | `ReplicatedMergeTree` | Partitioned by month |
| `analytics.events_dist` | `Distributed` | `Distributed` | Unchanged |
| `analytics.metrics_local` | `SummingMergeTree` | `ReplicatedSummingMergeTree` | Extra args preserved |
| `analytics.metrics_dist` | `Distributed` | `Distributed` | Unchanged |
| `analytics.user_sessions` | `AggregatingMergeTree` | `ReplicatedAggregatingMergeTree` | |
| `analytics.page_views` | `ReplacingMergeTree(updated_at)` | `ReplicatedReplacingMergeTree(…, updated_at)` | Version col preserved |
| `analytics.audit_log` | `MergeTree` | `ReplicatedMergeTree` | |
| `analytics.v_daily_summary` | `View` | `View` | No conversion |
| `inventory.products` | `MergeTree` | `ReplicatedMergeTree` | |
| `inventory.stock_movements` | `CollapsingMergeTree(sign)` | `ReplicatedCollapsingMergeTree(…, sign)` | Sign col preserved |
| `inventory.products_dist` | `Distributed` | `Distributed` | Unchanged |

### What the verification checks

1. **Engine transformations** – every MergeTree-family table is now `Replicated*`
2. **ZooKeeper paths** – contain `{shard}` and `{replica}` macros
3. **Row counts** – old shard1 + shard2 totals match new cluster counts
4. **ON CLUSTER** – present in every `CREATE TABLE` DDL on the new cluster
5. **Replica health** – `system.replicas` shows no `is_readonly` or `is_session_expired`
6. **Replica count** – every replicated table has ≥ 2 replicas
7. **Dry-run** (script2 only) – `--dry-run` flag prints DDL without executing

---

## Troubleshooting

**Runner exits before ClickHouse is ready**  
Healthchecks have a 20-retry × 5 s budget (100 s total).  If your machine is
slow, increase `retries` in `docker-compose.yml`.

**`Connection refused` on TLS ports (script2)**  
Make sure `tls/gen_certs.sh` was run before `docker compose up`.  The server
will refuse to start if the cert files are missing.

**`Table already exists` on re-run**  
Run `docker compose down -v` first to wipe all volumes.

**ZooKeeper session errors on new cluster**  
Wait ~10 s after `docker compose up` for ZooKeeper to elect a leader before
the runner starts.  The `depends_on: condition: service_healthy` chain
handles this automatically.
