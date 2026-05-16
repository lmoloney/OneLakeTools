# Integration Tests

## Prerequisites

- `az login` with access to the test Fabric workspace
- Test tables provisioned in the workspace (see [Setup](#test-table-setup))

## Running

```bash
cd TUI
uv sync --extra dev
uv run pytest tests/integration/ -v        # All integration tests
uv run pytest tests/integration/ -v -k cdf  # CDF tests only
```

## Configuration

Tests load environment config from (in order):

1. `ONELAKE_TEST_ENV_FILE` environment variable
2. `tests/integration/fabric-test-env.json` (in-repo)
3. `~/.config/onelaketools/fabric-test-env.json` (user fallback)

The manifest defines workspace IDs, lakehouse names, and expected table
configurations. See `fabric-test-env.json` for the full schema.

## Test Table Setup

Test tables are pre-provisioned in the Fabric workspace. To recreate them:

1. Open a Spark notebook in the test workspace
2. Copy the PySpark cells from `setup_test_tables.py`
3. Run each cell in order

### Key tables

| Table | Feature | Notes |
|-------|---------|-------|
| `customers` | Basic Delta | Simple schema, single commit |
| `cdf_tracking` | CDF enabled after creation | Created without CDF → data inserted → CDF enabled → more data |
| `partitioned_sales` | Partition columns | Two partition columns |
| `optimized_events` | OPTIMIZE + ZORDER | High version from micro-batches |

### CDF test table (`cdf_tracking`)

This table specifically tests the scenario where CDF was enabled *after*
table creation. The setup script:

1. Drops and recreates the table without `delta.enableChangeDataFeed` (version 0)
2. Inserts initial rows (version 1, no CDF data)
3. Enables CDF via `ALTER TABLE SET TBLPROPERTIES` (version 2)
4. Inserts, updates, and deletes rows (versions 3–5, with CDF data)

This means `read_cdf(starting_version=0)` will fail, but
`read_cdf(starting_version=2)` and `find_cdf_start_version()` should work.

> **Note:** The existing `TestReadCdf` integration tests use
> `starting_version=0` with a skip guard for the CDF-not-enabled error.
> This is intentional — the tests verify both the success and the
> error-handling paths depending on the table's actual state.
