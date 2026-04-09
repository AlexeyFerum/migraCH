#!/bin/bash
# =============================================================================
# entrypoint.sh  –  Runner container entry point for Script 1 test.
#
# Sequence
# --------
#   1. Seed the old cluster with test data
#   2. Run ch_migration_asis.sh  (cross-cluster migration)
#   3. Run verify_result.sh      (post-migration checks)
# =============================================================================

set -euo pipefail

PASS='\033[0;32m'
FAIL='\033[0;31m'
HEAD='\033[1;36m'
NC='\033[0m'

section() { echo -e "\n${HEAD}══════════════════════════════════════════${NC}"; \
            echo -e "${HEAD}  $1${NC}"; \
            echo -e "${HEAD}══════════════════════════════════════════${NC}\n"; }

section "STEP 1 – Seeding old cluster with test data"
bash /scripts/seed_data.sh

section "STEP 2 – Running cross-cluster migration"
bash /scripts/run_migration.sh

section "STEP 3 – Post-migration verification"
bash /scripts/verify_result.sh

echo -e "\n${PASS}All steps completed. Check output above for any warnings or failures.${NC}\n"
