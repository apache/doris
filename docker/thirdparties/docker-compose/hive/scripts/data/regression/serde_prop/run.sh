#!/bin/bash
set -x

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

DATA_FILE="$(mktemp /tmp/test_single_col_null_format_text.XXXXXX)"
trap 'rm -f "${DATA_FILE}"' EXIT
cat > "${DATA_FILE}" <<'EOF'
null_value

non-null
\N
EOF

hadoop fs -rm -r -f /user/doris/suites/regression/serde_prop/test_single_col_null_format_text || true
hadoop fs -mkdir -p /user/doris/suites/regression/serde_prop/test_single_col_null_format_text
hadoop fs -put "${DATA_FILE}" /user/doris/suites/regression/serde_prop/test_single_col_null_format_text/part-00000

# create table
hive -f "${CUR_DIR}"/some_serde_table.hql


