#!/bin/bash
set -x

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

SINGLE_COL_DATA_FILE="$(mktemp /tmp/test_single_col_null_format_text.XXXXXX)"
DEFAULT_MULTI_COL_DATA_FILE="$(mktemp /tmp/test_default_null_format_multi_col_text.XXXXXX)"
trap 'rm -f "${SINGLE_COL_DATA_FILE}" "${DEFAULT_MULTI_COL_DATA_FILE}"' EXIT
cat > "${SINGLE_COL_DATA_FILE}" <<'EOF'
null_value
null_value
non-null

\N
EOF

{
    printf 'a\tb\n'
    printf '\n'
    printf '\\N\t\\N\n'
} > "${DEFAULT_MULTI_COL_DATA_FILE}"

hadoop fs -rm -r -f /user/doris/suites/regression/serde_prop/test_single_col_null_format_text || true
hadoop fs -mkdir -p /user/doris/suites/regression/serde_prop/test_single_col_null_format_text
hadoop fs -put "${SINGLE_COL_DATA_FILE}" /user/doris/suites/regression/serde_prop/test_single_col_null_format_text/part-00000

hadoop fs -rm -r -f /user/doris/suites/regression/serde_prop/test_default_null_format_multi_col_text || true
hadoop fs -mkdir -p /user/doris/suites/regression/serde_prop/test_default_null_format_multi_col_text
hadoop fs -put "${DEFAULT_MULTI_COL_DATA_FILE}" /user/doris/suites/regression/serde_prop/test_default_null_format_multi_col_text/part-00000

# create table
hive -f "${CUR_DIR}"/some_serde_table.hql


