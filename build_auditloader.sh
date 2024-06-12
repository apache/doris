#!/bin/bash

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# export DORIS_HOME="${ROOT}"

# . "${DORIS_HOME}/env.sh"

DORIS_OUTPUT="$1"
DORIS_HOME="$2"
AUDITLOADER_DIR="${DORIS_OUTPUT}/audit_loader"
mkdir -p ${AUDITLOADER_DIR}
cd "${DORIS_HOME}/fe_plugins/auditloader"

echo "build auditloader start ..."
./build.sh
echo "build auditloader end ..."

#mkdir $AUDITLOADER_DIR

echo "copy auditloader packages"

echo "list dir /fe_plugins/auditloader/output"
ls -l "${DORIS_HOME}/fe_plugins/auditloader/output"
cp -a "${DORIS_HOME}/fe_plugins/auditloader/output/auditloader.zip" "${AUDITLOADER_DIR}/"
echo "list dir auditloader dir"
ls -l $AUDITLOADER_DIR
cp -a $AUDITLOADER_DIR "${DORIS_OUTPUT}/output/"
ls -l "${DORIS_OUTPUT}/output/audit_loader"
#cp -r "${DORIS_OUTPUT}/audit_loader" "${DORIS_HOME}/output"

exit 0
