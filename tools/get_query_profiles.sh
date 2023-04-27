#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# usage: get_query_profiles.sh -q query_id -o output_dir

FE_HOST="127.0.0.1"
FE_QUERY_PORT=9939
USER=root
PASSWORD=""
QUERY_ID=""
OUTPUT_DIR=""

SHOW_QUERY_PROFILE="show query profile"
MYSQL_CMD_ARGS=(-h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}")
if [[ -n "${PASSWORD}" ]]; then
    MYSQL_CMD_ARGS+=(-p"${PASSWORD}")
fi

while getopts ":q:o:" opt; do
    case "${opt}" in
    q)
        QUERY_ID="${OPTARG}"
        ;;
    o)
        OUTPUT_DIR="${OPTARG}"
        ;;
    *) ;;

    esac
done

if [[ -z "${QUERY_ID}" ]]; then
    # QUERY_ID=$(mysql -h$FE_HOST -u$USER -P$FE_QUERY_PORT -e"show query profile '/'" | cut -f 1 | sed -n '3p')
    echo "no query id"
    exit
fi
if [[ -z "${OUTPUT_DIR}" ]]; then
    echo "no output dir"
    exit
fi

echo "get profiles for query ${QUERY_ID}, output dir: ${OUTPUT_DIR}"

mkdir -p "${OUTPUT_DIR}"

PLAN_GRAPH_FILE=${OUTPUT_DIR}/${QUERY_ID}_plan_graph
FRAGS_FILE=${OUTPUT_DIR}/${QUERY_ID}_frags
FRAG_IDS_FILE=${OUTPUT_DIR}/${QUERY_ID}_frag_ids

mysql "${MYSQL_CMD_ARGS[@]}" -e "${SHOW_QUERY_PROFILE} '/${QUERY_ID}'" | sed 's/\\n/\n/g' >"${PLAN_GRAPH_FILE}"

grep "Fragment: " "${PLAN_GRAPH_FILE}" | sort | uniq >"${FRAGS_FILE}"
sed 's/Fragment: \{1,\}\([0-9]\{1,\}\)/\n\1\n/g' "${PLAN_GRAPH_FILE}" | grep -E "^[0-9]{1,}$" | sort | uniq >"${FRAG_IDS_FILE}"

# frag inst ids
while read -r frag_id; do
    sql="${SHOW_QUERY_PROFILE} '/${QUERY_ID}/${frag_id}'"
    frag_inst_ids_outfile="${OUTPUT_DIR}/frag_${frag_id}_instances"

    echo "sql: ${sql}"
    mysql "${MYSQL_CMD_ARGS[@]}" -e "${sql}" | sed -n '2,$p' >"${frag_inst_ids_outfile}"
    while read -r line; do
        frag_inst_id=$(echo "${line}" | cut -f1)
        frag_inst_profile="${OUTPUT_DIR}/frag_${frag_id}_${frag_inst_id}"
        sql="${SHOW_QUERY_PROFILE} '/${QUERY_ID}/${frag_id}/${frag_inst_id}'"
        mysql "${MYSQL_CMD_ARGS[@]}" -e "${sql}" | sed 's/\\n/\n/g' >"${frag_inst_profile}"
    done <"${frag_inst_ids_outfile}"
done <"${FRAG_IDS_FILE}"
