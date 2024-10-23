#!/bin/bash
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

generate_bulk_request() {
    local index_name=$1
    local type_value=$2
    local id_prefix=$3
    local data_file=$4
    local output_file=$5

    // clear output file
    echo "" > "$output_file"

    local id=1
    while IFS= read -r line; do
        if [ -n "$type_value" ]; then
            echo "{\"index\": {\"_index\": \"$index_name\", \"_type\": \"$type_value\", \"_id\": \"${id_prefix}${id}\"}}"  >> "$output_file"
        else
            echo "{\"index\": {\"_index\": \"$index_name\", \"_id\": \"${id_prefix}${id}\"}}"  >> "$output_file"
        fi
        echo "$line"  >> "$output_file"
        id=$((id + 1))
    done < "$data_file"
}

array_data_file="/mnt/scripts/data/composite_type_array_bulk.json"

# es 5
# create index test1
# shellcheck disable=SC2154
curl "http://${ES_5_HOST}:9200/test1" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/es6_test1.json"
# create index test2_20220808
curl "http://${ES_5_HOST}:9200/test2_20220808" -H "Content-Type:application/json" -X PUT -d '@/mnt/scripts/index/es6_test2.json'
# create index test2_20220809
curl "http://${ES_5_HOST}:9200/test2_20220809" -H "Content-Type:application/json" -X PUT -d '@/mnt/scripts/index/es6_test2.json'
# put data for test1
curl "http://${ES_5_HOST}:9200/test1/doc/1" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data1_es6.json'
curl "http://${ES_5_HOST}:9200/test1/doc/2" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data2_es6.json'
# only difference between es5 and es6
curl "http://${ES_5_HOST}:9200/test1/doc/3" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data3_es5.json'
curl "http://${ES_5_HOST}:9200/test1/doc/4" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data4_es6.json'
# put data for test2_20220808
curl "http://${ES_5_HOST}:9200/test2_20220808/doc/1" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data1_es6.json'
curl "http://${ES_5_HOST}:9200/test2_20220808/doc/2" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data2_es6.json'
curl "http://${ES_5_HOST}:9200/test2_20220808/doc/3" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data3_es5.json'
# put data for test2_20220809
curl "http://${ES_5_HOST}:9200/test2_20220809/doc/1" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data1_es6.json'
curl "http://${ES_5_HOST}:9200/test2_20220809/doc/2" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data2_es6.json'
curl "http://${ES_5_HOST}:9200/test2_20220809/doc/3" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data3_es5.json'
# put _meta for array
curl "http://${ES_5_HOST}:9200/test1/doc/_mapping" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/array_meta.json"
curl "http://${ES_5_HOST}:9200/test2_20220808/doc/_mapping" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/array_meta.json"
curl "http://${ES_5_HOST}:9200/test2_20220809/doc/_mapping" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/array_meta.json"
# create index .hide
curl "http://${ES_5_HOST}:9200/.hide" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/es6_hide.json"
# create index composite_type_array
curl "http://${ES_5_HOST}:9200/composite_type_array" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/es6_composite_type_array.json"
# put data with bulk for composite_type_array
bulk_request_file="/mnt/scripts/data/bulk_request_es5.json"
generate_bulk_request "composite_type_array" "doc" "item_" "$array_data_file" "$bulk_request_file"
curl -X POST "http://${ES_5_HOST}:9200/_bulk" --data-binary "@$bulk_request_file" -H "Content-Type: application/json"
# put _meta for composite_type_array
curl "http://${ES_5_HOST}:9200/composite_type_array/doc/_mapping" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/array_meta_composite_type_array.json"

# es 6
# create index test1
# shellcheck disable=SC2154
curl "http://${ES_6_HOST}:9200/test1" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/es6_test1.json"
# create index test2_20220808
curl "http://${ES_6_HOST}:9200/test2_20220808" -H "Content-Type:application/json" -X PUT -d '@/mnt/scripts/index/es6_test2.json'
# create index test2_20220809
curl "http://${ES_6_HOST}:9200/test2_20220809" -H "Content-Type:application/json" -X PUT -d '@/mnt/scripts/index/es6_test2.json'
# put data for test1
curl "http://${ES_6_HOST}:9200/test1/doc/1" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data1_es6.json'
curl "http://${ES_6_HOST}:9200/test1/doc/2" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data2_es6.json'
curl "http://${ES_6_HOST}:9200/test1/doc/3" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data3_es6.json'
curl "http://${ES_6_HOST}:9200/test1/doc/4" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data4_es6.json'
# put data for test2_20220808
curl "http://${ES_6_HOST}:9200/test2_20220808/doc/1" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data1_es6.json'
curl "http://${ES_6_HOST}:9200/test2_20220808/doc/2" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data2_es6.json'
curl "http://${ES_6_HOST}:9200/test2_20220808/doc/3" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data3_es6.json'
# put data for test2_20220809
curl "http://${ES_6_HOST}:9200/test2_20220809/doc/1" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data1_es6.json'
curl "http://${ES_6_HOST}:9200/test2_20220809/doc/2" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data2_es6.json'
curl "http://${ES_6_HOST}:9200/test2_20220809/doc/3" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data3_es6.json'
# put _meta for array
curl "http://${ES_6_HOST}:9200/test1/doc/_mapping" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/array_meta.json"
curl "http://${ES_6_HOST}:9200/test2_20220808/doc/_mapping" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/array_meta.json"
curl "http://${ES_6_HOST}:9200/test2_20220809/doc/_mapping" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/array_meta.json"
# create index .hide
curl "http://${ES_6_HOST}:9200/.hide" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/es6_hide.json"
# create index composite_type_array
curl "http://${ES_6_HOST}:9200/composite_type_array" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/es6_composite_type_array.json"
# put data with bulk for composite_type_array
bulk_request_file="/mnt/scripts/data/bulk_request_es6.json"
generate_bulk_request "composite_type_array" "doc" "item_" "$array_data_file" "$bulk_request_file"
curl -X POST "http://${ES_6_HOST}:9200/_bulk" --data-binary "@$bulk_request_file" -H "Content-Type: application/json"
# put _meta for composite_type_array
curl "http://${ES_6_HOST}:9200/composite_type_array/doc/_mapping" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/array_meta_composite_type_array.json"

# es7
# create index test1
curl "http://${ES_7_HOST}:9200/test1" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/es7_test1.json"
# create index test2_20220808
curl "http://${ES_7_HOST}:9200/test2_20220808" -H "Content-Type:application/json" -X PUT -d '@/mnt/scripts/index/es7_test2.json'
# create index test2_20220808
curl "http://${ES_7_HOST}:9200/test2_20220809" -H "Content-Type:application/json" -X PUT -d '@/mnt/scripts/index/es7_test2.json'
# create index test3_20231005
curl "http://${ES_7_HOST}:9200/test3_20231005" -H "Content-Type:application/json" -X PUT -d '@/mnt/scripts/index/es7_test3.json'
# put data for tese1
curl "http://${ES_7_HOST}:9200/test1/_doc/1" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data1.json'
curl "http://${ES_7_HOST}:9200/test1/_doc/2" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data2.json'
curl "http://${ES_7_HOST}:9200/test1/_doc/3" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data3.json'
curl "http://${ES_7_HOST}:9200/test1/_doc/4" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data4.json'
curl "http://${ES_7_HOST}:9200/test1/_doc/5" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data5.json'
# put data for test2_20220808
curl "http://${ES_7_HOST}:9200/test2_20220808/_doc/1" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data1.json'
curl "http://${ES_7_HOST}:9200/test2_20220808/_doc/2" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data2.json'
curl "http://${ES_7_HOST}:9200/test2_20220808/_doc/3" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data3.json'
curl "http://${ES_7_HOST}:9200/test2_20220808/_doc/4" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data4.json'
# put data for test2_20220809
curl "http://${ES_7_HOST}:9200/test2_20220809/_doc/1" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data1.json'
curl "http://${ES_7_HOST}:9200/test2_20220809/_doc/2" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data2.json'
curl "http://${ES_7_HOST}:9200/test2_20220809/_doc/3" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data3.json'
curl "http://${ES_7_HOST}:9200/test2_20220809/_doc/4" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data4.json'
# put data for test3_20231005
curl "http://${ES_7_HOST}:9200/test3_20231005/_doc/1" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data6.json'

# put _meta for array
curl "http://${ES_7_HOST}:9200/test1/_mapping" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/array_meta.json"
curl "http://${ES_7_HOST}:9200/test2_20220808/_mapping" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/array_meta.json"
curl "http://${ES_7_HOST}:9200/test2_20220809/_mapping" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/array_meta.json"

# create index .hide
curl "http://${ES_7_HOST}:9200/.hide" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/es7_hide.json"

# create index composite_type_array
curl "http://${ES_7_HOST}:9200/composite_type_array" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/es7_composite_type_array.json"
# put data with bulk for composite_type_array
bulk_request_file="/mnt/scripts/data/bulk_request_es7.json"
generate_bulk_request "composite_type_array" "_doc" "item_" "$array_data_file" "$bulk_request_file"
curl -X POST "http://${ES_7_HOST}:9200/_bulk" --data-binary "@$bulk_request_file" -H "Content-Type: application/json"
# put _meta for composite_type_array
curl "http://${ES_7_HOST}:9200/composite_type_array/_mapping" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/array_meta_composite_type_array.json"

# es8
# create index test1
curl "http://${ES_8_HOST}:9200/test1" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/es7_test1.json"
# create index test2_20220808
curl "http://${ES_8_HOST}:9200/test2_20220808" -H "Content-Type:application/json" -X PUT -d '@/mnt/scripts/index/es7_test2.json'
# create index test2_20220809
curl "http://${ES_8_HOST}:9200/test2_20220809" -H "Content-Type:application/json" -X PUT -d '@/mnt/scripts/index/es7_test2.json'
# create index test3_20231005
curl "http://${ES_8_HOST}:9200/test3_20231005" -H "Content-Type:application/json" -X PUT -d '@/mnt/scripts/index/es7_test3.json'

# put data for tese1
curl "http://${ES_8_HOST}:9200/test1/_doc/1" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data1.json'
curl "http://${ES_8_HOST}:9200/test1/_doc/2" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data2.json'
curl "http://${ES_8_HOST}:9200/test1/_doc/3" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data3.json'
curl "http://${ES_8_HOST}:9200/test1/_doc/4" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data4.json'
curl "http://${ES_8_HOST}:9200/test1/_doc/5" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data5.json'
# put data for test2_20220808
curl "http://${ES_8_HOST}:9200/test2_20220808/_doc/1" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data1.json'
curl "http://${ES_8_HOST}:9200/test2_20220808/_doc/2" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data2.json'
curl "http://${ES_8_HOST}:9200/test2_20220808/_doc/3" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data3.json'
curl "http://${ES_8_HOST}:9200/test2_20220808/_doc/4" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data4.json'
# put data for test2_20220809
curl "http://${ES_8_HOST}:9200/test2_20220809/_doc/1" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data1.json'
curl "http://${ES_8_HOST}:9200/test2_20220809/_doc/2" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data2.json'
curl "http://${ES_8_HOST}:9200/test2_20220809/_doc/3" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data3.json'
curl "http://${ES_8_HOST}:9200/test2_20220809/_doc/4" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data4.json'
# put data for test3_20231005
curl "http://${ES_8_HOST}:9200/test3_20231005/_doc/1" -H "Content-Type:application/json" -X POST -d '@/mnt/scripts/data/data6.json'

# put _meta for array
curl "http://${ES_8_HOST}:9200/test1/_mapping" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/array_meta.json"
curl "http://${ES_8_HOST}:9200/test2_20220808/_mapping" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/array_meta.json"
curl "http://${ES_8_HOST}:9200/test2_20220809/_mapping" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/array_meta.json"

# create index composite_type_array
curl "http://${ES_8_HOST}:9200/composite_type_array" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/es7_composite_type_array.json"
# put data with bulk for composite_type_array
bulk_request_file="/mnt/scripts/data/bulk_request_es8.json"
generate_bulk_request "composite_type_array" "" "item_" "$array_data_file" "$bulk_request_file"
curl -X POST "http://${ES_8_HOST}:9200/_bulk" --data-binary "@$bulk_request_file" -H "Content-Type: application/json"
# put _meta for composite_type_array
curl "http://${ES_8_HOST}:9200/composite_type_array/_mapping" -H "Content-Type:application/json" -X PUT -d "@/mnt/scripts/index/array_meta_composite_type_array.json"