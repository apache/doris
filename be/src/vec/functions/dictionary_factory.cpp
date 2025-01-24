// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "vec/functions/dictionary_factory.h"

#include <gen_cpp/DataSinks_types.h>

#include <memory>

#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/functions/hash_map_dictionary.h"
#include "vec/functions/ip_address_dictionary.h"
namespace doris::vectorized {

template <typename DataType>
ColumnWithTypeAndName create_with_type(const std::string& name) {
    return ColumnWithTypeAndName {DataType::ColumnType::create(), std::make_shared<DataType>(),
                                  name};
}

DictionaryFactory::DictionaryFactory()
        : _mem_tracker(MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::GLOBAL,
                                                        "GLOBAL_DICT_FACTORY")) {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_mem_tracker);
    register_dict_when_load_be();
}

DictionaryFactory::~DictionaryFactory() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_mem_tracker);
    _dict_map.clear();
}

void DictionaryFactory::register_dict_when_load_be() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_mem_tracker);
    {
        std::string name = "dict_key_int64";

        auto key_column = ColumnInt64::create();
        auto key_data = std::make_shared<DataTypeInt64>();
        key_column->insert_value(2);
        key_column->insert_value(3);
        key_column->insert_value(5);
        key_column->insert_value(7);

        auto f32_column = DataTypeFloat32::ColumnType::create();
        auto f32_type = std::make_shared<DataTypeFloat32>();
        f32_column->insert_value(3.14);
        f32_column->insert_value(0.14);
        f32_column->insert_value(0.001);
        f32_column->insert_value(11110.001);

        auto i64_column = DataTypeInt64::ColumnType::create();
        auto i64_type = std::make_shared<DataTypeInt64>();
        i64_column->insert_value(114514);
        i64_column->insert_value(1919810);
        i64_column->insert_value(123456);
        i64_column->insert_value(654321);

        auto str_column = DataTypeString::ColumnType::create();
        auto str_type = std::make_shared<DataTypeString>();
        str_column->insert_value("hello world");
        str_column->insert_value("dict");
        str_column->insert_value("????");
        str_column->insert_value("doris");

        auto dict = create_hash_map_dict_from_column(
                name, ColumnWithTypeAndName {std::move(key_column), key_data, ""},
                ColumnsWithTypeAndName {
                        ColumnWithTypeAndName {std::move(f32_column), f32_type, "f32"},
                        ColumnWithTypeAndName {std::move(i64_column), i64_type, "i64"},
                        ColumnWithTypeAndName {std::move(str_column), str_type, "str"}});

        register_dict(dict);
    }

    {
        std::string name = "dict_key_string";

        auto key_column = DataTypeString::ColumnType::create();
        auto key_data = std::make_shared<DataTypeString>();
        key_column->insert_value("abc");
        key_column->insert_value("def");
        key_column->insert_value("some null");

        auto f32_column = DataTypeFloat32::ColumnType::create();
        auto f32_type = std::make_shared<DataTypeFloat32>();
        f32_column->insert_value(3.14);
        f32_column->insert_value(0.14);
        f32_column->insert_value(0.001);

        auto i64_column = DataTypeInt64::ColumnType::create();
        auto i64_type = std::make_shared<DataTypeInt64>();
        i64_column->insert_value(114514);
        i64_column->insert_value(1919810);
        i64_column->insert_value(123456);

        auto str_column = DataTypeString::ColumnType::create();
        auto str_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        auto null_map = ColumnUInt8::create();
        str_column->insert_value("hello world");
        null_map->insert_value(0);
        str_column->insert_value("dict");
        null_map->insert_value(0);
        str_column->insert_value("????");
        null_map->insert_value(1);

        auto dict = create_hash_map_dict_from_column(
                name, ColumnWithTypeAndName {std::move(key_column), key_data, ""},
                ColumnsWithTypeAndName {
                        ColumnWithTypeAndName {std::move(f32_column), f32_type, "f32"},
                        ColumnWithTypeAndName {std::move(i64_column), i64_type, "i64"},
                        ColumnWithTypeAndName {
                                ColumnNullable::create(std::move(str_column), std::move(null_map)),
                                str_type, "str"}});

        register_dict(dict);
    }

    {
        std::vector<std::string> ips = {"192.168.0.0/16",     "192.168.1.0/24",  "192.168.1.128/25",
                                        "1:288:2080::/41",    "10.0.0.0/8",      "10.1.0.0/16",
                                        "172.16.0.0/12",      "172.16.1.0/24",   "172.16.1.128/25",
                                        "203.0.113.0/24",     "198.51.100.0/24", "2001:db8::/32",
                                        "2001:db8:abcd::/48", "fc00::/7",        "fe80::/10"};

        std::vector<std::string> names = {"A", "B", "C", "D", "E", "F", "G", "H",
                                          "I", "J", "K", "L", "M", "N", "O"};

        auto key_column = DataTypeString::ColumnType::create();
        auto key_data = std::make_shared<DataTypeString>();

        auto str_column1 = DataTypeString::ColumnType::create();
        auto str_type1 = std::make_shared<DataTypeString>();

        auto str_column2 = DataTypeString::ColumnType::create();
        auto str_type2 = std::make_shared<DataTypeString>();

        auto toLowerCase = [](const std::string& input) {
            std::string result = input;
            std::transform(result.begin(), result.end(), result.begin(),
                           [](unsigned char c) { return std::tolower(c); });
            return result;
        };

        for (int i = 0; i < ips.size(); i++) {
            key_column->insert_value(ips[i]);
            str_column1->insert_value(names[i]);
            str_column2->insert_value(toLowerCase(names[i]));
        }

        std::string name = "ip_map";
        auto dict = create_ip_trie_dict_from_column(
                name, ColumnWithTypeAndName {std::move(key_column), key_data, ""},
                ColumnsWithTypeAndName {
                        ColumnWithTypeAndName {std::move(str_column1), str_type1, "ATT"},
                        ColumnWithTypeAndName {std::move(str_column2), str_type2, "att"},
                });

        register_dict(dict);
    }
}

void test_sink(const TDictSink& dict_sink, Block* block) {
    auto key_data = block->get_by_position(dict_sink.key_column_id);

    ColumnsWithTypeAndName attribute_data;
    for (int64_t i = 0; i < dict_sink.attribute_column_ids.size(); i++) {
        auto att_col_id = dict_sink.attribute_column_ids[i];
        auto att_name = dict_sink.attribute_name[i];
        auto att_data = block->get_by_position(att_col_id);
        att_data.name = att_name;
        attribute_data.push_back(att_data);
    }

    auto dict =
            create_ip_trie_dict_from_column(dict_sink.dictionary_name, key_data, attribute_data);
}

} // namespace doris::vectorized