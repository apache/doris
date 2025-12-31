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

#include <cctz/time_zone.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/object_pool.h"
#include "exec/olap_common.h"
#include "io/fs/file_meta_cache.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/timezone_utils.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_struct.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/exec/format/parquet/vparquet_reader.h"
#include "vec/exec/format/table/hive_reader.h"

namespace doris::vectorized {

// Define the column access path configuration structure
struct ColumnAccessPathConfig {
    std::string column_name;
    std::vector<std::vector<std::string>> all_column_paths; // For all_column_access_paths
    std::vector<std::vector<std::string>> predicate_paths;  // For predicate_column_access_paths
};

// ORC column IDs are assigned in a tree-increment order, root is 0 and children increment sequentially.
// 0: struct (table/root)
//   1: id (bigint)
//   2: name (string)
//   3: profile (struct)
//     4: address (struct)
//       5: street (string)
//       6: city (string)
//       7: coordinates (struct)
//         8: lat (double)
//         9: lng (double)
//     10: contact (struct)
//       11: email (string)
//       12: phone (struct)
//         13: country_code (string)
//         14: number (string)
//     15: hobbies (array)
//       16: element (struct)
//         17: name (string)
//         18: level (int)
//   19: tags (array)
//     20: element (string)
//   21: friends (array)
//     22: element (struct)
//       23: user_id (bigint)
//       24: nickname (string)
//       25: friendship_level (int)
//   26: recent_activity (array)
//     27: element (struct)
//       28: action (string)
//       29: details (array)
//         30: element (struct)
//           31: key (string)
//           32: value (string)
//   33: attributes (map)
//     34: key (string)
//     35: value (string)
//   36: complex_attributes (map)
//     37: key (string)
//     38: value (struct)
//       39: metadata (struct)
//         40: version (string)
//         41: created_time (timestamp)
//         42: last_updated (timestamp)
//         43: quality_score (double)
//       44: historical_scores (array)
//         45: element (struct)
//           46: period (string)
//           47: score (double)
//           48: components (array)
//             49: element (struct)
//               50: component_name (string)
//               51: weight (float)
//               52: sub_scores (map)
//                 53: key (string)
//                 54: value (double)
//           55: trends (struct)
//             56: direction (string)
//             57: magnitude (double)
//             58: confidence_interval (struct)
//               59: lower (double)
//               60: upper (double)
//       61: hierarchical_data (map)
//         62: key (string)
//         63: value (struct)
//           64: category (string)
//           65: sub_items (array)
//             66: element (struct)
//               67: item_id (bigint)
//               68: properties (map)
//                 69: key (string)
//                 70: value (string)
//               71: metrics (struct)
//                 72: value (double)
//                 73: unit (string)
//           74: summary (struct)
//             75: total_count (int)
//             76: average_value (double)
//       77: validation_rules (struct)
//         78: required (boolean)
//         79: constraints (array)
//           80: element (struct)
//             81: rule_type (string)
//             82: parameters (map)
//               83: key (string)
//               84: value (string)
//             85: error_message (string)
//         86: complex_constraint (struct)
//           87: logic_operator (string)
//           88: operands (array)
//             89: element (struct)
//               90: field_path (string)
//               91: operator (string)
//               92: comparison_value (string)

class HiveReaderCreateColumnIdsTest : public ::testing::Test {
protected:
    void SetUp() override {
        cache = std::make_unique<doris::FileMetaCache>(1024);

        // Setup timezone
        doris::TimezoneUtils::find_cctz_time_zone(doris::TimezoneUtils::default_time_zone,
                                                  timezone_obj);
    }

    void TearDown() override { cache.reset(); }

    // Helper function to create tuple descriptor
    const TupleDescriptor* create_tuple_descriptor(
            DescriptorTbl** desc_tbl, ObjectPool& obj_pool, TDescriptorTable& t_desc_table,
            TTableDescriptor& t_table_desc, const std::vector<std::string>& column_names,
            const std::vector<int>& column_positions,
            const std::vector<TPrimitiveType::type>& column_types,
            const std::vector<ColumnAccessPathConfig>& access_configs = {}) {
        // Create table descriptor with complex schema
        auto create_table_desc = [this, &access_configs](
                                         TDescriptorTable& t_desc_table,
                                         TTableDescriptor& t_table_desc,
                                         const std::vector<std::string>& table_column_names,
                                         const std::vector<int>& table_column_positions,
                                         const std::vector<TPrimitiveType::type>& types) {
            t_table_desc.__set_id(0);
            t_table_desc.__set_tableType(TTableType::HIVE_TABLE);
            t_table_desc.__set_numCols(0);
            t_table_desc.__set_numClusteringCols(0);
            t_desc_table.tableDescriptors.push_back(t_table_desc);
            t_desc_table.__isset.tableDescriptors = true;
            for (int i = 0; i < table_column_names.size(); i++) {
                TSlotDescriptor tslot_desc;
                TTypeDesc type;
                if (table_column_names[i] == "id") {
                    // id: bigint
                    TTypeNode node;
                    node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType scalar_type;
                    scalar_type.__set_type(TPrimitiveType::BIGINT);
                    node.__set_scalar_type(scalar_type);
                    type.types.push_back(node);
                    tslot_desc.__set_slotType(type);
                } else if (table_column_names[i] == "name") {
                    // name: string
                    TTypeNode node;
                    node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType scalar_type;
                    scalar_type.__set_type(TPrimitiveType::STRING);
                    node.__set_scalar_type(scalar_type);
                    type.types.push_back(node);
                    tslot_desc.__set_slotType(type);
                } else if (table_column_names[i] == "tags") {
                    // tags: array<string>
                    TTypeNode array_node;
                    array_node.__set_type(TTypeNodeType::ARRAY);
                    array_node.__set_contains_nulls({true});
                    type.types.push_back(array_node);
                    TTypeNode element_node;
                    element_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType element_scalar;
                    element_scalar.__set_type(TPrimitiveType::STRING);
                    element_node.__set_scalar_type(element_scalar);
                    type.types.push_back(element_node);
                    tslot_desc.__set_slotType(type);
                } else if (table_column_names[i] == "friends") {
                    // friends: array<struct<user_id:bigint, nickname:string, friendship_level:int>>
                    TTypeNode array_node;
                    array_node.__set_type(TTypeNodeType::ARRAY);
                    array_node.__set_contains_nulls({true});
                    type.types.push_back(array_node);
                    TTypeNode element_struct_node;
                    element_struct_node.__set_type(TTypeNodeType::STRUCT);
                    std::vector<TStructField> element_fields;
                    TStructField user_id_field;
                    user_id_field.__set_name("user_id");
                    user_id_field.__set_contains_null(true);
                    element_fields.push_back(user_id_field);
                    TStructField nickname_field;
                    nickname_field.__set_name("nickname");
                    nickname_field.__set_contains_null(true);
                    element_fields.push_back(nickname_field);
                    TStructField friendship_level_field;
                    friendship_level_field.__set_name("friendship_level");
                    friendship_level_field.__set_contains_null(true);
                    element_fields.push_back(friendship_level_field);
                    element_struct_node.__set_struct_fields(element_fields);
                    type.types.push_back(element_struct_node);
                    // user_id: bigint
                    TTypeNode user_id_node;
                    user_id_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType user_id_scalar;
                    user_id_scalar.__set_type(TPrimitiveType::BIGINT);
                    user_id_node.__set_scalar_type(user_id_scalar);
                    type.types.push_back(user_id_node);
                    // nickname: string
                    TTypeNode nickname_node;
                    nickname_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType nickname_scalar;
                    nickname_scalar.__set_type(TPrimitiveType::STRING);
                    nickname_node.__set_scalar_type(nickname_scalar);
                    type.types.push_back(nickname_node);
                    // friendship_level: int
                    TTypeNode friendship_level_node;
                    friendship_level_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType friendship_level_scalar;
                    friendship_level_scalar.__set_type(TPrimitiveType::INT);
                    friendship_level_node.__set_scalar_type(friendship_level_scalar);
                    type.types.push_back(friendship_level_node);
                    tslot_desc.__set_slotType(type);
                } else if (table_column_names[i] == "recent_activity") {
                    // recent_activity: array<struct<action:string, details:array<struct<key:string, value:string>>>>
                    TTypeNode array_node;
                    array_node.__set_type(TTypeNodeType::ARRAY);
                    array_node.__set_contains_nulls({true});
                    type.types.push_back(array_node);
                    TTypeNode element_struct_node;
                    element_struct_node.__set_type(TTypeNodeType::STRUCT);
                    std::vector<TStructField> element_fields;
                    TStructField action_field;
                    action_field.__set_name("action");
                    action_field.__set_contains_null(true);
                    element_fields.push_back(action_field);
                    TStructField details_field;
                    details_field.__set_name("details");
                    details_field.__set_contains_null(true);
                    element_fields.push_back(details_field);
                    element_struct_node.__set_struct_fields(element_fields);
                    type.types.push_back(element_struct_node);
                    // action: string
                    TTypeNode action_node;
                    action_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType action_scalar;
                    action_scalar.__set_type(TPrimitiveType::STRING);
                    action_node.__set_scalar_type(action_scalar);
                    type.types.push_back(action_node);
                    // details: array<struct<key:string, value:string>>
                    TTypeNode details_array_node;
                    details_array_node.__set_type(TTypeNodeType::ARRAY);
                    details_array_node.__set_contains_nulls({true});
                    type.types.push_back(details_array_node);
                    TTypeNode details_element_struct_node;
                    details_element_struct_node.__set_type(TTypeNodeType::STRUCT);
                    std::vector<TStructField> details_element_fields;
                    TStructField key_field;
                    key_field.__set_name("key");
                    key_field.__set_contains_null(true);
                    details_element_fields.push_back(key_field);
                    TStructField value_field;
                    value_field.__set_name("value");
                    value_field.__set_contains_null(true);
                    details_element_fields.push_back(value_field);
                    details_element_struct_node.__set_struct_fields(details_element_fields);
                    type.types.push_back(details_element_struct_node);
                    // key: string
                    TTypeNode key_node;
                    key_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType key_scalar;
                    key_scalar.__set_type(TPrimitiveType::STRING);
                    key_node.__set_scalar_type(key_scalar);
                    type.types.push_back(key_node);
                    // value: string
                    TTypeNode value_node;
                    value_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType value_scalar;
                    value_scalar.__set_type(TPrimitiveType::STRING);
                    value_node.__set_scalar_type(value_scalar);
                    type.types.push_back(value_node);
                    tslot_desc.__set_slotType(type);
                } else if (table_column_names[i] == "attributes") {
                    // attributes: map<string, string>
                    TTypeNode map_node;
                    map_node.__set_type(TTypeNodeType::MAP);
                    map_node.__set_contains_nulls({true, true});
                    type.types.push_back(map_node);
                    // key: string
                    TTypeNode key_node;
                    key_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType key_scalar;
                    key_scalar.__set_type(TPrimitiveType::STRING);
                    key_node.__set_scalar_type(key_scalar);
                    type.types.push_back(key_node);
                    // value: string
                    TTypeNode value_node;
                    value_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType value_scalar;
                    value_scalar.__set_type(TPrimitiveType::STRING);
                    value_node.__set_scalar_type(value_scalar);
                    type.types.push_back(value_node);
                    tslot_desc.__set_slotType(type);
                } else if (table_column_names[i] == "complex_attributes") {
                    // complex_attributes: map<string, struct<metadata:struct<version:string, created_time:timestamp, last_updated:timestamp>>>
                    TTypeNode map_node;
                    map_node.__set_type(TTypeNodeType::MAP);
                    map_node.__set_contains_nulls({true, true});
                    type.types.push_back(map_node);
                    // key: string
                    TTypeNode key_node;
                    key_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType key_scalar;
                    key_scalar.__set_type(TPrimitiveType::STRING);
                    key_node.__set_scalar_type(key_scalar);
                    type.types.push_back(key_node);
                    // value: struct<metadata:struct<version:string, created_time:timestamp, last_updated:timestamp>>
                    TTypeNode value_struct_node;
                    value_struct_node.__set_type(TTypeNodeType::STRUCT);
                    std::vector<TStructField> value_fields;
                    TStructField metadata_field;
                    metadata_field.__set_name("metadata");
                    metadata_field.__set_contains_null(true);
                    value_fields.push_back(metadata_field);
                    value_struct_node.__set_struct_fields(value_fields);
                    type.types.push_back(value_struct_node);
                    // metadata: struct<version:string, created_time:timestamp, last_updated:timestamp>
                    TTypeNode metadata_struct_node;
                    metadata_struct_node.__set_type(TTypeNodeType::STRUCT);
                    std::vector<TStructField> metadata_fields;
                    TStructField version_field;
                    version_field.__set_name("version");
                    version_field.__set_contains_null(true);
                    metadata_fields.push_back(version_field);
                    TStructField created_time_field;
                    created_time_field.__set_name("created_time");
                    created_time_field.__set_contains_null(true);
                    metadata_fields.push_back(created_time_field);
                    TStructField last_updated_field;
                    last_updated_field.__set_name("last_updated");
                    last_updated_field.__set_contains_null(true);
                    metadata_fields.push_back(last_updated_field);
                    metadata_struct_node.__set_struct_fields(metadata_fields);
                    type.types.push_back(metadata_struct_node);
                    // version: string
                    TTypeNode version_node;
                    version_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType version_scalar;
                    version_scalar.__set_type(TPrimitiveType::STRING);
                    version_node.__set_scalar_type(version_scalar);
                    type.types.push_back(version_node);
                    // created_time: timestamp
                    TTypeNode created_time_node;
                    created_time_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType created_time_scalar;
                    created_time_scalar.__set_type(TPrimitiveType::DATETIME);
                    created_time_node.__set_scalar_type(created_time_scalar);
                    type.types.push_back(created_time_node);
                    // last_updated: timestamp
                    TTypeNode last_updated_node;
                    last_updated_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType last_updated_scalar;
                    last_updated_scalar.__set_type(TPrimitiveType::DATETIME);
                    last_updated_node.__set_scalar_type(last_updated_scalar);
                    type.types.push_back(last_updated_node);
                    tslot_desc.__set_slotType(type);
                } else if (table_column_names[i] == "profile") {
                    // STRUCT/ARRAY nodes need contains_nulls, SCALAR nodes do not
                    TTypeNode struct_node;
                    struct_node.__set_type(TTypeNodeType::STRUCT);
                    std::vector<TStructField> struct_fields;
                    TStructField address_field;
                    address_field.__set_name("address");
                    address_field.__set_contains_null(true);
                    struct_fields.push_back(address_field);
                    TStructField contact_field;
                    contact_field.__set_name("contact");
                    contact_field.__set_contains_null(true);
                    struct_fields.push_back(contact_field);
                    TStructField hobbies_field;
                    hobbies_field.__set_name("hobbies");
                    hobbies_field.__set_contains_null(true);
                    struct_fields.push_back(hobbies_field);
                    struct_node.__set_struct_fields(struct_fields);
                    type.types.push_back(struct_node);
                    TTypeNode address_node;
                    address_node.__set_type(TTypeNodeType::STRUCT);
                    std::vector<TStructField> address_fields;
                    TStructField street_field;
                    street_field.__set_name("street");
                    street_field.__set_contains_null(true);
                    address_fields.push_back(street_field);
                    TStructField city_field;
                    city_field.__set_name("city");
                    city_field.__set_contains_null(true);
                    address_fields.push_back(city_field);
                    TStructField coordinates_field;
                    coordinates_field.__set_name("coordinates");
                    coordinates_field.__set_contains_null(true);
                    address_fields.push_back(coordinates_field);
                    address_node.__set_struct_fields(address_fields);
                    type.types.push_back(address_node);
                    TTypeNode street_node;
                    street_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType street_scalar;
                    street_scalar.__set_type(TPrimitiveType::STRING);
                    street_node.__set_scalar_type(street_scalar);
                    type.types.push_back(street_node);
                    TTypeNode city_node;
                    city_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType city_scalar;
                    city_scalar.__set_type(TPrimitiveType::STRING);
                    city_node.__set_scalar_type(city_scalar);
                    type.types.push_back(city_node);
                    TTypeNode coordinates_node;
                    coordinates_node.__set_type(TTypeNodeType::STRUCT);
                    std::vector<TStructField> coordinates_fields;
                    TStructField lat_field;
                    lat_field.__set_name("lat");
                    lat_field.__set_contains_null(true);
                    coordinates_fields.push_back(lat_field);
                    TStructField lng_field;
                    lng_field.__set_name("lng");
                    lng_field.__set_contains_null(true);
                    coordinates_fields.push_back(lng_field);
                    coordinates_node.__set_struct_fields(coordinates_fields);
                    type.types.push_back(coordinates_node);
                    TTypeNode lat_node;
                    lat_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType lat_scalar;
                    lat_scalar.__set_type(TPrimitiveType::DOUBLE);
                    lat_node.__set_scalar_type(lat_scalar);
                    type.types.push_back(lat_node);
                    TTypeNode lng_node;
                    lng_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType lng_scalar;
                    lng_scalar.__set_type(TPrimitiveType::DOUBLE);
                    lng_node.__set_scalar_type(lng_scalar);
                    type.types.push_back(lng_node);
                    TTypeNode contact_node;
                    contact_node.__set_type(TTypeNodeType::STRUCT);
                    std::vector<TStructField> contact_fields;
                    TStructField email_field;
                    email_field.__set_name("email");
                    email_field.__set_contains_null(true);
                    contact_fields.push_back(email_field);
                    TStructField phone_field;
                    phone_field.__set_name("phone");
                    phone_field.__set_contains_null(true);
                    contact_fields.push_back(phone_field);
                    contact_node.__set_struct_fields(contact_fields);
                    type.types.push_back(contact_node);
                    TTypeNode email_node;
                    email_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType email_scalar;
                    email_scalar.__set_type(TPrimitiveType::STRING);
                    email_node.__set_scalar_type(email_scalar);
                    type.types.push_back(email_node);
                    TTypeNode phone_node;
                    phone_node.__set_type(TTypeNodeType::STRUCT);
                    std::vector<TStructField> phone_fields;
                    TStructField country_code_field;
                    country_code_field.__set_name("country_code");
                    country_code_field.__set_contains_null(true);
                    phone_fields.push_back(country_code_field);
                    TStructField number_field;
                    number_field.__set_name("number");
                    number_field.__set_contains_null(true);
                    phone_fields.push_back(number_field);
                    phone_node.__set_struct_fields(phone_fields);
                    type.types.push_back(phone_node);
                    TTypeNode country_code_node;
                    country_code_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType country_code_scalar;
                    country_code_scalar.__set_type(TPrimitiveType::STRING);
                    country_code_node.__set_scalar_type(country_code_scalar);
                    type.types.push_back(country_code_node);
                    TTypeNode number_node;
                    number_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType number_scalar;
                    number_scalar.__set_type(TPrimitiveType::STRING);
                    number_node.__set_scalar_type(number_scalar);
                    type.types.push_back(number_node);
                    TTypeNode hobbies_node;
                    hobbies_node.__set_type(TTypeNodeType::ARRAY);
                    hobbies_node.__set_contains_nulls({true});
                    type.types.push_back(hobbies_node);
                    TTypeNode hobby_element_node;
                    hobby_element_node.__set_type(TTypeNodeType::STRUCT);
                    std::vector<TStructField> hobby_element_fields;
                    TStructField hobby_name_field;
                    hobby_name_field.__set_name("name");
                    hobby_name_field.__set_contains_null(true);
                    hobby_element_fields.push_back(hobby_name_field);
                    TStructField hobby_level_field;
                    hobby_level_field.__set_name("level");
                    hobby_level_field.__set_contains_null(true);
                    hobby_element_fields.push_back(hobby_level_field);
                    hobby_element_node.__set_struct_fields(hobby_element_fields);
                    type.types.push_back(hobby_element_node);
                    TTypeNode hobby_name_node;
                    hobby_name_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType hobby_name_scalar;
                    hobby_name_scalar.__set_type(TPrimitiveType::STRING);
                    hobby_name_node.__set_scalar_type(hobby_name_scalar);
                    type.types.push_back(hobby_name_node);
                    TTypeNode hobby_level_node;
                    hobby_level_node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType hobby_level_scalar;
                    hobby_level_scalar.__set_type(TPrimitiveType::INT);
                    hobby_level_node.__set_scalar_type(hobby_level_scalar);
                    type.types.push_back(hobby_level_node);
                    tslot_desc.__set_slotType(type);
                } else {
                    // Regular types
                    TTypeNode node;
                    node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType scalar_type;
                    scalar_type.__set_type(types[i]);
                    node.__set_scalar_type(scalar_type);
                    type.types.push_back(node);
                    tslot_desc.__set_slotType(type);
                }
                tslot_desc.__set_id(i);
                tslot_desc.__set_parent(0);
                tslot_desc.__set_colName(table_column_names[i]);
                tslot_desc.__set_columnPos(table_column_positions[i]);
                tslot_desc.__set_byteOffset(0);
                tslot_desc.__set_nullIndicatorByte(0);
                tslot_desc.__set_nullIndicatorBit(-1);
                tslot_desc.__set_slotIdx(0);
                tslot_desc.__set_isMaterialized(true);

                // Use configuration to set column_access_paths
                for (const auto& config : access_configs) {
                    set_column_access_paths(tslot_desc, config);
                }

                t_desc_table.slotDescriptors.push_back(tslot_desc);
            }
            t_desc_table.__isset.slotDescriptors = true;
            TTupleDescriptor t_tuple_desc;
            t_tuple_desc.__set_id(0);
            t_tuple_desc.__set_byteSize(16);
            t_tuple_desc.__set_numNullBytes(0);
            t_tuple_desc.__set_tableId(0);
            t_tuple_desc.__isset.tableId = true;
            t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
        };

        create_table_desc(t_desc_table, t_table_desc, column_names, column_positions, column_types);
        EXPECT_TRUE(DescriptorTbl::create(&obj_pool, t_desc_table, desc_tbl).ok());
        return (*desc_tbl)->get_tuple_descriptor(0);
    }

    // Helper function: Set column access paths
    void set_column_access_paths(TSlotDescriptor& tslot_desc,
                                 const ColumnAccessPathConfig& config) {
        if (config.column_name != tslot_desc.colName) {
            return; // Not the target column, skip
        }

        // Set all_column_access_paths
        if (!config.all_column_paths.empty()) {
            std::vector<TColumnAccessPath> access_paths;
            for (const auto& path_vector : config.all_column_paths) {
                TColumnAccessPath access_path;
                access_path.__set_type(doris::TAccessPathType::DATA);
                TDataAccessPath data_path;
                data_path.__set_path(path_vector);
                access_path.__set_data_access_path(data_path);
                access_paths.push_back(access_path);
            }
            tslot_desc.__set_all_access_paths(access_paths);
        }

        // Set predicate_column_access_paths
        if (!config.predicate_paths.empty()) {
            std::vector<TColumnAccessPath> access_paths;
            for (const auto& path_vector : config.predicate_paths) {
                TColumnAccessPath access_path;
                access_path.__set_type(doris::TAccessPathType::DATA);
                TDataAccessPath data_path;
                data_path.__set_path(path_vector);
                access_path.__set_data_access_path(data_path);
                access_paths.push_back(access_path);
            }
            tslot_desc.__set_predicate_access_paths(access_paths);
        }
    }

    std::unique_ptr<doris::FileMetaCache> cache;
    cctz::time_zone timezone_obj;

    // Helper function: Create and setup ParquetReader
    std::tuple<std::unique_ptr<HiveParquetReader>, const FieldDescriptor*> create_parquet_reader(
            const std::string& test_file) {
        auto local_fs = io::global_local_filesystem();
        io::FileReaderSPtr file_reader;
        auto st = local_fs->open_file(test_file, &file_reader);
        if (!st.ok()) {
            return {nullptr, nullptr};
        }

        RuntimeState runtime_state = RuntimeState(TQueryOptions(), TQueryGlobals());
        TFileScanRangeParams scan_params;
        scan_params.format_type = TFileFormatType::FORMAT_PARQUET;
        TFileRangeDesc scan_range;
        scan_range.start_offset = 0;
        scan_range.size = file_reader->size();
        scan_range.path = test_file;
        RuntimeProfile profile("test_profile");

        cctz::time_zone ctz;
        TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
        auto generic_reader =
                ParquetReader::create_unique(&profile, scan_params, scan_range, 1024, &ctz, nullptr,
                                             &runtime_state, cache.get());
        if (!generic_reader) {
            return {nullptr, nullptr};
        }

        auto parquet_reader = static_cast<ParquetReader*>(generic_reader.get());
        parquet_reader->set_file_reader(file_reader);

        const FieldDescriptor* field_desc = nullptr;
        st = parquet_reader->get_file_metadata_schema(&field_desc);
        if (!st.ok() || !field_desc) {
            return {nullptr, nullptr};
        }

        auto hive_reader = std::make_unique<HiveParquetReader>(
                std::move(generic_reader), &profile, &runtime_state, scan_params, scan_range,
                nullptr, nullptr, cache.get());

        return {std::move(hive_reader), field_desc};
    }

    // Helper function: Create and setup OrcReader
    std::tuple<std::unique_ptr<HiveOrcReader>, const orc::Type*> create_orc_reader(
            const std::string& test_file) {
        // Open the Hive Orc test file
        auto local_fs = io::global_local_filesystem();
        io::FileReaderSPtr file_reader;
        auto st = local_fs->open_file(test_file, &file_reader);
        if (!st.ok()) {
            return {nullptr, nullptr};
        }

        // Setup runtime state
        RuntimeState runtime_state = RuntimeState(TQueryOptions(), TQueryGlobals());

        // Setup scan parameters
        TFileScanRangeParams scan_params;
        scan_params.format_type = TFileFormatType::FORMAT_ORC;

        TFileRangeDesc scan_range;
        scan_range.start_offset = 0;
        scan_range.size = file_reader->size(); // Read entire file
        scan_range.path = test_file;

        // Create mock profile
        RuntimeProfile profile("test_profile");

        // Create OrcReader as the underlying file format reader
        cctz::time_zone ctz;
        TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);

        auto generic_reader =
                OrcReader::create_unique(&profile, &runtime_state, scan_params, scan_range, 1024,
                                         "CST", nullptr, cache.get());
        if (!generic_reader) {
            return {nullptr, nullptr};
        }

        auto orc_reader = static_cast<OrcReader*>(generic_reader.get());
        // Get FieldDescriptor from Orc file
        const orc::Type* orc_type_ptr = nullptr;
        st = orc_reader->get_file_type(&orc_type_ptr);
        if (!st.ok() || !orc_type_ptr) {
            return {nullptr, nullptr};
        }

        // Create HiveOrcReader
        auto hive_reader = std::make_unique<HiveOrcReader>(std::move(generic_reader), &profile,
                                                           &runtime_state, scan_params, scan_range,
                                                           nullptr, nullptr, cache.get());

        return {std::move(hive_reader), orc_type_ptr};
    }

    // Helper function: Run Parquet test with different column ID extraction methods
    void run_parquet_test(const std::vector<std::string>& table_column_names,
                          const std::vector<ColumnAccessPathConfig>& access_configs,
                          const std::set<uint64_t>& expected_column_ids,
                          const std::set<uint64_t>& expected_filter_column_ids,
                          bool use_top_level_method = false, bool should_skip_assertion = false) {
        std::string test_file =
                "./be/test/exec/test_data/nested_user_profiles_parquet/"
                "part-00000-64a7a390-1a03-4efc-ab51-557e9369a1f9-c000.snappy.parquet";

        auto [hive_reader, field_desc] = create_parquet_reader(test_file);
        if (!hive_reader || !field_desc) {
            GTEST_SKIP() << "Test file not found or failed to create reader: " << test_file;
            return;
        }

        // Create tuple descriptor based on full schema
        DescriptorTbl* desc_tbl;
        ObjectPool obj_pool;
        TDescriptorTable t_desc_table;
        TTableDescriptor t_table_desc;

        // Define all columns according to the schema
        std::vector<std::string> all_table_column_names = {"id",         "name",
                                                           "profile",    "tags",
                                                           "friends",    "recent_activity",
                                                           "attributes", "complex_attributes"};
        std::vector<int> all_table_column_positions = {0, 1, 2, 3, 4, 5, 6, 7};
        std::vector<TPrimitiveType::type> all_table_column_types = {
                TPrimitiveType::BIGINT, // id
                TPrimitiveType::STRING, // name
                TPrimitiveType::STRUCT, // profile
                TPrimitiveType::ARRAY,  // tags
                TPrimitiveType::ARRAY,  // friends
                TPrimitiveType::ARRAY,  // recent_activity
                TPrimitiveType::MAP,    // attributes
                TPrimitiveType::MAP     // complex_attributes
        };

        std::vector<int> table_column_positions;
        std::vector<TPrimitiveType::type> table_column_types;
        for (const auto& col_name : table_column_names) {
            auto it = std::find(all_table_column_names.begin(), all_table_column_names.end(),
                                col_name);
            if (it != all_table_column_names.end()) {
                int idx = std::distance(all_table_column_names.begin(), it);
                table_column_positions.push_back(idx);
                table_column_types.push_back(all_table_column_types[idx]);
            }
        }

        const TupleDescriptor* tuple_descriptor = create_tuple_descriptor(
                &desc_tbl, obj_pool, t_desc_table, t_table_desc, table_column_names,
                table_column_positions, table_column_types, access_configs);

        // Execute test based on method choice
        ColumnIdResult actual_result;
        if (use_top_level_method) {
            actual_result = HiveParquetReader::_create_column_ids_by_top_level_col_index(
                    field_desc, tuple_descriptor);
        } else {
            actual_result = HiveParquetReader::_create_column_ids(field_desc, tuple_descriptor);
        }

        if (!should_skip_assertion) {
            EXPECT_EQ(actual_result.column_ids, expected_column_ids);
            EXPECT_EQ(actual_result.filter_column_ids, expected_filter_column_ids);
        }
    }

    // Helper function: Run ORC test with different column ID extraction methods
    void run_orc_test(const std::vector<std::string>& table_column_names,
                      const std::vector<ColumnAccessPathConfig>& access_configs,
                      const std::set<uint64_t>& expected_column_ids,
                      const std::set<uint64_t>& expected_filter_column_ids,
                      bool use_top_level_method = false, bool should_skip_assertion = false) {
        std::string test_file =
                "./be/test/exec/test_data/nested_user_profiles_orc/"
                "part-00000-62614f23-05d1-4043-a533-b155ef52b720-c000.snappy.orc";

        auto [hive_reader, orc_type] = create_orc_reader(test_file);
        if (!hive_reader || !orc_type) {
            GTEST_SKIP() << "Test file not found or failed to create reader: " << test_file;
            return;
        }

        // Create tuple descriptor based on full schema
        DescriptorTbl* desc_tbl;
        ObjectPool obj_pool;
        TDescriptorTable t_desc_table;
        TTableDescriptor t_table_desc;

        // Define all columns according to the schema
        std::vector<std::string> all_table_column_names = {"id",         "name",
                                                           "profile",    "tags",
                                                           "friends",    "recent_activity",
                                                           "attributes", "complex_attributes"};
        std::vector<int> all_table_column_positions = {0, 1, 2, 3, 4, 5, 6, 7};
        std::vector<TPrimitiveType::type> all_table_column_types = {
                TPrimitiveType::BIGINT, // id
                TPrimitiveType::STRING, // name
                TPrimitiveType::STRUCT, // profile
                TPrimitiveType::ARRAY,  // tags
                TPrimitiveType::ARRAY,  // friends
                TPrimitiveType::ARRAY,  // recent_activity
                TPrimitiveType::MAP,    // attributes
                TPrimitiveType::MAP     // complex_attributes
        };

        std::vector<int> table_column_positions;
        std::vector<TPrimitiveType::type> table_column_types;
        for (const auto& col_name : table_column_names) {
            auto it = std::find(all_table_column_names.begin(), all_table_column_names.end(),
                                col_name);
            if (it != all_table_column_names.end()) {
                int idx = std::distance(all_table_column_names.begin(), it);
                table_column_positions.push_back(idx);
                table_column_types.push_back(all_table_column_types[idx]);
            }
        }

        const TupleDescriptor* tuple_descriptor = create_tuple_descriptor(
                &desc_tbl, obj_pool, t_desc_table, t_table_desc, table_column_names,
                table_column_positions, table_column_types, access_configs);

        // Execute test based on method choice
        ColumnIdResult actual_result;
        if (use_top_level_method) {
            actual_result = HiveOrcReader::_create_column_ids_by_top_level_col_index(
                    orc_type, tuple_descriptor);
        } else {
            actual_result = HiveOrcReader::_create_column_ids(orc_type, tuple_descriptor);
        }

        if (!should_skip_assertion) {
            EXPECT_EQ(actual_result.column_ids, expected_column_ids);
            EXPECT_EQ(actual_result.filter_column_ids, expected_filter_column_ids);
        }
    }
};

TEST_F(HiveReaderCreateColumnIdsTest, test_create_column_ids_1) {
    ColumnAccessPathConfig access_config;
    access_config.column_name = "profile";

    access_config.all_column_paths = {{"profile", "address", "coordinates", "lat"},
                                      {"profile", "address", "coordinates", "lng"},
                                      {"profile", "contact", "email"},
                                      {"profile", "hobbies", "*", "level"}};
    access_config.predicate_paths = {{"profile", "address", "coordinates", "lat"},
                                     {"profile", "contact", "email"}};

    // column_ids should contain all necessary column IDs (set automatically deduplicates)
    std::vector<std::string> table_column_names = {"name", "profile"};
    std::set<uint64_t> expected_column_ids = {2, 3, 4, 7, 8, 9, 10, 11, 15, 16, 18};
    std::set<uint64_t> expected_filter_column_ids = {3, 4, 7, 8, 10, 11};

    run_parquet_test(table_column_names, {access_config}, expected_column_ids,
                     expected_filter_column_ids);
    run_parquet_test(table_column_names, {access_config}, expected_column_ids,
                     expected_filter_column_ids, true);

    run_orc_test(table_column_names, {access_config}, expected_column_ids,
                 expected_filter_column_ids);
    run_orc_test(table_column_names, {access_config}, expected_column_ids,
                 expected_filter_column_ids, true);
}

TEST_F(HiveReaderCreateColumnIdsTest, test_create_column_ids_2) {
    // ORC column IDs are assigned in a tree-like incremental manner: the root node is 0, and child nodes increase sequentially.
    // Currently, Parquet uses a similar design.
    // 0: struct (table/root)
    //   1: id (int64)
    //   2: name (string)
    //   3: profile (struct)
    //     4: address (struct)
    //       5: street (string)
    //       6: city (string)
    //       7: coordinates (struct)
    //         8: lat (double)
    //         9: lng (double)
    //     10: contact (struct)
    //       11: email (string)
    //       12: phone (struct)
    //         13: country_code (string)
    //         14: number (string)
    //     15: hobbies (list/array)
    //         16: element (struct)
    //           17: name (string)
    //           18: level (int32)

    ColumnAccessPathConfig access_config;
    access_config.column_name = "profile";

    access_config.all_column_paths = {{"profile"}};
    access_config.predicate_paths = {{"profile", "address", "coordinates", "lat"},
                                     {"profile", "contact", "email"}};

    std::vector<std::string> table_column_names = {"name", "profile"};
    std::set<uint64_t> expected_column_ids = {2,  3,  4,  5,  6,  7,  8,  9, 10,
                                              11, 12, 13, 14, 15, 16, 17, 18};
    std::set<uint64_t> expected_filter_column_ids = {3, 4, 7, 8, 10, 11};

    run_parquet_test(table_column_names, {access_config}, expected_column_ids,
                     expected_filter_column_ids);
    run_parquet_test(table_column_names, {access_config}, expected_column_ids,
                     expected_filter_column_ids, true);

    run_orc_test(table_column_names, {access_config}, expected_column_ids,
                 expected_filter_column_ids);
    run_orc_test(table_column_names, {access_config}, expected_column_ids,
                 expected_filter_column_ids, true);
}

TEST_F(HiveReaderCreateColumnIdsTest, test_create_column_ids_3) {
    // ORC column IDs are assigned in a tree-like incremental manner: the root node is 0, and child nodes increase sequentially.
    // Currently, Parquet uses a similar design.
    // 0: struct (table/root)
    //   1: id (int64)
    //   2: name (string)
    //   3: profile (struct)
    //     4: address (struct)
    //       5: street (string)
    //       6: city (string)
    //       7: coordinates (struct)
    //         8: lat (double)
    //         9: lng (double)
    //     10: contact (struct)
    //       11: email (string)
    //       12: phone (struct)
    //         13: country_code (string)
    //         14: number (string)
    //     15: hobbies (list/array)
    //         16: element (struct)
    //           17: name (string)
    //           18: level (int32)

    ColumnAccessPathConfig access_config;
    access_config.column_name = "profile";

    access_config.all_column_paths = {{"profile", "contact"}, {"profile", "address"}};
    access_config.predicate_paths = {{"profile", "address", "coordinates"},
                                     {"profile", "contact", "email"}};

    std::vector<std::string> table_column_names = {"name", "profile"};
    std::set<uint64_t> expected_column_ids = {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14};
    std::set<uint64_t> expected_filter_column_ids = {3, 4, 7, 8, 9, 10, 11};

    run_parquet_test(table_column_names, {access_config}, expected_column_ids,
                     expected_filter_column_ids);
    run_parquet_test(table_column_names, {access_config}, expected_column_ids,
                     expected_filter_column_ids, true);

    run_orc_test(table_column_names, {access_config}, expected_column_ids,
                 expected_filter_column_ids);
    run_orc_test(table_column_names, {access_config}, expected_column_ids,
                 expected_filter_column_ids, true);
}

TEST_F(HiveReaderCreateColumnIdsTest, test_create_column_ids_4) {
    ColumnAccessPathConfig access_config;
    access_config.column_name = "profile";

    access_config.all_column_paths = {};
    access_config.predicate_paths = {};

    std::vector<std::string> table_column_names = {"name", "profile"};
    std::set<uint64_t> expected_column_ids = {2,  3,  4,  5,  6,  7,  8,  9, 10,
                                              11, 12, 13, 14, 15, 16, 17, 18};
    std::set<uint64_t> expected_filter_column_ids = {};

    run_parquet_test(table_column_names, {access_config}, expected_column_ids,
                     expected_filter_column_ids);
    run_parquet_test(table_column_names, {access_config}, expected_column_ids,
                     expected_filter_column_ids, true);

    run_orc_test(table_column_names, {access_config}, expected_column_ids,
                 expected_filter_column_ids);
    run_orc_test(table_column_names, {access_config}, expected_column_ids,
                 expected_filter_column_ids, true);
}

TEST_F(HiveReaderCreateColumnIdsTest, test_create_column_ids_5) {
    // ORC column IDs are assigned in a tree-like incremental manner: the root node is 0, and child nodes increase sequentially.
    // Currently, Parquet uses a similar design.
    //   19: tags (array)
    //     20: element (string)
    //   21: friends (array)
    //     22: element (struct)
    //       23: user_id (bigint)
    //       24: nickname (string)
    //       25: friendship_level (int)
    //   26: recent_activity (array)
    //     27: element (struct)
    //       28: action (string)
    //       29: details (array)
    //         30: element (struct)
    //           31: key (string)
    //           32: value (string)

    std::vector<ColumnAccessPathConfig> access_configs;

    {
        // Configure access paths for friends column
        ColumnAccessPathConfig access_config;
        access_config.column_name = "friends";

        access_config.all_column_paths = {{"friends", "*", "nickname"},
                                          {"friends", "*", "friendship_level"}};
        access_config.predicate_paths = {
                {"friends", "*", "nickname"},
        };
        access_configs.push_back(access_config);
    }

    {
        // Configure access paths for recent_activity column
        ColumnAccessPathConfig access_config;
        access_config.column_name = "recent_activity";

        access_config.all_column_paths = {{"recent_activity", "*", "action"},
                                          {"recent_activity", "*", "details", "*", "value"}};
        access_config.predicate_paths = {
                {"recent_activity", "*", "action"},
        };
        access_configs.push_back(access_config);
    }

    std::vector<std::string> table_column_names = {"name", "friends", "recent_activity"};
    std::set<uint64_t> expected_column_ids = {2, 21, 22, 24, 25, 26, 27, 28, 29, 30, 32};
    std::set<uint64_t> expected_filter_column_ids = {21, 22, 24, 26, 27, 28};

    run_parquet_test(table_column_names, access_configs, expected_column_ids,
                     expected_filter_column_ids);
    run_parquet_test(table_column_names, access_configs, expected_column_ids,
                     expected_filter_column_ids, true);

    run_orc_test(table_column_names, access_configs, expected_column_ids,
                 expected_filter_column_ids);
    run_orc_test(table_column_names, access_configs, expected_column_ids,
                 expected_filter_column_ids, true);
}

TEST_F(HiveReaderCreateColumnIdsTest, test_create_column_ids_6) {
    // ORC column IDs are assigned in a tree-like incremental manner: the root node is 0, and child nodes increase sequentially.
    // Currently, Parquet uses a similar design.
    //   33: attributes (map)
    //     34: key (string)
    //     35: value (string)
    //   36: complex_attributes (map)
    //     37: key (string)
    //     38: value (struct)
    //       39: metadata (struct)
    //         40: version (string)
    //         41: created_time (timestamp)
    //         42: last_updated (timestamp)
    //         43: quality_score (double)
    //       44: historical_scores (array)
    //         45: element (struct)
    //           46: period (string)
    //           47: score (double)
    //           48: components (array)
    //             49: element (struct)
    //               50: component_name (string)
    //               51: weight (float)
    //               52: sub_scores (map)
    //                 53: key (string)
    //                 54: value (double)
    //           55: trends (struct)
    //             56: direction (string)
    //             57: magnitude (double)
    //             58: confidence_interval (struct)
    //               59: lower (double)
    //               60: upper (double)
    //       61: hierarchical_data (map)
    //         62: key (string)
    //         63: value (struct)
    //           64: category (string)
    //           65: sub_items (array)
    //             66: element (struct)
    //               67: item_id (bigint)
    //               68: properties (map)
    //                 69: key (string)
    //                 70: value (string)
    //               71: metrics (struct)
    //                 72: value (double)
    //                 73: unit (string)
    //           74: summary (struct)
    //             75: total_count (int)
    //             76: average_value (double)
    //       77: validation_rules (struct)
    //         78: required (boolean)
    //         79: constraints (array)
    //           80: element (struct)
    //             81: rule_type (string)
    //             82: parameters (map)
    //               83: key (string)
    //               84: value (string)
    //             85: error_message (string)
    //         86: complex_constraint (struct)
    //           87: logic_operator (string)
    //           88: operands (array)
    //             89: element (struct)
    //               90: field_path (string)
    //               91: operator (string)
    //               92: comparison_value (string)
    std::vector<ColumnAccessPathConfig> access_configs;

    {
        // Configure access paths for complex_attributes column
        ColumnAccessPathConfig access_config;
        access_config.column_name = "complex_attributes";

        access_config.all_column_paths = {
                {"complex_attributes", "*", "metadata", "version"},
                {"complex_attributes", "*", "historical_scores", "*", "components", "*",
                 "sub_scores"},
                {"complex_attributes", "*", "hierarchical_data", "VALUES"},
                {"complex_attributes", "VALUES", "validation_rules", "constraints", "*",
                 "parameters", "KEYS"}};
        access_config.predicate_paths = {{"complex_attributes", "*", "metadata", "version"}

        };
        access_configs.push_back(access_config);
    }

    {
        std::vector<std::string> table_column_names = {"name", "complex_attributes"};
        // parquet values should access keys
        std::set<uint64_t> expected_column_ids = {2,  36, 37, 38, 39, 40, 44, 45, 48, 49, 52, 53,
                                                  54, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71,
                                                  72, 73, 74, 75, 76, 77, 79, 80, 82, 83};
        std::set<uint64_t> expected_filter_column_ids = {36, 37, 38, 39, 40};

        run_parquet_test(table_column_names, access_configs, expected_column_ids,
                         expected_filter_column_ids);
        run_parquet_test(table_column_names, access_configs, expected_column_ids,
                         expected_filter_column_ids, true);
    }

    {
        std::vector<std::string> table_column_names = {"name", "complex_attributes"};
        // orc values should access keys because need to deduplicate by keys
        std::set<uint64_t> expected_column_ids = {2,  36, 37, 38, 39, 40, 44, 45, 48, 49, 52, 53,
                                                  54, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71,
                                                  72, 73, 74, 75, 76, 77, 79, 80, 82, 83};
        std::set<uint64_t> expected_filter_column_ids = {36, 37, 38, 39, 40};
        run_orc_test(table_column_names, access_configs, expected_column_ids,
                     expected_filter_column_ids);
        run_orc_test(table_column_names, access_configs, expected_column_ids,
                     expected_filter_column_ids, true);
    }
}

} // namespace doris::vectorized