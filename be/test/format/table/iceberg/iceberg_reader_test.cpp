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

#include "format/table/iceberg_reader.h"

#include <cctz/time_zone.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/object_pool.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "format/parquet/vparquet_column_chunk_reader.h"
#include "format/parquet/vparquet_reader.h"
#include "format/table/iceberg_scan_semantics.h"
#include "io/fs/file_meta_cache.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/olap_scan_common.h"
#include "util/timezone_utils.h"

namespace doris {

class IcebergReaderTestHelper : public IcebergTableReader {
public:
    using IcebergTableReader::_is_fully_dictionary_encoded;
};

class CapturingMissingColumnReader final : public GenericReader {
public:
    Status get_next_block(Block*, size_t*, bool*) override { return Status::OK(); }

    Status set_fill_columns(
            const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&,
            const std::unordered_map<std::string, VExprContextSPtr>& missing_columns,
            const std::unordered_map<std::string, bool>&) override {
        const auto entry = missing_columns.find("payload");
        if (entry != missing_columns.end()) {
            payload_default = entry->second;
        }
        return Status::OK();
    }

    VExprContextSPtr payload_default;
};

class IcebergMaterializationTestReader final : public IcebergTableReader {
public:
    IcebergMaterializationTestReader(RuntimeProfile* profile, RuntimeState* state,
                                     const TFileScanRangeParams& params,
                                     const TFileRangeDesc& range)
            : IcebergTableReader(nullptr, profile, state, params, range, nullptr, nullptr,
                                 nullptr) {}

    IcebergMaterializationTestReader(std::unique_ptr<GenericReader> file_reader,
                                     RuntimeProfile* profile, RuntimeState* state,
                                     const TFileScanRangeParams& params,
                                     const TFileRangeDesc& range)
            : IcebergTableReader(std::move(file_reader), profile, state, params, range, nullptr,
                                 nullptr, nullptr) {}

    void set_delete_rows() final {}

    void set_missing_table_field(const std::string& name,
                                 std::shared_ptr<const schema::external::TField> field) {
        auto node = std::make_shared<TableSchemaChangeHelper::StructNode>();
        node->add_not_exist_children(name, std::move(field));
        table_info_node_ptr = std::move(node);
        _all_required_col_names = {name};
    }

    void set_column_name_to_block_index(
            std::unordered_map<std::string, uint32_t>* column_name_to_block_index) {
        _col_name_to_block_idx = column_name_to_block_index;
    }

    void set_required_column_type(const std::string& name, const DataTypePtr& type) {
        _required_column_types[name] = type;
    }

    void set_projected_table_field(int32_t field_id, const std::string& name,
                                   const DataTypePtr& type) {
        _id_to_block_column_name[field_id] = name;
        _required_column_types[name] = type;
    }

    Status materialize_missing_table_columns(Block* block) {
        return _materialize_missing_table_columns(block);
    }

    Status validate_required_table_columns(Block* block) {
        return _validate_required_table_columns(block);
    }

    Status register_missing_equality_delete_column(int32_t field_id, const std::string& name,
                                                   const DataTypePtr& type) {
        return _register_missing_equality_delete_column(field_id, name, type);
    }

    Status materialize_missing_equality_delete_columns(Block* block) {
        return _materialize_missing_equality_delete_columns(block);
    }

    void set_hidden_equality_delete_column(const std::string& name) {
        table_info_node_ptr = std::make_shared<TableSchemaChangeHelper::StructNode>();
        _all_required_col_names = {name};
        _physical_missing_equality_delete_columns.insert(name);
    }

    Status extract_nested_equality_delete_column(const ColumnPtr& root_column,
                                                 const DataTypePtr& source_leaf_type,
                                                 const DataTypePtr& target_leaf_type,
                                                 ColumnPtr* leaf_column) {
        NestedEqualityDeleteColumn nested_field {
                .field_id = 7,
                .block_name = "nested_key",
                .source_block_name = {},
                .source_leaf_type = source_leaf_type,
                .leaf_type = target_leaf_type,
                .child_indexes = {0},
                .missing_value = nullptr,
                .cast_context = nullptr,
        };
        RETURN_IF_ERROR(_prepare_nested_equality_delete_column(&nested_field));
        return _extract_nested_equality_delete_column(root_column, nested_field, leaf_column);
    }

    Status materialize_shared_nested_equality_delete_columns(
            Block* block, std::unordered_map<std::string, uint32_t>* column_name_to_block_index,
            const DataTypePtr& first_type, const DataTypePtr& second_type) {
        _col_name_to_block_idx = column_name_to_block_index;
        _nested_equality_delete_columns = {{
                                                   .field_id = 7,
                                                   .block_name = "first_key",
                                                   .source_block_name = "shared_root",
                                                   .source_leaf_type = first_type,
                                                   .leaf_type = first_type,
                                                   .child_indexes = {0},
                                                   .missing_value = nullptr,
                                                   .cast_context = nullptr,
                                           },
                                           {
                                                   .field_id = 8,
                                                   .block_name = "second_key",
                                                   .source_block_name = "shared_root",
                                                   .source_leaf_type = second_type,
                                                   .leaf_type = second_type,
                                                   .child_indexes = {1},
                                                   .missing_value = nullptr,
                                                   .cast_context = nullptr,
                                           }};
        for (auto& nested_field : _nested_equality_delete_columns) {
            RETURN_IF_ERROR(_prepare_nested_equality_delete_column(&nested_field));
        }
        return _materialize_nested_equality_delete_columns(block);
    }

    std::string register_equality_delete_carrier(int32_t field_id, const std::string& source_name,
                                                 const DataTypePtr& type) {
        return _get_or_register_equality_delete_carrier(field_id, source_name, type);
    }

private:
    Status _process_equality_delete(const std::vector<TIcebergDeleteFileDesc>& delete_files) final {
        return Status::OK();
    }
};

std::shared_ptr<schema::external::TField> iceberg_int_field(
        const std::string& name, int32_t id, bool is_optional,
        const std::optional<std::string>& initial_default = std::nullopt) {
    auto field = std::make_shared<schema::external::TField>();
    field->__set_name(name);
    field->__set_id(id);
    field->__set_is_optional(is_optional);
    TColumnType type;
    type.__set_type(TPrimitiveType::INT);
    field->__set_type(type);
    if (initial_default.has_value()) {
        field->__set_initial_default_value(*initial_default);
    }
    return field;
}

void expect_repeated_nullable_int(const Block& block, size_t rows, int32_t expected) {
    ASSERT_EQ(block.columns(), 1);
    const auto& nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    ASSERT_EQ(nullable.size(), rows);
    const auto& values = assert_cast<const ColumnInt32&>(nullable.get_nested_column()).get_data();
    ASSERT_EQ(values.size(), rows);
    for (size_t index = 0; index < rows; ++index) {
        EXPECT_FALSE(nullable.is_null_at(index));
        EXPECT_EQ(values[index], expected);
    }
}

class IcebergReaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        cache = std::make_unique<doris::FileMetaCache>(1024);

        // Setup timezone
        doris::TimezoneUtils::find_cctz_time_zone(doris::TimezoneUtils::default_time_zone,
                                                  timezone_obj);
    }

    void TearDown() override { cache.reset(); }

    std::string mixed_position_delete_file() const {
        return "./be/test/exec/test_data/iceberg_mixed_position_delete_parquet/"
               "mixed_encoding_position_delete.parquet";
    }

    std::unique_ptr<ParquetReader> create_delete_file_parquet_reader(
            RuntimeProfile* profile, RuntimeState* runtime_state, TFileScanRangeParams* scan_params,
            TFileRangeDesc* scan_range, io::FileReaderSPtr* file_reader,
            const tparquet::FileMetaData** file_meta_data) {
        auto local_fs = io::global_local_filesystem();
        auto st = local_fs->open_file(mixed_position_delete_file(), file_reader);
        EXPECT_TRUE(st.ok()) << st;
        if (!st.ok()) {
            return nullptr;
        }

        scan_params->format_type = TFileFormatType::FORMAT_PARQUET;

        scan_range->start_offset = 0;
        scan_range->size = (*file_reader)->size();
        scan_range->path = mixed_position_delete_file();

        auto parquet_reader =
                ParquetReader::create_unique(profile, *scan_params, *scan_range, 1024,
                                             &timezone_obj, nullptr, runtime_state, cache.get());
        EXPECT_NE(parquet_reader, nullptr);
        if (parquet_reader == nullptr) {
            return nullptr;
        }

        parquet_reader->set_file_reader(*file_reader);

        phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>> predicates;
        st = parquet_reader->init_reader(delete_file_column_names,
                                         &delete_file_col_name_to_block_idx, {}, predicates,
                                         nullptr, nullptr, nullptr, nullptr, nullptr);
        EXPECT_TRUE(st.ok()) << st;
        if (!st.ok()) {
            return nullptr;
        }

        std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
                partition_columns;
        std::unordered_map<std::string, VExprContextSPtr> missing_columns;
        st = parquet_reader->set_fill_columns(partition_columns, missing_columns);
        EXPECT_TRUE(st.ok()) << st;
        if (!st.ok()) {
            return nullptr;
        }

        *file_meta_data = parquet_reader->get_meta_data();
        return parquet_reader;
    }

    // Helper function to create complex struct types for testing
    void create_complex_struct_types(DataTypePtr& coordinates_struct_type,
                                     DataTypePtr& address_struct_type,
                                     DataTypePtr& phone_struct_type,
                                     DataTypePtr& contact_struct_type,
                                     DataTypePtr& hobby_element_struct_type,
                                     DataTypePtr& hobbies_array_type,
                                     DataTypePtr& profile_struct_type, DataTypePtr& name_type) {
        // Create name column type (direct field)
        name_type = make_nullable(std::make_shared<DataTypeString>());

        // First create coordinates struct type
        std::vector<DataTypePtr> coordinates_types = {
                make_nullable(std::make_shared<DataTypeFloat64>()), // lat (field ID 10)
                make_nullable(std::make_shared<DataTypeFloat64>())  // lng (field ID 11)
        };
        std::vector<std::string> coordinates_names = {"lat", "lng"};
        coordinates_struct_type = make_nullable(
                std::make_shared<DataTypeStruct>(coordinates_types, coordinates_names));

        // Create address struct type (with street, city, coordinates)
        std::vector<DataTypePtr> address_types = {
                make_nullable(std::make_shared<DataTypeString>()), // street (field ID 7)
                make_nullable(std::make_shared<DataTypeString>()), // city (field ID 8)
                coordinates_struct_type                            // coordinates (field ID 9)
        };
        std::vector<std::string> address_names = {"street", "city", "coordinates"};
        address_struct_type =
                make_nullable(std::make_shared<DataTypeStruct>(address_types, address_names));

        // Create phone struct type
        std::vector<DataTypePtr> phone_types = {
                make_nullable(std::make_shared<DataTypeString>()), // country_code (field ID 14)
                make_nullable(std::make_shared<DataTypeString>())  // number (field ID 15)
        };
        std::vector<std::string> phone_names = {"country_code", "number"};
        phone_struct_type =
                make_nullable(std::make_shared<DataTypeStruct>(phone_types, phone_names));

        // Create contact struct type (with email, phone)
        std::vector<DataTypePtr> contact_types = {
                make_nullable(std::make_shared<DataTypeString>()), // email (field ID 12)
                phone_struct_type                                  // phone (field ID 13)
        };
        std::vector<std::string> contact_names = {"email", "phone"};
        contact_struct_type =
                make_nullable(std::make_shared<DataTypeStruct>(contact_types, contact_names));

        // Create hobby element struct type for array elements
        std::vector<DataTypePtr> hobby_element_types = {
                make_nullable(std::make_shared<DataTypeString>()), // name (field ID 17)
                make_nullable(std::make_shared<DataTypeInt32>())   // level (field ID 18)
        };
        std::vector<std::string> hobby_element_names = {"name", "level"};
        hobby_element_struct_type = make_nullable(
                std::make_shared<DataTypeStruct>(hobby_element_types, hobby_element_names));

        // Create hobbies array type
        hobbies_array_type =
                make_nullable(std::make_shared<DataTypeArray>(hobby_element_struct_type));

        // Create complete profile struct type (with address, contact, hobbies)
        std::vector<DataTypePtr> profile_types = {
                address_struct_type, // address (field ID 4)
                contact_struct_type, // contact (field ID 5)
                hobbies_array_type   // hobbies (field ID 6)
        };
        std::vector<std::string> profile_names = {"address", "contact", "hobbies"};
        profile_struct_type =
                make_nullable(std::make_shared<DataTypeStruct>(profile_types, profile_names));
    }

    // Helper function to create tuple descriptor
    const TupleDescriptor* create_tuple_descriptor(DescriptorTbl** desc_tbl, ObjectPool& obj_pool,
                                                   TDescriptorTable& t_desc_table,
                                                   TTableDescriptor& t_table_desc) {
        std::vector<std::string> table_column_names = {"name", "profile"};
        std::vector<TPrimitiveType::type> table_column_types = {
                TPrimitiveType::STRING, TPrimitiveType::STRUCT // profile uses STRUCT type
        };

        // Create table descriptor with complex schema
        auto create_table_desc = [](TDescriptorTable& t_desc_table, TTableDescriptor& t_table_desc,
                                    const std::vector<std::string>& table_column_names,
                                    const std::vector<TPrimitiveType::type>& types) {
            t_table_desc.__set_id(0);
            t_table_desc.__set_tableType(TTableType::OLAP_TABLE);
            t_table_desc.__set_numCols(0);
            t_table_desc.__set_numClusteringCols(0);
            t_desc_table.tableDescriptors.push_back(t_table_desc);
            t_desc_table.__isset.tableDescriptors = true;
            // iceberg_id must correspond to table_column_names order
            std::vector<int32_t> iceberg_ids = {2, 3}; // name:2, profile:3
            for (int i = 0; i < table_column_names.size(); i++) {
                TSlotDescriptor tslot_desc;
                tslot_desc.__set_id(i);
                tslot_desc.__set_parent(0);
                tslot_desc.__set_col_unique_id(iceberg_ids[i]);
                TTypeDesc type;
                if (table_column_names[i] == "profile") {
                    // STRUCT/ARRAY nodes set contains_nulls; SCALAR nodes do not
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
                    // Regular type
                    TTypeNode node;
                    node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType scalar_type;
                    scalar_type.__set_type(types[i]);
                    node.__set_scalar_type(scalar_type);
                    type.types.push_back(node);
                    tslot_desc.__set_slotType(type);
                }
                tslot_desc.__set_columnPos(0);
                tslot_desc.__set_byteOffset(0);
                tslot_desc.__set_nullIndicatorByte(0);
                tslot_desc.__set_nullIndicatorBit(-1);
                tslot_desc.__set_colName(table_column_names[i]);
                tslot_desc.__set_slotIdx(0);
                tslot_desc.__set_isMaterialized(true);
                // Set column_access_paths only for the profile field
                if (table_column_names[i] == "profile") {
                    std::vector<TColumnAccessPath> access_paths;
                    // address.coordinates.lat
                    TColumnAccessPath path1;
                    path1.__set_type(doris::TAccessPathType::DATA);
                    TDataAccessPath data_path1;
                    data_path1.__set_path({"3", "4", "9", "10"});
                    path1.__set_data_access_path(data_path1);
                    access_paths.push_back(path1);
                    // address.coordinates.lng
                    TColumnAccessPath path2;
                    path2.__set_type(doris::TAccessPathType::DATA);
                    TDataAccessPath data_path2;
                    data_path2.__set_path({"3", "4", "9", "11"});
                    path2.__set_data_access_path(data_path2);
                    access_paths.push_back(path2);
                    // contact.email
                    TColumnAccessPath path3;
                    path3.__set_type(doris::TAccessPathType::DATA);
                    TDataAccessPath data_path3;
                    data_path3.__set_path({"3", "5", "12"});
                    path3.__set_data_access_path(data_path3);
                    access_paths.push_back(path3);
                    // hobbies[].element.level
                    TColumnAccessPath path4;
                    path4.__set_type(doris::TAccessPathType::DATA);
                    TDataAccessPath data_path4;
                    data_path4.__set_path({"3", "6", "*", "18"});
                    path4.__set_data_access_path(data_path4);
                    access_paths.push_back(path4);
                    tslot_desc.__set_all_access_paths(access_paths);
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

        create_table_desc(t_desc_table, t_table_desc, table_column_names, table_column_types);
        EXPECT_TRUE(DescriptorTbl::create(&obj_pool, t_desc_table, desc_tbl).ok());
        return (*desc_tbl)->get_tuple_descriptor(0);
    }

    const TupleDescriptor* create_missing_required_nested_tuple_descriptor(
            DescriptorTbl** desc_tbl, ObjectPool& obj_pool, TDescriptorTable& t_desc_table) {
        TTableDescriptor table_desc;
        table_desc.__set_id(0);
        table_desc.__set_tableType(TTableType::OLAP_TABLE);
        table_desc.__set_numCols(0);
        table_desc.__set_numClusteringCols(0);
        t_desc_table.tableDescriptors.push_back(table_desc);
        t_desc_table.__isset.tableDescriptors = true;

        TSlotDescriptor slot_desc;
        slot_desc.__set_id(0);
        slot_desc.__set_parent(0);
        slot_desc.__set_col_unique_id(3);
        slot_desc.__set_colName("profile");
        slot_desc.__set_columnPos(0);
        slot_desc.__set_byteOffset(0);
        slot_desc.__set_nullIndicatorByte(0);
        slot_desc.__set_nullIndicatorBit(-1);
        slot_desc.__set_slotIdx(0);
        slot_desc.__set_isMaterialized(true);

        TTypeDesc type;
        TTypeNode struct_node;
        struct_node.__set_type(TTypeNodeType::STRUCT);
        TStructField required_child;
        required_child.__set_name("required_added");
        // Iceberg requiredness travels separately from Doris type nullability.
        required_child.__set_contains_null(true);
        struct_node.__set_struct_fields({required_child});
        type.types.push_back(struct_node);
        TTypeNode child_node;
        child_node.__set_type(TTypeNodeType::SCALAR);
        TScalarType child_scalar;
        child_scalar.__set_type(TPrimitiveType::INT);
        child_node.__set_scalar_type(child_scalar);
        type.types.push_back(child_node);
        slot_desc.__set_slotType(type);
        t_desc_table.slotDescriptors.push_back(slot_desc);
        t_desc_table.__isset.slotDescriptors = true;

        TTupleDescriptor tuple_desc;
        tuple_desc.__set_id(0);
        tuple_desc.__set_byteSize(16);
        tuple_desc.__set_numNullBytes(0);
        tuple_desc.__set_tableId(0);
        tuple_desc.__isset.tableId = true;
        t_desc_table.tupleDescriptors.push_back(tuple_desc);

        EXPECT_TRUE(DescriptorTbl::create(&obj_pool, t_desc_table, desc_tbl).ok());
        return (*desc_tbl)->get_tuple_descriptor(0);
    }

    void set_missing_required_nested_schema(TFileScanRangeParams* scan_params) {
        const auto required_child = iceberg_int_field("required_added", 100, false);
        schema::external::TFieldPtr child_ptr;
        child_ptr.__set_field_ptr(required_child);
        schema::external::TStructField profile_fields;
        profile_fields.__set_fields({child_ptr});

        auto profile = std::make_shared<schema::external::TField>();
        profile->__set_name("profile");
        profile->__set_id(3);
        profile->__set_is_optional(true);
        TColumnType profile_type;
        profile_type.__set_type(TPrimitiveType::STRUCT);
        profile->__set_type(profile_type);
        profile->nestedField.__set_struct_field(profile_fields);
        profile->__isset.nestedField = true;
        schema::external::TFieldPtr profile_ptr;
        profile_ptr.__set_field_ptr(profile);

        schema::external::TStructField root;
        root.__set_fields({profile_ptr});
        schema::external::TSchema schema;
        schema.__set_schema_id(100);
        schema.__set_root_field(root);
        scan_params->__set_current_schema_id(100);
        scan_params->__set_history_schema_info({schema});
        scan_params->__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    }

    Status init_missing_required_nested_reader(TFileFormatType::type format,
                                               const std::string& file_path,
                                               const io::FileReaderSPtr& file_reader) {
        RuntimeState runtime_state {TQueryGlobals()};
        TFileScanRangeParams scan_params;
        scan_params.__set_format_type(format);
        set_missing_required_nested_schema(&scan_params);
        TFileRangeDesc scan_range;
        scan_range.__set_start_offset(0);
        scan_range.__set_size(file_reader->size());
        scan_range.__set_path(file_path);
        RuntimeProfile profile("test_profile");

        DescriptorTbl* desc_tbl;
        ObjectPool obj_pool;
        TDescriptorTable t_desc_table;
        const auto* tuple_descriptor =
                create_missing_required_nested_tuple_descriptor(&desc_tbl, obj_pool, t_desc_table);
        std::vector<std::string> table_col_names = {"profile"};
        std::unordered_map<std::string, uint32_t> column_positions = {{"profile", 0}};
        VExprContextSPtrs conjuncts;
        if (format == TFileFormatType::FORMAT_PARQUET) {
            auto parquet_reader = ParquetReader::create_unique(&profile, scan_params, scan_range,
                                                               1024, &timezone_obj, nullptr,
                                                               &runtime_state, cache.get());
            DORIS_CHECK(parquet_reader != nullptr);
            parquet_reader->set_file_reader(file_reader);
            IcebergParquetReader reader(std::move(parquet_reader), &profile, &runtime_state,
                                        scan_params, scan_range, nullptr, nullptr, cache.get());
            phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>> predicates;
            return reader.init_reader(table_col_names, &column_positions, conjuncts, predicates,
                                      tuple_descriptor, nullptr, nullptr, nullptr, nullptr);
        }

        DORIS_CHECK(format == TFileFormatType::FORMAT_ORC);
        auto orc_reader = OrcReader::create_unique(&profile, &runtime_state, scan_params,
                                                   scan_range, 1024, "CST", nullptr, cache.get());
        DORIS_CHECK(orc_reader != nullptr);
        IcebergOrcReader reader(std::move(orc_reader), &profile, &runtime_state, scan_params,
                                scan_range, nullptr, nullptr, cache.get());
        return reader.init_reader(table_col_names, &column_positions, conjuncts, tuple_descriptor,
                                  nullptr, nullptr, nullptr, nullptr);
    }

    // Helper function to verify test results
    void verify_test_results(Block& block, size_t read_rows) {
        // Verify that we read some data
        EXPECT_GT(read_rows, 0) << "Should read at least one row";
        EXPECT_EQ(block.rows(), read_rows);

        // Verify column count matches expected (2 columns: name, profile)
        EXPECT_EQ(block.columns(), 2);

        // Verify column names and types
        auto columns_with_names = block.get_columns_with_type_and_name();
        std::vector<std::string> expected_column_names = {"name", "profile"};
        for (size_t i = 0; i < expected_column_names.size(); i++) {
            EXPECT_EQ(columns_with_names[i].name, expected_column_names[i]);
        }

        // Verify column types
        EXPECT_TRUE(columns_with_names[0].type->get_name().find("String") !=
                    std::string::npos); // name is STRING
        EXPECT_TRUE(columns_with_names[1].type->get_name().find("Struct") !=
                    std::string::npos); // profile is STRUCT

        // Print row count for each column and nested subcolumns
        std::cout << "Block rows: " << block.rows() << std::endl;

        // Helper function to recursively print column row counts and check size > 0
        std::function<void(const ColumnPtr&, const DataTypePtr&, const std::string&, int)>
                print_column_rows = [&](const ColumnPtr& col, const DataTypePtr& type,
                                        const std::string& name, int depth) {
                    std::string indent(depth * 2, ' ');
                    std::cout << indent << name << " row count: " << col->size() << std::endl;
                    EXPECT_GT(col->size(), 0) << name << " column/subcolumn size should be > 0";

                    // Check if it's a nullable column
                    if (const auto* nullable_col =
                                check_and_get_column<ColumnNullable>(col.get())) {
                        auto nested_type =
                                assert_cast<const DataTypeNullable*>(type.get())->get_nested_type();

                        // Only add ".nested" suffix for non-leaf (complex) nullable columns
                        // Leaf columns like String, Int, etc. should not get the ".nested" suffix
                        bool is_complex_type =
                                (typeid_cast<const DataTypeStruct*>(nested_type.get()) !=
                                 nullptr) ||
                                (typeid_cast<const DataTypeArray*>(nested_type.get()) != nullptr) ||
                                (typeid_cast<const DataTypeMap*>(nested_type.get()) != nullptr);

                        std::string nested_name = is_complex_type ? name + ".nested" : name;
                        print_column_rows(nullable_col->get_nested_column_ptr(), nested_type,
                                          nested_name, depth + (is_complex_type ? 1 : 0));
                    }
                    // Check if it's a struct column
                    else if (const auto* struct_col =
                                     check_and_get_column<ColumnStruct>(col.get())) {
                        auto struct_type = assert_cast<const DataTypeStruct*>(type.get());
                        for (size_t i = 0; i < struct_col->tuple_size(); ++i) {
                            std::string field_name = struct_type->get_element_name(i);
                            auto field_type = struct_type->get_element(i);
                            print_column_rows(struct_col->get_column_ptr(i), field_type,
                                              name + "." + field_name, depth + 1);
                        }
                    }
                    // Check if it's an array column
                    else if (const auto* array_col = check_and_get_column<ColumnArray>(col.get())) {
                        auto array_type = assert_cast<const DataTypeArray*>(type.get());
                        auto element_type = array_type->get_nested_type();
                        print_column_rows(array_col->get_data_ptr(), element_type, name + ".data",
                                          depth + 1);
                    }
                };

        // Print row counts for all columns
        for (size_t i = 0; i < block.columns(); ++i) {
            const auto& column_with_name = block.get_by_position(i);
            print_column_rows(column_with_name.column, column_with_name.type, column_with_name.name,
                              0);
            EXPECT_EQ(column_with_name.column->size(), block.rows())
                    << "Column " << column_with_name.name << " size mismatch";
        }
    }

    std::unique_ptr<doris::FileMetaCache> cache;
    cctz::time_zone timezone_obj;
    std::vector<std::string> delete_file_column_names = {"file_path", "pos"};
    std::unordered_map<std::string, uint32_t> delete_file_col_name_to_block_idx = {{"file_path", 0},
                                                                                   {"pos", 1}};
};

TEST_F(IcebergReaderTest, detects_fully_dictionary_encoded_parquet_column) {
    tparquet::ColumnMetaData column_metadata;
    column_metadata.type = tparquet::Type::BYTE_ARRAY;
    column_metadata.__isset.encoding_stats = true;

    tparquet::PageEncodingStats dict_page;
    dict_page.page_type = tparquet::PageType::DATA_PAGE;
    dict_page.encoding = tparquet::Encoding::RLE_DICTIONARY;
    dict_page.count = 3;

    column_metadata.encoding_stats = {dict_page};

    EXPECT_TRUE(IcebergReaderTestHelper::_is_fully_dictionary_encoded(column_metadata));
}

TEST_F(IcebergReaderTest, materializes_top_level_initial_default_with_v1_reader) {
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state {TQueryGlobals()};
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    TFileRangeDesc scan_range;
    IcebergMaterializationTestReader reader(&profile, &runtime_state, scan_params, scan_range);

    const auto field = iceberg_int_field("added", 7, true, "17");
    reader.set_missing_table_field("added", field);
    std::unordered_map<std::string, uint32_t> column_name_to_block_index {{"added", 0}};
    reader.set_column_name_to_block_index(&column_name_to_block_index);

    const auto type = make_nullable(std::make_shared<DataTypeInt32>());
    Block block;
    auto placeholders = type->create_column();
    placeholders->insert_many_defaults(3);
    block.insert({std::move(placeholders), type, "added"});
    ASSERT_TRUE(reader.materialize_missing_table_columns(&block).ok());
    expect_repeated_nullable_int(block, 3, 17);
}

TEST_F(IcebergReaderTest, materializes_empty_block_after_missing_column_predicate_filter) {
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state {TQueryGlobals()};
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    TFileRangeDesc scan_range;
    IcebergMaterializationTestReader reader(&profile, &runtime_state, scan_params, scan_range);

    const auto field = iceberg_int_field("added", 7, true, "17");
    reader.set_missing_table_field("added", field);
    std::unordered_map<std::string, uint32_t> column_name_to_block_index {{"added", 0}};
    reader.set_column_name_to_block_index(&column_name_to_block_index);

    const auto type = make_nullable(std::make_shared<DataTypeInt32>());
    Block block;
    block.insert({type->create_column(), type, "added"});
    ASSERT_TRUE(reader.materialize_missing_table_columns(&block).ok());
    EXPECT_EQ(block.rows(), 0);
}

TEST_F(IcebergReaderTest, materializes_timestamptz_initial_default_in_session_timezone) {
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state {TQueryGlobals()};
    runtime_state.set_timezone("Asia/Shanghai");
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    TFileRangeDesc scan_range;
    IcebergMaterializationTestReader reader(&profile, &runtime_state, scan_params, scan_range);

    auto field = std::make_shared<schema::external::TField>();
    field->__set_name("event_time");
    field->__set_id(7);
    field->__set_is_optional(true);
    field->__set_initial_default_value("2025-01-18 01:02:03.654321+00:00");
    TColumnType thrift_type;
    thrift_type.__set_type(TPrimitiveType::DATETIMEV2);
    field->__set_type(thrift_type);
    reader.set_missing_table_field("event_time", field);
    std::unordered_map<std::string, uint32_t> positions {{"event_time", 0}};
    reader.set_column_name_to_block_index(&positions);

    auto type = make_nullable(std::make_shared<DataTypeDateTimeV2>(6));
    Block block;
    auto placeholders = type->create_column();
    placeholders->insert_default();
    block.insert({std::move(placeholders), type, "event_time"});
    ASSERT_TRUE(reader.materialize_missing_table_columns(&block).ok());
    EXPECT_EQ(type->to_string(*block.get_by_position(0).column, 0), "2025-01-18 09:02:03.654321");
}

TEST_F(IcebergReaderTest, sends_complex_initial_default_to_v1_physical_filter) {
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state {TQueryGlobals()};
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    TFileRangeDesc scan_range;
    auto capturing_reader = std::make_unique<CapturingMissingColumnReader>();
    auto* captured = capturing_reader.get();
    IcebergMaterializationTestReader reader(std::move(capturing_reader), &profile, &runtime_state,
                                            scan_params, scan_range);

    const auto child = iceberg_int_field("value", 2, true);
    schema::external::TFieldPtr child_ptr;
    child_ptr.__set_field_ptr(child);
    schema::external::TStructField struct_fields;
    struct_fields.__set_fields({child_ptr});
    auto payload = std::make_shared<schema::external::TField>();
    payload->__set_name("payload");
    payload->__set_id(1);
    payload->__set_is_optional(true);
    payload->__set_initial_default_value("{\"2\":7}");
    TColumnType struct_thrift_type;
    struct_thrift_type.__set_type(TPrimitiveType::STRUCT);
    payload->__set_type(struct_thrift_type);
    payload->nestedField.__set_struct_field(struct_fields);
    payload->__isset.nestedField = true;
    reader.set_missing_table_field("payload", payload);

    auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
    auto payload_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"value"}));
    reader.set_required_column_type("payload", payload_type);
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns {{"payload", nullptr}};
    ASSERT_TRUE(reader.set_fill_columns(partition_columns, missing_columns).ok());
    ASSERT_NE(captured->payload_default, nullptr);

    Block input;
    auto row_count = ColumnInt32::create();
    row_count->insert_value(1);
    input.insert({std::move(row_count), std::make_shared<DataTypeInt32>(), "row_count"});
    ColumnPtr default_column;
    ASSERT_TRUE(captured->payload_default->execute(&input, default_column).ok());
    default_column = default_column->convert_to_full_column_if_const();
    const auto& nullable = assert_cast<const ColumnNullable&>(*default_column);
    ASSERT_FALSE(nullable.is_null_at(0));
    const auto& struct_column = assert_cast<const ColumnStruct&>(nullable.get_nested_column());
    const auto& child_column = assert_cast<const ColumnNullable&>(struct_column.get_column(0));
    ASSERT_FALSE(child_column.is_null_at(0));
    EXPECT_EQ(assert_cast<const ColumnInt32&>(child_column.get_nested_column()).get_data()[0], 7);
}

TEST_F(IcebergReaderTest, replaces_reader_placeholders_across_rowid_fetch_batches) {
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state {TQueryGlobals()};
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    TFileRangeDesc scan_range;
    IcebergMaterializationTestReader reader(&profile, &runtime_state, scan_params, scan_range);

    const auto field = iceberg_int_field("added", 7, true, "17");
    reader.set_missing_table_field("added", field);
    std::unordered_map<std::string, uint32_t> column_name_to_block_index {{"added", 0}};
    reader.set_column_name_to_block_index(&column_name_to_block_index);

    const auto type = make_nullable(std::make_shared<DataTypeInt32>());
    Block block;
    auto placeholders = type->create_column();
    placeholders->insert_default();
    block.insert({std::move(placeholders), type, "added"});

    // Parquet and ORC fill a placeholder before each row-id batch reaches the Iceberg reader.
    ASSERT_TRUE(reader.materialize_missing_table_columns(&block).ok());
    {
        auto column = block.mutate_column_scoped(0);
        column.mutable_column()->insert_default();
    }
    ASSERT_TRUE(reader.materialize_missing_table_columns(&block).ok());
    ASSERT_TRUE(reader.materialize_missing_table_columns(&block).ok());
    expect_repeated_nullable_int(block, 2, 17);
}

TEST_F(IcebergReaderTest, preserves_generated_row_lineage_values_with_v1_reader) {
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state {TQueryGlobals()};
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    TFileRangeDesc scan_range;
    IcebergMaterializationTestReader reader(&profile, &runtime_state, scan_params, scan_range);

    auto field = iceberg_int_field(IcebergTableReader::ROW_LINEAGE_ROW_ID, 2147483540, true);
    TColumnType long_type;
    long_type.__set_type(TPrimitiveType::BIGINT);
    field->__set_type(long_type);
    reader.set_missing_table_field(IcebergTableReader::ROW_LINEAGE_ROW_ID, field);
    reader.set_row_lineage_columns(std::make_shared<RowLineageColumns>());
    std::unordered_map<std::string, uint32_t> column_name_to_block_index {
            {IcebergTableReader::ROW_LINEAGE_ROW_ID, 0}};
    reader.set_column_name_to_block_index(&column_name_to_block_index);

    auto values = ColumnInt64::create();
    values->insert_value(101);
    values->insert_value(102);
    const auto type = make_nullable(std::make_shared<DataTypeInt64>());
    Block block;
    block.insert({ColumnNullable::create(std::move(values), ColumnUInt8::create(2, 0)), type,
                  IcebergTableReader::ROW_LINEAGE_ROW_ID});
    ASSERT_TRUE(reader.materialize_missing_table_columns(&block).ok());

    const auto& nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& preserved =
            assert_cast<const ColumnInt64&>(nullable.get_nested_column()).get_data();
    ASSERT_EQ(preserved.size(), 2);
    EXPECT_EQ(preserved[0], 101);
    EXPECT_EQ(preserved[1], 102);
}

TEST_F(IcebergReaderTest, skips_hidden_equality_carrier_during_table_default_materialization) {
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state {TQueryGlobals()};
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    TFileRangeDesc scan_range;
    IcebergMaterializationTestReader reader(&profile, &runtime_state, scan_params, scan_range);
    reader.set_hidden_equality_delete_column("__equality_delete_column__7_0");

    auto values = ColumnInt32::create();
    values->insert_value(17);
    values->insert_value(19);
    Block block;
    block.insert({std::move(values), std::make_shared<DataTypeInt32>(),
                  "__equality_delete_column__7_0"});

    ASSERT_TRUE(reader.materialize_missing_table_columns(&block).ok());
    ASSERT_EQ(block.rows(), 2);
    const auto& preserved =
            assert_cast<const ColumnInt32&>(*block.get_by_position(0).column).get_data();
    EXPECT_EQ(preserved[0], 17);
    EXPECT_EQ(preserved[1], 19);
}

TEST_F(IcebergReaderTest, promotes_nested_equality_key_with_v1_reader) {
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state {TQueryGlobals()};
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    TFileRangeDesc scan_range;
    IcebergMaterializationTestReader reader(&profile, &runtime_state, scan_params, scan_range);

    auto values = ColumnInt32::create();
    values->insert_value(17);
    values->insert_value(-9);
    Columns children;
    children.emplace_back(std::move(values));
    ColumnPtr leaf;
    ASSERT_TRUE(reader.extract_nested_equality_delete_column(
                              ColumnStruct::create(std::move(children)),
                              make_nullable(std::make_shared<DataTypeInt32>()),
                              make_nullable(std::make_shared<DataTypeInt64>()), &leaf)
                        .ok());

    const auto& nullable = assert_cast<const ColumnNullable&>(*leaf);
    const auto& promoted = assert_cast<const ColumnInt64&>(nullable.get_nested_column()).get_data();
    ASSERT_EQ(promoted.size(), 2);
    EXPECT_EQ(promoted[0], 17);
    EXPECT_EQ(promoted[1], -9);
}

TEST_F(IcebergReaderTest, casts_current_promoted_key_to_historical_delete_type) {
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state {TQueryGlobals()};
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    TFileRangeDesc scan_range;
    IcebergMaterializationTestReader reader(&profile, &runtime_state, scan_params, scan_range);

    auto values = ColumnInt64::create();
    values->insert_value(17);
    values->insert_value(-9);
    Columns children;
    children.emplace_back(std::move(values));
    ColumnPtr leaf;
    ASSERT_TRUE(reader.extract_nested_equality_delete_column(
                              ColumnStruct::create(std::move(children)),
                              make_nullable(std::make_shared<DataTypeInt64>()),
                              make_nullable(std::make_shared<DataTypeInt32>()), &leaf)
                        .ok());

    const auto& nullable = assert_cast<const ColumnNullable&>(*leaf);
    const auto& historical =
            assert_cast<const ColumnInt32&>(nullable.get_nested_column()).get_data();
    ASSERT_EQ(historical.size(), 2);
    EXPECT_EQ(historical[0], 17);
    EXPECT_EQ(historical[1], -9);
}

TEST_F(IcebergReaderTest, materializes_multiple_equality_keys_from_shared_root) {
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state {TQueryGlobals()};
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    TFileRangeDesc scan_range;
    IcebergMaterializationTestReader reader(&profile, &runtime_state, scan_params, scan_range);

    auto first_values = ColumnInt32::create();
    first_values->insert_value(10);
    first_values->insert_value(11);
    auto second_values = ColumnInt32::create();
    second_values->insert_value(20);
    second_values->insert_value(21);
    Columns root_children;
    root_children.emplace_back(std::move(first_values));
    root_children.emplace_back(std::move(second_values));
    auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
    auto root_type = std::make_shared<DataTypeStruct>(DataTypes {int_type, int_type},
                                                      Strings {"first", "second"});
    Block block;
    block.insert({ColumnStruct::create(std::move(root_children)), root_type, "shared_root"});
    block.insert({int_type->create_column(), int_type, "first_key"});
    block.insert({int_type->create_column(), int_type, "second_key"});
    std::unordered_map<std::string, uint32_t> positions {
            {"shared_root", 0}, {"first_key", 1}, {"second_key", 2}};

    ASSERT_TRUE(reader.materialize_shared_nested_equality_delete_columns(&block, &positions,
                                                                         int_type, int_type)
                        .ok());
    const auto& first = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    const auto& second = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
    const auto& first_data = assert_cast<const ColumnInt32&>(first.get_nested_column()).get_data();
    const auto& second_data =
            assert_cast<const ColumnInt32&>(second.get_nested_column()).get_data();
    ASSERT_EQ(first_data.size(), 2);
    ASSERT_EQ(second_data.size(), 2);
    EXPECT_EQ(first_data[0], 10);
    EXPECT_EQ(first_data[1], 11);
    EXPECT_EQ(second_data[0], 20);
    EXPECT_EQ(second_data[1], 21);
}

TEST_F(IcebergReaderTest, uses_distinct_carriers_for_historical_equality_key_types) {
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state {TQueryGlobals()};
    TFileScanRangeParams scan_params;
    TFileRangeDesc scan_range;
    IcebergMaterializationTestReader reader(&profile, &runtime_state, scan_params, scan_range);
    auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
    auto long_type = make_nullable(std::make_shared<DataTypeInt64>());

    const std::string int_carrier = reader.register_equality_delete_carrier(7, "key", int_type);
    const std::string repeated_int_carrier =
            reader.register_equality_delete_carrier(7, "renamed_key", int_type);
    const std::string long_carrier = reader.register_equality_delete_carrier(7, "key", long_type);

    EXPECT_EQ(int_carrier, repeated_int_carrier);
    EXPECT_NE(int_carrier, long_carrier);
}

TEST_F(IcebergReaderTest, rejects_missing_required_top_level_field_with_v1_reader) {
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state {TQueryGlobals()};
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    TFileRangeDesc scan_range;
    IcebergMaterializationTestReader reader(&profile, &runtime_state, scan_params, scan_range);

    const auto field = iceberg_int_field("required_added", 8, false);
    reader.set_missing_table_field("required_added", field);
    std::unordered_map<std::string, uint32_t> column_name_to_block_index {{"required_added", 0}};
    reader.set_column_name_to_block_index(&column_name_to_block_index);

    const auto type = std::make_shared<DataTypeInt32>();
    Block block;
    block.insert({type->create_column(), type, "required_added"});
    const Status status = reader.materialize_missing_table_columns(&block);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("has no initial default"), std::string::npos);
}

TEST_F(IcebergReaderTest, rejects_visible_null_for_required_v1_field) {
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state {TQueryGlobals()};
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    scan_params.__set_current_schema_id(100);
    const auto field = iceberg_int_field("required_value", 8, false);
    schema::external::TFieldPtr field_ptr;
    field_ptr.__set_field_ptr(field);
    schema::external::TStructField root;
    root.__set_fields({field_ptr});
    schema::external::TSchema schema;
    schema.__set_schema_id(100);
    schema.__set_root_field(root);
    scan_params.__set_history_schema_info({schema});
    TFileRangeDesc scan_range;
    IcebergMaterializationTestReader reader(&profile, &runtime_state, scan_params, scan_range);

    auto type = make_nullable(std::make_shared<DataTypeInt32>());
    reader.set_projected_table_field(8, "required_value", type);
    std::unordered_map<std::string, uint32_t> positions {{"required_value", 0}};
    reader.set_column_name_to_block_index(&positions);
    auto values = ColumnInt32::create();
    values->insert_default();
    Block block;
    block.insert({ColumnNullable::create(std::move(values), ColumnUInt8::create(1, 1)), type,
                  "required_value"});

    const auto status = reader.validate_required_table_columns(&block);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("required_value"), std::string::npos);
}

TEST_F(IcebergReaderTest, materializes_missing_equality_key_from_split_schema_using_block_rows) {
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state {TQueryGlobals()};
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);

    const auto field = iceberg_int_field("dropped_key", 9, true, "23");
    schema::external::TFieldPtr field_ptr;
    field_ptr.field_ptr = field;
    field_ptr.__isset.field_ptr = true;
    schema::external::TStructField root;
    root.__set_fields({field_ptr});
    schema::external::TSchema split_schema;
    split_schema.__set_schema_id(-1);
    split_schema.__set_root_field(root);
    TIcebergFileDesc iceberg_params;
    iceberg_params.__set_equality_delete_schema(split_schema);
    TTableFormatFileDesc table_format_params;
    table_format_params.__set_iceberg_params(iceberg_params);
    TFileRangeDesc scan_range;
    scan_range.__set_table_format_params(table_format_params);

    IcebergMaterializationTestReader reader(&profile, &runtime_state, scan_params, scan_range);
    std::unordered_map<std::string, uint32_t> column_name_to_block_index {
            {"__equality_delete_column__9_dropped_key", 0}};
    reader.set_column_name_to_block_index(&column_name_to_block_index);

    const auto type = make_nullable(std::make_shared<DataTypeInt32>());
    ASSERT_TRUE(reader.register_missing_equality_delete_column(
                              9, "__equality_delete_column__9_dropped_key", type)
                        .ok());
    Block block;
    auto placeholders = type->create_column();
    placeholders->insert_default();
    placeholders->insert_default();
    block.insert({std::move(placeholders), type, "__equality_delete_column__9_dropped_key"});
    ASSERT_TRUE(reader.materialize_missing_equality_delete_columns(&block).ok());
    expect_repeated_nullable_int(block, 2, 23);

    // A physical reader may report its pre-filter row count after clearing a fully filtered block.
    // Missing equality carriers must follow the visible block size, which is zero here.
    std::unordered_map<std::string, uint32_t> filtered_column_name_to_block_index {
            {"__equality_delete_column__9_dropped_key", 1}};
    reader.set_column_name_to_block_index(&filtered_column_name_to_block_index);
    Block filtered_block;
    filtered_block.insert({std::make_shared<DataTypeInt32>()->create_column(),
                           std::make_shared<DataTypeInt32>(), "projected_id"});
    filtered_block.insert({type->create_column(), type, "__equality_delete_column__9_dropped_key"});
    ASSERT_TRUE(reader.materialize_missing_equality_delete_columns(&filtered_block).ok());
    EXPECT_EQ(filtered_block.rows(), 0);
    EXPECT_EQ(filtered_block.get_by_position(1).column->size(), 0);
}

TEST_F(IcebergReaderTest, rejects_mixed_dictionary_and_plain_parquet_column) {
    tparquet::ColumnMetaData column_metadata;
    column_metadata.type = tparquet::Type::BYTE_ARRAY;
    column_metadata.__isset.encoding_stats = true;

    tparquet::PageEncodingStats dict_page;
    dict_page.page_type = tparquet::PageType::DATA_PAGE;
    dict_page.encoding = tparquet::Encoding::RLE_DICTIONARY;
    dict_page.count = 2;

    tparquet::PageEncodingStats plain_page;
    plain_page.page_type = tparquet::PageType::DATA_PAGE;
    plain_page.encoding = tparquet::Encoding::PLAIN;
    plain_page.count = 1;

    column_metadata.encoding_stats = {dict_page, plain_page};

    EXPECT_FALSE(IcebergReaderTestHelper::_is_fully_dictionary_encoded(column_metadata));
}

TEST_F(IcebergReaderTest, rejects_mixed_dictionary_and_plain_parquet_v2_column) {
    tparquet::ColumnMetaData column_metadata;
    column_metadata.type = tparquet::Type::BYTE_ARRAY;
    column_metadata.__isset.encoding_stats = true;

    tparquet::PageEncodingStats dict_page;
    dict_page.page_type = tparquet::PageType::DATA_PAGE_V2;
    dict_page.encoding = tparquet::Encoding::RLE_DICTIONARY;
    dict_page.count = 2;

    tparquet::PageEncodingStats plain_page;
    plain_page.page_type = tparquet::PageType::DATA_PAGE_V2;
    plain_page.encoding = tparquet::Encoding::PLAIN;
    plain_page.count = 1;

    column_metadata.encoding_stats = {dict_page, plain_page};

    EXPECT_FALSE(IcebergReaderTestHelper::_is_fully_dictionary_encoded(column_metadata));
}

TEST_F(IcebergReaderTest, rejects_non_dictionary_encoding_without_encoding_stats) {
    tparquet::ColumnMetaData column_metadata;
    column_metadata.type = tparquet::Type::BYTE_ARRAY;
    column_metadata.__isset.encoding_stats = false;
    column_metadata.encodings = {tparquet::Encoding::PLAIN_DICTIONARY, tparquet::Encoding::PLAIN,
                                 tparquet::Encoding::RLE};

    EXPECT_FALSE(IcebergReaderTestHelper::_is_fully_dictionary_encoded(column_metadata));
}

TEST_F(IcebergReaderTest, falls_back_to_encodings_when_data_page_stats_are_missing) {
    tparquet::ColumnMetaData column_metadata;
    column_metadata.type = tparquet::Type::BYTE_ARRAY;
    column_metadata.__isset.encoding_stats = true;

    tparquet::PageEncodingStats dict_page_header;
    dict_page_header.page_type = tparquet::PageType::DICTIONARY_PAGE;
    dict_page_header.encoding = tparquet::Encoding::PLAIN;
    dict_page_header.count = 1;
    column_metadata.encoding_stats = {dict_page_header};

    column_metadata.encodings = {tparquet::Encoding::PLAIN, tparquet::Encoding::RLE,
                                 tparquet::Encoding::RLE_DICTIONARY};

    EXPECT_FALSE(IcebergReaderTestHelper::_is_fully_dictionary_encoded(column_metadata));
}

TEST_F(IcebergReaderTest, generated_position_delete_file_is_mixed_encoded) {
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state {TQueryGlobals()};
    TFileScanRangeParams scan_params;
    TFileRangeDesc scan_range;
    io::FileReaderSPtr file_reader;
    const tparquet::FileMetaData* file_meta_data = nullptr;
    auto parquet_reader = create_delete_file_parquet_reader(
            &profile, &runtime_state, &scan_params, &scan_range, &file_reader, &file_meta_data);
    ASSERT_NE(parquet_reader, nullptr);
    ASSERT_NE(file_meta_data, nullptr);
    ASSERT_EQ(file_meta_data->row_groups.size(), 1);

    const auto& file_path_meta = file_meta_data->row_groups[0].columns[0].meta_data;
    EXPECT_TRUE(file_meta_data->row_groups[0].columns[0].__isset.meta_data);
    EXPECT_TRUE(has_dict_page(file_path_meta));
    bool has_plain_encoding = false;
    bool has_dictionary_encoding = false;
    for (const auto encoding : file_path_meta.encodings) {
        if (encoding == tparquet::Encoding::PLAIN) {
            has_plain_encoding = true;
        }
        if (encoding == tparquet::Encoding::PLAIN_DICTIONARY ||
            encoding == tparquet::Encoding::RLE_DICTIONARY) {
            has_dictionary_encoding = true;
        }
    }
    EXPECT_TRUE(has_plain_encoding);
    EXPECT_TRUE(has_dictionary_encoding);
}

// Test reading real Iceberg Parquet file using IcebergTableReader
TEST_F(IcebergReaderTest, read_iceberg_parquet_file) {
    // Read only: name, profile.address.coordinates.lat, profile.address.coordinates.lng, profile.contact.email
    // Setup table descriptor for test columns with new schema:
    /**
    Schema:
    message table {
    required int64 id = 1;
    required binary name (STRING) = 2;
    required group profile = 3 {
        optional group address = 4 {
        optional binary street (STRING) = 7;
        optional binary city (STRING) = 8;
        optional group coordinates = 9 {
            optional double lat = 10;
            optional double lng = 11;
        }
        }
        optional group contact = 5 {
        optional binary email (STRING) = 12;
        optional group phone = 13 {
            optional binary country_code (STRING) = 14;
            optional binary number (STRING) = 15;
        }
        }
        optional group hobbies (LIST) = 6 {
        repeated group list {
            optional group element = 16 {
            optional binary name (STRING) = 17;
            optional int32 level = 18;
            }
        }
        }
    }
    }
    */

    // Open the Iceberg Parquet test file
    auto local_fs = io::global_local_filesystem();
    io::FileReaderSPtr file_reader;
    std::string test_file =
            "./be/test/exec/test_data/complex_user_profiles_iceberg_parquet/data/"
            "00000-0-a0022aad-d3b6-4e73-b181-f0a09aac7034-0-00001.parquet";
    auto st = local_fs->open_file(test_file, &file_reader);
    if (!st.ok()) {
        GTEST_SKIP() << "Test file not found: " << test_file;
        return;
    }

    // Setup runtime state
    RuntimeState runtime_state((TQueryGlobals()));

    // Setup scan parameters
    TFileScanRangeParams scan_params;
    scan_params.format_type = TFileFormatType::FORMAT_PARQUET;

    TFileRangeDesc scan_range;
    scan_range.start_offset = 0;
    scan_range.size = file_reader->size(); // Read entire file
    scan_range.path = test_file;

    // Create mock profile
    RuntimeProfile profile("test_profile");

    // Create ParquetReader as the underlying file format reader
    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);

    auto generic_reader = ParquetReader::create_unique(&profile, scan_params, scan_range, 1024,
                                                       &ctz, nullptr, &runtime_state, cache.get());
    ASSERT_NE(generic_reader, nullptr);

    // Set file reader for the generic reader
    auto parquet_reader = static_cast<ParquetReader*>(generic_reader.get());
    parquet_reader->set_file_reader(file_reader);

    // Create IcebergParquetReader
    auto iceberg_reader = std::make_unique<IcebergParquetReader>(
            std::move(generic_reader), &profile, &runtime_state, scan_params, scan_range, nullptr,
            nullptr, cache.get());

    // Create complex struct types using helper function
    DataTypePtr coordinates_struct_type, address_struct_type, phone_struct_type;
    DataTypePtr contact_struct_type, hobby_element_struct_type, hobbies_array_type;
    DataTypePtr profile_struct_type, name_type;
    create_complex_struct_types(coordinates_struct_type, address_struct_type, phone_struct_type,
                                contact_struct_type, hobby_element_struct_type, hobbies_array_type,
                                profile_struct_type, name_type);

    // Create tuple descriptor using helper function
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    const TupleDescriptor* tuple_descriptor =
            create_tuple_descriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc);

    VExprContextSPtrs conjuncts; // Empty conjuncts for this test
    std::vector<std::string> table_col_names = {"name", "profile"};
    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {
            {"name", 0},
            {"profile", 1},
    };
    const RowDescriptor* row_descriptor = nullptr;
    const std::unordered_map<std::string, int>* colname_to_slot_id = nullptr;
    const VExprContextSPtrs* not_single_slot_filter_conjuncts = nullptr;
    const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts = nullptr;

    phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>> tmp;
    st = iceberg_reader->init_reader(table_col_names, &col_name_to_block_idx, conjuncts, tmp,
                                     tuple_descriptor, row_descriptor, colname_to_slot_id,
                                     not_single_slot_filter_conjuncts, slot_id_to_filter_conjuncts);
    ASSERT_TRUE(st.ok()) << st;

    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;
    ASSERT_TRUE(iceberg_reader->set_fill_columns(partition_columns, missing_columns).ok());

    // Create block for reading nested structure (not flattened)
    Block block;
    {
        MutableColumnPtr name_column = name_type->create_column();
        block.insert(ColumnWithTypeAndName(std::move(name_column), name_type, "name"));
        // Add profile column (nested struct)
        MutableColumnPtr profile_column = profile_struct_type->create_column();
        block.insert(
                ColumnWithTypeAndName(std::move(profile_column), profile_struct_type, "profile"));
    }

    // Read data from the file
    size_t read_rows = 0;
    bool eof = false;
    st = iceberg_reader->get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(st.ok()) << st;

    // Verify test results using helper function
    verify_test_results(block, read_rows);
}

TEST_F(IcebergReaderTest, rejects_missing_required_nested_field_before_parquet_lazy_read) {
    const std::string test_file =
            "./be/test/exec/test_data/complex_user_profiles_iceberg_parquet/data/"
            "00000-0-a0022aad-d3b6-4e73-b181-f0a09aac7034-0-00001.parquet";
    io::FileReaderSPtr file_reader;
    const auto open_status = io::global_local_filesystem()->open_file(test_file, &file_reader);
    if (!open_status.ok()) {
        GTEST_SKIP() << "Test file not found: " << test_file;
    }

    const auto status = init_missing_required_nested_reader(TFileFormatType::FORMAT_PARQUET,
                                                            test_file, file_reader);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("required_added"), std::string::npos);
    EXPECT_NE(status.to_string().find("has no initial default"), std::string::npos);
}

// Test reading real Iceberg Orc file using IcebergTableReader
TEST_F(IcebergReaderTest, read_iceberg_orc_file) {
    // Read only: name, profile.address.coordinates.lat, profile.address.coordinates.lng, profile.contact.email
    // Setup table descriptor for test columns with new schema:
    /**
    Schema:
    message table {
    required int64 id = 1;
    required binary name (STRING) = 2;
    required group profile = 3 {
        optional group address = 4 {
        optional binary street (STRING) = 7;
        optional binary city (STRING) = 8;
        optional group coordinates = 9 {
            optional double lat = 10;
            optional double lng = 11;
        }
        }
        optional group contact = 5 {
        optional binary email (STRING) = 12;
        optional group phone = 13 {
            optional binary country_code (STRING) = 14;
            optional binary number (STRING) = 15;
        }
        }
        optional group hobbies (LIST) = 6 {
        repeated group list {
            optional group element = 16 {
            optional binary name (STRING) = 17;
            optional int32 level = 18;
            }
        }
        }
    }
    }
    */

    // Open the Iceberg Orc test file
    auto local_fs = io::global_local_filesystem();
    io::FileReaderSPtr file_reader;
    std::string test_file =
            "./be/test/exec/test_data/complex_user_profiles_iceberg_orc/data/"
            "00000-0-e4897963-0081-4127-bebe-35dc7dc1edeb-0-00001.orc";
    auto st = local_fs->open_file(test_file, &file_reader);
    if (!st.ok()) {
        GTEST_SKIP() << "Test file not found: " << test_file;
        return;
    }

    // Setup runtime state
    RuntimeState runtime_state((TQueryGlobals()));

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

    auto generic_reader = OrcReader::create_unique(&profile, &runtime_state, scan_params,
                                                   scan_range, 1024, "CST", nullptr, cache.get());
    ASSERT_NE(generic_reader, nullptr);

    // Create IcebergOrcReader
    auto iceberg_reader = std::make_unique<IcebergOrcReader>(
            std::move(generic_reader), &profile, &runtime_state, scan_params, scan_range, nullptr,
            nullptr, cache.get());

    // Create complex struct types using helper function
    DataTypePtr coordinates_struct_type, address_struct_type, phone_struct_type;
    DataTypePtr contact_struct_type, hobby_element_struct_type, hobbies_array_type;
    DataTypePtr profile_struct_type, name_type;
    create_complex_struct_types(coordinates_struct_type, address_struct_type, phone_struct_type,
                                contact_struct_type, hobby_element_struct_type, hobbies_array_type,
                                profile_struct_type, name_type);

    // Create tuple descriptor using helper function
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    const TupleDescriptor* tuple_descriptor =
            create_tuple_descriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc);

    VExprContextSPtrs conjuncts; // Empty conjuncts for this test
    std::vector<std::string> table_col_names = {"name", "profile"};
    const RowDescriptor* row_descriptor = nullptr;
    const std::unordered_map<std::string, int>* colname_to_slot_id = nullptr;
    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {
            {"name", 0},
            {"profile", 1},
    };
    const VExprContextSPtrs* not_single_slot_filter_conjuncts = nullptr;
    const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts = nullptr;

    st = iceberg_reader->init_reader(table_col_names, &col_name_to_block_idx, conjuncts,
                                     tuple_descriptor, row_descriptor, colname_to_slot_id,
                                     not_single_slot_filter_conjuncts, slot_id_to_filter_conjuncts);
    ASSERT_TRUE(st.ok()) << st;

    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;
    ASSERT_TRUE(iceberg_reader->set_fill_columns(partition_columns, missing_columns).ok());

    // Create block for reading nested structure (not flattened)
    Block block;
    {
        MutableColumnPtr name_column = name_type->create_column();
        block.insert(ColumnWithTypeAndName(std::move(name_column), name_type, "name"));
        // Add profile column (nested struct)
        MutableColumnPtr profile_column = profile_struct_type->create_column();
        block.insert(
                ColumnWithTypeAndName(std::move(profile_column), profile_struct_type, "profile"));
    }

    // Read data from the file
    size_t read_rows = 0;
    bool eof = false;
    st = iceberg_reader->get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(st.ok()) << st;

    // Verify test results using helper function
    verify_test_results(block, read_rows);
}

TEST_F(IcebergReaderTest, rejects_missing_required_nested_field_before_orc_lazy_read) {
    const std::string test_file =
            "./be/test/exec/test_data/complex_user_profiles_iceberg_orc/data/"
            "00000-0-e4897963-0081-4127-bebe-35dc7dc1edeb-0-00001.orc";
    io::FileReaderSPtr file_reader;
    const auto open_status = io::global_local_filesystem()->open_file(test_file, &file_reader);
    if (!open_status.ok()) {
        GTEST_SKIP() << "Test file not found: " << test_file;
    }

    const auto status = init_missing_required_nested_reader(TFileFormatType::FORMAT_ORC, test_file,
                                                            file_reader);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("required_added"), std::string::npos);
    EXPECT_NE(status.to_string().find("has no initial default"), std::string::npos);
}

} // namespace doris
