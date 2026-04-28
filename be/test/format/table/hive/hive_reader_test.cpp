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

#include "format/table/hive_reader.h"

#include <cctz/time_zone.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/consts.h"
#include "common/object_pool.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "format/orc/vorc_reader.h"
#include "format/parquet/vparquet_reader.h"
#include "format/table/reader_test_util.h"
#include "io/fs/file_meta_cache.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/olap_scan_common.h"
#include "storage/segment/column_reader.h"
#include "storage/utils.h"
#include "util/timezone_utils.h"

namespace doris::table {

class HiveReaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        cache = std::make_unique<doris::FileMetaCache>(1024);

        // Setup timezone
        doris::TimezoneUtils::find_cctz_time_zone(doris::TimezoneUtils::default_time_zone,
                                                  timezone_obj);
    }

    void TearDown() override { cache.reset(); }

    // Helper function to create complex struct types for testing
    void create_complex_struct_types(DataTypePtr& coordinates_struct_type,
                                     DataTypePtr& address_struct_type,
                                     DataTypePtr& phone_struct_type,
                                     DataTypePtr& contact_struct_type,
                                     DataTypePtr& hobby_element_struct_type,
                                     DataTypePtr& hobbies_array_type,
                                     DataTypePtr& profile_struct_type, DataTypePtr& name_type) {
        doris::reader_test::create_complex_user_profile_types(
                coordinates_struct_type, address_struct_type, phone_struct_type,
                contact_struct_type, hobby_element_struct_type, hobbies_array_type,
                profile_struct_type, name_type);
    }

    // Helper function to create tuple descriptor
    const TupleDescriptor* create_tuple_descriptor(
            DescriptorTbl** desc_tbl, ObjectPool& obj_pool, TDescriptorTable& t_desc_table,
            TTableDescriptor& t_table_desc, const std::vector<std::string>& column_names,
            const std::vector<int>& column_positions,
            const std::vector<TPrimitiveType::type>& column_types) {
        // Create table descriptor with complex schema
        auto create_table_desc = [](TDescriptorTable& t_desc_table, TTableDescriptor& t_table_desc,
                                    const std::vector<std::string>& table_column_names,
                                    const std::vector<int>& table_column_positions,
                                    const std::vector<TPrimitiveType::type>& types) {
            t_table_desc.__set_id(0);
            t_table_desc.__set_tableType(TTableType::OLAP_TABLE);
            t_table_desc.__set_numCols(0);
            t_table_desc.__set_numClusteringCols(0);
            t_desc_table.tableDescriptors.push_back(t_table_desc);
            t_desc_table.__isset.tableDescriptors = true;
            for (int i = 0; i < table_column_names.size(); i++) {
                TSlotDescriptor tslot_desc;
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
                tslot_desc.__set_id(i);
                tslot_desc.__set_parent(0);
                tslot_desc.__set_colName(table_column_names[i]);
                tslot_desc.__set_columnPos(table_column_positions[i]);
                tslot_desc.__set_byteOffset(0);
                tslot_desc.__set_nullIndicatorByte(0);
                tslot_desc.__set_nullIndicatorBit(-1);
                tslot_desc.__set_slotIdx(0);
                tslot_desc.__set_isMaterialized(true);
                // Set column_access_paths only for the profile field
                if (table_column_names[i] == "profile") {
                    {
                        std::vector<TColumnAccessPath> access_paths;
                        // address.coordinates.lat
                        TColumnAccessPath path1;
                        path1.__set_type(doris::TAccessPathType::DATA);
                        TDataAccessPath data_path1;
                        data_path1.__set_path({"profile", "address", "coordinates", "lat"});
                        path1.__set_data_access_path(data_path1);
                        access_paths.push_back(path1);
                        // address.coordinates.lng
                        TColumnAccessPath path2;
                        path2.__set_type(doris::TAccessPathType::DATA);
                        TDataAccessPath data_path2;
                        data_path2.__set_path({"profile", "address", "coordinates", "lng"});
                        path2.__set_data_access_path(data_path2);
                        access_paths.push_back(path2);
                        // contact.email
                        TColumnAccessPath path3;
                        path3.__set_type(doris::TAccessPathType::DATA);
                        TDataAccessPath data_path3;
                        data_path3.__set_path({"profile", "contact", "email"});
                        path3.__set_data_access_path(data_path3);
                        access_paths.push_back(path3);
                        // hobbies[].element.level
                        TColumnAccessPath path4;
                        path4.__set_type(doris::TAccessPathType::DATA);
                        TDataAccessPath data_path4;
                        data_path4.__set_path({"profile", "hobbies", "*", "level"});
                        path4.__set_data_access_path(data_path4);
                        access_paths.push_back(path4);
                        tslot_desc.__set_all_access_paths(access_paths);
                    }
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

    // Helper function to recursively print column row counts and check size > 0
    void verify_test_results(Block& block, size_t read_rows) {
        doris::reader_test::verify_nested_reader_block(block, read_rows);
    }

    Block make_string_block(const std::vector<std::pair<std::string, DataTypePtr>>& columns) {
        Block block;
        for (const auto& [name, type] : columns) {
            block.insert(ColumnWithTypeAndName(type->create_column(), type, name));
        }
        return block;
    }

    void verify_nullable_string_column_values(const Block& block, const std::string& column_name,
                                              const std::vector<std::string>& expected_values) {
        const auto& column = block.get_by_position(block.get_position_by_name(column_name));
        auto* nullable = check_and_get_column<ColumnNullable>(column.column.get());
        ASSERT_NE(nullable, nullptr);
        const auto& nested = static_cast<const ColumnString&>(nullable->get_nested_column());
        ASSERT_EQ(nullable->size(), expected_values.size());
        for (size_t i = 0; i < expected_values.size(); ++i) {
            ASSERT_FALSE(nullable->is_null_at(i));
            EXPECT_EQ(nested.get_data_at(i).to_string(), expected_values[i]);
        }
    }

    void verify_nullable_string_column_has_rows(const Block& block, const std::string& column_name,
                                                size_t expected_rows) {
        const auto& column = block.get_by_position(block.get_position_by_name(column_name));
        auto* nullable = check_and_get_column<ColumnNullable>(column.column.get());
        ASSERT_NE(nullable, nullptr);
        const auto& nested = static_cast<const ColumnString&>(nullable->get_nested_column());
        ASSERT_EQ(nullable->size(), expected_rows);
        ASSERT_EQ(nested.size(), expected_rows);
        if (expected_rows > 0) {
            EXPECT_FALSE(nullable->is_null_at(0));
        }
    }

    void verify_nullable_string_column_all_null(const Block& block, const std::string& column_name,
                                                size_t expected_rows) {
        const auto& column = block.get_by_position(block.get_position_by_name(column_name));
        auto* nullable = check_and_get_column<ColumnNullable>(column.column.get());
        ASSERT_NE(nullable, nullptr);
        ASSERT_EQ(nullable->size(), expected_rows);
        for (size_t i = 0; i < expected_rows; ++i) {
            EXPECT_TRUE(nullable->is_null_at(i));
        }
    }

    void verify_global_rowid_column(const Block& block, const std::string& column_name,
                                    uint8_t expected_version, int64_t expected_backend_id,
                                    uint32_t expected_file_id) {
        const auto& column = block.get_by_position(block.get_position_by_name(column_name));
        const auto& string_column = static_cast<const ColumnString&>(*column.column);
        for (size_t i = 0; i < string_column.size(); ++i) {
            ASSERT_EQ(string_column.get_data_at(i).size, sizeof(GlobalRowLoacationV2));
            auto location = *reinterpret_cast<const GlobalRowLoacationV2*>(
                    string_column.get_data_at(i).data);
            EXPECT_EQ(location.version, expected_version);
            EXPECT_EQ(location.backend_id, expected_backend_id);
            EXPECT_EQ(location.file_id, expected_file_id);
            EXPECT_EQ(location.row_id, static_cast<uint32_t>(i));
        }
    }

    void read_hive_parquet_block(
            const std::string& actual_file, TFileRangeDesc scan_range,
            const TupleDescriptor* tuple_descriptor,
            const std::unordered_map<std::string, uint32_t>& col_name_to_block_idx,
            std::vector<ColumnDescriptor> column_descs,
            const std::vector<std::pair<std::string, DataTypePtr>>& block_columns,
            size_t* read_rows, Block* block,
            const std::function<void(TFileScanRangeParams*, RuntimeState*)>& customize_context =
                    nullptr,
            const std::function<void(HiveParquetReader*)>& customize_reader = nullptr) {
        auto local_fs = io::global_local_filesystem();
        int64_t file_size = 0;
        auto st = local_fs->file_size(actual_file, &file_size);
        if (!st.ok()) {
            GTEST_SKIP() << "Test file not found: " << actual_file;
            return;
        }

        RuntimeState runtime_state((TQueryOptions()), TQueryGlobals());
        TFileScanRangeParams scan_params;
        scan_params.format_type = TFileFormatType::FORMAT_PARQUET;
        if (customize_context) {
            customize_context(&scan_params, &runtime_state);
        }

        scan_range.start_offset = 0;
        scan_range.size = file_size;
        scan_range.path = actual_file;

        RuntimeProfile profile("test_profile");
        cctz::time_zone ctz;
        TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
        auto hive_reader =
                std::make_unique<HiveParquetReader>(&profile, scan_params, scan_range, 1024, &ctz,
                                                    nullptr, &runtime_state, nullptr, cache.get());
        ASSERT_NE(hive_reader, nullptr);
        if (customize_reader) {
            customize_reader(hive_reader.get());
        }

        ParquetInitContext pq_ctx;
        pq_ctx.column_descs = &column_descs;
        pq_ctx.col_name_to_block_idx =
                const_cast<std::unordered_map<std::string, uint32_t>*>(&col_name_to_block_idx);
        pq_ctx.tuple_descriptor = tuple_descriptor;
        pq_ctx.params = &scan_params;
        pq_ctx.range = &scan_range;
        st = hive_reader->init_reader(&pq_ctx);
        ASSERT_TRUE(st.ok()) << st;

        *block = make_string_block(block_columns);
        bool eof = false;
        st = hive_reader->get_next_block(block, read_rows, &eof);
        ASSERT_TRUE(st.ok()) << st;
    }

    void read_hive_orc_block(
            const std::string& actual_file, TFileRangeDesc scan_range,
            const TupleDescriptor* tuple_descriptor,
            const std::unordered_map<std::string, uint32_t>& col_name_to_block_idx,
            std::vector<ColumnDescriptor> column_descs,
            const std::vector<std::pair<std::string, DataTypePtr>>& block_columns,
            size_t* read_rows, Block* block,
            const std::function<void(TFileScanRangeParams*, RuntimeState*)>& customize_context =
                    nullptr,
            const std::function<void(HiveOrcReader*)>& customize_reader = nullptr) {
        auto local_fs = io::global_local_filesystem();
        int64_t file_size = 0;
        auto st = local_fs->file_size(actual_file, &file_size);
        if (!st.ok()) {
            GTEST_SKIP() << "Test file not found: " << actual_file;
            return;
        }

        RuntimeState runtime_state((TQueryOptions()), TQueryGlobals());
        TFileScanRangeParams scan_params;
        scan_params.format_type = TFileFormatType::FORMAT_ORC;
        if (customize_context) {
            customize_context(&scan_params, &runtime_state);
        }

        scan_range.start_offset = 0;
        scan_range.size = file_size;
        scan_range.path = actual_file;

        RuntimeProfile profile("test_profile");
        auto hive_reader =
                std::make_unique<HiveOrcReader>(&profile, &runtime_state, scan_params, scan_range,
                                                1024, "CST", nullptr, nullptr, cache.get());
        ASSERT_NE(hive_reader, nullptr);
        if (customize_reader) {
            customize_reader(hive_reader.get());
        }

        OrcInitContext orc_ctx;
        orc_ctx.column_descs = &column_descs;
        orc_ctx.col_name_to_block_idx =
                const_cast<std::unordered_map<std::string, uint32_t>*>(&col_name_to_block_idx);
        orc_ctx.tuple_descriptor = tuple_descriptor;
        orc_ctx.params = &scan_params;
        orc_ctx.range = &scan_range;
        st = hive_reader->init_reader(&orc_ctx);
        ASSERT_TRUE(st.ok()) << st;

        *block = make_string_block(block_columns);
        bool eof = false;
        st = hive_reader->get_next_block(block, read_rows, &eof);
        ASSERT_TRUE(st.ok()) << st;
    }

    void read_hive_parquet_test_file(const std::string& test_file) {
        auto local_fs = io::global_local_filesystem();
        int64_t file_size = 0;
        auto st = local_fs->file_size(test_file, &file_size);
        if (!st.ok()) {
            GTEST_SKIP() << "Test file not found: " << test_file;
        }

        RuntimeState runtime_state((TQueryOptions()), TQueryGlobals());
        TFileScanRangeParams scan_params;
        scan_params.format_type = TFileFormatType::FORMAT_PARQUET;

        TFileRangeDesc scan_range;
        scan_range.start_offset = 0;
        scan_range.size = file_size;
        scan_range.path = test_file;

        RuntimeProfile profile("test_profile");
        cctz::time_zone ctz;
        TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
        auto hive_reader =
                std::make_unique<HiveParquetReader>(&profile, scan_params, scan_range, 1024, &ctz,
                                                    nullptr, &runtime_state, nullptr, cache.get());
        ASSERT_NE(hive_reader, nullptr);

        DataTypePtr coordinates_struct_type, address_struct_type, phone_struct_type;
        DataTypePtr contact_struct_type, hobby_element_struct_type, hobbies_array_type;
        DataTypePtr profile_struct_type, name_type;
        create_complex_struct_types(coordinates_struct_type, address_struct_type, phone_struct_type,
                                    contact_struct_type, hobby_element_struct_type,
                                    hobbies_array_type, profile_struct_type, name_type);

        DescriptorTbl* desc_tbl;
        ObjectPool obj_pool;
        TDescriptorTable t_desc_table;
        TTableDescriptor t_table_desc;
        std::vector<std::string> table_column_names = {"name", "profile"};
        std::vector<int> table_column_positions = {1, 2};
        std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING,
                                                                TPrimitiveType::STRUCT};
        const TupleDescriptor* tuple_descriptor = create_tuple_descriptor(
                &desc_tbl, obj_pool, t_desc_table, t_table_desc, table_column_names,
                table_column_positions, table_column_types);

        std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0},
                                                                           {"profile", 1}};

        ParquetInitContext pq_ctx;
        pq_ctx.column_names = table_column_names;
        pq_ctx.col_name_to_block_idx = &col_name_to_block_idx;
        pq_ctx.tuple_descriptor = tuple_descriptor;
        pq_ctx.params = &scan_params;
        pq_ctx.range = &scan_range;
        st = hive_reader->init_reader(&pq_ctx);
        ASSERT_TRUE(st.ok()) << st;

        Block block = doris::reader_test::make_name_profile_block(name_type, profile_struct_type);
        size_t read_rows = 0;
        bool eof = false;
        st = hive_reader->get_next_block(&block, &read_rows, &eof);
        ASSERT_TRUE(st.ok()) << st;
        verify_test_results(block, read_rows);
    }

    void read_hive_orc_test_file(const std::string& test_file) {
        auto local_fs = io::global_local_filesystem();
        int64_t file_size = 0;
        auto st = local_fs->file_size(test_file, &file_size);
        if (!st.ok()) {
            GTEST_SKIP() << "Test file not found: " << test_file;
        }

        RuntimeState runtime_state((TQueryOptions()), TQueryGlobals());
        TFileScanRangeParams scan_params;
        scan_params.format_type = TFileFormatType::FORMAT_ORC;

        TFileRangeDesc scan_range;
        scan_range.start_offset = 0;
        scan_range.size = file_size;
        scan_range.path = test_file;

        RuntimeProfile profile("test_profile");
        auto hive_reader =
                std::make_unique<HiveOrcReader>(&profile, &runtime_state, scan_params, scan_range,
                                                1024, "CST", nullptr, nullptr, cache.get());
        ASSERT_NE(hive_reader, nullptr);

        DataTypePtr coordinates_struct_type, address_struct_type, phone_struct_type;
        DataTypePtr contact_struct_type, hobby_element_struct_type, hobbies_array_type;
        DataTypePtr profile_struct_type, name_type;
        create_complex_struct_types(coordinates_struct_type, address_struct_type, phone_struct_type,
                                    contact_struct_type, hobby_element_struct_type,
                                    hobbies_array_type, profile_struct_type, name_type);

        DescriptorTbl* desc_tbl;
        ObjectPool obj_pool;
        TDescriptorTable t_desc_table;
        TTableDescriptor t_table_desc;
        std::vector<std::string> table_column_names = {"name", "profile"};
        std::vector<int> table_column_positions = {1, 2};
        std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING,
                                                                TPrimitiveType::STRUCT};
        const TupleDescriptor* tuple_descriptor = create_tuple_descriptor(
                &desc_tbl, obj_pool, t_desc_table, t_table_desc, table_column_names,
                table_column_positions, table_column_types);

        std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0},
                                                                           {"profile", 1}};

        OrcInitContext orc_ctx;
        orc_ctx.column_names = table_column_names;
        orc_ctx.col_name_to_block_idx = &col_name_to_block_idx;
        orc_ctx.tuple_descriptor = tuple_descriptor;
        orc_ctx.params = &scan_params;
        orc_ctx.range = &scan_range;
        st = hive_reader->init_reader(&orc_ctx);
        ASSERT_TRUE(st.ok()) << st;

        Block block = doris::reader_test::make_name_profile_block(name_type, profile_struct_type);
        size_t read_rows = 0;
        bool eof = false;
        st = hive_reader->get_next_block(&block, &read_rows, &eof);
        ASSERT_TRUE(st.ok()) << st;
        verify_test_results(block, read_rows);
    }

    std::unique_ptr<doris::FileMetaCache> cache;
    cctz::time_zone timezone_obj;
};

TEST_F(HiveReaderTest, read_hive_parquet_file) {
    read_hive_parquet_test_file(
            "./be/test/exec/test_data/complex_user_profiles_iceberg_parquet/data/"
            "00000-0-a0022aad-d3b6-4e73-b181-f0a09aac7034-0-00001.parquet");
}

TEST_F(HiveReaderTest, read_hive_orc_file) {
    read_hive_orc_test_file(
            "./be/test/exec/test_data/complex_user_profiles_iceberg_orc/data/"
            "00000-0-e4897963-0081-4127-bebe-35dc7dc1edeb-0-00001.orc");
}

TEST_F(HiveReaderTest, read_nested_hive_parquet_file) {
    read_hive_parquet_test_file(
            "./be/test/exec/test_data/nested_user_profiles_parquet/"
            "part-00000-64a7a390-1a03-4efc-ab51-557e9369a1f9-c000.snappy.parquet");
}

TEST_F(HiveReaderTest, read_nested_hive_orc_file) {
    read_hive_orc_test_file(
            "./be/test/exec/test_data/nested_user_profiles_orc/"
            "part-00000-62614f23-05d1-4043-a533-b155ef52b720-c000.snappy.orc");
}

TEST_F(HiveReaderTest, fills_partition_column_from_scan_range_for_parquet) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"name", "year"};
    std::vector<int> table_column_positions = {1, 2};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING,
                                                            TPrimitiveType::STRING};
    const TupleDescriptor* tuple_descriptor =
            create_tuple_descriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc,
                                    table_column_names, table_column_positions, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0}, {"year", 1}};
    std::vector<ColumnDescriptor> column_descs = {
            {"name", tuple_descriptor->slots()[0], ColumnCategory::REGULAR, nullptr},
            {"year", tuple_descriptor->slots()[1], ColumnCategory::PARTITION_KEY, nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "file:///warehouse/nested_user_profiles/year=2024/part-00000-64a7a390-1a03-4efc-"
            "ab51-557e9369a1f9-c000.snappy.parquet";
    scan_range.__set_columns_from_path_keys({"year"});
    scan_range.__set_columns_from_path({"2024"});

    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    size_t read_rows = 0;
    Block block;
    read_hive_parquet_block(
            "./be/test/exec/test_data/nested_user_profiles_parquet/"
            "part-00000-64a7a390-1a03-4efc-ab51-557e9369a1f9-c000.snappy.parquet",
            scan_range, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"name", nullable_string}, {"year", nullable_string}}, &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_values(block, "year",
                                         std::vector<std::string>(read_rows, "2024"));
}

TEST_F(HiveReaderTest, fills_partition_column_from_scan_range_for_orc) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"name", "year"};
    std::vector<int> table_column_positions = {1, 2};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING,
                                                            TPrimitiveType::STRING};
    const TupleDescriptor* tuple_descriptor =
            create_tuple_descriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc,
                                    table_column_names, table_column_positions, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0}, {"year", 1}};
    std::vector<ColumnDescriptor> column_descs = {
            {"name", tuple_descriptor->slots()[0], ColumnCategory::REGULAR, nullptr},
            {"year", tuple_descriptor->slots()[1], ColumnCategory::PARTITION_KEY, nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_orc/"
            "part-00000-62614f23-05d1-4043-a533-b155ef52b720-c000.snappy.orc";
    scan_range.__set_columns_from_path_keys({"year"});
    scan_range.__set_columns_from_path({"2024"});

    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    size_t read_rows = 0;
    Block block;
    read_hive_orc_block(
            "./be/test/exec/test_data/nested_user_profiles_orc/"
            "part-00000-62614f23-05d1-4043-a533-b155ef52b720-c000.snappy.orc",
            scan_range, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"name", nullable_string}, {"year", nullable_string}}, &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_values(block, "year",
                                         std::vector<std::string>(read_rows, "2024"));
}

TEST_F(HiveReaderTest, fills_missing_column_with_null_for_parquet) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"name", "country"};
    std::vector<int> table_column_positions = {1, 2};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING,
                                                            TPrimitiveType::STRING};
    const TupleDescriptor* tuple_descriptor =
            create_tuple_descriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc,
                                    table_column_names, table_column_positions, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0}, {"country", 1}};
    std::vector<ColumnDescriptor> column_descs = {
            {"name", tuple_descriptor->slots()[0], ColumnCategory::REGULAR, nullptr},
            {"country", tuple_descriptor->slots()[1], ColumnCategory::REGULAR, nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_parquet/"
            "part-00000-64a7a390-1a03-4efc-ab51-557e9369a1f9-c000.snappy.parquet";

    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    size_t read_rows = 0;
    Block block;
    read_hive_parquet_block(scan_range.path, scan_range, tuple_descriptor, col_name_to_block_idx,
                            column_descs, {{"name", nullable_string}, {"country", nullable_string}},
                            &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_all_null(block, "country", read_rows);
}

TEST_F(HiveReaderTest, fills_missing_column_with_null_for_orc) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"name", "country"};
    std::vector<int> table_column_positions = {1, 2};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING,
                                                            TPrimitiveType::STRING};
    const TupleDescriptor* tuple_descriptor =
            create_tuple_descriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc,
                                    table_column_names, table_column_positions, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0}, {"country", 1}};
    std::vector<ColumnDescriptor> column_descs = {
            {"name", tuple_descriptor->slots()[0], ColumnCategory::REGULAR, nullptr},
            {"country", tuple_descriptor->slots()[1], ColumnCategory::REGULAR, nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_orc/"
            "part-00000-62614f23-05d1-4043-a533-b155ef52b720-c000.snappy.orc";

    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    size_t read_rows = 0;
    Block block;
    read_hive_orc_block(scan_range.path, scan_range, tuple_descriptor, col_name_to_block_idx,
                        column_descs, {{"name", nullable_string}, {"country", nullable_string}},
                        &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_all_null(block, "country", read_rows);
}

TEST_F(HiveReaderTest, reads_generated_column_from_file_for_parquet) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"name"};
    std::vector<int> table_column_positions = {1};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING};
    const TupleDescriptor* tuple_descriptor =
            create_tuple_descriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc,
                                    table_column_names, table_column_positions, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0}};
    std::vector<ColumnDescriptor> column_descs = {
            {"name", tuple_descriptor->slots()[0], ColumnCategory::GENERATED, nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_parquet/"
            "part-00000-64a7a390-1a03-4efc-ab51-557e9369a1f9-c000.snappy.parquet";

    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    size_t read_rows = 0;
    Block block;
    read_hive_parquet_block(scan_range.path, scan_range, tuple_descriptor, col_name_to_block_idx,
                            column_descs, {{"name", nullable_string}}, &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "name", read_rows);
}

TEST_F(HiveReaderTest, reads_generated_column_from_file_for_orc) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"name"};
    std::vector<int> table_column_positions = {1};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING};
    const TupleDescriptor* tuple_descriptor =
            create_tuple_descriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc,
                                    table_column_names, table_column_positions, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0}};
    std::vector<ColumnDescriptor> column_descs = {
            {"name", tuple_descriptor->slots()[0], ColumnCategory::GENERATED, nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_orc/"
            "part-00000-62614f23-05d1-4043-a533-b155ef52b720-c000.snappy.orc";

    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    size_t read_rows = 0;
    Block block;
    read_hive_orc_block(scan_range.path, scan_range, tuple_descriptor, col_name_to_block_idx,
                        column_descs, {{"name", nullable_string}}, &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "name", read_rows);
}

TEST_F(HiveReaderTest, uses_column_indexes_when_parquet_name_mapping_is_disabled) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"alias_name"};
    std::vector<int> table_column_positions = {0};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING};
    const TupleDescriptor* tuple_descriptor =
            create_tuple_descriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc,
                                    table_column_names, table_column_positions, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"alias_name", 0}};
    std::vector<ColumnDescriptor> column_descs = {
            {"alias_name", tuple_descriptor->slots()[0], ColumnCategory::REGULAR, nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_parquet/"
            "part-00000-64a7a390-1a03-4efc-ab51-557e9369a1f9-c000.snappy.parquet";

    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    size_t read_rows = 0;
    Block block;
    read_hive_parquet_block(scan_range.path, scan_range, tuple_descriptor, col_name_to_block_idx,
                            column_descs, {{"alias_name", nullable_string}}, &read_rows, &block,
                            [](TFileScanRangeParams* scan_params, RuntimeState* runtime_state) {
                                scan_params->column_idxs = {0};
                                TQueryOptions query_options = runtime_state->query_options();
                                query_options.__set_hive_parquet_use_column_names(false);
                                runtime_state->set_query_options(query_options);
                            });

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "alias_name", read_rows);
}

TEST_F(HiveReaderTest, uses_column_indexes_when_orc_name_mapping_is_disabled) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"alias_name"};
    std::vector<int> table_column_positions = {0};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING};
    const TupleDescriptor* tuple_descriptor =
            create_tuple_descriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc,
                                    table_column_names, table_column_positions, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"alias_name", 0}};
    std::vector<ColumnDescriptor> column_descs = {
            {"alias_name", tuple_descriptor->slots()[0], ColumnCategory::REGULAR, nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_orc/"
            "part-00000-62614f23-05d1-4043-a533-b155ef52b720-c000.snappy.orc";

    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    size_t read_rows = 0;
    Block block;
    read_hive_orc_block(scan_range.path, scan_range, tuple_descriptor, col_name_to_block_idx,
                        column_descs, {{"alias_name", nullable_string}}, &read_rows, &block,
                        [](TFileScanRangeParams* scan_params, RuntimeState* runtime_state) {
                            scan_params->column_idxs = {0};
                            TQueryOptions query_options = runtime_state->query_options();
                            query_options.__set_hive_orc_use_column_names(false);
                            runtime_state->set_query_options(query_options);
                        });

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "alias_name", read_rows);
}

TEST_F(HiveReaderTest, fills_global_rowid_synthesized_column_for_parquet) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::string global_rowid_col_name = BeConsts::GLOBAL_ROWID_COL + "test";
    std::vector<std::string> table_column_names = {"name", global_rowid_col_name};
    std::vector<int> table_column_positions = {1, 2};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING,
                                                            TPrimitiveType::STRING};
    const TupleDescriptor* tuple_descriptor =
            create_tuple_descriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc,
                                    table_column_names, table_column_positions, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0},
                                                                       {global_rowid_col_name, 1}};
    std::vector<ColumnDescriptor> column_descs = {
            {"name", tuple_descriptor->slots()[0], ColumnCategory::REGULAR, nullptr},
            {global_rowid_col_name, tuple_descriptor->slots()[1], ColumnCategory::SYNTHESIZED,
             nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_parquet/"
            "part-00000-64a7a390-1a03-4efc-ab51-557e9369a1f9-c000.snappy.parquet";

    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    auto global_rowid_type = std::make_shared<DataTypeString>();
    size_t read_rows = 0;
    Block block;
    constexpr uint8_t kVersion = 1;
    constexpr int64_t kBackendId = 10020;
    constexpr uint32_t kFileId = 31;
    read_hive_parquet_block(scan_range.path, scan_range, tuple_descriptor, col_name_to_block_idx,
                            column_descs,
                            {{"name", nullable_string}, {global_rowid_col_name, global_rowid_type}},
                            &read_rows, &block, nullptr, [&](HiveParquetReader* reader) {
                                reader->set_create_row_id_column_iterator_func([&]() {
                                    return std::make_shared<segment_v2::RowIdColumnIteratorV2>(
                                            kVersion, kBackendId, kFileId);
                                });
                            });

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "name", read_rows);
    verify_global_rowid_column(block, global_rowid_col_name, kVersion, kBackendId, kFileId);
}

TEST_F(HiveReaderTest, fills_global_rowid_synthesized_column_for_orc) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::string global_rowid_col_name = BeConsts::GLOBAL_ROWID_COL + "test";
    std::vector<std::string> table_column_names = {"name", global_rowid_col_name};
    std::vector<int> table_column_positions = {1, 2};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING,
                                                            TPrimitiveType::STRING};
    const TupleDescriptor* tuple_descriptor =
            create_tuple_descriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc,
                                    table_column_names, table_column_positions, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0},
                                                                       {global_rowid_col_name, 1}};
    std::vector<ColumnDescriptor> column_descs = {
            {"name", tuple_descriptor->slots()[0], ColumnCategory::REGULAR, nullptr},
            {global_rowid_col_name, tuple_descriptor->slots()[1], ColumnCategory::SYNTHESIZED,
             nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_orc/"
            "part-00000-62614f23-05d1-4043-a533-b155ef52b720-c000.snappy.orc";

    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    auto global_rowid_type = std::make_shared<DataTypeString>();
    size_t read_rows = 0;
    Block block;
    constexpr uint8_t kVersion = 1;
    constexpr int64_t kBackendId = 10021;
    constexpr uint32_t kFileId = 32;
    read_hive_orc_block(scan_range.path, scan_range, tuple_descriptor, col_name_to_block_idx,
                        column_descs,
                        {{"name", nullable_string}, {global_rowid_col_name, global_rowid_type}},
                        &read_rows, &block, nullptr, [&](HiveOrcReader* reader) {
                            reader->set_create_row_id_column_iterator_func([&]() {
                                return std::make_shared<segment_v2::RowIdColumnIteratorV2>(
                                        kVersion, kBackendId, kFileId);
                            });
                        });

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "name", read_rows);
    verify_global_rowid_column(block, global_rowid_col_name, kVersion, kBackendId, kFileId);
}

} // namespace doris::table
