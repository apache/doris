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

#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/object_pool.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "format/column_descriptor.h"
#include "format/orc/vorc_reader.h"
#include "format/parquet/vparquet_column_chunk_reader.h"
#include "format/parquet/vparquet_reader.h"
#include "format/table/reader_test_util.h"
#include "gen_cpp/ExternalTableSchema_types.h"
#include "io/fs/file_meta_cache.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/olap_scan_common.h"
#include "storage/segment/column_reader.h"
#include "storage/utils.h"
#include "util/defer_op.h"
#include "util/timezone_utils.h"

namespace doris {

class IcebergReaderTestHelper : public IcebergTableReader {
public:
    using IcebergTableReader::_is_fully_dictionary_encoded;
};

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
            TFileRangeDesc* scan_range, const tparquet::FileMetaData** file_meta_data) {
        auto local_fs = io::global_local_filesystem();
        int64_t file_size = 0;
        auto st = local_fs->file_size(mixed_position_delete_file(), &file_size);
        EXPECT_TRUE(st.ok()) << st;
        if (!st.ok()) {
            return nullptr;
        }

        scan_params->format_type = TFileFormatType::FORMAT_PARQUET;

        scan_range->start_offset = 0;
        scan_range->size = file_size;
        scan_range->path = mixed_position_delete_file();

        auto parquet_reader =
                ParquetReader::create_unique(profile, *scan_params, *scan_range, 1024,
                                             &timezone_obj, nullptr, runtime_state, cache.get());
        EXPECT_NE(parquet_reader, nullptr);
        if (parquet_reader == nullptr) {
            return nullptr;
        }

        ParquetInitContext pq_ctx;
        pq_ctx.column_names = delete_file_column_names;
        pq_ctx.col_name_to_block_idx = &delete_file_col_name_to_block_idx;
        pq_ctx.params = scan_params;
        pq_ctx.range = scan_range;
        st = parquet_reader->init_reader(&pq_ctx);
        EXPECT_TRUE(st.ok()) << st;
        if (!st.ok()) {
            return nullptr;
        }

        // Partition/missing column logic is now inlined in _do_init_reader.

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
        reader_test::create_complex_user_profile_types(
                coordinates_struct_type, address_struct_type, phone_struct_type,
                contact_struct_type, hobby_element_struct_type, hobbies_array_type,
                profile_struct_type, name_type);
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

    const TupleDescriptor* create_simple_tuple_descriptor(
            DescriptorTbl** desc_tbl, ObjectPool& obj_pool, TDescriptorTable& t_desc_table,
            TTableDescriptor& t_table_desc, const std::vector<std::string>& column_names,
            const std::vector<int32_t>& column_unique_ids,
            const std::vector<TPrimitiveType::type>& column_types) {
        t_table_desc.__set_id(0);
        t_table_desc.__set_tableType(TTableType::OLAP_TABLE);
        t_table_desc.__set_numCols(0);
        t_table_desc.__set_numClusteringCols(0);
        t_desc_table.tableDescriptors.push_back(t_table_desc);
        t_desc_table.__isset.tableDescriptors = true;

        for (size_t i = 0; i < column_names.size(); ++i) {
            TSlotDescriptor tslot_desc;
            tslot_desc.__set_id(static_cast<int>(i));
            tslot_desc.__set_parent(0);
            tslot_desc.__set_col_unique_id(column_unique_ids[i]);

            TTypeDesc type;
            if (column_types[i] == TPrimitiveType::STRUCT &&
                column_names[i] == BeConsts::ICEBERG_ROWID_COL) {
                TTypeNode struct_node;
                struct_node.__set_type(TTypeNodeType::STRUCT);
                std::vector<TStructField> struct_fields;
                TStructField file_path_field;
                file_path_field.__set_name("file_path");
                file_path_field.__set_contains_null(false);
                struct_fields.push_back(file_path_field);
                TStructField pos_field;
                pos_field.__set_name("pos");
                pos_field.__set_contains_null(false);
                struct_fields.push_back(pos_field);
                TStructField partition_spec_id_field;
                partition_spec_id_field.__set_name("partition_spec_id");
                partition_spec_id_field.__set_contains_null(false);
                struct_fields.push_back(partition_spec_id_field);
                TStructField partition_data_field;
                partition_data_field.__set_name("partition_data");
                partition_data_field.__set_contains_null(false);
                struct_fields.push_back(partition_data_field);
                struct_node.__set_struct_fields(struct_fields);
                type.types.push_back(struct_node);

                TTypeNode file_path_node;
                file_path_node.__set_type(TTypeNodeType::SCALAR);
                TScalarType file_path_scalar_type;
                file_path_scalar_type.__set_type(TPrimitiveType::STRING);
                file_path_node.__set_scalar_type(file_path_scalar_type);
                type.types.push_back(file_path_node);

                TTypeNode pos_node;
                pos_node.__set_type(TTypeNodeType::SCALAR);
                TScalarType pos_scalar_type;
                pos_scalar_type.__set_type(TPrimitiveType::BIGINT);
                pos_node.__set_scalar_type(pos_scalar_type);
                type.types.push_back(pos_node);

                TTypeNode partition_spec_id_node;
                partition_spec_id_node.__set_type(TTypeNodeType::SCALAR);
                TScalarType partition_spec_id_scalar_type;
                partition_spec_id_scalar_type.__set_type(TPrimitiveType::INT);
                partition_spec_id_node.__set_scalar_type(partition_spec_id_scalar_type);
                type.types.push_back(partition_spec_id_node);

                TTypeNode partition_data_node;
                partition_data_node.__set_type(TTypeNodeType::SCALAR);
                TScalarType partition_data_scalar_type;
                partition_data_scalar_type.__set_type(TPrimitiveType::STRING);
                partition_data_node.__set_scalar_type(partition_data_scalar_type);
                type.types.push_back(partition_data_node);
            } else {
                TTypeNode node;
                node.__set_type(TTypeNodeType::SCALAR);
                TScalarType scalar_type;
                scalar_type.__set_type(column_types[i]);
                node.__set_scalar_type(scalar_type);
                type.types.push_back(node);
            }
            tslot_desc.__set_slotType(type);

            tslot_desc.__set_columnPos(0);
            tslot_desc.__set_byteOffset(0);
            tslot_desc.__set_nullIndicatorByte(0);
            tslot_desc.__set_nullIndicatorBit(-1);
            tslot_desc.__set_colName(column_names[i]);
            tslot_desc.__set_slotIdx(0);
            tslot_desc.__set_isMaterialized(true);
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

        EXPECT_TRUE(DescriptorTbl::create(&obj_pool, t_desc_table, desc_tbl).ok());
        return (*desc_tbl)->get_tuple_descriptor(0);
    }

    // Helper function to verify test results
    void verify_test_results(Block& block, size_t read_rows) {
        reader_test::verify_nested_reader_block(block, read_rows);
    }

    Block make_scalar_block(const std::vector<std::pair<std::string, DataTypePtr>>& columns) {
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

    void verify_nullable_int64_column_sequence(const Block& block, const std::string& column_name,
                                               int64_t start_value) {
        const auto& column = block.get_by_position(block.get_position_by_name(column_name));
        auto* nullable = check_and_get_column<ColumnNullable>(column.column.get());
        ASSERT_NE(nullable, nullptr);
        const auto& nested = static_cast<const ColumnInt64&>(nullable->get_nested_column());
        for (size_t i = 0; i < nullable->size(); ++i) {
            ASSERT_FALSE(nullable->is_null_at(i));
            EXPECT_EQ(nested.get_element(i), start_value + static_cast<int64_t>(i));
        }
    }

    void verify_nullable_int64_column_constant(const Block& block, const std::string& column_name,
                                               int64_t expected_value) {
        const auto& column = block.get_by_position(block.get_position_by_name(column_name));
        auto* nullable = check_and_get_column<ColumnNullable>(column.column.get());
        ASSERT_NE(nullable, nullptr);
        const auto& nested = static_cast<const ColumnInt64&>(nullable->get_nested_column());
        for (size_t i = 0; i < nullable->size(); ++i) {
            ASSERT_FALSE(nullable->is_null_at(i));
            EXPECT_EQ(nested.get_element(i), expected_value);
        }
    }

    void verify_iceberg_rowid_column(const Block& block, const std::string& column_name,
                                     const std::string& expected_file_path,
                                     int32_t expected_partition_spec_id,
                                     const std::string& expected_partition_data_json) {
        const auto& column = block.get_by_position(block.get_position_by_name(column_name));
        auto* struct_column = check_and_get_column<ColumnStruct>(column.column.get());
        ASSERT_NE(struct_column, nullptr);
        ASSERT_EQ(struct_column->tuple_size(), 4);

        const auto& file_path_column =
                assert_cast<const ColumnString&>(struct_column->get_column(0));
        const auto& row_pos_column = assert_cast<const ColumnInt64&>(struct_column->get_column(1));
        const auto& spec_id_column = assert_cast<const ColumnInt32&>(struct_column->get_column(2));
        const auto& partition_data_column =
                assert_cast<const ColumnString&>(struct_column->get_column(3));

        for (size_t i = 0; i < struct_column->size(); ++i) {
            EXPECT_EQ(file_path_column.get_data_at(i).to_string(), expected_file_path);
            EXPECT_EQ(row_pos_column.get_element(i), static_cast<int64_t>(i));
            EXPECT_EQ(spec_id_column.get_element(i), expected_partition_spec_id);
            EXPECT_EQ(partition_data_column.get_data_at(i).to_string(),
                      expected_partition_data_json);
        }
    }

    void verify_global_rowid_column(const Block& block, const std::string& column_name,
                                    uint8_t expected_version, int64_t expected_backend_id,
                                    uint32_t expected_file_id) {
        const auto& column = block.get_by_position(block.get_position_by_name(column_name));
        const auto& string_column = assert_cast<const ColumnString&>(*column.column);
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

    doris::schema::external::TSchema build_history_schema_with_name_field(int32_t field_id) {
        doris::schema::external::TSchema schema;
        schema.__set_schema_id(1);

        TColumnType struct_type;
        struct_type.type = TPrimitiveType::STRUCT;
        doris::schema::external::TStructField root_field;

        TColumnType string_type;
        string_type.type = TPrimitiveType::STRING;
        auto name_field = std::make_shared<doris::schema::external::TField>();
        name_field->name = "name";
        name_field->id = field_id;
        name_field->type = string_type;

        doris::schema::external::TFieldPtr name_ptr;
        name_ptr.__set_field_ptr(name_field);
        root_field.fields.emplace_back(name_ptr);

        schema.__set_root_field(root_field);
        return schema;
    }

    void read_iceberg_parquet_scalar_block(
            const std::string& actual_file, TFileRangeDesc scan_range,
            const TupleDescriptor* tuple_descriptor,
            const std::unordered_map<std::string, uint32_t>& col_name_to_block_idx,
            std::vector<ColumnDescriptor> column_descs,
            const std::vector<std::pair<std::string, DataTypePtr>>& block_columns,
            size_t* read_rows, Block* block,
            const TFileScanRangeParams* custom_scan_params = nullptr,
            const std::function<void(IcebergParquetReader*)>& customize_reader = nullptr) {
        auto local_fs = io::global_local_filesystem();
        int64_t file_size = 0;
        auto st = local_fs->file_size(actual_file, &file_size);
        if (!st.ok()) {
            GTEST_SKIP() << "Test file not found: " << actual_file;
            return;
        }

        RuntimeState runtime_state((TQueryOptions()), TQueryGlobals());
        TFileScanRangeParams scan_params =
                custom_scan_params ? *custom_scan_params : TFileScanRangeParams {};
        scan_params.format_type = TFileFormatType::FORMAT_PARQUET;

        scan_range.start_offset = 0;
        scan_range.size = file_size;
        scan_range.path = actual_file;

        RuntimeProfile profile("test_profile");
        cctz::time_zone ctz;
        TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
        auto iceberg_reader = std::make_unique<IcebergParquetReader>(
                nullptr, &profile, scan_params, scan_range, 1024, &ctz, nullptr, &runtime_state,
                cache.get());
        ASSERT_NE(iceberg_reader, nullptr);
        if (customize_reader) {
            customize_reader(iceberg_reader.get());
        }

        ParquetInitContext pq_ctx;
        pq_ctx.column_descs = &column_descs;
        pq_ctx.col_name_to_block_idx =
                const_cast<std::unordered_map<std::string, uint32_t>*>(&col_name_to_block_idx);
        pq_ctx.tuple_descriptor = tuple_descriptor;
        pq_ctx.params = &scan_params;
        pq_ctx.range = &scan_range;
        st = iceberg_reader->init_reader(&pq_ctx);
        ASSERT_TRUE(st.ok()) << st;

        *block = make_scalar_block(block_columns);
        bool eof = false;
        st = iceberg_reader->get_next_block(block, read_rows, &eof);
        ASSERT_TRUE(st.ok()) << st;
    }

    void read_iceberg_orc_scalar_block(
            const std::string& actual_file, TFileRangeDesc scan_range,
            const TupleDescriptor* tuple_descriptor,
            const std::unordered_map<std::string, uint32_t>& col_name_to_block_idx,
            std::vector<ColumnDescriptor> column_descs,
            const std::vector<std::pair<std::string, DataTypePtr>>& block_columns,
            size_t* read_rows, Block* block,
            const TFileScanRangeParams* custom_scan_params = nullptr,
            const std::function<void(IcebergOrcReader*)>& customize_reader = nullptr) {
        auto local_fs = io::global_local_filesystem();
        int64_t file_size = 0;
        auto st = local_fs->file_size(actual_file, &file_size);
        if (!st.ok()) {
            GTEST_SKIP() << "Test file not found: " << actual_file;
            return;
        }

        RuntimeState runtime_state((TQueryOptions()), TQueryGlobals());
        TFileScanRangeParams scan_params =
                custom_scan_params ? *custom_scan_params : TFileScanRangeParams {};
        scan_params.format_type = TFileFormatType::FORMAT_ORC;

        scan_range.start_offset = 0;
        scan_range.size = file_size;
        scan_range.path = actual_file;

        RuntimeProfile profile("test_profile");
        auto iceberg_reader =
                std::make_unique<IcebergOrcReader>(nullptr, &profile, &runtime_state, scan_params,
                                                   scan_range, 1024, "CST", nullptr, cache.get());
        ASSERT_NE(iceberg_reader, nullptr);
        if (customize_reader) {
            customize_reader(iceberg_reader.get());
        }

        OrcInitContext orc_ctx;
        orc_ctx.column_descs = &column_descs;
        orc_ctx.col_name_to_block_idx =
                const_cast<std::unordered_map<std::string, uint32_t>*>(&col_name_to_block_idx);
        orc_ctx.tuple_descriptor = tuple_descriptor;
        orc_ctx.row_descriptor = nullptr;
        orc_ctx.params = &scan_params;
        orc_ctx.range = &scan_range;
        st = iceberg_reader->init_reader(&orc_ctx);
        ASSERT_TRUE(st.ok()) << st;

        *block = make_scalar_block(block_columns);
        bool eof = false;
        st = iceberg_reader->get_next_block(block, read_rows, &eof);
        ASSERT_TRUE(st.ok()) << st;
    }

    void read_iceberg_parquet_test_file(const std::string& test_file) {
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
        auto iceberg_reader = std::make_unique<IcebergParquetReader>(
                nullptr, &profile, scan_params, scan_range, 1024, &ctz, nullptr, &runtime_state,
                cache.get());
        ASSERT_NE(iceberg_reader, nullptr);

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
        const TupleDescriptor* tuple_descriptor =
                create_tuple_descriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc);

        std::vector<std::string> table_col_names = {"name", "profile"};
        std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0},
                                                                           {"profile", 1}};
        std::vector<ColumnDescriptor> column_descs;
        for (const auto& name : table_col_names) {
            ColumnDescriptor desc;
            desc.name = name;
            column_descs.push_back(desc);
        }

        ParquetInitContext pq_ctx;
        pq_ctx.column_descs = &column_descs;
        pq_ctx.col_name_to_block_idx = &col_name_to_block_idx;
        pq_ctx.tuple_descriptor = tuple_descriptor;
        pq_ctx.params = &scan_params;
        pq_ctx.range = &scan_range;
        st = iceberg_reader->init_reader(&pq_ctx);
        ASSERT_TRUE(st.ok()) << st;

        Block block = reader_test::make_name_profile_block(name_type, profile_struct_type);
        size_t read_rows = 0;
        bool eof = false;
        st = iceberg_reader->get_next_block(&block, &read_rows, &eof);
        ASSERT_TRUE(st.ok()) << st;
        verify_test_results(block, read_rows);
    }

    void read_iceberg_orc_test_file(const std::string& test_file) {
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
        auto iceberg_reader =
                std::make_unique<IcebergOrcReader>(nullptr, &profile, &runtime_state, scan_params,
                                                   scan_range, 1024, "CST", nullptr, cache.get());
        ASSERT_NE(iceberg_reader, nullptr);

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
        const TupleDescriptor* tuple_descriptor =
                create_tuple_descriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc);

        std::vector<std::string> table_col_names = {"name", "profile"};
        std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0},
                                                                           {"profile", 1}};
        std::vector<ColumnDescriptor> column_descs;
        for (const auto& name : table_col_names) {
            ColumnDescriptor desc;
            desc.name = name;
            column_descs.push_back(desc);
        }

        OrcInitContext orc_ctx;
        orc_ctx.column_descs = &column_descs;
        orc_ctx.col_name_to_block_idx = &col_name_to_block_idx;
        orc_ctx.tuple_descriptor = tuple_descriptor;
        orc_ctx.row_descriptor = nullptr;
        orc_ctx.params = &scan_params;
        orc_ctx.range = &scan_range;
        st = iceberg_reader->init_reader(&orc_ctx);
        ASSERT_TRUE(st.ok()) << st;

        Block block = reader_test::make_name_profile_block(name_type, profile_struct_type);
        size_t read_rows = 0;
        bool eof = false;
        st = iceberg_reader->get_next_block(&block, &read_rows, &eof);
        ASSERT_TRUE(st.ok()) << st;
        verify_test_results(block, read_rows);
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
    RuntimeState runtime_state((TQueryOptions()), TQueryGlobals());
    TFileScanRangeParams scan_params;
    TFileRangeDesc scan_range;
    const tparquet::FileMetaData* file_meta_data = nullptr;
    auto parquet_reader = create_delete_file_parquet_reader(&profile, &runtime_state, &scan_params,
                                                            &scan_range, &file_meta_data);
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

TEST_F(IcebergReaderTest, read_iceberg_parquet_file) {
    read_iceberg_parquet_test_file(
            "./be/test/exec/test_data/complex_user_profiles_iceberg_parquet/data/"
            "00000-0-a0022aad-d3b6-4e73-b181-f0a09aac7034-0-00001.parquet");
}

TEST_F(IcebergReaderTest, read_iceberg_orc_file) {
    read_iceberg_orc_test_file(
            "./be/test/exec/test_data/complex_user_profiles_iceberg_orc/data/"
            "00000-0-e4897963-0081-4127-bebe-35dc7dc1edeb-0-00001.orc");
}

TEST_F(IcebergReaderTest, read_nested_iceberg_parquet_file) {
    read_iceberg_parquet_test_file(
            "./be/test/exec/test_data/nested_user_profiles_iceberg_parquet/data/"
            "00000-9-a7e0135f-d581-40e4-8d56-a929aded99e4-0-00001.parquet");
}

TEST_F(IcebergReaderTest, read_nested_iceberg_orc_file) {
    read_iceberg_orc_test_file(
            "./be/test/exec/test_data/nested_user_profiles_iceberg_orc/data/"
            "00000-8-5a144c37-16a4-47c6-96db-0007175b5c90-0-00001.orc");
}

TEST_F(IcebergReaderTest, fills_partition_column_from_scan_range_for_parquet) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"name", "year"};
    std::vector<int32_t> table_column_unique_ids = {2, 100};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING,
                                                            TPrimitiveType::STRING};
    const TupleDescriptor* tuple_descriptor = create_simple_tuple_descriptor(
            &desc_tbl, obj_pool, t_desc_table, t_table_desc, table_column_names,
            table_column_unique_ids, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0}, {"year", 1}};
    std::vector<ColumnDescriptor> column_descs = {
            {"name", tuple_descriptor->slots()[0], ColumnCategory::REGULAR, nullptr},
            {"year", tuple_descriptor->slots()[1], ColumnCategory::REGULAR, nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_iceberg_parquet/data/"
            "00000-9-a7e0135f-d581-40e4-8d56-a929aded99e4-0-00001.parquet";
    scan_range.__isset.table_format_params = true;
    scan_range.__set_columns_from_path_keys({"year"});
    scan_range.__set_columns_from_path({"2024"});

    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    size_t read_rows = 0;
    Block block;
    read_iceberg_parquet_scalar_block(
            scan_range.path, scan_range, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"name", nullable_string}, {"year", nullable_string}}, &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_values(block, "year",
                                         std::vector<std::string>(read_rows, "2024"));
}

TEST_F(IcebergReaderTest, history_schema_without_field_ids_falls_back_to_name_for_parquet) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"name"};
    std::vector<int32_t> table_column_unique_ids = {2};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING};
    const TupleDescriptor* tuple_descriptor = create_simple_tuple_descriptor(
            &desc_tbl, obj_pool, t_desc_table, t_table_desc, table_column_names,
            table_column_unique_ids, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0}};
    std::vector<ColumnDescriptor> column_descs = {
            {"name", tuple_descriptor->slots()[0], ColumnCategory::REGULAR, nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_parquet/"
            "part-00000-64a7a390-1a03-4efc-ab51-557e9369a1f9-c000.snappy.parquet";

    TFileScanRangeParams scan_params;
    scan_params.__set_current_schema_id(1);
    scan_params.__set_history_schema_info({build_history_schema_with_name_field(2)});

    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    size_t read_rows = 0;
    Block block;
    read_iceberg_parquet_scalar_block(
            scan_range.path, scan_range, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"name", nullable_string}}, &read_rows, &block, &scan_params);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "name", read_rows);
}

TEST_F(IcebergReaderTest, disabled_partition_fallback_keeps_missing_partition_null_for_parquet) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"name", "year"};
    std::vector<int32_t> table_column_unique_ids = {2, 100};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING,
                                                            TPrimitiveType::STRING};
    const TupleDescriptor* tuple_descriptor = create_simple_tuple_descriptor(
            &desc_tbl, obj_pool, t_desc_table, t_table_desc, table_column_names,
            table_column_unique_ids, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0}, {"year", 1}};
    std::vector<ColumnDescriptor> column_descs = {
            {"name", tuple_descriptor->slots()[0], ColumnCategory::REGULAR, nullptr},
            {"year", tuple_descriptor->slots()[1], ColumnCategory::REGULAR, nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_iceberg_parquet/data/"
            "00000-9-a7e0135f-d581-40e4-8d56-a929aded99e4-0-00001.parquet";
    scan_range.__isset.table_format_params = true;
    scan_range.__set_columns_from_path_keys({"year"});
    scan_range.__set_columns_from_path({"2024"});

    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    size_t read_rows = 0;
    Block block;
    auto old_value = config::enable_iceberg_partition_column_fallback;
    Defer restore_fallback =
            Defer([&]() { config::enable_iceberg_partition_column_fallback = old_value; });
    config::enable_iceberg_partition_column_fallback = false;
    read_iceberg_parquet_scalar_block(
            scan_range.path, scan_range, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"name", nullable_string}, {"year", nullable_string}}, &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_all_null(block, "year", read_rows);
}

TEST_F(IcebergReaderTest, fills_iceberg_rowid_synthesized_column_for_parquet) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {BeConsts::ICEBERG_ROWID_COL};
    std::vector<int32_t> table_column_unique_ids = {200};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRUCT};
    const TupleDescriptor* tuple_descriptor = create_simple_tuple_descriptor(
            &desc_tbl, obj_pool, t_desc_table, t_table_desc, table_column_names,
            table_column_unique_ids, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {
            {BeConsts::ICEBERG_ROWID_COL, 0}};
    std::vector<ColumnDescriptor> column_descs = {{BeConsts::ICEBERG_ROWID_COL,
                                                   tuple_descriptor->slots()[0],
                                                   ColumnCategory::SYNTHESIZED, nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_iceberg_parquet/data/"
            "00000-9-a7e0135f-d581-40e4-8d56-a929aded99e4-0-00001.parquet";
    scan_range.__isset.table_format_params = true;
    scan_range.table_format_params.table_format_type = "iceberg";
    scan_range.table_format_params.iceberg_params.__set_original_file_path(scan_range.path);
    scan_range.table_format_params.iceberg_params.__set_partition_spec_id(7);
    scan_range.table_format_params.iceberg_params.__set_partition_data_json("{\"year\":2024}");

    auto rowid_struct_type = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt64>(),
                       std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()},
            Strings {"file_path", "pos", "partition_spec_id", "partition_data"});
    size_t read_rows = 0;
    Block block;
    read_iceberg_parquet_scalar_block(
            scan_range.path, scan_range, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{BeConsts::ICEBERG_ROWID_COL, rowid_struct_type}}, &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_iceberg_rowid_column(block, BeConsts::ICEBERG_ROWID_COL, scan_range.path, 7,
                                "{\"year\":2024}");
}

TEST_F(IcebergReaderTest, fills_global_rowid_synthesized_column_for_parquet) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::string global_rowid_col_name = BeConsts::GLOBAL_ROWID_COL + "test";
    std::vector<std::string> table_column_names = {global_rowid_col_name};
    std::vector<int32_t> table_column_unique_ids = {201};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING};
    const TupleDescriptor* tuple_descriptor = create_simple_tuple_descriptor(
            &desc_tbl, obj_pool, t_desc_table, t_table_desc, table_column_names,
            table_column_unique_ids, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{global_rowid_col_name, 0}};
    std::vector<ColumnDescriptor> column_descs = {{global_rowid_col_name,
                                                   tuple_descriptor->slots()[0],
                                                   ColumnCategory::SYNTHESIZED, nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_iceberg_parquet/data/"
            "00000-9-a7e0135f-d581-40e4-8d56-a929aded99e4-0-00001.parquet";

    auto global_rowid_type = std::make_shared<DataTypeString>();
    size_t read_rows = 0;
    Block block;
    constexpr uint8_t kVersion = 1;
    constexpr int64_t kBackendId = 10086;
    constexpr uint32_t kFileId = 7;
    read_iceberg_parquet_scalar_block(
            scan_range.path, scan_range, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{global_rowid_col_name, global_rowid_type}}, &read_rows, &block, nullptr,
            [&](IcebergParquetReader* reader) {
                reader->set_create_row_id_column_iterator_func([&]() {
                    return std::make_shared<segment_v2::RowIdColumnIteratorV2>(kVersion, kBackendId,
                                                                               kFileId);
                });
            });

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_global_rowid_column(block, global_rowid_col_name, kVersion, kBackendId, kFileId);
}

TEST_F(IcebergReaderTest, reads_generated_column_from_file_for_parquet) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"name"};
    std::vector<int32_t> table_column_unique_ids = {2};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING};
    const TupleDescriptor* tuple_descriptor = create_simple_tuple_descriptor(
            &desc_tbl, obj_pool, t_desc_table, t_table_desc, table_column_names,
            table_column_unique_ids, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0}};
    std::vector<ColumnDescriptor> column_descs = {
            {"name", tuple_descriptor->slots()[0], ColumnCategory::GENERATED, nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_iceberg_parquet/data/"
            "00000-9-a7e0135f-d581-40e4-8d56-a929aded99e4-0-00001.parquet";

    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    size_t read_rows = 0;
    Block block;
    read_iceberg_parquet_scalar_block(scan_range.path, scan_range, tuple_descriptor,
                                      col_name_to_block_idx, column_descs,
                                      {{"name", nullable_string}}, &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "name", read_rows);
}

TEST_F(IcebergReaderTest, fills_partition_column_from_scan_range_for_orc) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"name", "year"};
    std::vector<int32_t> table_column_unique_ids = {2, 100};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING,
                                                            TPrimitiveType::STRING};
    const TupleDescriptor* tuple_descriptor = create_simple_tuple_descriptor(
            &desc_tbl, obj_pool, t_desc_table, t_table_desc, table_column_names,
            table_column_unique_ids, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0}, {"year", 1}};
    std::vector<ColumnDescriptor> column_descs = {
            {"name", tuple_descriptor->slots()[0], ColumnCategory::REGULAR, nullptr},
            {"year", tuple_descriptor->slots()[1], ColumnCategory::REGULAR, nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_iceberg_orc/data/"
            "00000-8-5a144c37-16a4-47c6-96db-0007175b5c90-0-00001.orc";
    scan_range.__isset.table_format_params = true;
    scan_range.__set_columns_from_path_keys({"year"});
    scan_range.__set_columns_from_path({"2024"});

    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    size_t read_rows = 0;
    Block block;
    read_iceberg_orc_scalar_block(
            scan_range.path, scan_range, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"name", nullable_string}, {"year", nullable_string}}, &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_values(block, "year",
                                         std::vector<std::string>(read_rows, "2024"));
}

TEST_F(IcebergReaderTest, history_schema_without_field_ids_falls_back_to_name_for_orc) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"name"};
    std::vector<int32_t> table_column_unique_ids = {2};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING};
    const TupleDescriptor* tuple_descriptor = create_simple_tuple_descriptor(
            &desc_tbl, obj_pool, t_desc_table, t_table_desc, table_column_names,
            table_column_unique_ids, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0}};
    std::vector<ColumnDescriptor> column_descs = {
            {"name", tuple_descriptor->slots()[0], ColumnCategory::REGULAR, nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_orc/"
            "part-00000-62614f23-05d1-4043-a533-b155ef52b720-c000.snappy.orc";

    TFileScanRangeParams scan_params;
    scan_params.__set_current_schema_id(1);
    scan_params.__set_history_schema_info({build_history_schema_with_name_field(2)});

    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    size_t read_rows = 0;
    Block block;
    read_iceberg_orc_scalar_block(scan_range.path, scan_range, tuple_descriptor,
                                  col_name_to_block_idx, column_descs, {{"name", nullable_string}},
                                  &read_rows, &block, &scan_params);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "name", read_rows);
}

TEST_F(IcebergReaderTest, disabled_partition_fallback_keeps_missing_partition_null_for_orc) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"name", "year"};
    std::vector<int32_t> table_column_unique_ids = {2, 100};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING,
                                                            TPrimitiveType::STRING};
    const TupleDescriptor* tuple_descriptor = create_simple_tuple_descriptor(
            &desc_tbl, obj_pool, t_desc_table, t_table_desc, table_column_names,
            table_column_unique_ids, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0}, {"year", 1}};
    std::vector<ColumnDescriptor> column_descs = {
            {"name", tuple_descriptor->slots()[0], ColumnCategory::REGULAR, nullptr},
            {"year", tuple_descriptor->slots()[1], ColumnCategory::REGULAR, nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_iceberg_orc/data/"
            "00000-8-5a144c37-16a4-47c6-96db-0007175b5c90-0-00001.orc";
    scan_range.__isset.table_format_params = true;
    scan_range.__set_columns_from_path_keys({"year"});
    scan_range.__set_columns_from_path({"2024"});

    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    size_t read_rows = 0;
    Block block;
    auto old_value = config::enable_iceberg_partition_column_fallback;
    Defer restore_fallback =
            Defer([&]() { config::enable_iceberg_partition_column_fallback = old_value; });
    config::enable_iceberg_partition_column_fallback = false;
    read_iceberg_orc_scalar_block(
            scan_range.path, scan_range, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"name", nullable_string}, {"year", nullable_string}}, &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_all_null(block, "year", read_rows);
}

TEST_F(IcebergReaderTest, fills_iceberg_rowid_synthesized_column_for_orc) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"name", BeConsts::ICEBERG_ROWID_COL};
    std::vector<int32_t> table_column_unique_ids = {2, 200};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING,
                                                            TPrimitiveType::STRUCT};
    const TupleDescriptor* tuple_descriptor = create_simple_tuple_descriptor(
            &desc_tbl, obj_pool, t_desc_table, t_table_desc, table_column_names,
            table_column_unique_ids, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {
            {"name", 0}, {BeConsts::ICEBERG_ROWID_COL, 1}};
    std::vector<ColumnDescriptor> column_descs = {
            {"name", tuple_descriptor->slots()[0], ColumnCategory::REGULAR, nullptr},
            {BeConsts::ICEBERG_ROWID_COL, tuple_descriptor->slots()[1], ColumnCategory::SYNTHESIZED,
             nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_iceberg_orc/data/"
            "00000-8-5a144c37-16a4-47c6-96db-0007175b5c90-0-00001.orc";
    scan_range.__isset.table_format_params = true;
    scan_range.table_format_params.table_format_type = "iceberg";
    scan_range.table_format_params.iceberg_params.__set_original_file_path(scan_range.path);
    scan_range.table_format_params.iceberg_params.__set_partition_spec_id(8);
    scan_range.table_format_params.iceberg_params.__set_partition_data_json("{\"year\":2024}");

    auto rowid_struct_type = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt64>(),
                       std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()},
            Strings {"file_path", "pos", "partition_spec_id", "partition_data"});
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    size_t read_rows = 0;
    Block block;
    read_iceberg_orc_scalar_block(
            scan_range.path, scan_range, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"name", nullable_string}, {BeConsts::ICEBERG_ROWID_COL, rowid_struct_type}},
            &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_iceberg_rowid_column(block, BeConsts::ICEBERG_ROWID_COL, scan_range.path, 8,
                                "{\"year\":2024}");
}

TEST_F(IcebergReaderTest, fills_global_rowid_synthesized_column_for_orc) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::string global_rowid_col_name = BeConsts::GLOBAL_ROWID_COL + "test";
    std::vector<std::string> table_column_names = {"name", global_rowid_col_name};
    std::vector<int32_t> table_column_unique_ids = {2, 201};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING,
                                                            TPrimitiveType::STRING};
    const TupleDescriptor* tuple_descriptor = create_simple_tuple_descriptor(
            &desc_tbl, obj_pool, t_desc_table, t_table_desc, table_column_names,
            table_column_unique_ids, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0},
                                                                       {global_rowid_col_name, 1}};
    std::vector<ColumnDescriptor> column_descs = {
            {"name", tuple_descriptor->slots()[0], ColumnCategory::REGULAR, nullptr},
            {global_rowid_col_name, tuple_descriptor->slots()[1], ColumnCategory::SYNTHESIZED,
             nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_iceberg_orc/data/"
            "00000-8-5a144c37-16a4-47c6-96db-0007175b5c90-0-00001.orc";

    auto global_rowid_type = std::make_shared<DataTypeString>();
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    size_t read_rows = 0;
    Block block;
    constexpr uint8_t kVersion = 1;
    constexpr int64_t kBackendId = 10010;
    constexpr uint32_t kFileId = 11;
    read_iceberg_orc_scalar_block(
            scan_range.path, scan_range, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"name", nullable_string}, {global_rowid_col_name, global_rowid_type}}, &read_rows,
            &block, nullptr, [&](IcebergOrcReader* reader) {
                reader->set_create_row_id_column_iterator_func([&]() {
                    return std::make_shared<segment_v2::RowIdColumnIteratorV2>(kVersion, kBackendId,
                                                                               kFileId);
                });
            });

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_global_rowid_column(block, global_rowid_col_name, kVersion, kBackendId, kFileId);
}

TEST_F(IcebergReaderTest, reads_generated_column_from_file_for_orc) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"name"};
    std::vector<int32_t> table_column_unique_ids = {2};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING};
    const TupleDescriptor* tuple_descriptor = create_simple_tuple_descriptor(
            &desc_tbl, obj_pool, t_desc_table, t_table_desc, table_column_names,
            table_column_unique_ids, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0}};
    std::vector<ColumnDescriptor> column_descs = {
            {"name", tuple_descriptor->slots()[0], ColumnCategory::GENERATED, nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_iceberg_orc/data/"
            "00000-8-5a144c37-16a4-47c6-96db-0007175b5c90-0-00001.orc";

    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    size_t read_rows = 0;
    Block block;
    read_iceberg_orc_scalar_block(scan_range.path, scan_range, tuple_descriptor,
                                  col_name_to_block_idx, column_descs, {{"name", nullable_string}},
                                  &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "name", read_rows);
}

TEST_F(IcebergReaderTest, fills_row_lineage_columns_from_scan_range_for_parquet) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"name", "_row_id",
                                                   "_last_updated_sequence_number"};
    std::vector<int32_t> table_column_unique_ids = {2, 200, 201};
    std::vector<TPrimitiveType::type> table_column_types = {
            TPrimitiveType::STRING, TPrimitiveType::BIGINT, TPrimitiveType::BIGINT};
    const TupleDescriptor* tuple_descriptor = create_simple_tuple_descriptor(
            &desc_tbl, obj_pool, t_desc_table, t_table_desc, table_column_names,
            table_column_unique_ids, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {
            {"name", 0}, {"_row_id", 1}, {"_last_updated_sequence_number", 2}};
    std::vector<ColumnDescriptor> column_descs = {
            {"name", tuple_descriptor->slots()[0], ColumnCategory::REGULAR, nullptr},
            {"_row_id", tuple_descriptor->slots()[1], ColumnCategory::GENERATED, nullptr},
            {"_last_updated_sequence_number", tuple_descriptor->slots()[2],
             ColumnCategory::GENERATED, nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_iceberg_parquet/data/"
            "00000-9-a7e0135f-d581-40e4-8d56-a929aded99e4-0-00001.parquet";
    scan_range.__isset.table_format_params = true;
    scan_range.table_format_params.table_format_type = "iceberg";
    scan_range.table_format_params.iceberg_params.__set_first_row_id(1000);
    scan_range.table_format_params.iceberg_params.__set_last_updated_sequence_number(77);

    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    auto nullable_int64 = make_nullable(std::make_shared<DataTypeInt64>());
    size_t read_rows = 0;
    Block block;
    read_iceberg_parquet_scalar_block(scan_range.path, scan_range, tuple_descriptor,
                                      col_name_to_block_idx, column_descs,
                                      {{"name", nullable_string},
                                       {"_row_id", nullable_int64},
                                       {"_last_updated_sequence_number", nullable_int64}},
                                      &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_int64_column_sequence(block, "_row_id", 1000);
    verify_nullable_int64_column_constant(block, "_last_updated_sequence_number", 77);
}

TEST_F(IcebergReaderTest, fills_row_lineage_columns_from_scan_range_for_orc) {
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"name", "_row_id",
                                                   "_last_updated_sequence_number"};
    std::vector<int32_t> table_column_unique_ids = {2, 200, 201};
    std::vector<TPrimitiveType::type> table_column_types = {
            TPrimitiveType::STRING, TPrimitiveType::BIGINT, TPrimitiveType::BIGINT};
    const TupleDescriptor* tuple_descriptor = create_simple_tuple_descriptor(
            &desc_tbl, obj_pool, t_desc_table, t_table_desc, table_column_names,
            table_column_unique_ids, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {
            {"name", 0}, {"_row_id", 1}, {"_last_updated_sequence_number", 2}};
    std::vector<ColumnDescriptor> column_descs = {
            {"name", tuple_descriptor->slots()[0], ColumnCategory::REGULAR, nullptr},
            {"_row_id", tuple_descriptor->slots()[1], ColumnCategory::GENERATED, nullptr},
            {"_last_updated_sequence_number", tuple_descriptor->slots()[2],
             ColumnCategory::GENERATED, nullptr}};

    TFileRangeDesc scan_range;
    scan_range.path =
            "./be/test/exec/test_data/nested_user_profiles_iceberg_orc/data/"
            "00000-8-5a144c37-16a4-47c6-96db-0007175b5c90-0-00001.orc";
    scan_range.__isset.table_format_params = true;
    scan_range.table_format_params.table_format_type = "iceberg";
    scan_range.table_format_params.iceberg_params.__set_first_row_id(2000);
    scan_range.table_format_params.iceberg_params.__set_last_updated_sequence_number(88);

    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    auto nullable_int64 = make_nullable(std::make_shared<DataTypeInt64>());
    size_t read_rows = 0;
    Block block;
    read_iceberg_orc_scalar_block(scan_range.path, scan_range, tuple_descriptor,
                                  col_name_to_block_idx, column_descs,
                                  {{"name", nullable_string},
                                   {"_row_id", nullable_int64},
                                   {"_last_updated_sequence_number", nullable_int64}},
                                  &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_int64_column_sequence(block, "_row_id", 2000);
    verify_nullable_int64_column_constant(block, "_last_updated_sequence_number", 88);
}

TEST_F(IcebergReaderTest, rejects_multiple_deletion_vectors_during_parquet_init) {
    auto local_fs = io::global_local_filesystem();
    std::string test_file =
            "./be/test/exec/test_data/nested_user_profiles_iceberg_parquet/data/"
            "00000-9-a7e0135f-d581-40e4-8d56-a929aded99e4-0-00001.parquet";
    int64_t file_size = 0;
    auto st = local_fs->file_size(test_file, &file_size);
    if (!st.ok()) {
        GTEST_SKIP() << "Test file not found: " << test_file;
    }

    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"name"};
    std::vector<int32_t> table_column_unique_ids = {2};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING};
    const TupleDescriptor* tuple_descriptor = create_simple_tuple_descriptor(
            &desc_tbl, obj_pool, t_desc_table, t_table_desc, table_column_names,
            table_column_unique_ids, table_column_types);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0}};
    std::vector<ColumnDescriptor> column_descs = {
            {"name", tuple_descriptor->slots()[0], ColumnCategory::REGULAR, nullptr}};

    RuntimeState runtime_state((TQueryOptions()), TQueryGlobals());
    TFileScanRangeParams scan_params;
    scan_params.format_type = TFileFormatType::FORMAT_PARQUET;

    TFileRangeDesc scan_range;
    scan_range.start_offset = 0;
    scan_range.size = file_size;
    scan_range.path = test_file;
    scan_range.__isset.table_format_params = true;
    scan_range.table_format_params.table_format_type = "iceberg";
    scan_range.table_format_params.iceberg_params.__set_format_version(2);
    scan_range.table_format_params.iceberg_params.__set_original_file_path(test_file);

    TIcebergDeleteFileDesc dv1;
    dv1.path = "memory://dv-1.bin";
    dv1.content = IcebergParquetReader::DELETION_VECTOR;
    TIcebergDeleteFileDesc dv2;
    dv2.path = "memory://dv-2.bin";
    dv2.content = IcebergParquetReader::DELETION_VECTOR;
    scan_range.table_format_params.iceberg_params.__set_delete_files({dv1, dv2});

    RuntimeProfile profile("multiple_dv_init_test");
    auto iceberg_reader = std::make_unique<IcebergParquetReader>(
            nullptr, &profile, scan_params, scan_range, 1024, &timezone_obj, nullptr,
            &runtime_state, cache.get());
    ASSERT_NE(iceberg_reader, nullptr);

    ParquetInitContext pq_ctx;
    pq_ctx.column_descs = &column_descs;
    pq_ctx.col_name_to_block_idx = &col_name_to_block_idx;
    pq_ctx.tuple_descriptor = tuple_descriptor;
    pq_ctx.params = &scan_params;
    pq_ctx.range = &scan_range;
    st = iceberg_reader->init_reader(&pq_ctx);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::DATA_QUALITY_ERROR>());
    EXPECT_NE(st.to_string().find("multiple DVs"), std::string::npos);
}

} // namespace doris
