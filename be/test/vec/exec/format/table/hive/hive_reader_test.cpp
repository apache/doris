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

#include "vec/exec/format/table/hive_reader.h"

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

namespace doris::vectorized::table {

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

        // Helper function to recursively print column row counts
        std::function<void(const ColumnPtr&, const DataTypePtr&, const std::string&, int)>
                print_column_rows = [&](const ColumnPtr& col, const DataTypePtr& type,
                                        const std::string& name, int depth) {
                    std::string indent(depth * 2, ' ');
                    std::cout << indent << name << " row count: " << col->size() << std::endl;
                    EXPECT_GT(col->size(), 0) << name << " column/subcolumn size should be > 0";

                    // Check if it's a nullable column
                    if (const auto* nullable_col = typeid_cast<const ColumnNullable*>(col.get())) {
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
                    else if (const auto* struct_col = typeid_cast<const ColumnStruct*>(col.get())) {
                        auto struct_type = assert_cast<const DataTypeStruct*>(type.get());
                        for (size_t i = 0; i < struct_col->tuple_size(); ++i) {
                            std::string field_name = struct_type->get_element_name(i);
                            auto field_type = struct_type->get_element(i);
                            print_column_rows(struct_col->get_column_ptr(i), field_type,
                                              name + "." + field_name, depth + 1);
                        }
                    }
                    // Check if it's an array column
                    else if (const auto* array_col = typeid_cast<const ColumnArray*>(col.get())) {
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
};

// Test reading real Hive Parquet file using HiveTableReader
TEST_F(HiveReaderTest, read_hive_parquet_file) {
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

    // Open the Hive Parquet test file
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

    // Create HiveParquetReader
    auto hive_reader = std::make_unique<HiveParquetReader>(std::move(generic_reader), &profile,
                                                           &runtime_state, scan_params, scan_range,
                                                           nullptr, nullptr, cache.get());

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
    std::vector<std::string> table_column_names = {"name", "profile"};
    std::vector<int> table_column_positions = {1, 2};
    std::vector<TPrimitiveType::type> table_column_types = {
            TPrimitiveType::STRING, TPrimitiveType::STRUCT // profile 用 STRUCT 类型
    };
    const TupleDescriptor* tuple_descriptor =
            create_tuple_descriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc,
                                    table_column_names, table_column_positions, table_column_types);

    VExprContextSPtrs conjuncts; // Empty conjuncts for this test
    std::vector<std::string> table_col_names = {"name", "profile"};
    const RowDescriptor* row_descriptor = nullptr;
    const std::unordered_map<std::string, int>* colname_to_slot_id = nullptr;
    const VExprContextSPtrs* not_single_slot_filter_conjuncts = nullptr;
    const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts = nullptr;

    st = hive_reader->init_reader(table_col_names, conjuncts, tuple_descriptor, row_descriptor,
                                  colname_to_slot_id, not_single_slot_filter_conjuncts,
                                  slot_id_to_filter_conjuncts);
    ASSERT_TRUE(st.ok()) << st;

    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;
    ASSERT_TRUE(hive_reader->set_fill_columns(partition_columns, missing_columns).ok());

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
    st = hive_reader->get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(st.ok()) << st;

    // Verify test results using helper function
    verify_test_results(block, read_rows);
}

// Test reading real Hive Orc file using HiveTableReader
TEST_F(HiveReaderTest, read_hive_rrc_file) {
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
    // Open the Hive Orc test file
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

    // Create HiveOrcReader
    auto hive_reader =
            std::make_unique<HiveOrcReader>(std::move(generic_reader), &profile, &runtime_state,
                                            scan_params, scan_range, nullptr, nullptr, cache.get());

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
    std::vector<std::string> table_column_names = {"name", "profile"};
    std::vector<int> table_column_positions = {1, 2};
    std::vector<TPrimitiveType::type> table_column_types = {
            TPrimitiveType::STRING, TPrimitiveType::STRUCT // profile 用 STRUCT 类型
    };
    const TupleDescriptor* tuple_descriptor =
            create_tuple_descriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc,
                                    table_column_names, table_column_positions, table_column_types);

    VExprContextSPtrs conjuncts; // Empty conjuncts for this test
    std::vector<std::string> table_col_names = {"name", "profile"};
    const RowDescriptor* row_descriptor = nullptr;
    const VExprContextSPtrs* not_single_slot_filter_conjuncts = nullptr;
    const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts = nullptr;

    st = hive_reader->init_reader(table_col_names, conjuncts, tuple_descriptor, row_descriptor,
                                  not_single_slot_filter_conjuncts, slot_id_to_filter_conjuncts);
    ASSERT_TRUE(st.ok()) << st;

    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;
    ASSERT_TRUE(hive_reader->set_fill_columns(partition_columns, missing_columns).ok());

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
    st = hive_reader->get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(st.ok()) << st;

    // Verify test results using helper function
    verify_test_results(block, read_rows);
}

} // namespace doris::vectorized::table
