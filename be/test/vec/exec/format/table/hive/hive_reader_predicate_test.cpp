// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.

// #include "vec/exec/format/table/hive_reader.h"

// #include <cctz/time_zone.h>
// #include <gen_cpp/Descriptors_types.h>
// #include <gen_cpp/PaloInternalService_types.h>
// #include <gen_cpp/PlanNodes_types.h>
// #include <gen_cpp/Types_types.h>
// #include <gtest/gtest.h>

// #include <iostream>
// #include <memory>
// #include <string>
// #include <unordered_map>
// #include <vector>

// #include "common/object_pool.h"
// #include "exec/olap_common.h"
// #include "io/fs/file_meta_cache.h"
// #include "io/fs/file_reader_writer_fwd.h"
// #include "io/fs/file_system.h"
// #include "io/fs/local_file_system.h"
// #include "runtime/descriptors.h"
// #include "runtime/runtime_state.h"
// #include "util/timezone_utils.h"
// #include "vec/columns/column.h"
// #include "vec/columns/column_array.h"
// #include "vec/columns/column_nullable.h"
// #include "vec/columns/column_struct.h"
// #include "vec/core/block.h"
// #include "vec/core/column_with_type_and_name.h"
// #include "vec/data_types/data_type.h"
// #include "vec/data_types/data_type_array.h"
// #include "vec/data_types/data_type_factory.hpp"
// #include "vec/data_types/data_type_nullable.h"
// #include "vec/data_types/data_type_number.h"
// #include "vec/data_types/data_type_string.h"
// #include "vec/data_types/data_type_struct.h"
// #include "vec/exec/format/parquet/vparquet_reader.h"
// #include "vec/exprs/vexpr.h"
// #include "vec/exprs/vexpr_context.h"
// #include "gen_cpp/Exprs_types.h"
// #include "gen_cpp/Opcodes_types.h"

// namespace doris::vectorized::table {

// class HiveReaderPredicateTest : public ::testing::Test {
// protected:
//     void SetUp() override {
//         cache = std::make_unique<doris::FileMetaCache>(1024);

//         // Setup timezone
//         doris::TimezoneUtils::find_cctz_time_zone(doris::TimezoneUtils::default_time_zone,
//                                                   timezone_obj);
//     }

//     void TearDown() override { cache.reset(); }

//     // Helper function to create complex struct types for testing
//     void createComplexStructTypes(DataTypePtr& coordinates_struct_type,
//                                   DataTypePtr& address_struct_type,
//                                   DataTypePtr& phone_struct_type,
//                                   DataTypePtr& contact_struct_type,
//                                   DataTypePtr& hobby_element_struct_type,
//                                   DataTypePtr& hobbies_array_type,
//                                   DataTypePtr& profile_struct_type,
//                                   DataTypePtr& name_type) {
//         // Create name column type (direct field)
//         name_type = make_nullable(std::make_shared<DataTypeString>());

//         // First create coordinates struct type
//         std::vector<DataTypePtr> coordinates_types = {
//             make_nullable(std::make_shared<DataTypeFloat64>()), // lat (field ID 10)
//             make_nullable(std::make_shared<DataTypeFloat64>())  // lng (field ID 11)
//         };
//         std::vector<std::string> coordinates_names = {"lat", "lng"};
//         coordinates_struct_type =
//             make_nullable(std::make_shared<DataTypeStruct>(coordinates_types, coordinates_names));

//         // Create address struct type (with street, city, coordinates)
//         std::vector<DataTypePtr> address_types = {
//             make_nullable(std::make_shared<DataTypeString>()), // street (field ID 7)
//             make_nullable(std::make_shared<DataTypeString>()), // city (field ID 8)
//             coordinates_struct_type                            // coordinates (field ID 9)
//         };
//         std::vector<std::string> address_names = {"street", "city", "coordinates"};
//         address_struct_type =
//             make_nullable(std::make_shared<DataTypeStruct>(address_types, address_names));

//         // Create phone struct type
//         std::vector<DataTypePtr> phone_types = {
//             make_nullable(std::make_shared<DataTypeString>()), // country_code (field ID 14)
//             make_nullable(std::make_shared<DataTypeString>())  // number (field ID 15)
//         };
//         std::vector<std::string> phone_names = {"country_code", "number"};
//         phone_struct_type =
//             make_nullable(std::make_shared<DataTypeStruct>(phone_types, phone_names));

//         // Create contact struct type (with email, phone)
//         std::vector<DataTypePtr> contact_types = {
//             make_nullable(std::make_shared<DataTypeString>()), // email (field ID 12)
//             phone_struct_type                                  // phone (field ID 13)
//         };
//         std::vector<std::string> contact_names = {"email", "phone"};
//         contact_struct_type =
//             make_nullable(std::make_shared<DataTypeStruct>(contact_types, contact_names));

//         // Create hobby element struct type for array elements
//         std::vector<DataTypePtr> hobby_element_types = {
//             make_nullable(std::make_shared<DataTypeString>()), // name (field ID 17)
//             make_nullable(std::make_shared<DataTypeInt32>())   // level (field ID 18)
//         };
//         std::vector<std::string> hobby_element_names = {"name", "level"};
//         hobby_element_struct_type = make_nullable(
//             std::make_shared<DataTypeStruct>(hobby_element_types, hobby_element_names));

//         // Create hobbies array type
//         hobbies_array_type =
//             make_nullable(std::make_shared<DataTypeArray>(hobby_element_struct_type));

//         // Create complete profile struct type (with address, contact, hobbies)
//         std::vector<DataTypePtr> profile_types = {
//             address_struct_type, // address (field ID 4)
//             contact_struct_type, // contact (field ID 5)
//             hobbies_array_type   // hobbies (field ID 6)
//         };
//         std::vector<std::string> profile_names = {"address", "contact", "hobbies"};
//         profile_struct_type =
//             make_nullable(std::make_shared<DataTypeStruct>(profile_types, profile_names));
//     }

//     // Helper function to create tuple descriptor
//     const TupleDescriptor* createTupleDescriptor(DescriptorTbl** desc_tbl,
//                                                  ObjectPool& obj_pool,
//                                                  TDescriptorTable& t_desc_table,
//                                                  TTableDescriptor& t_table_desc,
//                                                  const std::vector<std::string>& column_names,
//                                                  const std::vector<int>& column_positions,
//                                                  const std::vector<TPrimitiveType::type>& column_types,
//                                                  bool use_name_paths = true) {
//         // Create table descriptor with complex schema
//         auto create_table_desc = [](TDescriptorTable& t_desc_table, TTableDescriptor& t_table_desc,
//                                     const std::vector<std::string>& table_column_names,
//                                     const std::vector<int>& table_column_positions,
//                                     const std::vector<TPrimitiveType::type>& types,
//                                     bool use_name_paths) {
//             t_table_desc.__set_id(0);
//             t_table_desc.__set_tableType(TTableType::OLAP_TABLE);
//             t_table_desc.__set_numCols(0);
//             t_table_desc.__set_numClusteringCols(0);
//             t_desc_table.tableDescriptors.push_back(t_table_desc);
//             t_desc_table.__isset.tableDescriptors = true;
//             for (int i = 0; i < table_column_names.size(); i++) {
//                 TSlotDescriptor tslot_desc;
//                 TTypeDesc type;
//                 if (table_column_names[i] == "profile") {
//                     // STRUCT/ARRAY 节点设置 contains_nulls，SCALAR 节点不设置
//                     TTypeNode struct_node;
//                     struct_node.__set_type(TTypeNodeType::STRUCT);
//                     std::vector<TStructField> struct_fields;
//                     TStructField address_field;
//                     address_field.__set_name("address");
//                     address_field.__set_contains_null(true);
//                     struct_fields.push_back(address_field);
//                     TStructField contact_field;
//                     contact_field.__set_name("contact");
//                     contact_field.__set_contains_null(true);
//                     struct_fields.push_back(contact_field);
//                     TStructField hobbies_field;
//                     hobbies_field.__set_name("hobbies");
//                     hobbies_field.__set_contains_null(true);
//                     struct_fields.push_back(hobbies_field);
//                     struct_node.__set_struct_fields(struct_fields);
//                     type.types.push_back(struct_node);
//                     TTypeNode address_node;
//                     address_node.__set_type(TTypeNodeType::STRUCT);
//                     std::vector<TStructField> address_fields;
//                     TStructField street_field;
//                     street_field.__set_name("street");
//                     street_field.__set_contains_null(true);
//                     address_fields.push_back(street_field);
//                     TStructField city_field;
//                     city_field.__set_name("city");
//                     city_field.__set_contains_null(true);
//                     address_fields.push_back(city_field);
//                     TStructField coordinates_field;
//                     coordinates_field.__set_name("coordinates");
//                     coordinates_field.__set_contains_null(true);
//                     address_fields.push_back(coordinates_field);
//                     address_node.__set_struct_fields(address_fields);
//                     type.types.push_back(address_node);
//                     TTypeNode street_node;
//                     street_node.__set_type(TTypeNodeType::SCALAR);
//                     TScalarType street_scalar;
//                     street_scalar.__set_type(TPrimitiveType::STRING);
//                     street_node.__set_scalar_type(street_scalar);
//                     type.types.push_back(street_node);
//                     TTypeNode city_node;
//                     city_node.__set_type(TTypeNodeType::SCALAR);
//                     TScalarType city_scalar;
//                     city_scalar.__set_type(TPrimitiveType::STRING);
//                     city_node.__set_scalar_type(city_scalar);
//                     type.types.push_back(city_node);
//                     TTypeNode coordinates_node;
//                     coordinates_node.__set_type(TTypeNodeType::STRUCT);
//                     std::vector<TStructField> coordinates_fields;
//                     TStructField lat_field;
//                     lat_field.__set_name("lat");
//                     lat_field.__set_contains_null(true);
//                     coordinates_fields.push_back(lat_field);
//                     TStructField lng_field;
//                     lng_field.__set_name("lng");
//                     lng_field.__set_contains_null(true);
//                     coordinates_fields.push_back(lng_field);
//                     coordinates_node.__set_struct_fields(coordinates_fields);
//                     type.types.push_back(coordinates_node);
//                     TTypeNode lat_node;
//                     lat_node.__set_type(TTypeNodeType::SCALAR);
//                     TScalarType lat_scalar;
//                     lat_scalar.__set_type(TPrimitiveType::DOUBLE);
//                     lat_node.__set_scalar_type(lat_scalar);
//                     type.types.push_back(lat_node);
//                     TTypeNode lng_node;
//                     lng_node.__set_type(TTypeNodeType::SCALAR);
//                     TScalarType lng_scalar;
//                     lng_scalar.__set_type(TPrimitiveType::DOUBLE);
//                     lng_node.__set_scalar_type(lng_scalar);
//                     type.types.push_back(lng_node);
//                     TTypeNode contact_node;
//                     contact_node.__set_type(TTypeNodeType::STRUCT);
//                     std::vector<TStructField> contact_fields;
//                     TStructField email_field;
//                     email_field.__set_name("email");
//                     email_field.__set_contains_null(true);
//                     contact_fields.push_back(email_field);
//                     TStructField phone_field;
//                     phone_field.__set_name("phone");
//                     phone_field.__set_contains_null(true);
//                     contact_fields.push_back(phone_field);
//                     contact_node.__set_struct_fields(contact_fields);
//                     type.types.push_back(contact_node);
//                     TTypeNode email_node;
//                     email_node.__set_type(TTypeNodeType::SCALAR);
//                     TScalarType email_scalar;
//                     email_scalar.__set_type(TPrimitiveType::STRING);
//                     email_node.__set_scalar_type(email_scalar);
//                     type.types.push_back(email_node);
//                     TTypeNode phone_node;
//                     phone_node.__set_type(TTypeNodeType::STRUCT);
//                     std::vector<TStructField> phone_fields;
//                     TStructField country_code_field;
//                     country_code_field.__set_name("country_code");
//                     country_code_field.__set_contains_null(true);
//                     phone_fields.push_back(country_code_field);
//                     TStructField number_field;
//                     number_field.__set_name("number");
//                     number_field.__set_contains_null(true);
//                     phone_fields.push_back(number_field);
//                     phone_node.__set_struct_fields(phone_fields);
//                     type.types.push_back(phone_node);
//                     TTypeNode country_code_node;
//                     country_code_node.__set_type(TTypeNodeType::SCALAR);
//                     TScalarType country_code_scalar;
//                     country_code_scalar.__set_type(TPrimitiveType::STRING);
//                     country_code_node.__set_scalar_type(country_code_scalar);
//                     type.types.push_back(country_code_node);
//                     TTypeNode number_node;
//                     number_node.__set_type(TTypeNodeType::SCALAR);
//                     TScalarType number_scalar;
//                     number_scalar.__set_type(TPrimitiveType::STRING);
//                     number_node.__set_scalar_type(number_scalar);
//                     type.types.push_back(number_node);
//                     TTypeNode hobbies_node;
//                     hobbies_node.__set_type(TTypeNodeType::ARRAY);
//                     hobbies_node.__set_contains_nulls({true});
//                     type.types.push_back(hobbies_node);
//                     TTypeNode hobby_element_node;
//                     hobby_element_node.__set_type(TTypeNodeType::STRUCT);
//                     std::vector<TStructField> hobby_element_fields;
//                     TStructField hobby_name_field;
//                     hobby_name_field.__set_name("name");
//                     hobby_name_field.__set_contains_null(true);
//                     hobby_element_fields.push_back(hobby_name_field);
//                     TStructField hobby_level_field;
//                     hobby_level_field.__set_name("level");
//                     hobby_level_field.__set_contains_null(true);
//                     hobby_element_fields.push_back(hobby_level_field);
//                     hobby_element_node.__set_struct_fields(hobby_element_fields);
//                     type.types.push_back(hobby_element_node);
//                     TTypeNode hobby_name_node;
//                     hobby_name_node.__set_type(TTypeNodeType::SCALAR);
//                     TScalarType hobby_name_scalar;
//                     hobby_name_scalar.__set_type(TPrimitiveType::STRING);
//                     hobby_name_node.__set_scalar_type(hobby_name_scalar);
//                     type.types.push_back(hobby_name_node);
//                     TTypeNode hobby_level_node;
//                     hobby_level_node.__set_type(TTypeNodeType::SCALAR);
//                     TScalarType hobby_level_scalar;
//                     hobby_level_scalar.__set_type(TPrimitiveType::INT);
//                     hobby_level_node.__set_scalar_type(hobby_level_scalar);
//                     type.types.push_back(hobby_level_node);
//                     tslot_desc.__set_slotType(type);
//                 } else {
//                     // 普通类型
//                     TTypeNode node;
//                     node.__set_type(TTypeNodeType::SCALAR);
//                     TScalarType scalar_type;
//                     scalar_type.__set_type(types[i]);
//                     node.__set_scalar_type(scalar_type);
//                     type.types.push_back(node);
//                     tslot_desc.__set_slotType(type);
//                 }
//                 tslot_desc.__set_id(i);
//                 tslot_desc.__set_parent(0);
//                 tslot_desc.__set_colName(table_column_names[i]);
//                 tslot_desc.__set_columnPos(table_column_positions[i]);
//                 tslot_desc.__set_byteOffset(0);
//                 tslot_desc.__set_nullIndicatorByte(0);
//                 tslot_desc.__set_nullIndicatorBit(-1);
//                 tslot_desc.__set_slotIdx(0);
//                 tslot_desc.__set_isMaterialized(true);
//                 // 设置 column_access_paths，仅对 profile 字段
//                 if (table_column_names[i] == "profile") {
//                     {
//                     TColumnAccessPaths access_paths;
//                     access_paths.__set_type(doris::TAccessPathType::NAME);
//                     std::vector<TColumnNameAccessPath> paths;
//                     if (use_name_paths) {
//                         // address.coordinates.lat (predicate: lat > 40.0)
//                         TColumnNameAccessPath path1;
//                         path1.__set_path({"address", "coordinates", "lat"});
//                         paths.push_back(path1);
//                         // address.coordinates.lng (predicate: lng < -70.0)
//                         TColumnNameAccessPath path2;
//                         path2.__set_path({"address", "coordinates", "lng"});
//                         paths.push_back(path2);
//                         // contact.email
//                         TColumnNameAccessPath path3;
//                         path3.__set_path({"contact", "email"});
//                         paths.push_back(path3);
//                         // hobbies[].element.level
//                         TColumnNameAccessPath path4;
//                         path4.__set_path({"hobbies", "*", "level"});
//                         paths.push_back(path4);
//                     } else {
//                         // address.coordinates.lat (index path with predicates)
//                         TColumnNameAccessPath path1;
//                         path1.__set_path({"0", "2", "0"});
//                         paths.push_back(path1);
//                         // address.coordinates.lng (index path with predicates)
//                         TColumnNameAccessPath path2;
//                         path2.__set_path({"0", "2", "1"});
//                         paths.push_back(path2);
//                         // contact.email
//                         TColumnNameAccessPath path3;
//                         path3.__set_path({"1", "0"});
//                         paths.push_back(path3);
//                         // hobbies[].element.level
//                         TColumnNameAccessPath path4;
//                         path4.__set_path({"2", "0", "1"});
//                         paths.push_back(path4);
//                     }
//                     access_paths.__set_name_access_paths(paths);
//                     tslot_desc.__set_all_column_access_paths(access_paths);
//                     }

//                     {
//                     TColumnAccessPaths access_paths;
//                     access_paths.__set_type(doris::TAccessPathType::NAME);
//                     std::vector<TColumnNameAccessPath> paths;
//                     if (use_name_paths) {
//                         // address.coordinates.lat (predicate: lat > 40.0)
//                         TColumnNameAccessPath path1;
//                         path1.__set_path({"address", "coordinates", "lat"});
//                         paths.push_back(path1);
//                         // address.coordinates.lng (predicate: lng < -70.0)
//                         TColumnNameAccessPath path2;
//                         path2.__set_path({"address", "coordinates", "lng"});
//                         paths.push_back(path2);
//                     } else {
//                         // address.coordinates.lat (index path with predicates)
//                         TColumnNameAccessPath path1;
//                         path1.__set_path({"0", "2", "0"});
//                         paths.push_back(path1);
//                         // address.coordinates.lng (index path with predicates)
//                         TColumnNameAccessPath path2;
//                         path2.__set_path({"0", "2", "1"});
//                         paths.push_back(path2);
//                     }
//                     access_paths.__set_name_access_paths(paths);
//                     tslot_desc.__set_predicate_column_access_paths(access_paths);
//                     }
//                 }
//                 t_desc_table.slotDescriptors.push_back(tslot_desc);
//             }
//             t_desc_table.__isset.slotDescriptors = true;
//             TTupleDescriptor t_tuple_desc;
//             t_tuple_desc.__set_id(0);
//             t_tuple_desc.__set_byteSize(16);
//             t_tuple_desc.__set_numNullBytes(0);
//             t_tuple_desc.__set_tableId(0);
//             t_tuple_desc.__isset.tableId = true;
//             t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
//         };

//         create_table_desc(t_desc_table, t_table_desc, column_names, column_positions, column_types, use_name_paths);
//         EXPECT_TRUE(DescriptorTbl::create(&obj_pool, t_desc_table, desc_tbl).ok());
//         return (*desc_tbl)->get_tuple_descriptor(0);
//     }

//     // Helper function to verify test results
//     void verifyTestResults(Block& block, size_t read_rows) {
//         // Verify that we read some data
//         EXPECT_GT(read_rows, 0) << "Should read at least one row";
//         EXPECT_EQ(block.rows(), read_rows);

//         // Verify column count matches expected (2 columns: name, profile)
//         EXPECT_EQ(block.columns(), 2);

//         // Verify column names and types
//         auto columns_with_names = block.get_columns_with_type_and_name();
//         std::vector<std::string> expected_column_names = {"name", "profile"};
//         for (size_t i = 0; i < expected_column_names.size(); i++) {
//             EXPECT_EQ(columns_with_names[i].name, expected_column_names[i]);
//         }

//         // Verify column types
//         EXPECT_TRUE(columns_with_names[0].type->get_name().find("String") !=
//                     std::string::npos); // name is STRING
//         EXPECT_TRUE(columns_with_names[1].type->get_name().find("Struct") !=
//                     std::string::npos); // profile is STRUCT

//         // Print row count for each column and nested subcolumns
//         std::cout << "Block rows: " << block.rows() << std::endl;

//         // Helper function to recursively print column row counts
//         std::function<void(const ColumnPtr&, const DataTypePtr&, const std::string&, int)>
//                 print_column_rows = [&](const ColumnPtr& col, const DataTypePtr& type,
//                                         const std::string& name, int depth) {
//                     std::string indent(depth * 2, ' ');
//                     std::cout << indent << name << " row count: " << col->size() << std::endl;

//                     // Check if it's a nullable column
//                     if (const auto* nullable_col = typeid_cast<const ColumnNullable*>(col.get())) {
//                         auto nested_type =
//                                 assert_cast<const DataTypeNullable*>(type.get())->get_nested_type();

//                         // Only add ".nested" suffix for non-leaf (complex) nullable columns
//                         // Leaf columns like String, Int, etc. should not get the ".nested" suffix
//                         bool is_complex_type =
//                                 (typeid_cast<const DataTypeStruct*>(nested_type.get()) != nullptr) ||
//                                 (typeid_cast<const DataTypeArray*>(nested_type.get()) != nullptr) ||
//                                 (typeid_cast<const DataTypeMap*>(nested_type.get()) != nullptr);

//                         std::string nested_name = is_complex_type ? name + ".nested" : name;
//                         print_column_rows(nullable_col->get_nested_column_ptr(), nested_type,
//                                           nested_name, depth + (is_complex_type ? 1 : 0));
//                     }
//                     // Check if it's a struct column
//                     else if (const auto* struct_col = typeid_cast<const ColumnStruct*>(col.get())) {
//                         auto struct_type = assert_cast<const DataTypeStruct*>(type.get());
//                         for (size_t i = 0; i < struct_col->tuple_size(); ++i) {
//                             std::string field_name = struct_type->get_element_name(i);
//                             auto field_type = struct_type->get_element(i);
//                             print_column_rows(struct_col->get_column_ptr(i), field_type,
//                                               name + "." + field_name, depth + 1);
//                         }
//                     }
//                     // Check if it's an array column
//                     else if (const auto* array_col = typeid_cast<const ColumnArray*>(col.get())) {
//                         auto array_type = assert_cast<const DataTypeArray*>(type.get());
//                         auto element_type = array_type->get_nested_type();
//                         print_column_rows(array_col->get_data_ptr(), element_type, name + ".data",
//                                           depth + 1);
//                     }
//                 };

//         // Print row counts for all columns
//         for (size_t i = 0; i < block.columns(); ++i) {
//             const auto& column_with_name = block.get_by_position(i);
//             print_column_rows(column_with_name.column, column_with_name.type, column_with_name.name, 0);
//             EXPECT_EQ(column_with_name.column->size(), block.rows())
//                     << "Column " << column_with_name.name << " size mismatch";
//         }
//     }

//     // Helper function to create nested struct_element expression for a field path
//     TExprNode createNestedStructElement(const SlotDescriptor* profile_slot,
//                                         const std::vector<std::string>& field_path,
//                                         const TTypeDesc& result_type) {
//         if (field_path.empty()) {
//             // Return slot reference for empty path
//             TExprNode slot_node;
//             slot_node.__set_node_type(TExprNodeType::SLOT_REF);
//             slot_node.__set_num_children(0);
//             slot_node.__set_is_nullable(false); // struct_element expects non-nullable STRUCT input

//             // Set type as STRUCT for profile with nested address->coordinates->{lat,lng}
//             TTypeDesc profile_type;
//             // profile STRUCT with one field 'address'
//             TTypeNode profile_struct_node;
//             profile_struct_node.__set_type(TTypeNodeType::STRUCT);
//             std::vector<TStructField> profile_fields;
//             TStructField address_field;
//             address_field.__set_name("address");
//             address_field.__set_contains_null(false); // Non-nullable for struct_element
//             profile_fields.push_back(address_field);
//             profile_struct_node.__set_struct_fields(profile_fields);
//             profile_type.types.push_back(profile_struct_node);
//             // address STRUCT with one field 'coordinates'
//             TTypeNode address_struct_node;
//             address_struct_node.__set_type(TTypeNodeType::STRUCT);
//             std::vector<TStructField> address_fields2;
//             TStructField coord_field;
//             coord_field.__set_name("coordinates");
//             coord_field.__set_contains_null(false); // Non-nullable for struct_element
//             address_fields2.push_back(coord_field);
//             address_struct_node.__set_struct_fields(address_fields2);
//             profile_type.types.push_back(address_struct_node);
//             // coordinates STRUCT with two fields 'lat' and 'lng'
//             TTypeNode coordinates_struct_node;
//             coordinates_struct_node.__set_type(TTypeNodeType::STRUCT);
//             std::vector<TStructField> coordinates_fields2;
//             TStructField lat_field2;
//             lat_field2.__set_name("lat");
//             lat_field2.__set_contains_null(false); // Non-nullable for struct_element
//             coordinates_fields2.push_back(lat_field2);
//             TStructField lng_field2;
//             lng_field2.__set_name("lng");
//             lng_field2.__set_contains_null(false); // Non-nullable for struct_element
//             coordinates_fields2.push_back(lng_field2);
//             coordinates_struct_node.__set_struct_fields(coordinates_fields2);
//             profile_type.types.push_back(coordinates_struct_node);
//             // SCALAR nodes for lat and lng
//             TTypeNode lat_scalar_node2;
//             lat_scalar_node2.__set_type(TTypeNodeType::SCALAR);
//             TScalarType lat_scalar2;
//             lat_scalar2.__set_type(TPrimitiveType::DOUBLE);
//             lat_scalar_node2.__set_scalar_type(lat_scalar2);
//             profile_type.types.push_back(lat_scalar_node2);
//             TTypeNode lng_scalar_node2;
//             lng_scalar_node2.__set_type(TTypeNodeType::SCALAR);
//             TScalarType lng_scalar2;
//             lng_scalar2.__set_type(TPrimitiveType::DOUBLE);
//             lng_scalar_node2.__set_scalar_type(lng_scalar2);
//             profile_type.types.push_back(lng_scalar_node2);
//             slot_node.__set_type(profile_type);

//             TSlotRef profile_slot_ref;
//             profile_slot_ref.__set_slot_id(profile_slot->id());
//             profile_slot_ref.__set_tuple_id(profile_slot->parent());
//             slot_node.__set_slot_ref(profile_slot_ref);

//             return slot_node;
//         }

//         // Create struct_element node
//         TExprNode struct_element_node;
//         struct_element_node.__set_node_type(TExprNodeType::FUNCTION_CALL);
//         struct_element_node.__set_num_children(2);
//         struct_element_node.__set_is_nullable(true);

//         // Set result type
//         struct_element_node.__set_type(result_type);

//         // Set function info for struct_element
//         TFunction struct_element_fn;
//         TFunctionName struct_element_fn_name;
//         struct_element_fn_name.__set_function_name("struct_element");
//         struct_element_fn.__set_name(struct_element_fn_name);
//         struct_element_node.__set_fn(struct_element_fn);

//         return struct_element_node;
//     }

//     // Helper function to create conjuncts with nested column predicates
//     // This creates a simpler but correct predicate expression that can actually be prepared and executed
//     VExprContextSPtrs createNestedColumnConjuncts(ObjectPool& obj_pool,
//                                                    const TupleDescriptor* tuple_descriptor,
//                                                    RuntimeState* runtime_state) {
//         VExprContextSPtrs conjuncts;

//         // Find the profile slot descriptor
//         const SlotDescriptor* profile_slot = nullptr;
//         for (const SlotDescriptor* slot : tuple_descriptor->slots()) {
//             if (slot->col_name() == "profile") {
//                 profile_slot = slot;
//                 break;
//             }
//         }

//         if (!profile_slot) {
//             return conjuncts; // No profile column found
//         }

//         // Create predicate: struct_element(struct_element(struct_element(profile, 'address'), 'coordinates'), 'lat') > 40.0
//         {
//             TExpr lat_predicate;

//             // Root node: binary predicate (GT)
//             TExprNode gt_node;
//             gt_node.__set_node_type(TExprNodeType::BINARY_PRED);
//             gt_node.__set_opcode(TExprOpcode::GT);
//             gt_node.__set_num_children(2);
//             gt_node.__set_is_nullable(true);

//             // Set return type as BOOLEAN
//             TTypeDesc bool_type;
//             TTypeNode bool_type_node;
//             bool_type_node.__set_type(TTypeNodeType::SCALAR);
//             TScalarType bool_scalar;
//             bool_scalar.__set_type(TPrimitiveType::BOOLEAN);
//             bool_type_node.__set_scalar_type(bool_scalar);
//             bool_type.types.push_back(bool_type_node);
//             gt_node.__set_type(bool_type);

//             // Set function info
//             TFunction gt_fn;
//             TFunctionName gt_fn_name;
//             gt_fn_name.__set_function_name("gt");
//             gt_fn.__set_name(gt_fn_name);
//             gt_node.__set_fn(gt_fn);

//             lat_predicate.nodes.push_back(gt_node);

//             // Left child: nested struct_element calls - struct_element(struct_element(struct_element(profile, 'address'), 'coordinates'), 'lat')

//             // Outermost struct_element for 'lat'
//             TExprNode lat_struct_element;
//             lat_struct_element.__set_node_type(TExprNodeType::FUNCTION_CALL);
//             lat_struct_element.__set_num_children(2);
//             lat_struct_element.__set_is_nullable(true);

//             // Set type as DOUBLE for the result
//             TTypeDesc double_type;
//             TTypeNode double_type_node;
//             double_type_node.__set_type(TTypeNodeType::SCALAR);
//             TScalarType double_scalar;
//             double_scalar.__set_type(TPrimitiveType::DOUBLE);
//             double_type_node.__set_scalar_type(double_scalar);
//             double_type.types.push_back(double_type_node);
//             lat_struct_element.__set_type(double_type);

//             // Set function info for struct_element (simplified - let Doris infer types)
//             TFunction struct_element_fn;
//             TFunctionName struct_element_fn_name;
//             struct_element_fn_name.__set_function_name("struct_element");
//             struct_element_fn.__set_name(struct_element_fn_name);
//             struct_element_fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
//             struct_element_fn.__set_has_var_args(false);
//             lat_struct_element.__set_fn(struct_element_fn);

//             lat_predicate.nodes.push_back(lat_struct_element);

//             // Middle struct_element for 'coordinates'
//             TExprNode coord_struct_element;
//             coord_struct_element.__set_node_type(TExprNodeType::FUNCTION_CALL);
//             coord_struct_element.__set_num_children(2);
//             coord_struct_element.__set_is_nullable(true);

//             // Set type as STRUCT for coordinates, with one field 'lat' of DOUBLE
//             TTypeDesc coord_struct_type;
//             TTypeNode coord_struct_type_node;
//             coord_struct_type_node.__set_type(TTypeNodeType::STRUCT);
//             std::vector<TStructField> coord_fields;
//             TStructField coord_lat_field;
//             coord_lat_field.__set_name("lat");
//             coord_lat_field.__set_contains_null(true);
//             coord_fields.push_back(coord_lat_field);
//             coord_struct_type_node.__set_struct_fields(coord_fields);
//             coord_struct_type.types.push_back(coord_struct_type_node);
//             // child type for 'lat'
//             TTypeNode coord_lat_scalar_node;
//             coord_lat_scalar_node.__set_type(TTypeNodeType::SCALAR);
//             TScalarType coord_lat_scalar;
//             coord_lat_scalar.__set_type(TPrimitiveType::DOUBLE);
//             coord_lat_scalar_node.__set_scalar_type(coord_lat_scalar);
//             coord_struct_type.types.push_back(coord_lat_scalar_node);
//             coord_struct_element.__set_type(coord_struct_type);

//             // Set function info for middle struct_element (simplified)
//             TFunction coord_struct_element_fn;
//             TFunctionName coord_fn_name;
//             coord_fn_name.__set_function_name("struct_element");
//             coord_struct_element_fn.__set_name(coord_fn_name);
//             coord_struct_element_fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
//             coord_struct_element_fn.__set_has_var_args(false);
//             coord_struct_element.__set_fn(coord_struct_element_fn);

//             lat_predicate.nodes.push_back(coord_struct_element);

//             // Inner struct_element for 'address'
//             TExprNode addr_struct_element;
//             addr_struct_element.__set_node_type(TExprNodeType::FUNCTION_CALL);
//             addr_struct_element.__set_num_children(2);
//             addr_struct_element.__set_is_nullable(true);

//             // Set type as STRUCT for address, with one field 'coordinates' (STRUCT with one field 'lat')
//             TTypeDesc addr_struct_type;
//             TTypeNode addr_struct_type_node;
//             addr_struct_type_node.__set_type(TTypeNodeType::STRUCT);
//             std::vector<TStructField> addr_fields;
//             TStructField coordinates_field;
//             coordinates_field.__set_name("coordinates");
//             coordinates_field.__set_contains_null(true);
//             addr_fields.push_back(coordinates_field);
//             addr_struct_type_node.__set_struct_fields(addr_fields);
//             addr_struct_type.types.push_back(addr_struct_type_node);
//             // nested coordinates struct definition inside address
//             TTypeNode nested_coord_struct_node;
//             nested_coord_struct_node.__set_type(TTypeNodeType::STRUCT);
//             std::vector<TStructField> nested_coord_fields;
//             TStructField nested_lat_field;
//             nested_lat_field.__set_name("lat");
//             nested_lat_field.__set_contains_null(true);
//             nested_coord_fields.push_back(nested_lat_field);
//             nested_coord_struct_node.__set_struct_fields(nested_coord_fields);
//             addr_struct_type.types.push_back(nested_coord_struct_node);
//             // nested lat scalar under coordinates
//             TTypeNode nested_lat_scalar_node;
//             nested_lat_scalar_node.__set_type(TTypeNodeType::SCALAR);
//             TScalarType nested_lat_scalar;
//             nested_lat_scalar.__set_type(TPrimitiveType::DOUBLE);
//             nested_lat_scalar_node.__set_scalar_type(nested_lat_scalar);
//             addr_struct_type.types.push_back(nested_lat_scalar_node);
//             addr_struct_element.__set_type(addr_struct_type);

//             // Set function info for inner struct_element (simplified)
//             TFunction addr_struct_element_fn;
//             TFunctionName addr_fn_name;
//             addr_fn_name.__set_function_name("struct_element");
//             addr_struct_element_fn.__set_name(addr_fn_name);
//             addr_struct_element_fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
//             addr_struct_element_fn.__set_has_var_args(false);
//             addr_struct_element.__set_fn(addr_struct_element_fn);

//             lat_predicate.nodes.push_back(addr_struct_element);

//             // Base: slot reference to profile (non-nullable for struct_element)
//             TExprNode profile_slot_node;
//             profile_slot_node.__set_node_type(TExprNodeType::SLOT_REF);
//             profile_slot_node.__set_num_children(0);
//             profile_slot_node.__set_is_nullable(false); // struct_element expects non-nullable STRUCT

//             // Set type as STRUCT for profile with nested address->coordinates->{lat,lng}
//             TTypeDesc profile_type;
//             // profile STRUCT with one field 'address'
//             TTypeNode profile_struct_node;
//             profile_struct_node.__set_type(TTypeNodeType::STRUCT);
//             std::vector<TStructField> profile_fields;
//             TStructField address_field;
//             address_field.__set_name("address");
//             address_field.__set_contains_null(false); // Non-nullable for struct_element
//             profile_fields.push_back(address_field);
//             profile_struct_node.__set_struct_fields(profile_fields);
//             profile_type.types.push_back(profile_struct_node);
//             // address STRUCT with one field 'coordinates'
//             TTypeNode address_struct_node;
//             address_struct_node.__set_type(TTypeNodeType::STRUCT);
//             std::vector<TStructField> address_fields2;
//             TStructField coord_field;
//             coord_field.__set_name("coordinates");
//             coord_field.__set_contains_null(false); // Non-nullable for struct_element
//             address_fields2.push_back(coord_field);
//             address_struct_node.__set_struct_fields(address_fields2);
//             profile_type.types.push_back(address_struct_node);
//             // coordinates STRUCT with two fields 'lat' and 'lng'
//             TTypeNode coordinates_struct_node;
//             coordinates_struct_node.__set_type(TTypeNodeType::STRUCT);
//             std::vector<TStructField> coordinates_fields2;
//             TStructField lat_field2;
//             lat_field2.__set_name("lat");
//             lat_field2.__set_contains_null(false); // Non-nullable for struct_element
//             coordinates_fields2.push_back(lat_field2);
//             TStructField lng_field2;
//             lng_field2.__set_name("lng");
//             lng_field2.__set_contains_null(false); // Non-nullable for struct_element
//             coordinates_fields2.push_back(lng_field2);
//             coordinates_struct_node.__set_struct_fields(coordinates_fields2);
//             profile_type.types.push_back(coordinates_struct_node);
//             // SCALAR nodes for lat and lng
//             TTypeNode lat_scalar_node2;
//             lat_scalar_node2.__set_type(TTypeNodeType::SCALAR);
//             TScalarType lat_scalar2;
//             lat_scalar2.__set_type(TPrimitiveType::DOUBLE);
//             lat_scalar_node2.__set_scalar_type(lat_scalar2);
//             profile_type.types.push_back(lat_scalar_node2);
//             TTypeNode lng_scalar_node2;
//             lng_scalar_node2.__set_type(TTypeNodeType::SCALAR);
//             TScalarType lng_scalar2;
//             lng_scalar2.__set_type(TPrimitiveType::DOUBLE);
//             lng_scalar_node2.__set_scalar_type(lng_scalar2);
//             profile_type.types.push_back(lng_scalar_node2);
//             profile_slot_node.__set_type(profile_type);

//             TSlotRef profile_slot_ref;
//             profile_slot_ref.__set_slot_id(profile_slot->id());
//             profile_slot_ref.__set_tuple_id(profile_slot->parent());
//             profile_slot_node.__set_slot_ref(profile_slot_ref);

//             lat_predicate.nodes.push_back(profile_slot_node);

//             // String literal for 'address'
//             TExprNode addr_field_node;
//             addr_field_node.__set_node_type(TExprNodeType::STRING_LITERAL);
//             addr_field_node.__set_num_children(0);
//             addr_field_node.__set_is_nullable(false);

//             TTypeDesc string_type;
//             TTypeNode string_type_node;
//             string_type_node.__set_type(TTypeNodeType::SCALAR);
//             TScalarType string_scalar;
//             string_scalar.__set_type(TPrimitiveType::STRING);
//             string_type_node.__set_scalar_type(string_scalar);
//             string_type.types.push_back(string_type_node);
//             addr_field_node.__set_type(string_type);

//             TStringLiteral addr_literal;
//             addr_literal.__set_value("address");
//             addr_field_node.__set_string_literal(addr_literal);

//             lat_predicate.nodes.push_back(addr_field_node);

//             // String literal for 'coordinates'
//             TExprNode coord_field_node;
//             coord_field_node.__set_node_type(TExprNodeType::STRING_LITERAL);
//             coord_field_node.__set_num_children(0);
//             coord_field_node.__set_is_nullable(false);
//             coord_field_node.__set_type(string_type);

//             TStringLiteral coord_literal;
//             coord_literal.__set_value("coordinates");
//             coord_field_node.__set_string_literal(coord_literal);

//             lat_predicate.nodes.push_back(coord_field_node);

//             // String literal for 'lat'
//             TExprNode lat_field_node;
//             lat_field_node.__set_node_type(TExprNodeType::STRING_LITERAL);
//             lat_field_node.__set_num_children(0);
//             lat_field_node.__set_is_nullable(false);
//             lat_field_node.__set_type(string_type);

//             TStringLiteral lat_literal;
//             lat_literal.__set_value("lat");
//             lat_field_node.__set_string_literal(lat_literal);

//             lat_predicate.nodes.push_back(lat_field_node);

//             // Right child: literal 40.0
//             TExprNode literal_40_node;
//             literal_40_node.__set_node_type(TExprNodeType::FLOAT_LITERAL);
//             literal_40_node.__set_num_children(0);
//             literal_40_node.__set_is_nullable(false);
//             literal_40_node.__set_type(double_type);

//             TFloatLiteral float_40;
//             float_40.__set_value(40.0);
//             literal_40_node.__set_float_literal(float_40);

//             lat_predicate.nodes.push_back(literal_40_node);

//             // Create VExprContext from TExpr
//             VExprContextSPtr lat_ctx;
//             auto status = VExpr::create_expr_tree(lat_predicate, lat_ctx);
//             if (status.ok() && lat_ctx) {
//                 conjuncts.push_back(lat_ctx);
//             }
//         }

//         // Create predicate: struct_element(struct_element(struct_element(profile, 'address'), 'coordinates'), 'lng') < -70.0
//         {
//             TExpr lng_predicate;

//             // Root node: binary predicate (LT)
//             TExprNode lt_node;
//             lt_node.__set_node_type(TExprNodeType::BINARY_PRED);
//             lt_node.__set_opcode(TExprOpcode::LT);
//             lt_node.__set_num_children(2);
//             lt_node.__set_is_nullable(true);

//             // Set return type as BOOLEAN
//             TTypeDesc bool_type;
//             TTypeNode bool_type_node;
//             bool_type_node.__set_type(TTypeNodeType::SCALAR);
//             TScalarType bool_scalar;
//             bool_scalar.__set_type(TPrimitiveType::BOOLEAN);
//             bool_type_node.__set_scalar_type(bool_scalar);
//             bool_type.types.push_back(bool_type_node);
//             lt_node.__set_type(bool_type);

//             // Set function info
//             TFunction lt_fn;
//             TFunctionName lt_fn_name;
//             lt_fn_name.__set_function_name("lt");
//             lt_fn.__set_name(lt_fn_name);
//             lt_node.__set_fn(lt_fn);

//             lng_predicate.nodes.push_back(lt_node);

//             // Left child: nested struct_element calls - struct_element(struct_element(struct_element(profile, 'address'), 'coordinates'), 'lng')

//             // Outermost struct_element for 'lng'
//             TExprNode lng_struct_element;
//             lng_struct_element.__set_node_type(TExprNodeType::FUNCTION_CALL);
//             lng_struct_element.__set_num_children(2);
//             lng_struct_element.__set_is_nullable(true);

//             // Set type as DOUBLE for the result
//             TTypeDesc double_type;
//             TTypeNode double_type_node;
//             double_type_node.__set_type(TTypeNodeType::SCALAR);
//             TScalarType double_scalar;
//             double_scalar.__set_type(TPrimitiveType::DOUBLE);
//             double_type_node.__set_scalar_type(double_scalar);
//             double_type.types.push_back(double_type_node);
//             lng_struct_element.__set_type(double_type);

//             // Set function info for struct_element (lng - simplified)
//             TFunction lng_struct_element_fn;
//             TFunctionName lng_fn_name;
//             lng_fn_name.__set_function_name("struct_element");
//             lng_struct_element_fn.__set_name(lng_fn_name);
//             lng_struct_element_fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
//             lng_struct_element_fn.__set_has_var_args(false);
//             lng_struct_element.__set_fn(lng_struct_element_fn);

//             lng_predicate.nodes.push_back(lng_struct_element);

//             // Middle struct_element for 'coordinates'
//             TExprNode coord_struct_element;
//             coord_struct_element.__set_node_type(TExprNodeType::FUNCTION_CALL);
//             coord_struct_element.__set_num_children(2);
//             coord_struct_element.__set_is_nullable(true);

//             // Set type as STRUCT for coordinates, with one field 'lng' of DOUBLE
//             TTypeDesc coord_struct_type;
//             TTypeNode coord_struct_type_node;
//             coord_struct_type_node.__set_type(TTypeNodeType::STRUCT);
//             std::vector<TStructField> coord_fields;
//             TStructField coord_lng_field;
//             coord_lng_field.__set_name("lng");
//             coord_lng_field.__set_contains_null(true);
//             coord_fields.push_back(coord_lng_field);
//             coord_struct_type_node.__set_struct_fields(coord_fields);
//             coord_struct_type.types.push_back(coord_struct_type_node);
//             // child type for 'lng'
//             TTypeNode coord_lng_scalar_node;
//             coord_lng_scalar_node.__set_type(TTypeNodeType::SCALAR);
//             TScalarType coord_lng_scalar;
//             coord_lng_scalar.__set_type(TPrimitiveType::DOUBLE);
//             coord_lng_scalar_node.__set_scalar_type(coord_lng_scalar);
//             coord_struct_type.types.push_back(coord_lng_scalar_node);
//             coord_struct_element.__set_type(coord_struct_type);

//             // Set function info for middle struct_element (simplified)
//             TFunction lng_coord_struct_element_fn;
//             TFunctionName lng_coord_fn_name;
//             lng_coord_fn_name.__set_function_name("struct_element");
//             lng_coord_struct_element_fn.__set_name(lng_coord_fn_name);
//             lng_coord_struct_element_fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
//             lng_coord_struct_element_fn.__set_has_var_args(false);
//             coord_struct_element.__set_fn(lng_coord_struct_element_fn);

//             lng_predicate.nodes.push_back(coord_struct_element);

//             // Inner struct_element for 'address'
//             TExprNode addr_struct_element;
//             addr_struct_element.__set_node_type(TExprNodeType::FUNCTION_CALL);
//             addr_struct_element.__set_num_children(2);
//             addr_struct_element.__set_is_nullable(true);

//             // Set type as STRUCT for address, with one field 'coordinates' (STRUCT with one field 'lng')
//             TTypeDesc addr_struct_type;
//             TTypeNode addr_struct_type_node;
//             addr_struct_type_node.__set_type(TTypeNodeType::STRUCT);
//             std::vector<TStructField> addr_fields;
//             TStructField coordinates_field;
//             coordinates_field.__set_name("coordinates");
//             coordinates_field.__set_contains_null(true);
//             addr_fields.push_back(coordinates_field);
//             addr_struct_type_node.__set_struct_fields(addr_fields);
//             addr_struct_type.types.push_back(addr_struct_type_node);
//             // nested coordinates struct definition inside address
//             TTypeNode nested_coord_struct_node;
//             nested_coord_struct_node.__set_type(TTypeNodeType::STRUCT);
//             std::vector<TStructField> nested_coord_fields;
//             TStructField nested_lng_field;
//             nested_lng_field.__set_name("lng");
//             nested_lng_field.__set_contains_null(true);
//             nested_coord_fields.push_back(nested_lng_field);
//             nested_coord_struct_node.__set_struct_fields(nested_coord_fields);
//             addr_struct_type.types.push_back(nested_coord_struct_node);
//             // nested lng scalar under coordinates
//             TTypeNode nested_lng_scalar_node;
//             nested_lng_scalar_node.__set_type(TTypeNodeType::SCALAR);
//             TScalarType nested_lng_scalar;
//             nested_lng_scalar.__set_type(TPrimitiveType::DOUBLE);
//             nested_lng_scalar_node.__set_scalar_type(nested_lng_scalar);
//             addr_struct_type.types.push_back(nested_lng_scalar_node);
//             addr_struct_element.__set_type(addr_struct_type);

//             // Set function info for inner struct_element (simplified)
//             TFunction lng_addr_struct_element_fn;
//             TFunctionName lng_addr_fn_name;
//             lng_addr_fn_name.__set_function_name("struct_element");
//             lng_addr_struct_element_fn.__set_name(lng_addr_fn_name);
//             lng_addr_struct_element_fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
//             lng_addr_struct_element_fn.__set_has_var_args(false);
//             addr_struct_element.__set_fn(lng_addr_struct_element_fn);

//             lng_predicate.nodes.push_back(addr_struct_element);

//             // Base: slot reference to profile (non-nullable for struct_element)
//             TExprNode profile_slot_node;
//             profile_slot_node.__set_node_type(TExprNodeType::SLOT_REF);
//             profile_slot_node.__set_num_children(0);
//             profile_slot_node.__set_is_nullable(false); // struct_element expects non-nullable STRUCT

//             // Set type as STRUCT for profile with nested address->coordinates->{lat,lng}
//             TTypeDesc profile_type;
//             // profile STRUCT with one field 'address'
//             TTypeNode profile_struct_node;
//             profile_struct_node.__set_type(TTypeNodeType::STRUCT);
//             std::vector<TStructField> profile_fields_lng;
//             TStructField address_field_lng;
//             address_field_lng.__set_name("address");
//             address_field_lng.__set_contains_null(false); // Non-nullable for struct_element
//             profile_fields_lng.push_back(address_field_lng);
//             profile_struct_node.__set_struct_fields(profile_fields_lng);
//             profile_type.types.push_back(profile_struct_node);
//             // address STRUCT with one field 'coordinates'
//             TTypeNode address_struct_node_lng;
//             address_struct_node_lng.__set_type(TTypeNodeType::STRUCT);
//             std::vector<TStructField> address_fields_lng;
//             TStructField coord_field_lng;
//             coord_field_lng.__set_name("coordinates");
//             coord_field_lng.__set_contains_null(false); // Non-nullable for struct_element
//             address_fields_lng.push_back(coord_field_lng);
//             address_struct_node_lng.__set_struct_fields(address_fields_lng);
//             profile_type.types.push_back(address_struct_node_lng);
//             // coordinates STRUCT with two fields 'lat' and 'lng'
//             TTypeNode coordinates_struct_node_lng;
//             coordinates_struct_node_lng.__set_type(TTypeNodeType::STRUCT);
//             std::vector<TStructField> coordinates_fields_lng;
//             TStructField lat_field_lng;
//             lat_field_lng.__set_name("lat");
//             lat_field_lng.__set_contains_null(false); // Non-nullable for struct_element
//             coordinates_fields_lng.push_back(lat_field_lng);
//             TStructField lng_field_lng;
//             lng_field_lng.__set_name("lng");
//             lng_field_lng.__set_contains_null(false); // Non-nullable for struct_element
//             coordinates_fields_lng.push_back(lng_field_lng);
//             coordinates_struct_node_lng.__set_struct_fields(coordinates_fields_lng);
//             profile_type.types.push_back(coordinates_struct_node_lng);
//             // SCALAR nodes for lat and lng
//             TTypeNode lat_scalar_node_lng;
//             lat_scalar_node_lng.__set_type(TTypeNodeType::SCALAR);
//             TScalarType lat_scalar_lng;
//             lat_scalar_lng.__set_type(TPrimitiveType::DOUBLE);
//             lat_scalar_node_lng.__set_scalar_type(lat_scalar_lng);
//             profile_type.types.push_back(lat_scalar_node_lng);
//             TTypeNode lng_scalar_node_lng;
//             lng_scalar_node_lng.__set_type(TTypeNodeType::SCALAR);
//             TScalarType lng_scalar_lng;
//             lng_scalar_lng.__set_type(TPrimitiveType::DOUBLE);
//             lng_scalar_node_lng.__set_scalar_type(lng_scalar_lng);
//             profile_type.types.push_back(lng_scalar_node_lng);
//             profile_slot_node.__set_type(profile_type);

//             TSlotRef profile_slot_ref;
//             profile_slot_ref.__set_slot_id(profile_slot->id());
//             profile_slot_ref.__set_tuple_id(profile_slot->parent());
//             profile_slot_node.__set_slot_ref(profile_slot_ref);

//             lng_predicate.nodes.push_back(profile_slot_node);

//             // String literal for 'address'
//             TExprNode addr_field_node;
//             addr_field_node.__set_node_type(TExprNodeType::STRING_LITERAL);
//             addr_field_node.__set_num_children(0);
//             addr_field_node.__set_is_nullable(false);

//             TTypeDesc string_type;
//             TTypeNode string_type_node;
//             string_type_node.__set_type(TTypeNodeType::SCALAR);
//             TScalarType string_scalar;
//             string_scalar.__set_type(TPrimitiveType::STRING);
//             string_type_node.__set_scalar_type(string_scalar);
//             string_type.types.push_back(string_type_node);
//             addr_field_node.__set_type(string_type);

//             TStringLiteral addr_literal;
//             addr_literal.__set_value("address");
//             addr_field_node.__set_string_literal(addr_literal);

//             lng_predicate.nodes.push_back(addr_field_node);

//             // String literal for 'coordinates'
//             TExprNode coord_field_node;
//             coord_field_node.__set_node_type(TExprNodeType::STRING_LITERAL);
//             coord_field_node.__set_num_children(0);
//             coord_field_node.__set_is_nullable(false);
//             coord_field_node.__set_type(string_type);

//             TStringLiteral coord_literal;
//             coord_literal.__set_value("coordinates");
//             coord_field_node.__set_string_literal(coord_literal);

//             lng_predicate.nodes.push_back(coord_field_node);

//             // String literal for 'lng'
//             TExprNode lng_field_node;
//             lng_field_node.__set_node_type(TExprNodeType::STRING_LITERAL);
//             lng_field_node.__set_num_children(0);
//             lng_field_node.__set_is_nullable(false);
//             lng_field_node.__set_type(string_type);

//             TStringLiteral lng_literal;
//             lng_literal.__set_value("lng");
//             lng_field_node.__set_string_literal(lng_literal);

//             lng_predicate.nodes.push_back(lng_field_node);

//             // Right child: literal -70.0
//             TExprNode literal_neg70_node;
//             literal_neg70_node.__set_node_type(TExprNodeType::FLOAT_LITERAL);
//             literal_neg70_node.__set_num_children(0);
//             literal_neg70_node.__set_is_nullable(false);
//             literal_neg70_node.__set_type(double_type);

//             TFloatLiteral float_neg70;
//             float_neg70.__set_value(-70.0);
//             literal_neg70_node.__set_float_literal(float_neg70);

//             lng_predicate.nodes.push_back(literal_neg70_node);

//             // Create VExprContext from TExpr
//             VExprContextSPtr lng_ctx;
//             auto status = VExpr::create_expr_tree(lng_predicate, lng_ctx);
//             if (status.ok() && lng_ctx) {
//                 conjuncts.push_back(lng_ctx);
//             }
//         }

//         return conjuncts; // Return the created predicate conjuncts
//     }

//     std::unique_ptr<doris::FileMetaCache> cache;
//     cctz::time_zone timezone_obj;
// };

// TEST_F(HiveReaderPredicateTest, test_parquet_create_column_ids_and_names) {
//     // Read only: name, profile.address.coordinates.lat, profile.address.coordinates.lng, profile.contact.email
//     // Setup table descriptor for test columns with new schema:
//     /**
//     Schema:
//     message table {
//     required int64 id = 1;
//     required binary name (STRING) = 2;
//     required group profile = 3 {
//         optional group address = 4 {
//         optional binary street (STRING) = 7;
//         optional binary city (STRING) = 8;
//         optional group coordinates = 9 {
//             optional double lat = 10;
//             optional double lng = 11;
//         }
//         }
//         optional group contact = 5 {
//         optional binary email (STRING) = 12;
//         optional group phone = 13 {
//             optional binary country_code (STRING) = 14;
//             optional binary number (STRING) = 15;
//         }
//         }
//         optional group hobbies (LIST) = 6 {
//         repeated group list {
//             optional group element = 16 {
//             optional binary name (STRING) = 17;
//             optional int32 level = 18;
//             }
//         }
//         }
//     }
//     }
//     */

//     // Open the Hive Parquet test file
//     auto local_fs = io::global_local_filesystem();
//     io::FileReaderSPtr file_reader;
//     std::string test_file =
//             "./be/test/exec/test_data/complex_user_profiles_iceberg_parquet/data/"
//             "00000-0-a0022aad-d3b6-4e73-b181-f0a09aac7034-0-00001.parquet";
//     auto st = local_fs->open_file(test_file, &file_reader);
//     if (!st.ok()) {
//         GTEST_SKIP() << "Test file not found: " << test_file;
//         return;
//     }

//     // Setup runtime state
//     RuntimeState runtime_state((TQueryGlobals()));

//     // Setup scan parameters
//     TFileScanRangeParams scan_params;
//     scan_params.format_type = TFileFormatType::FORMAT_PARQUET;

//     TFileRangeDesc scan_range;
//     scan_range.start_offset = 0;
//     scan_range.size = file_reader->size(); // Read entire file
//     scan_range.path = test_file;

//     // Create mock profile
//     RuntimeProfile profile("test_profile");

//     // Create ParquetReader as the underlying file format reader
//     cctz::time_zone ctz;
//     TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);

//     auto generic_reader = ParquetReader::create_unique(&profile, scan_params, scan_range, 1024,
//                                                        &ctz, nullptr, &runtime_state, cache.get());
//     ASSERT_NE(generic_reader, nullptr);

//     // Set file reader for the generic reader
//     auto parquet_reader = static_cast<ParquetReader*>(generic_reader.get());
//     parquet_reader->set_file_reader(file_reader);

//     // Get FieldDescriptor from Parquet file
//     const FieldDescriptor* field_desc = nullptr;
//     st = parquet_reader->get_file_metadata_schema(&field_desc);
//     ASSERT_TRUE(st.ok()) << st;
//     ASSERT_NE(field_desc, nullptr);

//     // Create HiveParquetReader
//     auto hive_reader = std::make_unique<HiveParquetReader>(std::move(generic_reader), &profile,
//                                                            &runtime_state, scan_params, scan_range,
//                                                            nullptr, nullptr, cache.get());

//     // Create complex struct types using helper function
//     DataTypePtr coordinates_struct_type, address_struct_type, phone_struct_type;
//     DataTypePtr contact_struct_type, hobby_element_struct_type, hobbies_array_type;
//     DataTypePtr profile_struct_type, name_type;
//     createComplexStructTypes(coordinates_struct_type, address_struct_type, phone_struct_type,
//                              contact_struct_type, hobby_element_struct_type, hobbies_array_type,
//                              profile_struct_type, name_type);

//     // Create tuple descriptor using helper function
//     DescriptorTbl* desc_tbl;
//     ObjectPool obj_pool;
//     TDescriptorTable t_desc_table;
//     TTableDescriptor t_table_desc;
//     std::vector<std::string> table_column_names = {"name", "profile"};
//     std::vector<int> table_column_positions = {1, 2};
//     std::vector<TPrimitiveType::type> table_column_types = {
//             TPrimitiveType::STRING, TPrimitiveType::STRUCT // profile 用 STRUCT 类型
//     };
//     const TupleDescriptor* tuple_descriptor = createTupleDescriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc,
//                                                                    table_column_names, table_column_positions, table_column_types);

//     VExprContextSPtrs conjuncts; // Empty conjuncts for this test
//     std::vector<std::string> table_col_names = {"name", "profile"};

//     auto actual_result =
//             HiveParquetReader::_create_column_ids_and_names(field_desc, tuple_descriptor);
//     // column_ids should contain all necessary column IDs (set automatically deduplicates)
//     // Expected IDs based on the schema: name(2), profile(3), address(4), coordinates(7), lat(8), lng(9), contact(10), email(11), hobbies(15), element(16), level(18)
//     std::set<uint64_t> expected_ids = {2, 3, 4, 7, 8, 9, 10, 11, 15, 16, 18};
//     EXPECT_EQ(actual_result.column_ids, expected_ids);
// }

// TEST_F(HiveReaderPredicateTest, test_parquet_create_column_ids_and_names_by_top_level_col_index) {
//     // Read only: name, profile.address.coordinates.lat, profile.address.coordinates.lng, profile.contact.email
//     // Setup table descriptor for test columns with new schema:
//     /**
//     Schema:
//     message table {
//     required int64 id = 1;
//     required binary name (STRING) = 2;
//     required group profile = 3 {
//         optional group address = 4 {
//         optional binary street (STRING) = 7;
//         optional binary city (STRING) = 8;
//         optional group coordinates = 9 {
//             optional double lat = 10;
//             optional double lng = 11;
//         }
//         }
//         optional group contact = 5 {
//         optional binary email (STRING) = 12;
//         optional group phone = 13 {
//             optional binary country_code (STRING) = 14;
//             optional binary number (STRING) = 15;
//         }
//         }
//         optional group hobbies (LIST) = 6 {
//         repeated group list {
//             optional group element = 16 {
//             optional binary name (STRING) = 17;
//             optional int32 level = 18;
//             }
//         }
//         }
//     }
//     }
//     */

//     // Open the Hive Parquet test file
//     auto local_fs = io::global_local_filesystem();
//     io::FileReaderSPtr file_reader;
//     std::string test_file =
//             "./be/test/exec/test_data/complex_user_profiles_iceberg_parquet/data/"
//             "00000-0-a0022aad-d3b6-4e73-b181-f0a09aac7034-0-00001.parquet";
//     auto st = local_fs->open_file(test_file, &file_reader);
//     if (!st.ok()) {
//         GTEST_SKIP() << "Test file not found: " << test_file;
//         return;
//     }

//     // Setup runtime state
//     RuntimeState runtime_state((TQueryGlobals()));

//     // Setup scan parameters
//     TFileScanRangeParams scan_params;
//     scan_params.format_type = TFileFormatType::FORMAT_PARQUET;

//     TFileRangeDesc scan_range;
//     scan_range.start_offset = 0;
//     scan_range.size = file_reader->size(); // Read entire file
//     scan_range.path = test_file;

//     // Create mock profile
//     RuntimeProfile profile("test_profile");

//     // Create ParquetReader as the underlying file format reader
//     cctz::time_zone ctz;
//     TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);

//     auto generic_reader = ParquetReader::create_unique(&profile, scan_params, scan_range, 1024,
//                                                        &ctz, nullptr, &runtime_state, cache.get());
//     ASSERT_NE(generic_reader, nullptr);

//     // Set file reader for the generic reader
//     auto parquet_reader = static_cast<ParquetReader*>(generic_reader.get());
//     parquet_reader->set_file_reader(file_reader);

//     // Get FieldDescriptor from Parquet file
//     const FieldDescriptor* field_desc = nullptr;
//     st = parquet_reader->get_file_metadata_schema(&field_desc);
//     ASSERT_TRUE(st.ok()) << st;
//     ASSERT_NE(field_desc, nullptr);

//     // Create HiveParquetReader
//     auto hive_reader = std::make_unique<HiveParquetReader>(std::move(generic_reader), &profile,
//                                                            &runtime_state, scan_params, scan_range,
//                                                            nullptr, nullptr, cache.get());

//     // Create complex struct types using helper function
//     DataTypePtr coordinates_struct_type, address_struct_type, phone_struct_type;
//     DataTypePtr contact_struct_type, hobby_element_struct_type, hobbies_array_type;
//     DataTypePtr profile_struct_type, name_type;
//     createComplexStructTypes(coordinates_struct_type, address_struct_type, phone_struct_type,
//                              contact_struct_type, hobby_element_struct_type, hobbies_array_type,
//                              profile_struct_type, name_type);

//     // Create tuple descriptor using helper function
//     DescriptorTbl* desc_tbl;
//     ObjectPool obj_pool;
//     TDescriptorTable t_desc_table;
//     TTableDescriptor t_table_desc;
//     std::vector<std::string> table_column_names = {"name", "profile"};
//     std::vector<int> table_column_positions = {1, 2};
//     std::vector<TPrimitiveType::type> table_column_types = {
//             TPrimitiveType::STRING, TPrimitiveType::STRUCT // profile 用 STRUCT 类型
//     };
//     const TupleDescriptor* tuple_descriptor = createTupleDescriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc,
//                                                                    table_column_names, table_column_positions, table_column_types);

//     VExprContextSPtrs conjuncts; // Empty conjuncts for this test
//     std::vector<std::string> table_col_names = {"name", "profile"};

//     auto actual_result = HiveParquetReader::_create_column_ids_and_names_by_top_level_col_index(
//             field_desc, tuple_descriptor);
//     // column_ids should contain all necessary column IDs (set automatically deduplicates)
//     // Expected IDs based on the schema: name(2), profile(3), address(4), coordinates(7), lat(8), lng(9), contact(10), email(11), hobbies(15), element(16), level(18)
//     std::set<uint64_t> expected_ids = {2, 3, 4, 7, 8, 9, 10, 11, 15, 16, 18};
//     EXPECT_EQ(actual_result.column_ids, expected_ids);
// }

// TEST_F(HiveReaderPredicateTest, test_parquet_create_column_ids_and_names_by_index) {
//     // Read only: name, profile.address.coordinates.lat, profile.address.coordinates.lng, profile.contact.email
//     // Setup table descriptor for test columns with new schema:
//     /**
//     Schema:
//     message table {
//     required int64 id = 1;
//     required binary name (STRING) = 2;
//     required group profile = 3 {
//         optional group address = 4 {
//         optional binary street (STRING) = 7;
//         optional binary city (STRING) = 8;
//         optional group coordinates = 9 {
//             optional double lat = 10;
//             optional double lng = 11;
//         }
//         }
//         optional group contact = 5 {
//         optional binary email (STRING) = 12;
//         optional group phone = 13 {
//             optional binary country_code (STRING) = 14;
//             optional binary number (STRING) = 15;
//         }
//         }
//         optional group hobbies (LIST) = 6 {
//         repeated group list {
//             optional group element = 16 {
//             optional binary name (STRING) = 17;
//             optional int32 level = 18;
//             }
//         }
//         }
//     }
//     }
//     */

//     // Open the Hive Parquet test file
//     auto local_fs = io::global_local_filesystem();
//     io::FileReaderSPtr file_reader;
//     std::string test_file =
//             "./be/test/exec/test_data/complex_user_profiles_iceberg_parquet/data/"
//             "00000-0-a0022aad-d3b6-4e73-b181-f0a09aac7034-0-00001.parquet";
//     auto st = local_fs->open_file(test_file, &file_reader);
//     if (!st.ok()) {
//         GTEST_SKIP() << "Test file not found: " << test_file;
//         return;
//     }

//     // Setup runtime state
//     RuntimeState runtime_state((TQueryGlobals()));

//     // Setup scan parameters
//     TFileScanRangeParams scan_params;
//     scan_params.format_type = TFileFormatType::FORMAT_PARQUET;

//     TFileRangeDesc scan_range;
//     scan_range.start_offset = 0;
//     scan_range.size = file_reader->size(); // Read entire file
//     scan_range.path = test_file;

//     // Create mock profile
//     RuntimeProfile profile("test_profile");

//     // Create ParquetReader as the underlying file format reader
//     cctz::time_zone ctz;
//     TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);

//     auto generic_reader = ParquetReader::create_unique(&profile, scan_params, scan_range, 1024,
//                                                        &ctz, nullptr, &runtime_state, cache.get());
//     ASSERT_NE(generic_reader, nullptr);

//     // Set file reader for the generic reader
//     auto parquet_reader = static_cast<ParquetReader*>(generic_reader.get());
//     parquet_reader->set_file_reader(file_reader);

//     // Get FieldDescriptor from Parquet file
//     const FieldDescriptor* field_desc = nullptr;
//     st = parquet_reader->get_file_metadata_schema(&field_desc);
//     ASSERT_TRUE(st.ok()) << st;
//     ASSERT_NE(field_desc, nullptr);

//     // Create HiveParquetReader
//     auto hive_reader = std::make_unique<HiveParquetReader>(std::move(generic_reader), &profile,
//                                                            &runtime_state, scan_params, scan_range,
//                                                            nullptr, nullptr, cache.get());

//     // Create complex struct types using helper function
//     DataTypePtr coordinates_struct_type, address_struct_type, phone_struct_type;
//     DataTypePtr contact_struct_type, hobby_element_struct_type, hobbies_array_type;
//     DataTypePtr profile_struct_type, name_type;
//     createComplexStructTypes(coordinates_struct_type, address_struct_type, phone_struct_type,
//                              contact_struct_type, hobby_element_struct_type, hobbies_array_type,
//                              profile_struct_type, name_type);

//     // Create tuple descriptor using helper function (with index paths)
//     DescriptorTbl* desc_tbl;
//     ObjectPool obj_pool;
//     TDescriptorTable t_desc_table;
//     TTableDescriptor t_table_desc;
//     std::vector<std::string> table_column_names = {"name", "profile"};
//     std::vector<int> table_column_positions = {1, 2};
//     std::vector<TPrimitiveType::type> table_column_types = {
//             TPrimitiveType::STRING, TPrimitiveType::STRUCT // profile 用 STRUCT 类型
//     };
//     const TupleDescriptor* tuple_descriptor = createTupleDescriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc,
//                                                                    table_column_names, table_column_positions, table_column_types, false);

//     VExprContextSPtrs conjuncts; // Empty conjuncts for this test
//     std::vector<std::string> table_col_names = {"name", "profile"};

//     auto actual_result =
//             HiveParquetReader::_create_column_ids_and_names_by_index(field_desc, tuple_descriptor);
//     // column_ids should contain all necessary column IDs (set automatically deduplicates)
//     // Expected IDs based on the schema: name(2), profile(3), address(4), coordinates(7), lat(8), lng(9), contact(10), email(11), hobbies(15), element(16), level(18)
//     std::set<uint64_t> expected_ids = {2, 3, 4, 7, 8, 9, 10, 11, 15, 16, 18};
//     EXPECT_EQ(actual_result.column_ids, expected_ids);
// }

// TEST_F(HiveReaderPredicateTest, test_orc_create_column_ids_and_names) {
//     // Read only: name, profile.address.coordinates.lat, profile.address.coordinates.lng, profile.contact.email
//     // Setup table descriptor for test columns with new schema:
//     /**
//     Schema:
//     message table {
//     required int64 id = 1;
//     required binary name (STRING) = 2;
//     required group profile = 3 {
//         optional group address = 4 {
//         optional binary street (STRING) = 7;
//         optional binary city (STRING) = 8;
//         optional group coordinates = 9 {
//             optional double lat = 10;
//             optional double lng = 11;
//         }
//         }
//         optional group contact = 5 {
//         optional binary email (STRING) = 12;
//         optional group phone = 13 {
//             optional binary country_code (STRING) = 14;
//             optional binary number (STRING) = 15;
//         }
//         }
//         optional group hobbies (LIST) = 6 {
//         repeated group list {
//             optional group element = 16 {
//             optional binary name (STRING) = 17;
//             optional int32 level = 18;
//             }
//         }
//         }
//     }
//     }
//     */

//     // Open the Hive Orc test file
//     auto local_fs = io::global_local_filesystem();
//     io::FileReaderSPtr file_reader;
//     std::string test_file =
//             "./be/test/exec/test_data/complex_user_profiles_iceberg_orc/data/"
//             "00000-0-e4897963-0081-4127-bebe-35dc7dc1edeb-0-00001.orc";
//     auto st = local_fs->open_file(test_file, &file_reader);
//     if (!st.ok()) {
//         GTEST_SKIP() << "Test file not found: " << test_file;
//         return;
//     }

//     // Setup runtime state
//     RuntimeState runtime_state((TQueryGlobals()));

//     // Setup scan parameters
//     TFileScanRangeParams scan_params;
//     scan_params.format_type = TFileFormatType::FORMAT_ORC;

//     TFileRangeDesc scan_range;
//     scan_range.start_offset = 0;
//     scan_range.size = file_reader->size(); // Read entire file
//     scan_range.path = test_file;

//     // Create mock profile
//     RuntimeProfile profile("test_profile");

//     // Create OrcReader as the underlying file format reader
//     cctz::time_zone ctz;
//     TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);

//     auto generic_reader = OrcReader::create_unique(&profile, &runtime_state, scan_params,
//                                                    scan_range, 1024, "CST", nullptr, cache.get());
//     ASSERT_NE(generic_reader, nullptr);

//     auto orc_reader = static_cast<OrcReader*>(generic_reader.get());
//     // Get FieldDescriptor from Orc file
//     const orc::Type* orc_type_ptr = nullptr;
//     st = orc_reader->get_file_type(&orc_type_ptr);
//     ASSERT_TRUE(st.ok()) << st;
//     ASSERT_NE(orc_type_ptr, nullptr);

//     // Create HiveOrcReader
//     auto hive_reader =
//             std::make_unique<HiveOrcReader>(std::move(generic_reader), &profile, &runtime_state,
//                                             scan_params, scan_range, nullptr, nullptr, cache.get());

//     // Create complex struct types using helper function
//     DataTypePtr coordinates_struct_type, address_struct_type, phone_struct_type;
//     DataTypePtr contact_struct_type, hobby_element_struct_type, hobbies_array_type;
//     DataTypePtr profile_struct_type, name_type;
//     createComplexStructTypes(coordinates_struct_type, address_struct_type, phone_struct_type,
//                              contact_struct_type, hobby_element_struct_type, hobbies_array_type,
//                              profile_struct_type, name_type);

//     // Create tuple descriptor using helper function
//     DescriptorTbl* desc_tbl;
//     ObjectPool obj_pool;
//     TDescriptorTable t_desc_table;
//     TTableDescriptor t_table_desc;
//     std::vector<std::string> table_column_names = {"name", "profile"};
//     std::vector<int> table_column_positions = {1, 2};
//     std::vector<TPrimitiveType::type> table_column_types = {
//             TPrimitiveType::STRING, TPrimitiveType::STRUCT // profile 用 STRUCT 类型
//     };
//     const TupleDescriptor* tuple_descriptor = createTupleDescriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc,
//                                                                    table_column_names, table_column_positions, table_column_types);

//     VExprContextSPtrs conjuncts; // Empty conjuncts for this test
//     std::vector<std::string> table_col_names = {"name", "profile"};

//     auto actual_result =
//             HiveOrcReader::_create_column_ids_and_names(orc_type_ptr, tuple_descriptor);
//     // column_ids should contain all necessary column IDs (set automatically deduplicates)
//     // Expected IDs based on the schema: name(2), profile(3), address(4), coordinates(7), lat(8), lng(9), contact(10), email(11), hobbies(15), element(16), level(18)
//     std::set<uint64_t> expected_ids = {2, 8, 9, 11, 18};
//     EXPECT_EQ(actual_result.column_ids, expected_ids);
// }

// TEST_F(HiveReaderPredicateTest, test_orc_create_column_ids_and_names_by_index) {
//     // Read only: name, profile.address.coordinates.lat, profile.address.coordinates.lng, profile.contact.email
//     // Setup table descriptor for test columns with new schema:
//     /**
//     Schema:
//     message table {
//     required int64 id = 1;
//     required binary name (STRING) = 2;
//     required group profile = 3 {
//         optional group address = 4 {
//         optional binary street (STRING) = 7;
//         optional binary city (STRING) = 8;
//         optional group coordinates = 9 {
//             optional double lat = 10;
//             optional double lng = 11;
//         }
//         }
//         optional group contact = 5 {
//         optional binary email (STRING) = 12;
//         optional group phone = 13 {
//             optional binary country_code (STRING) = 14;
//             optional binary number (STRING) = 15;
//         }
//         }
//         optional group hobbies (LIST) = 6 {
//         repeated group list {
//             optional group element = 16 {
//             optional binary name (STRING) = 17;
//             optional int32 level = 18;
//             }
//         }
//         }
//     }
//     }
//     */

//     // Open the Hive Orc test file
//     auto local_fs = io::global_local_filesystem();
//     io::FileReaderSPtr file_reader;
//     std::string test_file =
//             "./be/test/exec/test_data/complex_user_profiles_iceberg_orc/data/"
//             "00000-0-e4897963-0081-4127-bebe-35dc7dc1edeb-0-00001.orc";
//     auto st = local_fs->open_file(test_file, &file_reader);
//     if (!st.ok()) {
//         GTEST_SKIP() << "Test file not found: " << test_file;
//         return;
//     }

//     // Setup runtime state
//     RuntimeState runtime_state((TQueryGlobals()));

//     // Setup scan parameters
//     TFileScanRangeParams scan_params;
//     scan_params.format_type = TFileFormatType::FORMAT_ORC;

//     TFileRangeDesc scan_range;
//     scan_range.start_offset = 0;
//     scan_range.size = file_reader->size(); // Read entire file
//     scan_range.path = test_file;

//     // Create mock profile
//     RuntimeProfile profile("test_profile");

//     // Create OrcReader as the underlying file format reader
//     cctz::time_zone ctz;
//     TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);

//     auto generic_reader = OrcReader::create_unique(&profile, &runtime_state, scan_params,
//                                                    scan_range, 1024, "CST", nullptr, cache.get());
//     ASSERT_NE(generic_reader, nullptr);

//     auto orc_reader = static_cast<OrcReader*>(generic_reader.get());
//     // Get FieldDescriptor from Orc file
//     const orc::Type* orc_type_ptr = nullptr;
//     st = orc_reader->get_file_type(&orc_type_ptr);
//     ASSERT_TRUE(st.ok()) << st;
//     ASSERT_NE(orc_type_ptr, nullptr);

//     // Create HiveOrcReader
//     auto hive_reader =
//             std::make_unique<HiveOrcReader>(std::move(generic_reader), &profile, &runtime_state,
//                                             scan_params, scan_range, nullptr, nullptr, cache.get());

//     // Create complex struct types using helper function
//     DataTypePtr coordinates_struct_type, address_struct_type, phone_struct_type;
//     DataTypePtr contact_struct_type, hobby_element_struct_type, hobbies_array_type;
//     DataTypePtr profile_struct_type, name_type;
//     createComplexStructTypes(coordinates_struct_type, address_struct_type, phone_struct_type,
//                              contact_struct_type, hobby_element_struct_type, hobbies_array_type,
//                              profile_struct_type, name_type);

//     // Create tuple descriptor using helper function (with index paths)
//     DescriptorTbl* desc_tbl;
//     ObjectPool obj_pool;
//     TDescriptorTable t_desc_table;
//     TTableDescriptor t_table_desc;
//     std::vector<std::string> table_column_names = {"name", "profile"};
//     std::vector<int> table_column_positions = {1, 2};
//     std::vector<TPrimitiveType::type> table_column_types = {
//             TPrimitiveType::STRING, TPrimitiveType::STRUCT // profile 用 STRUCT 类型
//     };
//     const TupleDescriptor* tuple_descriptor = createTupleDescriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc,
//                                                                    table_column_names, table_column_positions, table_column_types, false);

//     VExprContextSPtrs conjuncts; // Empty conjuncts for this test
//     std::vector<std::string> table_col_names = {"name", "profile"};

//     auto actual_result =
//             HiveOrcReader::_create_column_ids_and_names_by_index(orc_type_ptr, tuple_descriptor);
//     // column_ids should contain all necessary column IDs (set automatically deduplicates)
//     // Expected IDs based on the schema: name(2), profile(3), address(4), coordinates(7), lat(8), lng(9), contact(10), email(11), hobbies(15), element(16), level(18)
//     std::set<uint64_t> expected_ids = {2, 8, 9, 11, 18};
//     EXPECT_EQ(actual_result.column_ids, expected_ids);
// }

// // Test reading real Hive Parquet file using HiveTableReader
// TEST_F(HiveReaderPredicateTest, ReadHiveParquetFile) {
//     // Read only: name, profile.address.coordinates.lat, profile.address.coordinates.lng, profile.contact.email
//     // Setup table descriptor for test columns with new schema:
//     /**
//     Schema:
//     message table {
//     required int64 id = 1;
//     required binary name (STRING) = 2;
//     required group profile = 3 {
//         optional group address = 4 {
//         optional binary street (STRING) = 7;
//         optional binary city (STRING) = 8;
//         optional group coordinates = 9 {
//             optional double lat = 10;
//             optional double lng = 11;
//         }
//         }
//         optional group contact = 5 {
//         optional binary email (STRING) = 12;
//         optional group phone = 13 {
//             optional binary country_code (STRING) = 14;
//             optional binary number (STRING) = 15;
//         }
//         }
//         optional group hobbies (LIST) = 6 {
//         repeated group list {
//             optional group element = 16 {
//             optional binary name (STRING) = 17;
//             optional int32 level = 18;
//             }
//         }
//         }
//     }
//     }
//     */

//     // Open the Hive Parquet test file
//     auto local_fs = io::global_local_filesystem();
//     io::FileReaderSPtr file_reader;
//     std::string test_file =
//             "./be/test/exec/test_data/complex_user_profiles_iceberg_parquet/data/"
//             "00000-0-a0022aad-d3b6-4e73-b181-f0a09aac7034-0-00001.parquet";
//     auto st = local_fs->open_file(test_file, &file_reader);
//     if (!st.ok()) {
//         GTEST_SKIP() << "Test file not found: " << test_file;
//         return;
//     }

//     // Setup runtime state
//     RuntimeState runtime_state((TQueryGlobals()));

//     // Setup scan parameters
//     TFileScanRangeParams scan_params;
//     scan_params.format_type = TFileFormatType::FORMAT_PARQUET;

//     TFileRangeDesc scan_range;
//     scan_range.start_offset = 0;
//     scan_range.size = file_reader->size(); // Read entire file
//     scan_range.path = test_file;

//     // Create mock profile
//     RuntimeProfile profile("test_profile");

//     // Create ParquetReader as the underlying file format reader
//     cctz::time_zone ctz;
//     TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);

//     auto generic_reader = ParquetReader::create_unique(&profile, scan_params, scan_range, 1024,
//                                                        &ctz, nullptr, &runtime_state, cache.get());
//     ASSERT_NE(generic_reader, nullptr);

//     // Set file reader for the generic reader
//     auto parquet_reader = static_cast<ParquetReader*>(generic_reader.get());
//     parquet_reader->set_file_reader(file_reader);

//     // Create HiveParquetReader
//     auto hive_reader = std::make_unique<HiveParquetReader>(std::move(generic_reader), &profile,
//                                                            &runtime_state, scan_params, scan_range,
//                                                            nullptr, nullptr, cache.get());

//     // Create complex struct types using helper function
//     DataTypePtr coordinates_struct_type, address_struct_type, phone_struct_type;
//     DataTypePtr contact_struct_type, hobby_element_struct_type, hobbies_array_type;
//     DataTypePtr profile_struct_type, name_type;
//     createComplexStructTypes(coordinates_struct_type, address_struct_type, phone_struct_type,
//                              contact_struct_type, hobby_element_struct_type, hobbies_array_type,
//                              profile_struct_type, name_type);

//     // Create tuple descriptor using helper function
//     DescriptorTbl* desc_tbl;
//     ObjectPool obj_pool;
//     TDescriptorTable t_desc_table;
//     TTableDescriptor t_table_desc;
//     std::vector<std::string> table_column_names = {"name", "profile"};
//     std::vector<int> table_column_positions = {1, 2};
//     std::vector<TPrimitiveType::type> table_column_types = {
//             TPrimitiveType::STRING, TPrimitiveType::STRUCT // profile 用 STRUCT 类型
//     };
//     const TupleDescriptor* tuple_descriptor = createTupleDescriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc,
//                                                                    table_column_names, table_column_positions, table_column_types);

//     VExprContextSPtrs conjuncts; // Empty conjuncts for this test
//     std::vector<std::string> table_col_names = {"name", "profile"};
//     const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range = nullptr;
//     const RowDescriptor* row_descriptor = nullptr;
//     const std::unordered_map<std::string, int>* colname_to_slot_id = nullptr;
//     const VExprContextSPtrs* not_single_slot_filter_conjuncts = nullptr;
//     const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts = nullptr;

//     st = hive_reader->init_reader(table_col_names, colname_to_value_range, conjuncts,
//                                   tuple_descriptor, row_descriptor, colname_to_slot_id,
//                                   not_single_slot_filter_conjuncts, slot_id_to_filter_conjuncts);
//     ASSERT_TRUE(st.ok()) << st;

//     std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
//             partition_columns;
//     std::unordered_map<std::string, VExprContextSPtr> missing_columns;
//     ASSERT_TRUE(hive_reader->set_fill_columns(partition_columns, missing_columns).ok());

//     // Create block for reading nested structure (not flattened)
//     Block block;
//     {
//         MutableColumnPtr name_column = name_type->create_column();
//         block.insert(ColumnWithTypeAndName(std::move(name_column), name_type, "name"));
//         // Add profile column (nested struct)
//         MutableColumnPtr profile_column = profile_struct_type->create_column();
//         block.insert(
//                 ColumnWithTypeAndName(std::move(profile_column), profile_struct_type, "profile"));
//     }

//     // Read data from the file
//     size_t read_rows = 0;
//     bool eof = false;
//     st = hive_reader->get_next_block(&block, &read_rows, &eof);
//     ASSERT_TRUE(st.ok()) << st;

//     // Verify test results using helper function
//     verifyTestResults(block, read_rows);
// }

// // Test reading real Hive Orc file using HiveTableReader
// TEST_F(HiveReaderPredicateTest, ReadHiveOrcFile) {
//     // Read only: name, profile.address.coordinates.lat, profile.address.coordinates.lng, profile.contact.email
//     // Setup table descriptor for test columns with new schema:
//     /**
//     Schema:
//     message table {
//     required int64 id = 1;
//     required binary name (STRING) = 2;
//     required group profile = 3 {
//         optional group address = 4 {
//         optional binary street (STRING) = 7;
//         optional binary city (STRING) = 8;
//         optional group coordinates = 9 {
//             optional double lat = 10;
//             optional double lng = 11;
//         }
//         }
//         optional group contact = 5 {
//         optional binary email (STRING) = 12;
//         optional group phone = 13 {
//             optional binary country_code (STRING) = 14;
//             optional binary number (STRING) = 15;
//         }
//         }
//         optional group hobbies (LIST) = 6 {
//         repeated group list {
//             optional group element = 16 {
//             optional binary name (STRING) = 17;
//             optional int32 level = 18;
//             }
//         }
//         }
//     }
//     }
//     */
//     // Open the Hive Orc test file
//     auto local_fs = io::global_local_filesystem();
//     io::FileReaderSPtr file_reader;
//     std::string test_file =
//             "./be/test/exec/test_data/complex_user_profiles_iceberg_orc/data/"
//             "00000-0-e4897963-0081-4127-bebe-35dc7dc1edeb-0-00001.orc";
//     auto st = local_fs->open_file(test_file, &file_reader);
//     if (!st.ok()) {
//         GTEST_SKIP() << "Test file not found: " << test_file;
//         return;
//     }

//     // Setup runtime state
//     RuntimeState runtime_state((TQueryGlobals()));

//     // Setup scan parameters
//     TFileScanRangeParams scan_params;
//     scan_params.format_type = TFileFormatType::FORMAT_ORC;

//     TFileRangeDesc scan_range;
//     scan_range.start_offset = 0;
//     scan_range.size = file_reader->size(); // Read entire file
//     scan_range.path = test_file;

//     // Create mock profile
//     RuntimeProfile profile("test_profile");

//     // Create OrcReader as the underlying file format reader
//     cctz::time_zone ctz;
//     TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);

//     auto generic_reader = OrcReader::create_unique(&profile, &runtime_state, scan_params,
//                                                    scan_range, 1024, "CST", nullptr, cache.get());
//     ASSERT_NE(generic_reader, nullptr);

//     // Create HiveOrcReader
//     auto hive_reader =
//             std::make_unique<HiveOrcReader>(std::move(generic_reader), &profile, &runtime_state,
//                                             scan_params, scan_range, nullptr, nullptr, cache.get());

//     // Create complex struct types using helper function
//     DataTypePtr coordinates_struct_type, address_struct_type, phone_struct_type;
//     DataTypePtr contact_struct_type, hobby_element_struct_type, hobbies_array_type;
//     DataTypePtr profile_struct_type, name_type;
//     createComplexStructTypes(coordinates_struct_type, address_struct_type, phone_struct_type,
//                              contact_struct_type, hobby_element_struct_type, hobbies_array_type,
//                              profile_struct_type, name_type);

//     // Create tuple descriptor using helper function
//     DescriptorTbl* desc_tbl;
//     ObjectPool obj_pool;
//     TDescriptorTable t_desc_table;
//     TTableDescriptor t_table_desc;
//     std::vector<std::string> table_column_names = {"name", "profile"};
//     std::vector<int> table_column_positions = {1, 2};
//     std::vector<TPrimitiveType::type> table_column_types = {
//             TPrimitiveType::STRING, TPrimitiveType::STRUCT // profile 用 STRUCT 类型
//     };
//     const TupleDescriptor* tuple_descriptor = createTupleDescriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc,
//                                                                    table_column_names, table_column_positions, table_column_types);

//     VExprContextSPtrs conjuncts; // Empty conjuncts for this test
//     std::vector<std::string> table_col_names = {"name", "profile"};
//     const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range = nullptr;
//     const RowDescriptor* row_descriptor = nullptr;
//     const VExprContextSPtrs* not_single_slot_filter_conjuncts = nullptr;
//     const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts = nullptr;

//     st = hive_reader->init_reader(table_col_names, colname_to_value_range, conjuncts,
//                                   tuple_descriptor, row_descriptor,
//                                   not_single_slot_filter_conjuncts, slot_id_to_filter_conjuncts);
//     ASSERT_TRUE(st.ok()) << st;

//     std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
//             partition_columns;
//     std::unordered_map<std::string, VExprContextSPtr> missing_columns;
//     ASSERT_TRUE(hive_reader->set_fill_columns(partition_columns, missing_columns).ok());

//     // Create block for reading nested structure (not flattened)
//     Block block;

//     {
//         MutableColumnPtr name_column = name_type->create_column();
//         block.insert(ColumnWithTypeAndName(std::move(name_column), name_type, "name"));
//         // Add profile column (nested struct)
//         MutableColumnPtr profile_column = profile_struct_type->create_column();
//         block.insert(
//                 ColumnWithTypeAndName(std::move(profile_column), profile_struct_type, "profile"));
//     }

//     // Read data from the file
//     size_t read_rows = 0;
//     bool eof = false;
//     st = hive_reader->get_next_block(&block, &read_rows, &eof);
//     ASSERT_TRUE(st.ok()) << st;

//     // Verify test results using helper function
//     verifyTestResults(block, read_rows);
// }

// // Test with complex WHERE conditions on nested columns
// TEST_F(HiveReaderPredicateTest, ReadHiveParquetFileWithComplexPredicates) {
//     // Test complex WHERE conditions:
//     // WHERE profile.address.coordinates.lat > 40.0
//     //   AND profile.address.coordinates.lng < -70.0
//     //   AND profile.contact.email LIKE '%@example.com'
//     //   AND EXISTS (SELECT 1 FROM profile.hobbies h WHERE h.level > 5)

//     // Open the Hive Parquet test file
//     auto local_fs = io::global_local_filesystem();
//     io::FileReaderSPtr file_reader;
//     std::string test_file =
//             "./be/test/exec/test_data/complex_user_profiles_iceberg_parquet/data/"
//             "00000-0-a0022aad-d3b6-4e73-b181-f0a09aac7034-0-00001.parquet";
//     auto st = local_fs->open_file(test_file, &file_reader);
//     if (!st.ok()) {
//         GTEST_SKIP() << "Test file not found: " << test_file;
//         return;
//     }

//     // Setup runtime state
//     RuntimeState runtime_state((TQueryGlobals()));

//     // Setup scan parameters
//     TFileScanRangeParams scan_params;
//     scan_params.format_type = TFileFormatType::FORMAT_PARQUET;

//     TFileRangeDesc scan_range;
//     scan_range.start_offset = 0;
//     scan_range.size = file_reader->size(); // Read entire file
//     scan_range.path = test_file;

//     // Create mock profile
//     RuntimeProfile profile("test_profile");

//     // Create ParquetReader as the underlying file format reader
//     cctz::time_zone ctz;
//     TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);

//     auto generic_reader = ParquetReader::create_unique(&profile, scan_params, scan_range, 1024,
//                                                        &ctz, nullptr, &runtime_state, cache.get());
//     ASSERT_NE(generic_reader, nullptr);

//     // Set file reader for the generic reader
//     auto parquet_reader = static_cast<ParquetReader*>(generic_reader.get());
//     parquet_reader->set_file_reader(file_reader);

//     // Create HiveParquetReader
//     auto hive_reader = std::make_unique<HiveParquetReader>(std::move(generic_reader), &profile,
//                                                            &runtime_state, scan_params, scan_range,
//                                                            nullptr, nullptr, cache.get());

//     // Create complex struct types using helper function
//     DataTypePtr coordinates_struct_type, address_struct_type, phone_struct_type;
//     DataTypePtr contact_struct_type, hobby_element_struct_type, hobbies_array_type;
//     DataTypePtr profile_struct_type, name_type;
//     createComplexStructTypes(coordinates_struct_type, address_struct_type, phone_struct_type,
//                              contact_struct_type, hobby_element_struct_type, hobbies_array_type,
//                              profile_struct_type, name_type);

//     // Create tuple descriptor using helper function WITH PREDICATES
//     DescriptorTbl* desc_tbl;
//     ObjectPool obj_pool;
//     TDescriptorTable t_desc_table;
//     TTableDescriptor t_table_desc;
//     std::vector<std::string> table_column_names = {"name", "profile"};
//     std::vector<int> table_column_positions = {1, 2};
//     std::vector<TPrimitiveType::type> table_column_types = {
//             TPrimitiveType::STRING, TPrimitiveType::STRUCT // profile 用 STRUCT 类型
//     };
//     // NOTE: Set with_predicates=true to mark nested columns as predicates
//     const TupleDescriptor* tuple_descriptor = createTupleDescriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc,
//                                                                    table_column_names, table_column_positions, table_column_types,
//                                                                    true); // use_name_paths=true

//     // For Parquet case, skip building complex conjuncts to avoid prepare errors
//     VExprContextSPtrs conjuncts;
//     // Prepare/open conjuncts so that VSlotRef::_column_name is initialized (expr_name not null)
//     runtime_state.set_desc_tbl(desc_tbl);
//     {
//         RowDescriptor row_desc(const_cast<TupleDescriptor*>(tuple_descriptor), false);
//         ASSERT_TRUE(VExpr::prepare(conjuncts, &runtime_state, row_desc).ok());
//         ASSERT_TRUE(VExpr::open(conjuncts, &runtime_state).ok());
//     }

//     std::vector<std::string> table_col_names = {"name", "profile"};
//     const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range = nullptr;
//     const RowDescriptor* row_descriptor = nullptr;
//     const std::unordered_map<std::string, int>* colname_to_slot_id = nullptr;
//     const VExprContextSPtrs* not_single_slot_filter_conjuncts = nullptr;
//     const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts = nullptr;

//     st = hive_reader->init_reader(table_col_names, colname_to_value_range, conjuncts,
//                                   tuple_descriptor, row_descriptor, colname_to_slot_id,
//                                   not_single_slot_filter_conjuncts, slot_id_to_filter_conjuncts);
//     ASSERT_TRUE(st.ok()) << st;

//     std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
//             partition_columns;
//     std::unordered_map<std::string, VExprContextSPtr> missing_columns;
//     ASSERT_TRUE(hive_reader->set_fill_columns(partition_columns, missing_columns).ok());

//     // Test column ID creation with predicates
//     const FieldDescriptor* field_desc = nullptr;
//     st = parquet_reader->get_file_metadata_schema(&field_desc);
//     ASSERT_TRUE(st.ok()) << st;
//     ASSERT_NE(field_desc, nullptr);

//     auto result = HiveParquetReader::_create_column_ids_and_names(field_desc, tuple_descriptor);

//     // Verify that filter_column_ids contains the predicate columns
//     // Expected filter IDs based on predicates: lat(8), lng(9), email(11), level(18)
//     std::set<uint64_t> expected_filter_ids = {8, 9, 11, 18};
//     EXPECT_EQ(result.filter_column_ids, expected_filter_ids);

//     // Verify that column_ids contains all accessed columns
//     std::set<uint64_t> expected_column_ids = {2, 3, 4, 7, 8, 9, 10, 11, 15, 16, 18};
//     EXPECT_EQ(result.column_ids, expected_column_ids);

//     // Create block for reading nested structure (not flattened)
//     Block block;
//     {
//         MutableColumnPtr name_column = name_type->create_column();
//         block.insert(ColumnWithTypeAndName(std::move(name_column), name_type, "name"));
//         // Add profile column (nested struct)
//         MutableColumnPtr profile_column = profile_struct_type->create_column();
//         block.insert(
//                 ColumnWithTypeAndName(std::move(profile_column), profile_struct_type, "profile"));
//     }

//     // Read data from the file
//     size_t read_rows = 0;
//     bool eof = false;
//     st = hive_reader->get_next_block(&block, &read_rows, &eof);
//     ASSERT_TRUE(st.ok()) << st;

//     // Verify test results using helper function
//     verifyTestResults(block, read_rows);

//     // Print predicate column information
//     std::cout << "\n=== Predicate Column Test Results ===" << std::endl;
//     std::cout << "Filter column IDs: ";
//     for (const auto& id : result.filter_column_ids) {
//         std::cout << id << " ";
//     }
//     std::cout << "\nTotal column IDs: ";
//     for (const auto& id : result.column_ids) {
//         std::cout << id << " ";
//     }
//     std::cout << std::endl;
// }

// // Test with complex WHERE conditions on nested columns for ORC
// TEST_F(HiveReaderPredicateTest, ReadHiveOrcFileWithComplexPredicates) {
//     // Test complex WHERE conditions on ORC file:
//     // WHERE profile.address.coordinates.lat > 40.0
//     //   AND profile.address.coordinates.lng < -70.0

//     // Open the Hive Orc test file
//     auto local_fs = io::global_local_filesystem();
//     io::FileReaderSPtr file_reader;
//     std::string test_file =
//             "./be/test/exec/test_data/complex_user_profiles_iceberg_orc/data/"
//             "00000-0-e4897963-0081-4127-bebe-35dc7dc1edeb-0-00001.orc";
//     auto st = local_fs->open_file(test_file, &file_reader);
//     if (!st.ok()) {
//         GTEST_SKIP() << "Test file not found: " << test_file;
//         return;
//     }

//     // Setup runtime state
//     RuntimeState runtime_state((TQueryGlobals()));

//     // Setup scan parameters
//     TFileScanRangeParams scan_params;
//     scan_params.format_type = TFileFormatType::FORMAT_ORC;

//     TFileRangeDesc scan_range;
//     scan_range.start_offset = 0;
//     scan_range.size = file_reader->size(); // Read entire file
//     scan_range.path = test_file;

//     // Create mock profile
//     RuntimeProfile profile("test_profile");

//     // Create OrcReader as the underlying file format reader
//     cctz::time_zone ctz;
//     TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);

//     auto generic_reader = OrcReader::create_unique(&profile, &runtime_state, scan_params,
//                                                    scan_range, 1024, "CST", nullptr, cache.get());
//     ASSERT_NE(generic_reader, nullptr);

//     auto orc_reader = static_cast<OrcReader*>(generic_reader.get());
//     // Get FieldDescriptor from Orc file
//     const orc::Type* orc_type_ptr = nullptr;
//     st = orc_reader->get_file_type(&orc_type_ptr);
//     ASSERT_TRUE(st.ok()) << st;
//     ASSERT_NE(orc_type_ptr, nullptr);

//     // Create HiveOrcReader
//     auto hive_reader =
//             std::make_unique<HiveOrcReader>(std::move(generic_reader), &profile, &runtime_state,
//                                             scan_params, scan_range, nullptr, nullptr, cache.get());

//     // Create complex struct types using helper function
//     DataTypePtr coordinates_struct_type, address_struct_type, phone_struct_type;
//     DataTypePtr contact_struct_type, hobby_element_struct_type, hobbies_array_type;
//     DataTypePtr profile_struct_type, name_type;
//     createComplexStructTypes(coordinates_struct_type, address_struct_type, phone_struct_type,
//                              contact_struct_type, hobby_element_struct_type, hobbies_array_type,
//                              profile_struct_type, name_type);

//     // Create tuple descriptor using helper function WITH PREDICATES
//     DescriptorTbl* desc_tbl;
//     ObjectPool obj_pool;
//     TDescriptorTable t_desc_table;
//     TTableDescriptor t_table_desc;
//     std::vector<std::string> table_column_names = {"name", "profile"};
//     std::vector<int> table_column_positions = {1, 2};
//     std::vector<TPrimitiveType::type> table_column_types = {
//             TPrimitiveType::STRING, TPrimitiveType::STRUCT // profile 用 STRUCT 类型
//     };
//     // NOTE: Set with_predicates=true to mark nested columns as predicates
//     const TupleDescriptor* tuple_descriptor = createTupleDescriptor(&desc_tbl, obj_pool, t_desc_table, t_table_desc,
//                                                                    table_column_names, table_column_positions, table_column_types,
//                                                                    true); // use_name_paths=true

//     // Create complex nested column conjuncts using the helper function
//     VExprContextSPtrs conjuncts = createNestedColumnConjuncts(obj_pool, tuple_descriptor, &runtime_state);

//     // Prepare/open conjuncts so that VSlotRef::_column_name is initialized (expr_name not null)
//     runtime_state.set_desc_tbl(desc_tbl);
//     if (!conjuncts.empty()) {
//         RowDescriptor row_desc(const_cast<TupleDescriptor*>(tuple_descriptor), false);
//         auto prepare_status = VExpr::prepare(conjuncts, &runtime_state, row_desc);
//         if (!prepare_status.ok()) {
//             std::cout << "Warning: Failed to prepare conjuncts: " << prepare_status.to_string() << std::endl;
//             // Continue without conjuncts to test column ID extraction
//             conjuncts.clear();
//         } else {
//             auto open_status = VExpr::open(conjuncts, &runtime_state);
//             if (!open_status.ok()) {
//                 std::cout << "Warning: Failed to open conjuncts: " << open_status.to_string() << std::endl;
//                 conjuncts.clear();
//             }
//         }
//     }

//     std::vector<std::string> table_col_names = {"name", "profile"};
//     const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range = nullptr;
//     const RowDescriptor* row_descriptor = nullptr;
//     const VExprContextSPtrs* not_single_slot_filter_conjuncts = nullptr;
//     const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts = nullptr;

//     st = hive_reader->init_reader(table_col_names, colname_to_value_range, conjuncts,
//                                   tuple_descriptor, row_descriptor,
//                                   not_single_slot_filter_conjuncts, slot_id_to_filter_conjuncts);
//     ASSERT_TRUE(st.ok()) << st;

//     std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
//             partition_columns;
//     std::unordered_map<std::string, VExprContextSPtr> missing_columns;
//     ASSERT_TRUE(hive_reader->set_fill_columns(partition_columns, missing_columns).ok());

//     // Test column ID creation with predicates
//     auto result = HiveOrcReader::_create_column_ids_and_names(orc_type_ptr, tuple_descriptor);

//     // Verify that filter_column_ids contains the predicate columns
//     // Expected filter IDs based on predicates: lat(8), lng(9), email(11), level(18)
//     std::set<uint64_t> expected_filter_ids = {8, 9};
//     EXPECT_EQ(result.filter_column_ids, expected_filter_ids);

//     // Verify that column_ids contains all accessed columns
//     std::set<uint64_t> expected_column_ids = {2, 8, 9, 11, 18};
//     EXPECT_EQ(result.column_ids, expected_column_ids);

//     // Create block for reading nested structure (not flattened)
//     Block block;
//     {
//         MutableColumnPtr name_column = name_type->create_column();
//         block.insert(ColumnWithTypeAndName(std::move(name_column), name_type, "name"));
//         // Add profile column (nested struct)
//         MutableColumnPtr profile_column = profile_struct_type->create_column();
//         block.insert(
//                 ColumnWithTypeAndName(std::move(profile_column), profile_struct_type, "profile"));
//     }

//     // Read data from the file
//     size_t read_rows = 0;
//     bool eof = false;
//     st = hive_reader->get_next_block(&block, &read_rows, &eof);
//     ASSERT_TRUE(st.ok()) << st;

//     // Verify test results using helper function
//     verifyTestResults(block, read_rows);

//     // Print predicate column information
//     std::cout << "\n=== ORC Predicate Column Test Results ===" << std::endl;
//     std::cout << "Filter column IDs: ";
//     for (const auto& id : result.filter_column_ids) {
//         std::cout << id << " ";
//     }
//     std::cout << "\nTotal column IDs: ";
//     for (const auto& id : result.column_ids) {
//         std::cout << id << " ";
//     }
//     std::cout << std::endl;
// }

// } // namespace doris::vectorized::table
