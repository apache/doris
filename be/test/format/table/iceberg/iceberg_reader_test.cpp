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

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <cctz/time_zone.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>
#include <parquet/arrow/writer.h>

#include <array>
#include <filesystem>
#include <fstream>
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
#include "core/data_type/data_type_varbinary.h"
#include "format/column_descriptor.h"
#include "format/format_common.h"
#include "format/orc/orc_memory_stream_test.h"
#include "format/orc/vorc_reader.h"
#include "format/parquet/vparquet_column_chunk_reader.h"
#include "format/parquet/vparquet_reader.h"
#include "io/fs/file_meta_cache.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "io/io_common.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/olap_scan_common.h"
#include "util/timezone_utils.h"

namespace doris {

void write_iceberg_int_orc_file(const std::string& file_path, const std::string& field_name,
                                int32_t field_id, const std::vector<int64_t>& values) {
    auto type = std::unique_ptr<::orc::Type>(
            ::orc::Type::buildTypeFromString("struct<" + field_name + ":int>"));
    type->getSubtype(0)->setAttribute("iceberg.id", std::to_string(field_id));

    MemoryOutputStream memory_stream(1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    auto writer = ::orc::createWriter(*type, &memory_stream, options);
    auto batch = writer->createRowBatch(values.size());
    auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
    auto& value_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[0]);
    for (size_t row = 0; row < values.size(); ++row) {
        value_batch.data[row] = values[row];
    }
    struct_batch.numElements = values.size();
    value_batch.numElements = values.size();
    writer->add(*batch);
    writer->close();

    std::ofstream output(file_path, std::ios::binary);
    output.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_iceberg_three_int_orc_file(
        const std::string& file_path, const std::string& first_name,
        std::optional<int32_t> first_id, const std::vector<int64_t>& first_values,
        const std::string& second_name, std::optional<int32_t> second_id,
        const std::vector<int64_t>& second_values, const std::string& third_name,
        std::optional<int32_t> third_id, const std::vector<int64_t>& third_values) {
    DORIS_CHECK(first_values.size() == second_values.size());
    DORIS_CHECK(first_values.size() == third_values.size());
    auto type = std::unique_ptr<::orc::Type>(::orc::Type::buildTypeFromString(
            "struct<" + first_name + ":int," + second_name + ":int," + third_name + ":int>"));
    const std::array field_ids = {first_id, second_id, third_id};
    for (size_t idx = 0; idx < field_ids.size(); ++idx) {
        if (field_ids[idx].has_value()) {
            type->getSubtype(idx)->setAttribute("iceberg.id", std::to_string(*field_ids[idx]));
        }
    }

    MemoryOutputStream memory_stream(1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    auto writer = ::orc::createWriter(*type, &memory_stream, options);
    auto batch = writer->createRowBatch(first_values.size());
    auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
    const std::array value_sets = {&first_values, &second_values, &third_values};
    for (size_t field_idx = 0; field_idx < value_sets.size(); ++field_idx) {
        auto& value_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[field_idx]);
        for (size_t row = 0; row < first_values.size(); ++row) {
            value_batch.data[row] = (*value_sets[field_idx])[row];
        }
        value_batch.numElements = first_values.size();
    }
    struct_batch.numElements = first_values.size();
    writer->add(*batch);
    writer->close();

    std::ofstream output(file_path, std::ios::binary);
    output.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

std::shared_ptr<arrow::Array> build_iceberg_int32_array(const std::vector<int32_t>& values) {
    arrow::Int32Builder builder;
    for (const auto value : values) {
        DORIS_CHECK(builder.Append(value).ok());
    }
    std::shared_ptr<arrow::Array> result;
    DORIS_CHECK(builder.Finish(&result).ok());
    return result;
}

void write_iceberg_three_int_parquet_file(
        const std::string& file_path, const std::string& first_name,
        std::optional<int32_t> first_id, const std::vector<int32_t>& first_values,
        const std::string& second_name, std::optional<int32_t> second_id,
        const std::vector<int32_t>& second_values, const std::string& third_name,
        std::optional<int32_t> third_id, const std::vector<int32_t>& third_values) {
    DORIS_CHECK(first_values.size() == second_values.size());
    DORIS_CHECK(first_values.size() == third_values.size());
    std::vector<std::shared_ptr<arrow::Field>> fields = {
            arrow::field(first_name, arrow::int32(), false),
            arrow::field(second_name, arrow::int32(), false),
            arrow::field(third_name, arrow::int32(), false)};
    const std::array field_ids = {first_id, second_id, third_id};
    for (size_t idx = 0; idx < fields.size(); ++idx) {
        if (field_ids[idx].has_value()) {
            fields[idx] = fields[idx]->WithMetadata(arrow::key_value_metadata(
                    {"PARQUET:field_id"}, {std::to_string(*field_ids[idx])}));
        }
    }
    auto schema = arrow::schema(fields);
    auto table = arrow::Table::Make(schema, {build_iceberg_int32_array(first_values),
                                             build_iceberg_int32_array(second_values),
                                             build_iceberg_int32_array(third_values)});

    auto output = arrow::io::FileOutputStream::Open(file_path);
    DORIS_CHECK(output.ok());
    ::parquet::WriterProperties::Builder properties;
    properties.version(::parquet::ParquetVersion::PARQUET_2_6);
    properties.data_page_version(::parquet::ParquetDataPageVersion::V2);
    properties.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *output,
                                                      static_cast<int64_t>(first_values.size()),
                                                      properties.build()));
}

void write_iceberg_int_equality_delete_parquet_file(const std::string& file_path,
                                                    const std::string& field_name, int32_t field_id,
                                                    int32_t value) {
    auto field = arrow::field(field_name, arrow::int32(), false)
                         ->WithMetadata(arrow::key_value_metadata({"PARQUET:field_id"},
                                                                  {std::to_string(field_id)}));
    auto table = arrow::Table::Make(arrow::schema({field}), {build_iceberg_int32_array({value})});
    auto output = arrow::io::FileOutputStream::Open(file_path);
    DORIS_CHECK(output.ok());
    ::parquet::WriterProperties::Builder properties;
    properties.version(::parquet::ParquetVersion::PARQUET_2_6);
    properties.data_page_version(::parquet::ParquetDataPageVersion::V2);
    properties.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *output,
                                                      1, properties.build()));
}

Status create_single_int_tuple_descriptor(ObjectPool* object_pool, const std::string& column_name,
                                          int32_t field_id, DescriptorTbl** descriptor_table,
                                          const TupleDescriptor** tuple_descriptor) {
    TDescriptorTable thrift_table;
    TTableDescriptor table_descriptor;
    table_descriptor.__set_id(0);
    table_descriptor.__set_tableType(TTableType::OLAP_TABLE);
    table_descriptor.__set_numCols(1);
    table_descriptor.__set_numClusteringCols(0);
    thrift_table.__set_tableDescriptors({table_descriptor});

    TTypeNode type_node;
    type_node.__set_type(TTypeNodeType::SCALAR);
    TScalarType scalar_type;
    scalar_type.__set_type(TPrimitiveType::INT);
    type_node.__set_scalar_type(scalar_type);
    TTypeDesc type;
    type.__set_types({type_node});

    TSlotDescriptor slot_descriptor;
    slot_descriptor.__set_id(0);
    slot_descriptor.__set_parent(0);
    slot_descriptor.__set_col_unique_id(field_id);
    slot_descriptor.__set_slotType(type);
    slot_descriptor.__set_columnPos(0);
    slot_descriptor.__set_byteOffset(0);
    slot_descriptor.__set_nullIndicatorByte(0);
    slot_descriptor.__set_nullIndicatorBit(-1);
    slot_descriptor.__set_colName(column_name);
    slot_descriptor.__set_slotIdx(0);
    slot_descriptor.__set_isMaterialized(true);
    thrift_table.__set_slotDescriptors({slot_descriptor});

    TTupleDescriptor thrift_tuple;
    thrift_tuple.__set_id(0);
    thrift_tuple.__set_byteSize(16);
    thrift_tuple.__set_numNullBytes(0);
    thrift_tuple.__set_tableId(0);
    thrift_table.__set_tupleDescriptors({thrift_tuple});

    RETURN_IF_ERROR(DescriptorTbl::create(object_pool, thrift_table, descriptor_table));
    *tuple_descriptor = (*descriptor_table)->get_tuple_descriptor(0);
    return Status::OK();
}

schema::external::TFieldPtr make_external_int_field(const std::string& name, int32_t field_id,
                                                    std::optional<std::string> initial_default,
                                                    std::vector<std::string> aliases = {}) {
    auto field = std::make_shared<schema::external::TField>();
    field->__set_name(name);
    field->__set_id(field_id);
    TColumnType type;
    type.__set_type(TPrimitiveType::INT);
    field->__set_type(type);
    if (initial_default.has_value()) {
        field->__set_initial_default_value(*initial_default);
    }
    if (!aliases.empty()) {
        field->__set_name_mapping(aliases);
    }
    schema::external::TFieldPtr field_ptr;
    field_ptr.field_ptr = std::move(field);
    field_ptr.__isset.field_ptr = true;
    return field_ptr;
}

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
    RuntimeState runtime_state = RuntimeState(TQueryOptions(), TQueryGlobals());

    // Setup scan parameters
    TFileScanRangeParams scan_params;
    scan_params.format_type = TFileFormatType::FORMAT_PARQUET;

    TFileRangeDesc scan_range;
    scan_range.start_offset = 0;
    scan_range.size = file_reader->size(); // Read entire file
    scan_range.path = test_file;

    // Create mock profile
    RuntimeProfile profile("test_profile");

    // Create IcebergParquetReader (IS-A ParquetReader via CRTP mixin)
    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);

    auto iceberg_reader = std::make_unique<IcebergParquetReader>(
            nullptr /* kv_cache */, &profile, scan_params, scan_range, 1024, &ctz,
            nullptr /* io_ctx */, &runtime_state, cache.get());
    ASSERT_NE(iceberg_reader, nullptr);

    // Set file reader for the iceberg reader (it IS the ParquetReader)
    iceberg_reader->set_file_reader(file_reader);

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

    std::vector<std::string> table_col_names = {"name", "profile"};
    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {
            {"name", 0},
            {"profile", 1},
    };

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

    // set_fill_columns logic is now inlined in _do_init_reader,
    // so no separate call is needed.

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

TEST_F(IcebergReaderTest, v1_deletion_vector_read_error_releases_cache_entry) {
    RuntimeState runtime_state = RuntimeState(TQueryOptions(), TQueryGlobals());
    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    scan_params.__set_format_type(TFileFormatType::FORMAT_PARQUET);

    TFileRangeDesc scan_range;
    scan_range.__set_fs_name("");
    scan_range.__set_path("data.parquet");
    scan_range.__set_start_offset(0);
    scan_range.__set_size(0);

    RuntimeProfile profile("test_profile");
    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    io::IOContext io_ctx;
    ShardedKVCache kv_cache(8);

    IcebergParquetReader iceberg_reader(&kv_cache, &profile, scan_params, scan_range, 1024, &ctz,
                                        &io_ctx, &runtime_state, cache.get());

    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_content(IcebergReaderMixin<ParquetReader>::DELETION_VECTOR);
    delete_file.__set_path("./be/test/exec/test_data/missing_iceberg_v1_delete_vector.bin");
    delete_file.__set_content_offset(0);
    delete_file.__set_content_size_in_bytes(16);

    const auto status =
            iceberg_reader.TEST_read_deletion_vector("file:///tmp/data.parquet", delete_file);

    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find(delete_file.path), std::string::npos);
}

TEST_F(IcebergReaderTest, v1_position_delete_read_error_releases_cache_entry) {
    RuntimeState runtime_state = RuntimeState(TQueryOptions(), TQueryGlobals());
    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    scan_params.__set_format_type(TFileFormatType::FORMAT_PARQUET);

    TFileRangeDesc scan_range;
    scan_range.__set_fs_name("");
    scan_range.__set_path("data.parquet");
    scan_range.__set_start_offset(0);
    scan_range.__set_size(0);

    RuntimeProfile profile("test_profile");
    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    io::IOContext io_ctx;
    ShardedKVCache kv_cache(8);

    IcebergParquetReader iceberg_reader(&kv_cache, &profile, scan_params, scan_range, 1024, &ctz,
                                        &io_ctx, &runtime_state, cache.get());

    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_content(IcebergReaderMixin<ParquetReader>::POSITION_DELETE);
    delete_file.__set_path("./be/test/exec/test_data/missing_iceberg_v1_position_delete.parquet");

    const auto status =
            iceberg_reader.TEST_position_delete_base("file:///tmp/data.parquet", {delete_file});

    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find(delete_file.path), std::string::npos);
}

TEST_F(IcebergReaderTest, v1_materializes_missing_equality_delete_initial_defaults) {
    auto make_field = [](const std::string& name, int32_t id, TPrimitiveType::type primitive_type,
                         const std::string& default_value, int32_t len, int32_t scale,
                         bool default_is_base64) {
        auto field = std::make_shared<schema::external::TField>();
        field->__set_name(name);
        field->__set_id(id);
        field->__set_initial_default_value(default_value);
        if (default_is_base64) {
            field->__set_initial_default_value_is_base64(true);
        }
        TColumnType type;
        type.__set_type(primitive_type);
        if (len >= 0) {
            type.__set_len(len);
        }
        if (scale >= 0) {
            type.__set_scale(scale);
        }
        field->__set_type(type);
        schema::external::TFieldPtr field_ptr;
        field_ptr.field_ptr = std::move(field);
        field_ptr.__isset.field_ptr = true;
        return field_ptr;
    };

    schema::external::TStructField root_field;
    root_field.__set_fields({make_field("added_timestamp", 1, TPrimitiveType::DATETIMEV2,
                                        "2024-01-01 00:00:00.123456", -1, 6, false),
                             make_field("added_binary", 2, TPrimitiveType::VARBINARY,
                                        "Ej5FZ+ibEtOkVkJmFBdAAA==", 16, -1, true),
                             make_field("added_string_binary", 3, TPrimitiveType::STRING,
                                        "AAEC/w==", -1, -1, true)});
    schema::external::TSchema current_schema;
    current_schema.__set_schema_id(-1);
    current_schema.__set_root_field(root_field);

    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    scan_params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    scan_params.__set_current_schema_id(-1);
    scan_params.__set_history_schema_info({std::move(current_schema)});
    TFileRangeDesc scan_range;
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state = RuntimeState(TQueryOptions(), TQueryGlobals());
    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    io::IOContext io_ctx;
    ShardedKVCache kv_cache(8);
    IcebergParquetReader reader(&kv_cache, &profile, scan_params, scan_range, 1024, &ctz, &io_ctx,
                                &runtime_state, cache.get());

    const auto timestamp_type = make_nullable(std::make_shared<DataTypeDateTimeV2>(6));
    const auto varbinary_type = make_nullable(std::make_shared<DataTypeVarbinary>(16));
    const auto string_type = make_nullable(std::make_shared<DataTypeString>());
    Block block;
    block.insert({timestamp_type->create_column(), timestamp_type, "added_timestamp"});
    block.insert({varbinary_type->create_column(), varbinary_type, "added_binary"});
    block.insert({string_type->create_column(), string_type, "added_string_binary"});
    std::unordered_map<std::string, uint32_t> positions = {
            {"added_timestamp", 0}, {"added_binary", 1}, {"added_string_binary", 2}};
    reader.TEST_set_column_name_to_block_index(&positions);
    ASSERT_TRUE(reader.TEST_register_missing_equality_delete_column(1, "added_timestamp",
                                                                    timestamp_type)
                        .ok());
    ASSERT_TRUE(
            reader.TEST_register_missing_equality_delete_column(2, "added_binary", varbinary_type)
                    .ok());
    ASSERT_TRUE(reader.TEST_register_missing_equality_delete_column(3, "added_string_binary",
                                                                    string_type)
                        .ok());
    ASSERT_TRUE(reader.TEST_materialize_missing_equality_delete_columns(&block, 3).ok());

    ASSERT_EQ(block.rows(), 3);
    EXPECT_EQ(timestamp_type->to_string(*block.get_by_position(0).column, 0),
              "2024-01-01 00:00:00.123456");
    EXPECT_EQ(varbinary_type->to_string(*block.get_by_position(1).column, 0),
              std::string("\x12\x3e\x45\x67\xe8\x9b\x12\xd3\xa4\x56\x42\x66\x14\x17\x40\x00", 16));
    EXPECT_EQ(string_type->to_string(*block.get_by_position(2).column, 0),
              std::string("\x00\x01\x02\xff", 4));
    EXPECT_FALSE(is_column_const(*block.get_by_position(0).column));
    EXPECT_FALSE(is_column_const(*block.get_by_position(1).column));
    EXPECT_FALSE(is_column_const(*block.get_by_position(2).column));
}

TEST_F(IcebergReaderTest, v1_top_level_missing_binary_prefers_iceberg_initial_default) {
    auto field = std::make_shared<schema::external::TField>();
    field->__set_name("added_uuid");
    field->__set_id(1);
    field->__set_initial_default_value("Ej5FZ+ibEtOkVkJmFBdAAA==");
    field->__set_initial_default_value_is_base64(true);
    TColumnType thrift_type;
    thrift_type.__set_type(TPrimitiveType::VARBINARY);
    thrift_type.__set_len(16);
    field->__set_type(thrift_type);
    schema::external::TFieldPtr field_ptr;
    field_ptr.field_ptr = std::move(field);
    field_ptr.__isset.field_ptr = true;
    schema::external::TStructField root_field;
    root_field.__set_fields({std::move(field_ptr)});
    schema::external::TSchema current_schema;
    current_schema.__set_schema_id(-1);
    current_schema.__set_root_field(std::move(root_field));

    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    scan_params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    scan_params.__set_current_schema_id(-1);
    scan_params.__set_history_schema_info({std::move(current_schema)});
    TFileRangeDesc scan_range;
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state = RuntimeState(TQueryOptions(), TQueryGlobals());
    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    io::IOContext io_ctx;
    ShardedKVCache kv_cache(8);
    IcebergParquetReader reader(&kv_cache, &profile, scan_params, scan_range, 1024, &ctz, &io_ctx,
                                &runtime_state, cache.get());

    const auto varbinary_type = make_nullable(std::make_shared<DataTypeVarbinary>(16));
    Block block;
    block.insert({varbinary_type->create_column(), varbinary_type, "added_uuid"});
    std::unordered_map<std::string, uint32_t> positions = {{"added_uuid", 0}};
    reader.TEST_set_column_name_to_block_index(&positions);

    ASSERT_TRUE(reader.on_fill_missing_columns(&block, 2, {"added_uuid"}).ok());

    ASSERT_EQ(block.rows(), 2);
    EXPECT_EQ(varbinary_type->to_string(*block.get_by_position(0).column, 0),
              std::string("\x12\x3e\x45\x67\xe8\x9b\x12\xd3\xa4\x56\x42\x66\x14\x17\x40\x00", 16));
}

TEST_F(IcebergReaderTest, v1_multi_equality_delete_hashes_materialized_missing_default) {
    schema::external::TStructField root_field;
    root_field.__set_fields({make_external_int_field("id", 0, std::nullopt),
                             make_external_int_field("added_column", 1, "7")});
    schema::external::TSchema current_schema;
    current_schema.__set_schema_id(-1);
    current_schema.__set_root_field(root_field);

    TFileScanRangeParams scan_params;
    scan_params.__set_current_schema_id(-1);
    scan_params.__set_history_schema_info({current_schema});
    TFileRangeDesc scan_range;
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state {TQueryOptions(), TQueryGlobals()};
    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    io::IOContext io_ctx;
    ShardedKVCache kv_cache(8);
    IcebergParquetReader reader(&kv_cache, &profile, scan_params, scan_range, 1024, &ctz, &io_ctx,
                                &runtime_state, cache.get());

    const auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
    auto id_column = int_type->create_column();
    id_column->insert(Field::create_field<TYPE_INT>(1));
    id_column->insert(Field::create_field<TYPE_INT>(2));
    id_column->insert(Field::create_field<TYPE_INT>(3));
    Block data_block;
    data_block.insert({std::move(id_column), int_type, "id"});
    data_block.insert({int_type->create_column(), int_type, "added_column"});
    std::unordered_map<std::string, uint32_t> positions = {{"id", 0}, {"added_column", 1}};
    reader.TEST_set_column_name_to_block_index(&positions);
    ASSERT_TRUE(
            reader.TEST_register_missing_equality_delete_column(1, "added_column", int_type).ok());
    ASSERT_TRUE(reader.TEST_materialize_missing_equality_delete_columns(&data_block, 3).ok());
    ASSERT_EQ(data_block.rows(), 3);
    ASSERT_FALSE(is_column_const(*data_block.get_by_position(1).column));

    auto delete_id_column = int_type->create_column();
    delete_id_column->insert(Field::create_field<TYPE_INT>(2));
    auto delete_default_column = int_type->create_column();
    delete_default_column->insert(Field::create_field<TYPE_INT>(7));
    Block delete_block;
    delete_block.insert({std::move(delete_id_column), int_type, "id"});
    delete_block.insert({std::move(delete_default_column), int_type, "added_column"});
    MultiEqualityDelete equality_delete(&delete_block, {0, 1});
    ASSERT_TRUE(equality_delete.init(&profile).ok());

    std::unordered_map<int, std::string> id_to_name = {{0, "id"}, {1, "added_column"}};
    IColumn::Filter filter(3, 1);
    ASSERT_TRUE(
            equality_delete.filter_data_block(&data_block, &positions, id_to_name, filter).ok());
    EXPECT_EQ(filter, IColumn::Filter({1, 0, 1}));
}

TEST_F(IcebergReaderTest, v1_missing_equality_key_returns_error_for_pruned_descriptor) {
    schema::external::TStructField root_field;
    root_field.__set_fields({make_external_int_field("id", 0, std::nullopt)});
    schema::external::TSchema current_schema;
    current_schema.__set_schema_id(-1);
    current_schema.__set_root_field(root_field);

    TFileScanRangeParams scan_params;
    scan_params.__set_current_schema_id(-1);
    scan_params.__set_history_schema_info({current_schema});
    TFileRangeDesc scan_range;
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state {TQueryOptions(), TQueryGlobals()};
    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    io::IOContext io_ctx;
    ShardedKVCache kv_cache(8);
    IcebergParquetReader reader(&kv_cache, &profile, scan_params, scan_range, 1024, &ctz, &io_ctx,
                                &runtime_state, cache.get());

    const auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
    const auto status =
            reader.TEST_register_missing_equality_delete_column(1, "hidden_key", int_type);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("field id 1"), std::string::npos);
}

TEST_F(IcebergReaderTest, v1_orc_equality_delete_matches_missing_initial_default) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_v1_orc_missing_equality_delete_default_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto data_file = (test_dir / "data.orc").string();
    const auto delete_file = (test_dir / "equality-delete.orc").string();
    write_iceberg_int_orc_file(data_file, "id", 0, {1, 2, 3});
    write_iceberg_int_orc_file(delete_file, "added_column", 1, {7});

    schema::external::TStructField root_field;
    root_field.__set_fields({make_external_int_field("id", 0, std::nullopt),
                             make_external_int_field("added_column", 1, "7")});
    schema::external::TSchema current_schema;
    current_schema.__set_schema_id(-1);
    current_schema.__set_root_field(root_field);

    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    scan_params.__set_format_type(TFileFormatType::FORMAT_ORC);
    scan_params.__set_current_schema_id(-1);
    scan_params.__set_history_schema_info({current_schema});

    TIcebergDeleteFileDesc delete_descriptor;
    delete_descriptor.__set_content(IcebergReaderMixin<OrcReader>::EQUALITY_DELETE);
    delete_descriptor.__set_path(delete_file);
    delete_descriptor.__set_field_ids({1});
    delete_descriptor.__set_file_format(TFileFormatType::FORMAT_ORC);
    TIcebergFileDesc iceberg_descriptor;
    iceberg_descriptor.__set_format_version(2);
    iceberg_descriptor.__set_original_file_path(data_file);
    iceberg_descriptor.__set_delete_files({delete_descriptor});
    TTableFormatFileDesc table_format_descriptor;
    table_format_descriptor.__set_iceberg_params(iceberg_descriptor);

    TFileRangeDesc scan_range;
    scan_range.__set_fs_name("");
    scan_range.__set_path(data_file);
    scan_range.__set_start_offset(0);
    scan_range.__set_size(static_cast<int64_t>(std::filesystem::file_size(data_file)));
    scan_range.__set_file_size(static_cast<int64_t>(std::filesystem::file_size(data_file)));
    scan_range.__set_table_format_params(table_format_descriptor);

    ObjectPool object_pool;
    DescriptorTbl* descriptor_table = nullptr;
    const TupleDescriptor* tuple_descriptor = nullptr;
    ASSERT_TRUE(create_single_int_tuple_descriptor(&object_pool, "id", 0, &descriptor_table,
                                                   &tuple_descriptor)
                        .ok());
    ASSERT_NE(tuple_descriptor, nullptr);

    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state {TQueryOptions(), TQueryGlobals()};
    runtime_state.set_timezone("UTC");
    io::IOContext io_ctx;
    ShardedKVCache kv_cache(8);
    IcebergOrcReader reader(&kv_cache, &profile, &runtime_state, scan_params, scan_range, 1024,
                            "UTC", &io_ctx, cache.get());

    std::vector<ColumnDescriptor> column_descriptors(1);
    column_descriptors[0].name = "id";
    std::unordered_map<std::string, uint32_t> block_positions = {{"id", 0}};
    OrcInitContext context;
    context.column_descs = &column_descriptors;
    context.col_name_to_block_idx = &block_positions;
    context.tuple_descriptor = tuple_descriptor;
    context.params = &scan_params;
    context.range = &scan_range;
    const auto init_status = reader.init_reader(&context);
    ASSERT_TRUE(init_status.ok()) << init_status;

    const auto id_type = make_nullable(std::make_shared<DataTypeInt32>());
    Block block;
    block.insert({id_type->create_column(), id_type, "id"});
    size_t read_rows = 0;
    bool eof = false;
    const auto status = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(read_rows, 0);
    EXPECT_EQ(block.rows(), 0);

    std::filesystem::remove_all(test_dir);
}

TEST_F(IcebergReaderTest, v1_parquet_mixed_ids_prefer_existing_equality_field_id) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_v1_parquet_partial_id_equality_delete_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto data_file = (test_dir / "data.parquet").string();
    const auto delete_file = (test_dir / "equality-delete.parquet").string();
    // Iceberg's hasIds rule is existential: once any physical field has an ID, equality keys bind
    // to that authoritative ID instead of an ID-less historical alias.
    write_iceberg_three_int_parquet_file(data_file, "id", 0, {1, 2, 3}, "legacy_added",
                                         std::nullopt, {5, 7, 9}, "stale_added", 1, {70, 70, 70});
    write_iceberg_int_equality_delete_parquet_file(delete_file, "added_column", 1, 7);

    schema::external::TStructField root_field;
    root_field.__set_fields(
            {make_external_int_field("id", 0, std::nullopt),
             make_external_int_field("added_column", 1, std::nullopt, {"legacy_added"})});
    schema::external::TSchema current_schema;
    current_schema.__set_schema_id(-1);
    current_schema.__set_root_field(root_field);

    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    scan_params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    scan_params.__set_current_schema_id(-1);
    scan_params.__set_history_schema_info({current_schema});

    TIcebergDeleteFileDesc delete_descriptor;
    delete_descriptor.__set_content(IcebergReaderMixin<ParquetReader>::EQUALITY_DELETE);
    delete_descriptor.__set_path(delete_file);
    delete_descriptor.__set_field_ids({1});
    delete_descriptor.__set_file_format(TFileFormatType::FORMAT_PARQUET);
    TIcebergFileDesc iceberg_descriptor;
    iceberg_descriptor.__set_format_version(2);
    iceberg_descriptor.__set_original_file_path(data_file);
    iceberg_descriptor.__set_delete_files({delete_descriptor});
    TTableFormatFileDesc table_format_descriptor;
    table_format_descriptor.__set_iceberg_params(iceberg_descriptor);

    TFileRangeDesc scan_range;
    scan_range.__set_fs_name("");
    scan_range.__set_path(data_file);
    scan_range.__set_start_offset(0);
    scan_range.__set_size(static_cast<int64_t>(std::filesystem::file_size(data_file)));
    scan_range.__set_file_size(static_cast<int64_t>(std::filesystem::file_size(data_file)));
    scan_range.__set_table_format_params(table_format_descriptor);

    ObjectPool object_pool;
    DescriptorTbl* descriptor_table = nullptr;
    const TupleDescriptor* tuple_descriptor = nullptr;
    ASSERT_TRUE(create_single_int_tuple_descriptor(&object_pool, "id", 0, &descriptor_table,
                                                   &tuple_descriptor)
                        .ok());
    ASSERT_NE(tuple_descriptor, nullptr);

    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state {TQueryOptions(), TQueryGlobals()};
    runtime_state.set_timezone("UTC");
    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone("UTC", ctz);
    io::IOContext io_ctx;
    ShardedKVCache kv_cache(8);
    IcebergParquetReader reader(&kv_cache, &profile, scan_params, scan_range, 1024, &ctz, &io_ctx,
                                &runtime_state, cache.get());
    io::FileReaderSPtr file_reader;
    ASSERT_TRUE(io::global_local_filesystem()->open_file(data_file, &file_reader).ok());
    reader.set_file_reader(file_reader);

    std::vector<ColumnDescriptor> column_descriptors(1);
    column_descriptors[0].name = "id";
    std::unordered_map<std::string, uint32_t> block_positions = {{"id", 0}};
    ParquetInitContext context;
    context.column_descs = &column_descriptors;
    context.col_name_to_block_idx = &block_positions;
    context.tuple_descriptor = tuple_descriptor;
    context.params = &scan_params;
    context.range = &scan_range;
    const auto init_status = reader.init_reader(&context);
    ASSERT_TRUE(init_status.ok()) << init_status;

    const auto id_type = make_nullable(std::make_shared<DataTypeInt32>());
    Block block;
    block.insert({id_type->create_column(), id_type, "id"});
    size_t read_rows = 0;
    bool eof = false;
    const auto status = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(read_rows, 3);
    ASSERT_EQ(block.rows(), 3);
    EXPECT_EQ(id_type->to_string(*block.get_by_position(0).column, 0), "1");
    EXPECT_EQ(id_type->to_string(*block.get_by_position(0).column, 1), "2");
    EXPECT_EQ(id_type->to_string(*block.get_by_position(0).column, 2), "3");

    std::filesystem::remove_all(test_dir);
}

TEST_F(IcebergReaderTest, v1_orc_mixed_ids_prefer_existing_equality_field_id) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_v1_orc_partial_id_equality_delete_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto data_file = (test_dir / "data.orc").string();
    const auto delete_file = (test_dir / "equality-delete.orc").string();
    // Mixed ORC schemas also stay in ID projection when any field has an ID; the key with ID 1 is
    // authoritative even though an ID-less historical alias is present.
    write_iceberg_three_int_orc_file(data_file, "id", 0, {1, 2, 3}, "legacy_added", std::nullopt,
                                     {5, 7, 9}, "stale_added", 1, {70, 70, 70});
    write_iceberg_int_orc_file(delete_file, "added_column", 1, {7});

    schema::external::TStructField root_field;
    root_field.__set_fields(
            {make_external_int_field("id", 0, std::nullopt),
             make_external_int_field("added_column", 1, std::nullopt, {"legacy_added"})});
    schema::external::TSchema current_schema;
    current_schema.__set_schema_id(-1);
    current_schema.__set_root_field(root_field);

    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    scan_params.__set_format_type(TFileFormatType::FORMAT_ORC);
    scan_params.__set_current_schema_id(-1);
    scan_params.__set_history_schema_info({current_schema});

    TIcebergDeleteFileDesc delete_descriptor;
    delete_descriptor.__set_content(IcebergReaderMixin<OrcReader>::EQUALITY_DELETE);
    delete_descriptor.__set_path(delete_file);
    delete_descriptor.__set_field_ids({1});
    delete_descriptor.__set_file_format(TFileFormatType::FORMAT_ORC);
    TIcebergFileDesc iceberg_descriptor;
    iceberg_descriptor.__set_format_version(2);
    iceberg_descriptor.__set_original_file_path(data_file);
    iceberg_descriptor.__set_delete_files({delete_descriptor});
    TTableFormatFileDesc table_format_descriptor;
    table_format_descriptor.__set_iceberg_params(iceberg_descriptor);

    TFileRangeDesc scan_range;
    scan_range.__set_fs_name("");
    scan_range.__set_path(data_file);
    scan_range.__set_start_offset(0);
    scan_range.__set_size(static_cast<int64_t>(std::filesystem::file_size(data_file)));
    scan_range.__set_file_size(static_cast<int64_t>(std::filesystem::file_size(data_file)));
    scan_range.__set_table_format_params(table_format_descriptor);

    ObjectPool object_pool;
    DescriptorTbl* descriptor_table = nullptr;
    const TupleDescriptor* tuple_descriptor = nullptr;
    ASSERT_TRUE(create_single_int_tuple_descriptor(&object_pool, "id", 0, &descriptor_table,
                                                   &tuple_descriptor)
                        .ok());
    ASSERT_NE(tuple_descriptor, nullptr);

    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state {TQueryOptions(), TQueryGlobals()};
    runtime_state.set_timezone("UTC");
    io::IOContext io_ctx;
    ShardedKVCache kv_cache(8);
    IcebergOrcReader reader(&kv_cache, &profile, &runtime_state, scan_params, scan_range, 1024,
                            "UTC", &io_ctx, cache.get());

    std::vector<ColumnDescriptor> column_descriptors(1);
    column_descriptors[0].name = "id";
    std::unordered_map<std::string, uint32_t> block_positions = {{"id", 0}};
    OrcInitContext context;
    context.column_descs = &column_descriptors;
    context.col_name_to_block_idx = &block_positions;
    context.tuple_descriptor = tuple_descriptor;
    context.params = &scan_params;
    context.range = &scan_range;
    const auto init_status = reader.init_reader(&context);
    ASSERT_TRUE(init_status.ok()) << init_status;

    const auto id_type = make_nullable(std::make_shared<DataTypeInt32>());
    Block block;
    block.insert({id_type->create_column(), id_type, "id"});
    size_t read_rows = 0;
    bool eof = false;
    const auto status = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(read_rows, 3);
    ASSERT_EQ(block.rows(), 3);
    EXPECT_EQ(id_type->to_string(*block.get_by_position(0).column, 0), "1");
    EXPECT_EQ(id_type->to_string(*block.get_by_position(0).column, 1), "2");
    EXPECT_EQ(id_type->to_string(*block.get_by_position(0).column, 2), "3");

    std::filesystem::remove_all(test_dir);
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

    // Create IcebergOrcReader (IS-A OrcReader via CRTP mixin)
    auto iceberg_reader = std::make_unique<IcebergOrcReader>(
            nullptr /* kv_cache */, &profile, &runtime_state, scan_params, scan_range, 1024, "CST",
            nullptr /* io_ctx */, cache.get());
    ASSERT_NE(iceberg_reader, nullptr);

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

    std::vector<std::string> table_col_names = {"name", "profile"};
    const RowDescriptor* row_descriptor = nullptr;
    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {
            {"name", 0},
            {"profile", 1},
    };

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
    orc_ctx.row_descriptor = row_descriptor;
    orc_ctx.params = &scan_params;
    orc_ctx.range = &scan_range;
    st = iceberg_reader->init_reader(&orc_ctx);
    ASSERT_TRUE(st.ok()) << st;

    // set_fill_columns logic is now inlined in _do_init_reader,
    // so no separate call is needed.

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

} // namespace doris
