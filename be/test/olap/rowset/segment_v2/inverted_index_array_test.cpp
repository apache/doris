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

#include <CLucene.h>
#include <CLucene/config/repl_wchar.h>
#include <CLucene/index/IndexReader.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <string.h>

#include <map>
#include <memory>
#include <string>

#include "gtest/gtest_pred_impl.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/index_file_reader.h"
#include "olap/rowset/segment_v2/index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/rowset/segment_v2/inverted_index_writer.h"
#include "olap/rowset/segment_v2/zone_map_index.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_helper.h"
#include "runtime/exec_env.h"
#include "util/faststring.h"
#include "util/slice.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_number.h"
#include "vec/olap/olap_data_convertor.h"

using namespace lucene::index;
using doris::segment_v2::IndexFileWriter;

namespace doris::segment_v2 {

class InvertedIndexArrayTest : public testing::Test {
    using ExpectedDocMap = std::map<std::string, std::vector<int>>;

public:
    const std::string kTestDir = "./ut_dir/inverted_index_array_test";

    void check_terms_stats(std::string index_prefix, ExpectedDocMap* expected,
                           std::vector<int> expected_null_bitmap = {},
                           InvertedIndexStorageFormatPB format = InvertedIndexStorageFormatPB::V1,
                           const TabletIndex* index_meta = nullptr) {
        std::string file_str;
        if (format == InvertedIndexStorageFormatPB::V1) {
            file_str = InvertedIndexDescriptor::get_index_file_path_v1(index_prefix,
                                                                       index_meta->index_id(), "");
        } else if (format == InvertedIndexStorageFormatPB::V2) {
            file_str = InvertedIndexDescriptor::get_index_file_path_v2(index_prefix);
        }
        std::unique_ptr<IndexFileReader> reader = std::make_unique<IndexFileReader>(
                io::global_local_filesystem(), index_prefix, format);
        auto st = reader->init();
        EXPECT_EQ(st, Status::OK());
        auto result = reader->open(index_meta);
        EXPECT_TRUE(result.has_value()) << "Failed to open compound reader" << result.error();
        auto compound_reader = std::move(result.value());
        try {
            CLuceneError err;
            CL_NS(store)::IndexInput* index_input = nullptr;
            auto ok = DorisFSDirectory::FSIndexInput::open(
                    io::global_local_filesystem(), file_str.c_str(), index_input, err, 4096);
            if (!ok) {
                throw err;
            }

            std::shared_ptr<roaring::Roaring> null_bitmap = std::make_shared<roaring::Roaring>();
            const char* null_bitmap_file_name =
                    InvertedIndexDescriptor::get_temporary_null_bitmap_file_name();
            if (compound_reader->fileExists(null_bitmap_file_name)) {
                std::unique_ptr<lucene::store::IndexInput> null_bitmap_in;
                assert(compound_reader->openInput(null_bitmap_file_name, null_bitmap_in, err,
                                                  4096));
                size_t null_bitmap_size = null_bitmap_in->length();
                doris::faststring buf;
                buf.resize(null_bitmap_size);
                null_bitmap_in->readBytes(reinterpret_cast<uint8_t*>(buf.data()), null_bitmap_size);
                *null_bitmap = roaring::Roaring::read(reinterpret_cast<char*>(buf.data()), false);
                EXPECT_TRUE(expected_null_bitmap.size() == null_bitmap->cardinality());
                for (int i : expected_null_bitmap) {
                    EXPECT_TRUE(null_bitmap->contains(i));
                }
            }
            index_input->close();
            _CLLDELETE(index_input);
        } catch (const CLuceneError& e) {
            EXPECT_TRUE(false) << "CLuceneError: " << e.what();
        }

        std::cout << "Term statistics for " << file_str << std::endl;
        std::cout << "==================================" << std::endl;
        lucene::store::Directory* dir = compound_reader.get();

        lucene::index::IndexReader* r = lucene::index::IndexReader::open(dir);

        printf("Max Docs: %d\n", r->maxDoc());
        printf("Num Docs: %d\n", r->numDocs());

        TermEnum* te = r->terms();
        int32_t nterms;
        for (nterms = 0; te->next(); nterms++) {
            /* empty */
            std::string token =
                    lucene_wcstoutf8string(te->term(false)->text(), te->term(false)->textLength());

            printf("Term: %s ", token.c_str());
            if (expected) {
                auto it = expected->find(token);
                if (it != expected->end()) {
                    TermDocs* td = r->termDocs(te->term(false));
                    std::vector<int> actual_docs;
                    while (td->next()) {
                        actual_docs.push_back(td->doc());
                    }
                    td->close();
                    _CLLDELETE(td);
                    EXPECT_EQ(actual_docs, it->second) << "Term: " << token;
                }
            }
            printf("Freq: %d\n", te->docFreq());
        }
        printf("Term count: %d\n\n", nterms);
        if (expected) {
            ASSERT_EQ(nterms, expected->size());
        }
        te->close();
        _CLLDELETE(te);

        r->close();
        _CLLDELETE(r);
        compound_reader->close();
    }

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        std::vector<StorePath> paths;
        paths.emplace_back(kTestDir, 1024);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        st = tmp_file_dirs->init();
        if (!st.ok()) {
            std::cout << "init tmp file dirs error:" << st.to_string() << std::endl;
            return;
        }
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    // create a TabletSchema with an array column (and a normal int column as key)
    TabletSchemaSPtr create_schema_with_array(KeysType keys_type = DUP_KEYS) {
        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(keys_type);

        tablet_schema->init_from_pb(tablet_schema_pb);
        TabletColumn array;
        array.set_name("arr1");
        array.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
        array.set_length(0);
        array.set_index_length(0);
        array.set_is_nullable(false);
        array.set_is_bf_column(false);
        TabletColumn child;
        child.set_name("arr_sub_string");
        child.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
        child.set_length(INT_MAX);
        array.add_sub_column(child);
        tablet_schema->append_column(array);
        return tablet_schema;
    }

    void test_non_null_string(std::string_view rowset_id, int seg_id, Field* field) {
        EXPECT_TRUE(field->type() == FieldType::OLAP_FIELD_TYPE_ARRAY);
        std::string index_path_prefix {InvertedIndexDescriptor::get_index_file_path_prefix(
                local_segment_path(kTestDir, rowset_id, seg_id))};
        int index_id = 26033;
        std::string index_path =
                InvertedIndexDescriptor::get_index_file_path_v1(index_path_prefix, index_id, "");
        auto fs = io::global_local_filesystem();

        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(index_id);
        index_meta_pb->set_index_name("index_inverted_arr1");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(0);

        TabletIndex idx_meta;
        idx_meta.index_type();
        idx_meta.init_from_pb(*index_meta_pb.get());
        auto index_file_writer =
                std::make_unique<IndexFileWriter>(fs, index_path_prefix, std::string {rowset_id},
                                                  seg_id, InvertedIndexStorageFormatPB::V1);
        std::unique_ptr<segment_v2::InvertedIndexColumnWriter> _inverted_index_builder = nullptr;
        EXPECT_EQ(InvertedIndexColumnWriter::create(field, &_inverted_index_builder,
                                                    index_file_writer.get(), &idx_meta),
                  Status::OK());

        // Construct two arrays: The first row is ["amory","doris"], and the second row is ["amory", "commiter"]
        vectorized::Array a1, a2;
        a1.push_back(vectorized::Field::create_field<TYPE_STRING>("amory"));
        a1.push_back(vectorized::Field::create_field<TYPE_STRING>("doris"));
        a2.push_back(vectorized::Field::create_field<TYPE_STRING>("amory"));
        a2.push_back(vectorized::Field::create_field<TYPE_STRING>("commiter"));

        // Construct array type: DataTypeArray(DataTypeString)
        vectorized::DataTypePtr s1 = std::make_shared<vectorized::DataTypeString>();
        vectorized::DataTypePtr array_type = std::make_shared<vectorized::DataTypeArray>(s1);
        vectorized::MutableColumnPtr col = array_type->create_column();
        col->insert(vectorized::Field::create_field<TYPE_ARRAY>(a1));
        col->insert(vectorized::Field::create_field<TYPE_ARRAY>(a2));
        vectorized::ColumnPtr column_array = std::move(col);
        vectorized::ColumnWithTypeAndName type_and_name(column_array, array_type, "arr1");

        // Put the array column into the Block (assuming only this column)
        vectorized::Block block;
        block.insert(type_and_name);
        // block.rows() should be 2

        // Use OlapBlockDataConvertor to convert
        // Note: Here we need a TabletSchema object, in this example we construct a simple schema,
        // Assuming that the 0th column in the schema is our array column (the actual UT has the corresponding TabletColumn)
        TabletSchemaSPtr tablet_schema = create_schema_with_array();
        vectorized::OlapBlockDataConvertor convertor(tablet_schema.get(), {0});
        convertor.set_source_content(&block, 0, block.rows());
        auto [st, accessor] = convertor.convert_column_data(0);
        EXPECT_EQ(st, Status::OK());
        // The conversion result is actually an array of 4 pointers:
        //   [0]: Total number of elements (elem_cnt)
        //   [1]: Offsets array pointer
        //   [2]: Nested item data pointer
        //   [3]: Nested nullmap pointer
        const auto* data_ptr = reinterpret_cast<const uint64_t*>(accessor->get_data());
        const auto* offsets_ptr = reinterpret_cast<const uint8_t*>(data_ptr[1]);
        const void* item_data = reinterpret_cast<const void*>(data_ptr[2]);
        const auto* item_nullmap = reinterpret_cast<const uint8_t*>(data_ptr[3]);

        // Get the length of the subfield, used for inverted index writing
        auto field_size = field->get_sub_field(0)->size();
        // Call the inverted index writing interface, passing in item_data, item_nullmap, offsets_ptr, and the number of rows (the number of array rows in the Block)
        st = _inverted_index_builder->add_array_values(field_size, item_data, item_nullmap,
                                                       offsets_ptr, block.rows());
        EXPECT_EQ(st, Status::OK());
        const auto* null_map = accessor->get_nullmap();
        // add nulls
        st = _inverted_index_builder->add_array_nulls(null_map, block.rows());
        EXPECT_EQ(st, Status::OK());

        EXPECT_EQ(_inverted_index_builder->finish(), Status::OK());
        EXPECT_EQ(index_file_writer->close(), Status::OK());

        ExpectedDocMap expected = {{"amory", {0, 1}}, {"doris", {0}}, {"commiter", {1}}};
        check_terms_stats(index_path_prefix, &expected, {}, InvertedIndexStorageFormatPB::V1,
                          &idx_meta);
    }

    void test_string(std::string_view rowset_id, int seg_id, Field* field) {
        EXPECT_TRUE(field->type() == FieldType::OLAP_FIELD_TYPE_ARRAY);
        std::string index_path_prefix {InvertedIndexDescriptor::get_index_file_path_prefix(
                local_segment_path(kTestDir, rowset_id, seg_id))};
        int index_id = 26033;
        std::string index_path =
                InvertedIndexDescriptor::get_index_file_path_v1(index_path_prefix, index_id, "");
        auto fs = io::global_local_filesystem();

        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(index_id);
        index_meta_pb->set_index_name("index_inverted_arr1");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(0);

        TabletIndex idx_meta;
        idx_meta.index_type();
        idx_meta.init_from_pb(*index_meta_pb.get());
        auto index_file_writer =
                std::make_unique<IndexFileWriter>(fs, index_path_prefix, std::string {rowset_id},
                                                  seg_id, InvertedIndexStorageFormatPB::V1);
        std::unique_ptr<segment_v2::InvertedIndexColumnWriter> _inverted_index_builder = nullptr;
        EXPECT_EQ(InvertedIndexColumnWriter::create(field, &_inverted_index_builder,
                                                    index_file_writer.get(), &idx_meta),
                  Status::OK());

        // Construct two arrays: The first row is ["amory","doris"], and the second row is [NULL, "amory", "commiter"]
        vectorized::Array a1, a2;
        a1.push_back(vectorized::Field::create_field<TYPE_STRING>("amory"));
        a1.push_back(vectorized::Field::create_field<TYPE_STRING>("doris"));
        a2.push_back(vectorized::Field());
        a2.push_back(vectorized::Field::create_field<TYPE_STRING>("amory"));
        a2.push_back(vectorized::Field::create_field<TYPE_STRING>("commiter"));

        // Construct array type: DataTypeArray(DataTypeNullable(DataTypeString))
        vectorized::DataTypePtr s1 = std::make_shared<vectorized::DataTypeNullable>(
                std::make_shared<vectorized::DataTypeString>());
        vectorized::DataTypePtr array_type = std::make_shared<vectorized::DataTypeArray>(s1);
        vectorized::MutableColumnPtr col = array_type->create_column();
        col->insert(vectorized::Field::create_field<TYPE_ARRAY>(a1));
        col->insert(vectorized::Field::create_field<TYPE_ARRAY>(a2));
        vectorized::ColumnPtr column_array = std::move(col);
        vectorized::ColumnWithTypeAndName type_and_name(column_array, array_type, "arr1");

        // Put the array column into the Block (assuming only this column)
        vectorized::Block block;
        block.insert(type_and_name);
        // block.rows() should be 2

        // Use OlapBlockDataConvertor to convert
        // Note: Here we need a TabletSchema object, in this example we construct a simple schema,
        // Assuming that the 0th column in the schema is our array column (the actual UT has the corresponding TabletColumn)
        TabletSchemaSPtr tablet_schema = create_schema_with_array();
        vectorized::OlapBlockDataConvertor convertor(tablet_schema.get(), {0});
        convertor.set_source_content(&block, 0, block.rows());
        auto [st, accessor] = convertor.convert_column_data(0);
        EXPECT_EQ(st, Status::OK());
        // The conversion result is actually an array of 4 pointers:
        //   [0]: Total number of elements (elem_cnt)
        //   [1]: Offsets array pointer
        //   [2]: Nested item data pointer
        //   [3]: Nested nullmap pointer
        const auto* data_ptr = reinterpret_cast<const uint64_t*>(accessor->get_data());
        const auto* offsets_ptr = reinterpret_cast<const uint8_t*>(data_ptr[1]);
        const void* item_data = reinterpret_cast<const void*>(data_ptr[2]);
        const auto* item_nullmap = reinterpret_cast<const uint8_t*>(data_ptr[3]);

        // Get the length of the subfield, used for inverted index writing
        auto field_size = field->get_sub_field(0)->size();
        // Call the inverted index writing interface, passing in item_data, item_nullmap, offsets_ptr, and the number of rows (the number of array rows in the Block)
        st = _inverted_index_builder->add_array_values(field_size, item_data, item_nullmap,
                                                       offsets_ptr, block.rows());
        EXPECT_EQ(st, Status::OK());
        const auto* null_map = accessor->get_nullmap();
        // add nulls
        st = _inverted_index_builder->add_array_nulls(null_map, block.rows());
        EXPECT_EQ(st, Status::OK());
        EXPECT_EQ(_inverted_index_builder->finish(), Status::OK());
        EXPECT_EQ(index_file_writer->close(), Status::OK());

        ExpectedDocMap expected = {{"amory", {0, 1}}, {"doris", {0}}, {"commiter", {1}}};
        check_terms_stats(index_path_prefix, &expected, {}, InvertedIndexStorageFormatPB::V1,
                          &idx_meta);
    }

    void test_null_write_v2(std::string_view rowset_id, int seg_id, Field* field) {
        EXPECT_TRUE(field->type() == FieldType::OLAP_FIELD_TYPE_ARRAY);
        std::string index_path_prefix {InvertedIndexDescriptor::get_index_file_path_prefix(
                local_segment_path(kTestDir, rowset_id, seg_id))};
        int index_id = 26033;
        std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix);
        auto fs = io::global_local_filesystem();

        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(index_id);
        index_meta_pb->set_index_name("index_inverted_arr1");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(0);

        TabletIndex idx_meta;
        idx_meta.index_type();
        idx_meta.init_from_pb(*index_meta_pb.get());
        io::FileWriterPtr file_writer;
        io::FileWriterOptions opts;
        Status sts = fs->create_file(index_path, &file_writer, &opts);
        ASSERT_TRUE(sts.ok());
        auto index_file_writer = std::make_unique<IndexFileWriter>(
                fs, index_path_prefix, std::string {rowset_id}, seg_id,
                InvertedIndexStorageFormatPB::V2, std::move(file_writer));
        std::unique_ptr<segment_v2::InvertedIndexColumnWriter> _inverted_index_builder = nullptr;
        EXPECT_EQ(InvertedIndexColumnWriter::create(field, &_inverted_index_builder,
                                                    index_file_writer.get(), &idx_meta),
                  Status::OK());

        // Simulate outer null cases: 5 rows, outer null map = {1, 0, 0, 1, 0}, i.e., rows 0 and 3 are null
        std::vector<uint8_t> outer_null_map = {1, 0, 0, 1, 0};

        // Construct inner array type: DataTypeArray(DataTypeNullable(DataTypeString))
        vectorized::DataTypePtr inner_string_type = std::make_shared<vectorized::DataTypeNullable>(
                std::make_shared<vectorized::DataTypeString>());
        vectorized::DataTypePtr array_type =
                std::make_shared<vectorized::DataTypeArray>(inner_string_type);
        // To support outer array null values, wrap it in a Nullable type
        vectorized::DataTypePtr final_type =
                std::make_shared<vectorized::DataTypeNullable>(array_type);

        // Construct 5 rows of data:
        // Row 0: null
        // Row 1: a2 = [Null, "test"]
        // Row 2: a3 = ["mixed", Null, "data"]
        // Row 3: null
        // Row 4: a5 = ["non-null"]
        vectorized::MutableColumnPtr col = final_type->create_column();
        // Row 0: insert null
        col->insert(vectorized::Field());
        // Row 1: insert a2
        vectorized::Array a2;
        a2.push_back(vectorized::Field());
        a2.push_back(vectorized::Field::create_field<TYPE_STRING>("test"));
        col->insert(vectorized::Field::create_field<TYPE_ARRAY>(a2));
        // Row 2: insert a3
        vectorized::Array a3;
        a3.push_back(vectorized::Field::create_field<TYPE_STRING>("mixed"));
        a3.push_back(vectorized::Field());
        a3.push_back(vectorized::Field::create_field<TYPE_STRING>("data"));
        col->insert(vectorized::Field::create_field<TYPE_ARRAY>(a3));
        // Row 3: insert null
        col->insert(vectorized::Field());
        // Row 4: insert a5
        vectorized::Array a5;
        a5.push_back(vectorized::Field::create_field<TYPE_STRING>("non-null"));
        col->insert(vectorized::Field::create_field<TYPE_ARRAY>(a5));

        vectorized::ColumnPtr column_array = std::move(col);
        vectorized::ColumnWithTypeAndName type_and_name(column_array, final_type, "arr1");

        // Construct Block, containing only the array column, with 5 rows
        vectorized::Block block;
        block.insert(type_and_name);

        // Construct TabletSchema (containing the array column) - reference the existing helper function
        TabletSchemaSPtr tablet_schema = create_schema_with_array();
        // In this schema, assume the 0th column is the key, and the arr1 column is the non-key column with index 1
        vectorized::OlapBlockDataConvertor convertor(tablet_schema.get(), {0});
        convertor.set_source_content(&block, 0, block.rows());

        // Convert array column data
        auto [st, accessor] = convertor.convert_column_data(0);
        EXPECT_EQ(st, Status::OK());
        // OlapColumnDataConvertorArray conversion result is a 4-tuple:
        //   [0]: element total count (elem_cnt, not used directly)
        //   [1]: offsets array pointer
        //   [2]: nested item data conversion result pointer
        //   [3]: nested nullmap pointer
        const auto* data_ptr = reinterpret_cast<const uint64_t*>(accessor->get_data());
        const auto* offsets_ptr = reinterpret_cast<const uint8_t*>(data_ptr[1]);
        const void* item_data = reinterpret_cast<const void*>(data_ptr[2]);
        const auto* item_nullmap = reinterpret_cast<const uint8_t*>(data_ptr[3]);

        // Call the inverted index writing interface, passing in the converted nested data, nullmap, and offsets
        auto field_size = field->get_sub_field(0)->size();
        st = _inverted_index_builder->add_array_values(field_size, item_data, item_nullmap,
                                                       offsets_ptr, block.rows());
        EXPECT_EQ(st, Status::OK());
        const auto* null_map = accessor->get_nullmap();
        // add nulls
        st = _inverted_index_builder->add_array_nulls(null_map, block.rows());
        EXPECT_EQ(st, Status::OK());
        EXPECT_EQ(_inverted_index_builder->finish(), Status::OK());
        EXPECT_EQ(index_file_writer->close(), Status::OK());

        // Expected inverted index result: only index non-null elements
        // Row 1: non-null in a2 is "test"
        // Row 2: non-null in a3 is "mixed" and "data"
        // Row 4: non-null in a5 is "non-null"
        ExpectedDocMap expected = {{"test", {1}}, {"mixed", {2}}, {"data", {2}}, {"non-null", {4}}};
        std::vector<int> expected_null_bitmap = {0, 3};
        check_terms_stats(index_path_prefix, &expected, expected_null_bitmap,
                          InvertedIndexStorageFormatPB::V2, &idx_meta);
    }

    void test_null_write(std::string_view rowset_id, int seg_id, Field* field) {
        EXPECT_TRUE(field->type() == FieldType::OLAP_FIELD_TYPE_ARRAY);
        std::string index_path_prefix {InvertedIndexDescriptor::get_index_file_path_prefix(
                local_segment_path(kTestDir, rowset_id, seg_id))};
        int index_id = 26033;
        std::string index_path =
                InvertedIndexDescriptor::get_index_file_path_v1(index_path_prefix, index_id, "");
        auto fs = io::global_local_filesystem();

        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(index_id);
        index_meta_pb->set_index_name("index_inverted_arr1");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(0);

        TabletIndex idx_meta;
        idx_meta.index_type();
        idx_meta.init_from_pb(*index_meta_pb.get());
        auto index_file_writer =
                std::make_unique<IndexFileWriter>(fs, index_path_prefix, std::string {rowset_id},
                                                  seg_id, InvertedIndexStorageFormatPB::V1);
        std::unique_ptr<segment_v2::InvertedIndexColumnWriter> _inverted_index_builder = nullptr;
        EXPECT_EQ(InvertedIndexColumnWriter::create(field, &_inverted_index_builder,
                                                    index_file_writer.get(), &idx_meta),
                  Status::OK());

        // Simulate outer null cases: 5 rows, outer null map = {1, 0, 0, 1, 0}, i.e., rows 0 and 3 are null
        std::vector<uint8_t> outer_null_map = {1, 0, 0, 1, 0};

        // Construct inner array type: DataTypeArray(DataTypeNullable(DataTypeString))
        vectorized::DataTypePtr inner_string_type = std::make_shared<vectorized::DataTypeNullable>(
                std::make_shared<vectorized::DataTypeString>());
        vectorized::DataTypePtr array_type =
                std::make_shared<vectorized::DataTypeArray>(inner_string_type);
        // To support outer array null values, wrap it in a Nullable type
        vectorized::DataTypePtr final_type =
                std::make_shared<vectorized::DataTypeNullable>(array_type);

        // Construct 5 rows of data:
        // Row 0: null
        // Row 1: a2 = [Null, "test"]
        // Row 2: a3 = ["mixed", Null, "data"]
        // Row 3: null
        // Row 4: a5 = ["non-null"]
        vectorized::MutableColumnPtr col = final_type->create_column();
        // Row 0: insert null
        col->insert(vectorized::Field());
        // Row 1: insert a2
        vectorized::Array a2;
        a2.push_back(vectorized::Field());
        a2.push_back(vectorized::Field::create_field<TYPE_STRING>("test"));
        col->insert(vectorized::Field::create_field<TYPE_ARRAY>(a2));
        // Row 2: insert a3
        vectorized::Array a3;
        a3.push_back(vectorized::Field::create_field<TYPE_STRING>("mixed"));
        a3.push_back(vectorized::Field());
        a3.push_back(vectorized::Field::create_field<TYPE_STRING>("data"));
        col->insert(vectorized::Field::create_field<TYPE_ARRAY>(a3));
        // Row 3: insert null
        col->insert(vectorized::Field());
        // Row 4: insert a5
        vectorized::Array a5;
        a5.push_back(vectorized::Field::create_field<TYPE_STRING>("non-null"));
        col->insert(vectorized::Field::create_field<TYPE_ARRAY>(a5));

        vectorized::ColumnPtr column_array = std::move(col);
        vectorized::ColumnWithTypeAndName type_and_name(column_array, final_type, "arr1");

        // Construct Block, containing only the array column, with 5 rows
        vectorized::Block block;
        block.insert(type_and_name);

        // Construct TabletSchema (containing the array column) - reference the existing helper function
        TabletSchemaSPtr tablet_schema = create_schema_with_array();
        // In this schema, assume the 0th column is the key, and the arr1 column is the non-key column with index 1
        vectorized::OlapBlockDataConvertor convertor(tablet_schema.get(), {0});
        convertor.set_source_content(&block, 0, block.rows());

        // Convert array column data
        auto [st, accessor] = convertor.convert_column_data(0);
        EXPECT_EQ(st, Status::OK());
        // OlapColumnDataConvertorArray conversion result is a 4-tuple:
        //   [0]: element total count (elem_cnt, not used directly)
        //   [1]: offsets array pointer
        //   [2]: nested item data conversion result pointer
        //   [3]: nested nullmap pointer
        const auto* data_ptr = reinterpret_cast<const uint64_t*>(accessor->get_data());
        const auto* offsets_ptr = reinterpret_cast<const uint8_t*>(data_ptr[1]);
        const void* item_data = reinterpret_cast<const void*>(data_ptr[2]);
        const auto* item_nullmap = reinterpret_cast<const uint8_t*>(data_ptr[3]);

        // Call the inverted index writing interface, passing in the converted nested data, nullmap, and offsets
        auto field_size = field->get_sub_field(0)->size();
        st = _inverted_index_builder->add_array_values(field_size, item_data, item_nullmap,
                                                       offsets_ptr, block.rows());
        EXPECT_EQ(st, Status::OK());
        const auto* null_map = accessor->get_nullmap();
        // add nulls
        st = _inverted_index_builder->add_array_nulls(null_map, block.rows());
        EXPECT_EQ(st, Status::OK());
        EXPECT_EQ(_inverted_index_builder->finish(), Status::OK());
        EXPECT_EQ(index_file_writer->close(), Status::OK());

        // Expected inverted index result: only index non-null elements
        // Row 1: non-null in a2 is "test"
        // Row 2: non-null in a3 is "mixed" and "data"
        // Row 4: non-null in a5 is "non-null"
        ExpectedDocMap expected = {{"test", {1}}, {"mixed", {2}}, {"data", {2}}, {"non-null", {4}}};
        std::vector<int> expected_null_bitmap = {0, 3};
        check_terms_stats(index_path_prefix, &expected, expected_null_bitmap,
                          InvertedIndexStorageFormatPB::V1, &idx_meta);
    }

    void test_multi_block_write(std::string_view rowset_id, int seg_id, Field* field) {
        EXPECT_TRUE(field->type() == FieldType::OLAP_FIELD_TYPE_ARRAY);
        std::string index_path_prefix {InvertedIndexDescriptor::get_index_file_path_prefix(
                local_segment_path(kTestDir, rowset_id, seg_id))};
        int index_id = 26033;
        std::string index_path =
                InvertedIndexDescriptor::get_index_file_path_v1(index_path_prefix, index_id, "");
        auto fs = io::global_local_filesystem();

        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(index_id);
        index_meta_pb->set_index_name("index_inverted_arr1");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(0);

        TabletIndex idx_meta;
        idx_meta.init_from_pb(*index_meta_pb.get());
        auto index_file_writer = std::make_unique<IndexFileWriter>(
                fs, index_path_prefix, "multi_block", 0, InvertedIndexStorageFormatPB::V1);
        std::unique_ptr<segment_v2::InvertedIndexColumnWriter> _inverted_index_builder = nullptr;
        EXPECT_EQ(InvertedIndexColumnWriter::create(field, &_inverted_index_builder,
                                                    index_file_writer.get(), &idx_meta),
                  Status::OK());

        ExpectedDocMap merged_expected;

        // --- Block 1 ---
        {
            const int row_num = 4;
            // construct data type: Nullable( Array( Nullable(String) ) )
            vectorized::DataTypePtr inner_string = std::make_shared<vectorized::DataTypeNullable>(
                    std::make_shared<vectorized::DataTypeString>());
            vectorized::DataTypePtr array_type =
                    std::make_shared<vectorized::DataTypeArray>(inner_string);
            vectorized::DataTypePtr final_type =
                    std::make_shared<vectorized::DataTypeNullable>(array_type);

            // construct MutableColumn
            vectorized::MutableColumnPtr col = final_type->create_column();
            // simulate outer null: row0 and row3 are null, the rest are non-null
            col->insert(vectorized::Field()); // row0: null
            {
                // row1: non-null, array with 1 element: "block1_data1"
                vectorized::Array arr;
                arr.push_back(vectorized::Field::create_field<TYPE_STRING>("block1_data1"));
                col->insert(vectorized::Field::create_field<TYPE_ARRAY>(arr));
            }
            {
                // row2: non-null, array with 1 element: "block1_data2"
                vectorized::Array arr;
                arr.push_back(vectorized::Field::create_field<TYPE_STRING>("block1_data2"));
                col->insert(vectorized::Field::create_field<TYPE_ARRAY>(arr));
            }
            col->insert(vectorized::Field()); // row3: null

            vectorized::ColumnPtr column_array = std::move(col);
            vectorized::ColumnWithTypeAndName type_and_name(column_array, final_type, "arr1");

            // construct Block (containing only the arr1 column)
            vectorized::Block block;
            block.insert(type_and_name);

            // use TabletSchema containing the array column (arr1 is the non-key column with index 1 in the schema)
            TabletSchemaSPtr tablet_schema = create_schema_with_array();
            vectorized::OlapBlockDataConvertor convertor(tablet_schema.get(), {0});
            convertor.set_source_content(&block, 0, block.rows());

            // convert the arr1 column in the block
            auto [st, accessor] = convertor.convert_column_data(0);
            EXPECT_EQ(st, Status::OK());
            // the conversion result is a 4-tuple: [0]: element count, [1]: offsets pointer, [2]: item data, [3]: item nullmap
            const auto* data_ptr = reinterpret_cast<const uint64_t*>(accessor->get_data());
            const auto* offsets_ptr = reinterpret_cast<const uint8_t*>(data_ptr[1]);
            const void* item_data = reinterpret_cast<const void*>(data_ptr[2]);
            const auto* item_nullmap = reinterpret_cast<const uint8_t*>(data_ptr[3]);
            auto field_size = field->get_sub_field(0)->size();
            st = _inverted_index_builder->add_array_values(field_size, item_data, item_nullmap,
                                                           offsets_ptr, row_num);
            EXPECT_EQ(st, Status::OK());
            const auto* null_map = accessor->get_nullmap();
            // add nulls
            st = _inverted_index_builder->add_array_nulls(null_map, row_num);
            EXPECT_EQ(st, Status::OK());

            // for Block1, the expected non-null behavior is row1 and row2
            ExpectedDocMap expected = {{"block1_data1", {1}}, {"block1_data2", {2}}};
            merged_expected.insert(expected.begin(), expected.end());
        }

        // --- Block 2 ---
        {
            const int row_num = 2;
            vectorized::DataTypePtr inner_string = std::make_shared<vectorized::DataTypeNullable>(
                    std::make_shared<vectorized::DataTypeString>());
            vectorized::DataTypePtr array_type =
                    std::make_shared<vectorized::DataTypeArray>(inner_string);
            vectorized::DataTypePtr final_type =
                    std::make_shared<vectorized::DataTypeNullable>(array_type);

            vectorized::MutableColumnPtr col = final_type->create_column();
            // row0: non-null, array with 1 element: "block2_data1"
            {
                vectorized::Array arr;
                arr.push_back(vectorized::Field::create_field<TYPE_STRING>("block2_data1"));
                col->insert(vectorized::Field::create_field<TYPE_ARRAY>(arr));
            }
            // row1: null
            col->insert(vectorized::Field());

            vectorized::ColumnPtr column_array = std::move(col);
            vectorized::ColumnWithTypeAndName type_and_name(column_array, final_type, "arr1");

            vectorized::Block block;
            block.insert(type_and_name);

            TabletSchemaSPtr tablet_schema = create_schema_with_array();
            vectorized::OlapBlockDataConvertor convertor(tablet_schema.get(), {0});
            convertor.set_source_content(&block, 0, block.rows());

            auto [st, accessor] = convertor.convert_column_data(0);
            EXPECT_EQ(st, Status::OK());
            const auto* data_ptr = reinterpret_cast<const uint64_t*>(accessor->get_data());
            const auto* offsets_ptr = reinterpret_cast<const uint8_t*>(data_ptr[1]);
            const void* item_data = reinterpret_cast<const void*>(data_ptr[2]);
            const auto* item_nullmap = reinterpret_cast<const uint8_t*>(data_ptr[3]);

            auto field_size = field->get_sub_field(0)->size();
            st = _inverted_index_builder->add_array_values(field_size, item_data, item_nullmap,
                                                           offsets_ptr, row_num);
            EXPECT_EQ(st, Status::OK());
            const auto* null_map = accessor->get_nullmap();
            // add nulls
            st = _inverted_index_builder->add_array_nulls(null_map, row_num);
            EXPECT_EQ(st, Status::OK());

            ExpectedDocMap expected = {{"block2_data1", {4}}};
            merged_expected.insert(expected.begin(), expected.end());
        }

        // --- Block 3 ---
        {
            const int row_num = 2;
            vectorized::DataTypePtr inner_string = std::make_shared<vectorized::DataTypeNullable>(
                    std::make_shared<vectorized::DataTypeString>());
            vectorized::DataTypePtr array_type =
                    std::make_shared<vectorized::DataTypeArray>(inner_string);
            vectorized::DataTypePtr final_type =
                    std::make_shared<vectorized::DataTypeNullable>(array_type);

            vectorized::MutableColumnPtr col = final_type->create_column();
            // row0: non-null, array with 1 element: "block3_data1"
            {
                vectorized::Array arr;
                arr.push_back(vectorized::Field::create_field<TYPE_STRING>("block3_data1"));
                col->insert(vectorized::Field::create_field<TYPE_ARRAY>(arr));
            }
            // row1: null
            col->insert(vectorized::Field());

            vectorized::ColumnPtr column_array = std::move(col);
            vectorized::ColumnWithTypeAndName type_and_name(column_array, final_type, "arr1");

            vectorized::Block block;
            block.insert(type_and_name);

            TabletSchemaSPtr tablet_schema = create_schema_with_array();
            vectorized::OlapBlockDataConvertor convertor(tablet_schema.get(), {0});
            convertor.set_source_content(&block, 0, block.rows());

            auto [st, accessor] = convertor.convert_column_data(0);
            EXPECT_EQ(st, Status::OK());
            const auto* data_ptr = reinterpret_cast<const uint64_t*>(accessor->get_data());
            const auto* offsets_ptr = reinterpret_cast<const uint8_t*>(data_ptr[1]);
            const void* item_data = reinterpret_cast<const void*>(data_ptr[2]);
            const auto* item_nullmap = reinterpret_cast<const uint8_t*>(data_ptr[3]);
            auto field_size = field->get_sub_field(0)->size();
            st = _inverted_index_builder->add_array_values(field_size, item_data, item_nullmap,
                                                           offsets_ptr, row_num);
            EXPECT_EQ(st, Status::OK());
            const auto* null_map = accessor->get_nullmap();
            // add nulls
            st = _inverted_index_builder->add_array_nulls(null_map, row_num);
            EXPECT_EQ(st, Status::OK());

            ExpectedDocMap expected = {{"block3_data1", {6}}};
            merged_expected.insert(expected.begin(), expected.end());
        }

        EXPECT_EQ(_inverted_index_builder->finish(), Status::OK());
        EXPECT_EQ(index_file_writer->close(), Status::OK());

        std::vector<int> expected_null_bitmap = {0, 3, 5, 7};
        check_terms_stats(index_path_prefix, &merged_expected, expected_null_bitmap,
                          InvertedIndexStorageFormatPB::V1, &idx_meta);
    }

    void test_array_numeric(std::string_view rowset_id, int seg_id, Field* field) {
        EXPECT_TRUE(field->type() == FieldType::OLAP_FIELD_TYPE_ARRAY);
        std::string index_path_prefix {InvertedIndexDescriptor::get_index_file_path_prefix(
                local_segment_path(kTestDir, rowset_id, seg_id))};
        int index_id = 26033;
        std::string index_path =
                InvertedIndexDescriptor::get_index_file_path_v1(index_path_prefix, index_id, "");
        auto fs = io::global_local_filesystem();

        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(index_id);
        index_meta_pb->set_index_name("index_inverted_arr_numeric");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(0);

        TabletIndex idx_meta;
        idx_meta.init_from_pb(*index_meta_pb.get());
        auto index_file_writer =
                std::make_unique<IndexFileWriter>(fs, index_path_prefix, std::string {rowset_id},
                                                  seg_id, InvertedIndexStorageFormatPB::V1);
        std::unique_ptr<segment_v2::InvertedIndexColumnWriter> _inverted_index_builder = nullptr;
        EXPECT_EQ(InvertedIndexColumnWriter::create(field, &_inverted_index_builder,
                                                    index_file_writer.get(), &idx_meta),
                  Status::OK());

        vectorized::DataTypePtr inner_int = std::make_shared<vectorized::DataTypeInt32>();
        vectorized::DataTypePtr array_type = std::make_shared<vectorized::DataTypeArray>(inner_int);
        vectorized::DataTypePtr final_type =
                std::make_shared<vectorized::DataTypeNullable>(array_type);

        // create a MutableColumnPtr
        vectorized::MutableColumnPtr col = final_type->create_column();
        // row0: non-null, array [123, 456]
        {
            vectorized::Array arr;
            arr.push_back(vectorized::Field::create_field<TYPE_INT>(123));
            arr.push_back(vectorized::Field::create_field<TYPE_INT>(456));
            col->insert(vectorized::Field::create_field<TYPE_ARRAY>(arr));
        }
        // row1: null
        col->insert(vectorized::Field());
        // row2: non-null, array [789, 101112]
        {
            vectorized::Array arr;
            arr.push_back(vectorized::Field::create_field<TYPE_INT>(789));
            arr.push_back(vectorized::Field::create_field<TYPE_INT>(101112));
            col->insert(vectorized::Field::create_field<TYPE_ARRAY>(arr));
        }
        // wrap the constructed column into a ColumnWithTypeAndName
        vectorized::ColumnPtr column_array = std::move(col);
        vectorized::ColumnWithTypeAndName type_and_name(column_array, final_type, "arr_num");

        // construct Block (containing only this column), with 3 rows
        vectorized::Block block;
        block.insert(type_and_name);

        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(KeysType::DUP_KEYS);

        tablet_schema->init_from_pb(tablet_schema_pb);
        TabletColumn array;
        array.set_name("arr1");
        array.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
        array.set_length(0);
        array.set_index_length(0);
        array.set_is_nullable(false);
        array.set_is_bf_column(false);
        TabletColumn child;
        child.set_name("arr_sub_int");
        child.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        child.set_length(INT_MAX);
        array.add_sub_column(child);
        tablet_schema->append_column(array);

        vectorized::OlapBlockDataConvertor convertor(tablet_schema.get(), {0});
        convertor.set_source_content(&block, 0, block.rows());
        auto [st, accessor] = convertor.convert_column_data(0);
        EXPECT_EQ(st, Status::OK());
        // the conversion result is a 4-tuple: [0]: element total count, [1]: offsets pointer, [2]: item data, [3]: item nullmap
        const auto* data_ptr = reinterpret_cast<const uint64_t*>(accessor->get_data());
        const auto* offsets_ptr = reinterpret_cast<const uint8_t*>(data_ptr[1]);
        const void* item_data = reinterpret_cast<const void*>(data_ptr[2]);
        const auto* item_nullmap = reinterpret_cast<const uint8_t*>(data_ptr[3]);

        // get the size of the sub field (4 bytes for INT type)
        auto field_size = field->get_sub_field(0)->size();
        st = _inverted_index_builder->add_array_values(field_size, item_data, item_nullmap,
                                                       offsets_ptr, block.rows());
        EXPECT_EQ(st, Status::OK());
        const auto* null_map = accessor->get_nullmap();
        // add nulls
        st = _inverted_index_builder->add_array_nulls(null_map, block.rows());
        EXPECT_EQ(st, Status::OK());
        EXPECT_EQ(_inverted_index_builder->finish(), Status::OK());
        EXPECT_EQ(index_file_writer->close(), Status::OK());

        // expected inverted index: row0 contains "123" and "456" (doc id 0), row1 is null, row2 contains "789" and "101112" (doc id 2)
        ExpectedDocMap expected = {{"123", {0}}, {"456", {0}}, {"789", {2}}, {"101112", {2}}};
        std::vector<int> expected_null_bitmap = {1};

        std::unique_ptr<IndexFileReader> reader = std::make_unique<IndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V1);
        auto sts = reader->init();
        EXPECT_EQ(sts, Status::OK());
        auto result = reader->open(&idx_meta);
        EXPECT_TRUE(result.has_value()) << "Failed to open compound reader" << result.error();
        auto compound_reader = std::move(result.value());
        try {
            CLuceneError err;
            CL_NS(store)::IndexInput* index_input = nullptr;
            auto ok = DorisFSDirectory::FSIndexInput::open(
                    io::global_local_filesystem(), index_path.c_str(), index_input, err, 4096);
            if (!ok) {
                throw err;
            }

            std::shared_ptr<roaring::Roaring> null_bitmap = std::make_shared<roaring::Roaring>();
            const char* null_bitmap_file_name =
                    InvertedIndexDescriptor::get_temporary_null_bitmap_file_name();
            if (compound_reader->fileExists(null_bitmap_file_name)) {
                std::unique_ptr<lucene::store::IndexInput> null_bitmap_in;
                assert(compound_reader->openInput(null_bitmap_file_name, null_bitmap_in, err,
                                                  4096));
                size_t null_bitmap_size = null_bitmap_in->length();
                doris::faststring buf;
                buf.resize(null_bitmap_size);
                null_bitmap_in->readBytes(reinterpret_cast<uint8_t*>(buf.data()), null_bitmap_size);
                *null_bitmap = roaring::Roaring::read(reinterpret_cast<char*>(buf.data()), false);
                assert(expected_null_bitmap.size() == null_bitmap->cardinality());
                for (int i : expected_null_bitmap) {
                    EXPECT_TRUE(null_bitmap->contains(i));
                }
            }
            index_input->close();
            _CLLDELETE(index_input);
        } catch (const CLuceneError& e) {
            EXPECT_TRUE(false) << "CLuceneError: " << e.what();
        }
    }

    void test_array_all_null(std::string_view rowset_id, int seg_id, Field* field) {
        EXPECT_TRUE(field->type() == FieldType::OLAP_FIELD_TYPE_ARRAY);
        std::string index_path_prefix {InvertedIndexDescriptor::get_index_file_path_prefix(
                local_segment_path(kTestDir, rowset_id, seg_id))};
        int index_id = 26034;
        std::string index_path =
                InvertedIndexDescriptor::get_index_file_path_v1(index_path_prefix, index_id, "");
        auto fs = io::global_local_filesystem();

        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(index_id);
        index_meta_pb->set_index_name("index_inverted_arr_all_null");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(0);

        TabletIndex idx_meta;
        idx_meta.init_from_pb(*index_meta_pb.get());
        auto index_file_writer =
                std::make_unique<IndexFileWriter>(fs, index_path_prefix, std::string {rowset_id},
                                                  seg_id, InvertedIndexStorageFormatPB::V1);
        std::unique_ptr<segment_v2::InvertedIndexColumnWriter> _inverted_index_builder = nullptr;
        EXPECT_EQ(InvertedIndexColumnWriter::create(field, &_inverted_index_builder,
                                                    index_file_writer.get(), &idx_meta),
                  Status::OK());

        // Construct inner array type: DataTypeArray(DataTypeNullable(DataTypeString))
        vectorized::DataTypePtr inner_string_type = std::make_shared<vectorized::DataTypeNullable>(
                std::make_shared<vectorized::DataTypeString>());
        vectorized::DataTypePtr array_type =
                std::make_shared<vectorized::DataTypeArray>(inner_string_type);
        // To support outer array null values, wrap it in a Nullable type
        vectorized::DataTypePtr final_type =
                std::make_shared<vectorized::DataTypeNullable>(array_type);

        vectorized::MutableColumnPtr col = final_type->create_column();
        col->insert(vectorized::Field());
        col->insert(vectorized::Field());

        vectorized::ColumnPtr column_array = std::move(col);
        vectorized::ColumnWithTypeAndName type_and_name(column_array, final_type, "arr1");

        vectorized::Block block;
        block.insert(type_and_name);

        TabletSchemaSPtr tablet_schema = create_schema_with_array();
        vectorized::OlapBlockDataConvertor convertor(tablet_schema.get(), {0});
        convertor.set_source_content(&block, 0, block.rows());

        auto [st, accessor] = convertor.convert_column_data(0);
        EXPECT_EQ(st, Status::OK());
        const auto* data_ptr = reinterpret_cast<const uint64_t*>(accessor->get_data());
        const auto* offsets_ptr = reinterpret_cast<const uint8_t*>(data_ptr[1]);
        const void* item_data = reinterpret_cast<const void*>(data_ptr[2]);
        const auto* item_nullmap = reinterpret_cast<const uint8_t*>(data_ptr[3]);
        const auto* null_map = accessor->get_nullmap();

        auto field_size = field->get_sub_field(0)->size();
        st = _inverted_index_builder->add_array_values(field_size, item_data, item_nullmap,
                                                       offsets_ptr, block.rows());
        EXPECT_EQ(st, Status::OK());
        st = _inverted_index_builder->add_array_nulls(null_map, block.rows());
        EXPECT_EQ(st, Status::OK());

        EXPECT_EQ(_inverted_index_builder->finish(), Status::OK());
        EXPECT_EQ(index_file_writer->close(), Status::OK());

        std::vector<int> expected_null_bitmap = {0, 1};
        ExpectedDocMap expected {};
        check_terms_stats(index_path_prefix, &expected, expected_null_bitmap,
                          InvertedIndexStorageFormatPB::V1, &idx_meta);
    }

private:
    static void build_slices(vectorized::PaddedPODArray<Slice>& slices,
                             const vectorized::ColumnPtr& column_array, size_t num_strings) {
        const auto* col_arr = assert_cast<const vectorized::ColumnArray*>(column_array.get());
        const vectorized::UInt8* nested_null_map =
                assert_cast<const vectorized::ColumnNullable*>(col_arr->get_data_ptr().get())
                        ->get_null_map_column()
                        .get_data()
                        .data();
        const auto* col_arr_str = assert_cast<const vectorized::ColumnString*>(
                assert_cast<const vectorized::ColumnNullable*>(col_arr->get_data_ptr().get())
                        ->get_nested_column_ptr()
                        .get());
        const char* char_data = (const char*)(col_arr_str->get_chars().data());
        const vectorized::ColumnString::Offset* offset_cur = col_arr_str->get_offsets().data();
        const vectorized::ColumnString::Offset* offset_end = offset_cur + num_strings;
        Slice* slice = slices.data();
        size_t string_offset = *(offset_cur - 1);
        const vectorized::UInt8* nullmap_cur = nested_null_map;
        while (offset_cur != offset_end) {
            if (!*nullmap_cur) {
                slice->data = const_cast<char*>(char_data + string_offset);
                slice->size = *offset_cur - string_offset;
            } else {
                slice->data = nullptr;
                slice->size = 0;
            }
            string_offset = *offset_cur;
            ++nullmap_cur;
            ++slice;
            ++offset_cur;
        }
    }
};

TEST_F(InvertedIndexArrayTest, ArrayString) {
    TabletColumn arrayTabletColumn;
    arrayTabletColumn.set_unique_id(0);
    arrayTabletColumn.set_name("arr1");
    arrayTabletColumn.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    TabletColumn arraySubColumn;
    arraySubColumn.set_unique_id(1);
    arraySubColumn.set_name("arr_sub_string");
    arraySubColumn.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    arrayTabletColumn.add_sub_column(arraySubColumn);
    Field* field = FieldFactory::create(arrayTabletColumn);
    test_string("rowset_id", 0, field);
    test_non_null_string("rowset_id_non_null", 0, field);
    delete field;
}

TEST_F(InvertedIndexArrayTest, ComplexNullCases) {
    TabletColumn arrayTabletColumn;
    arrayTabletColumn.set_unique_id(0);
    arrayTabletColumn.set_name("arr1");
    arrayTabletColumn.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    TabletColumn arraySubColumn;
    arraySubColumn.set_unique_id(1);
    arraySubColumn.set_name("arr_sub_string");
    arraySubColumn.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    arrayTabletColumn.add_sub_column(arraySubColumn);
    Field* field = FieldFactory::create(arrayTabletColumn);
    test_null_write("complex_null", 0, field);
    test_null_write_v2("complex_null_v2", 0, field);
    test_array_all_null("complex_null_all_null", 0, field);
    delete field;
}

TEST_F(InvertedIndexArrayTest, MultiBlockWrite) {
    TabletColumn arrayTabletColumn;
    arrayTabletColumn.set_unique_id(0);
    arrayTabletColumn.set_name("arr1");
    arrayTabletColumn.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    TabletColumn arraySubColumn;
    arraySubColumn.set_unique_id(1);
    arraySubColumn.set_name("arr_sub_string");
    arraySubColumn.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    arrayTabletColumn.add_sub_column(arraySubColumn);
    Field* field = FieldFactory::create(arrayTabletColumn);
    test_multi_block_write("multi_block", 0, field);
    delete field;
}

TEST_F(InvertedIndexArrayTest, ArrayInt) {
    TabletColumn arrayTabletColumn;
    arrayTabletColumn.set_unique_id(0);
    arrayTabletColumn.set_name("arr1");
    arrayTabletColumn.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    TabletColumn arraySubColumn;
    arraySubColumn.set_unique_id(1);
    arraySubColumn.set_name("arr_sub_int");
    arraySubColumn.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    arrayTabletColumn.add_sub_column(arraySubColumn);
    Field* field = FieldFactory::create(arrayTabletColumn);
    test_array_numeric("int_test", 0, field);
    delete field;
}
} // namespace doris::segment_v2
