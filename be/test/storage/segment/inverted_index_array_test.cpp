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

#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_number.h"
#include "core/field.h"
#include "core/pod_array_fwd.h"
#include "core/types.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/inverted_index_compound_reader.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_fs_directory.h"
#include "storage/index/inverted/inverted_index_writer.h"
#include "storage/index/zone_map/zone_map_index.h"
#include "storage/segment/array_index_input_helper.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/tablet/tablet_schema_helper.h"
#include "storage/types.h"
#include "util/faststring.h"
#include "util/slice.h"

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

    void test_non_null_string(std::string_view rowset_id, int seg_id, const TabletColumn* field) {
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
        std::unique_ptr<segment_v2::IndexColumnWriter> _inverted_index_builder = nullptr;
        EXPECT_EQ(IndexColumnWriter::create(field, &_inverted_index_builder,
                                            index_file_writer.get(), &idx_meta),
                  Status::OK());

        // Construct two arrays: The first row is ["amory","doris"], and the second row is ["amory", "commiter"]
        Array a1, a2;
        a1.push_back(Field::create_field<TYPE_STRING>("amory"));
        a1.push_back(Field::create_field<TYPE_STRING>("doris"));
        a2.push_back(Field::create_field<TYPE_STRING>("amory"));
        a2.push_back(Field::create_field<TYPE_STRING>("commiter"));

        // Construct array type: DataTypeArray(DataTypeString)
        DataTypePtr s1 = std::make_shared<DataTypeString>();
        DataTypePtr array_type = std::make_shared<DataTypeArray>(s1);
        MutableColumnPtr col = array_type->create_column();
        col->insert(Field::create_field<TYPE_ARRAY>(a1));
        col->insert(Field::create_field<TYPE_ARRAY>(a2));
        ColumnPtr column_array = std::move(col);
        ColumnWithTypeAndName type_and_name(column_array, array_type, "arr1");

        // Put the array column into the Block (assuming only this column)
        Block block;
        block.insert(type_and_name);
        // block.rows() should be 2

        auto st = feed_array_rows(*_inverted_index_builder, *block.get_by_position(0).column,
                                  block.rows());
        EXPECT_EQ(st, Status::OK());

        EXPECT_EQ(_inverted_index_builder->finish(), Status::OK());
        EXPECT_EQ(index_file_writer->begin_close(), Status::OK());
        EXPECT_EQ(index_file_writer->finish_close(), Status::OK());

        ExpectedDocMap expected = {{"amory", {0, 1}}, {"doris", {0}}, {"commiter", {1}}};
        check_terms_stats(index_path_prefix, &expected, {}, InvertedIndexStorageFormatPB::V1,
                          &idx_meta);
    }

    // ArrayColumnWriter hands add_array the block's whole item column and the
    // index of the batch's first element; the elements before it belong to
    // earlier batches.
    void test_string_batch_from_first_item(std::string_view rowset_id, int seg_id,
                                           const TabletColumn* field) {
        EXPECT_TRUE(field->type() == FieldType::OLAP_FIELD_TYPE_ARRAY);
        std::string index_path_prefix {InvertedIndexDescriptor::get_index_file_path_prefix(
                local_segment_path(kTestDir, rowset_id, seg_id))};
        int index_id = 26033;
        auto fs = io::global_local_filesystem();

        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(index_id);
        index_meta_pb->set_index_name("index_inverted_arr1");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(0);

        TabletIndex idx_meta;
        idx_meta.init_from_pb(*index_meta_pb.get());
        auto index_file_writer =
                std::make_unique<IndexFileWriter>(fs, index_path_prefix, std::string {rowset_id},
                                                  seg_id, InvertedIndexStorageFormatPB::V1);
        std::unique_ptr<segment_v2::IndexColumnWriter> _inverted_index_builder = nullptr;
        EXPECT_EQ(IndexColumnWriter::create(field, &_inverted_index_builder,
                                            index_file_writer.get(), &idx_meta),
                  Status::OK());

        // Rows 0 and 1 belong to an earlier batch; the batch is rows [2, 5).
        DataTypePtr array_type =
                std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
        MutableColumnPtr col = array_type->create_column();
        for (const auto& row : std::vector<std::vector<std::string>> {{"skip0", "skip1"},
                                                                      {"skip2"},
                                                                      {"amory", "doris"},
                                                                      {},
                                                                      {"amory", "commiter"}}) {
            Array arr;
            for (const auto& value : row) {
                arr.push_back(Field::create_field<TYPE_STRING>(value));
            }
            col->insert(Field::create_field<TYPE_ARRAY>(arr));
        }
        const auto& col_array = assert_cast<const ColumnArray&>(*col);
        auto offsets = ColumnOffset64::create();
        rebase_offsets(col_array.get_offsets(), 2, 3, 0, offsets.get());
        auto st = feed_array_index(_inverted_index_builder.get(), col_array.get_data(),
                                   col_array.get_offsets()[1], *offsets, nullptr);
        EXPECT_EQ(st, Status::OK());

        EXPECT_EQ(_inverted_index_builder->finish(), Status::OK());
        EXPECT_EQ(index_file_writer->begin_close(), Status::OK());
        EXPECT_EQ(index_file_writer->finish_close(), Status::OK());

        ExpectedDocMap expected = {{"amory", {0, 2}}, {"doris", {0}}, {"commiter", {2}}};
        check_terms_stats(index_path_prefix, &expected, {}, InvertedIndexStorageFormatPB::V1,
                          &idx_meta);
    }

    void test_string(std::string_view rowset_id, int seg_id, const TabletColumn* field) {
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
        std::unique_ptr<segment_v2::IndexColumnWriter> _inverted_index_builder = nullptr;
        EXPECT_EQ(IndexColumnWriter::create(field, &_inverted_index_builder,
                                            index_file_writer.get(), &idx_meta),
                  Status::OK());

        // Construct two arrays: The first row is ["amory","doris"], and the second row is [NULL, "amory", "commiter"]
        Array a1, a2;
        a1.push_back(Field::create_field<TYPE_STRING>("amory"));
        a1.push_back(Field::create_field<TYPE_STRING>("doris"));
        a2.push_back(Field());
        a2.push_back(Field::create_field<TYPE_STRING>("amory"));
        a2.push_back(Field::create_field<TYPE_STRING>("commiter"));

        // Construct array type: DataTypeArray(DataTypeNullable(DataTypeString))
        DataTypePtr s1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        DataTypePtr array_type = std::make_shared<DataTypeArray>(s1);
        MutableColumnPtr col = array_type->create_column();
        col->insert(Field::create_field<TYPE_ARRAY>(a1));
        col->insert(Field::create_field<TYPE_ARRAY>(a2));
        ColumnPtr column_array = std::move(col);
        ColumnWithTypeAndName type_and_name(column_array, array_type, "arr1");

        // Put the array column into the Block (assuming only this column)
        Block block;
        block.insert(type_and_name);
        // block.rows() should be 2

        auto st = feed_array_rows(*_inverted_index_builder, *block.get_by_position(0).column,
                                  block.rows());
        EXPECT_EQ(st, Status::OK());
        EXPECT_EQ(_inverted_index_builder->finish(), Status::OK());
        EXPECT_EQ(index_file_writer->begin_close(), Status::OK());
        EXPECT_EQ(index_file_writer->finish_close(), Status::OK());

        ExpectedDocMap expected = {{"amory", {0, 1}}, {"doris", {0}}, {"commiter", {1}}};
        check_terms_stats(index_path_prefix, &expected, {}, InvertedIndexStorageFormatPB::V1,
                          &idx_meta);
    }

    void test_null_write_v2(std::string_view rowset_id, int seg_id, const TabletColumn* field) {
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
        std::unique_ptr<segment_v2::IndexColumnWriter> _inverted_index_builder = nullptr;
        EXPECT_EQ(IndexColumnWriter::create(field, &_inverted_index_builder,
                                            index_file_writer.get(), &idx_meta),
                  Status::OK());

        // Simulate outer null cases: 5 rows, outer null map = {1, 0, 0, 1, 0}, i.e., rows 0 and 3 are null
        std::vector<uint8_t> outer_null_map = {1, 0, 0, 1, 0};

        // Construct inner array type: DataTypeArray(DataTypeNullable(DataTypeString))
        DataTypePtr inner_string_type =
                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        DataTypePtr array_type = std::make_shared<DataTypeArray>(inner_string_type);
        // To support outer array null values, wrap it in a Nullable type
        DataTypePtr final_type = std::make_shared<DataTypeNullable>(array_type);

        // Construct 5 rows of data:
        // Row 0: null
        // Row 1: a2 = [Null, "test"]
        // Row 2: a3 = ["mixed", Null, "data"]
        // Row 3: null
        // Row 4: a5 = ["non-null"]
        MutableColumnPtr col = final_type->create_column();
        // Row 0: insert null
        col->insert(Field());
        // Row 1: insert a2
        Array a2;
        a2.push_back(Field());
        a2.push_back(Field::create_field<TYPE_STRING>("test"));
        col->insert(Field::create_field<TYPE_ARRAY>(a2));
        // Row 2: insert a3
        Array a3;
        a3.push_back(Field::create_field<TYPE_STRING>("mixed"));
        a3.push_back(Field());
        a3.push_back(Field::create_field<TYPE_STRING>("data"));
        col->insert(Field::create_field<TYPE_ARRAY>(a3));
        // Row 3: insert null
        col->insert(Field());
        // Row 4: insert a5
        Array a5;
        a5.push_back(Field::create_field<TYPE_STRING>("non-null"));
        col->insert(Field::create_field<TYPE_ARRAY>(a5));

        ColumnPtr column_array = std::move(col);
        ColumnWithTypeAndName type_and_name(column_array, final_type, "arr1");

        // Construct Block, containing only the array column, with 5 rows
        Block block;
        block.insert(type_and_name);

        auto st = feed_array_rows(*_inverted_index_builder, *block.get_by_position(0).column,
                                  block.rows());
        EXPECT_EQ(st, Status::OK());
        EXPECT_EQ(_inverted_index_builder->finish(), Status::OK());
        EXPECT_EQ(index_file_writer->begin_close(), Status::OK());
        EXPECT_EQ(index_file_writer->finish_close(), Status::OK());

        // Expected inverted index result: only index non-null elements
        // Row 1: non-null in a2 is "test"
        // Row 2: non-null in a3 is "mixed" and "data"
        // Row 4: non-null in a5 is "non-null"
        ExpectedDocMap expected = {{"test", {1}}, {"mixed", {2}}, {"data", {2}}, {"non-null", {4}}};
        std::vector<int> expected_null_bitmap = {0, 3};
        check_terms_stats(index_path_prefix, &expected, expected_null_bitmap,
                          InvertedIndexStorageFormatPB::V2, &idx_meta);
    }

    void test_null_write(std::string_view rowset_id, int seg_id, const TabletColumn* field) {
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
        std::unique_ptr<segment_v2::IndexColumnWriter> _inverted_index_builder = nullptr;
        EXPECT_EQ(IndexColumnWriter::create(field, &_inverted_index_builder,
                                            index_file_writer.get(), &idx_meta),
                  Status::OK());

        // Simulate outer null cases: 5 rows, outer null map = {1, 0, 0, 1, 0}, i.e., rows 0 and 3 are null
        std::vector<uint8_t> outer_null_map = {1, 0, 0, 1, 0};

        // Construct inner array type: DataTypeArray(DataTypeNullable(DataTypeString))
        DataTypePtr inner_string_type =
                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        DataTypePtr array_type = std::make_shared<DataTypeArray>(inner_string_type);
        // To support outer array null values, wrap it in a Nullable type
        DataTypePtr final_type = std::make_shared<DataTypeNullable>(array_type);

        // Construct 5 rows of data:
        // Row 0: null
        // Row 1: a2 = [Null, "test"]
        // Row 2: a3 = ["mixed", Null, "data"]
        // Row 3: null
        // Row 4: a5 = ["non-null"]
        MutableColumnPtr col = final_type->create_column();
        // Row 0: insert null
        col->insert(Field());
        // Row 1: insert a2
        Array a2;
        a2.push_back(Field());
        a2.push_back(Field::create_field<TYPE_STRING>("test"));
        col->insert(Field::create_field<TYPE_ARRAY>(a2));
        // Row 2: insert a3
        Array a3;
        a3.push_back(Field::create_field<TYPE_STRING>("mixed"));
        a3.push_back(Field());
        a3.push_back(Field::create_field<TYPE_STRING>("data"));
        col->insert(Field::create_field<TYPE_ARRAY>(a3));
        // Row 3: insert null
        col->insert(Field());
        // Row 4: insert a5
        Array a5;
        a5.push_back(Field::create_field<TYPE_STRING>("non-null"));
        col->insert(Field::create_field<TYPE_ARRAY>(a5));

        ColumnPtr column_array = std::move(col);
        ColumnWithTypeAndName type_and_name(column_array, final_type, "arr1");

        // Construct Block, containing only the array column, with 5 rows
        Block block;
        block.insert(type_and_name);

        auto st = feed_array_rows(*_inverted_index_builder, *block.get_by_position(0).column,
                                  block.rows());
        EXPECT_EQ(st, Status::OK());
        EXPECT_EQ(_inverted_index_builder->finish(), Status::OK());
        EXPECT_EQ(index_file_writer->begin_close(), Status::OK());
        EXPECT_EQ(index_file_writer->finish_close(), Status::OK());

        // Expected inverted index result: only index non-null elements
        // Row 1: non-null in a2 is "test"
        // Row 2: non-null in a3 is "mixed" and "data"
        // Row 4: non-null in a5 is "non-null"
        ExpectedDocMap expected = {{"test", {1}}, {"mixed", {2}}, {"data", {2}}, {"non-null", {4}}};
        std::vector<int> expected_null_bitmap = {0, 3};
        check_terms_stats(index_path_prefix, &expected, expected_null_bitmap,
                          InvertedIndexStorageFormatPB::V1, &idx_meta);
    }

    void test_multi_block_write(std::string_view rowset_id, int seg_id, const TabletColumn* field) {
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
        std::unique_ptr<segment_v2::IndexColumnWriter> _inverted_index_builder = nullptr;
        EXPECT_EQ(IndexColumnWriter::create(field, &_inverted_index_builder,
                                            index_file_writer.get(), &idx_meta),
                  Status::OK());

        ExpectedDocMap merged_expected;

        // --- Block 1 ---
        {
            const int row_num = 4;
            // construct data type: Nullable( Array( Nullable(String) ) )
            DataTypePtr inner_string =
                    std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
            DataTypePtr array_type = std::make_shared<DataTypeArray>(inner_string);
            DataTypePtr final_type = std::make_shared<DataTypeNullable>(array_type);

            // construct MutableColumn
            MutableColumnPtr col = final_type->create_column();
            // simulate outer null: row0 and row3 are null, the rest are non-null
            col->insert(Field()); // row0: null
            {
                // row1: non-null, array with 1 element: "block1_data1"
                Array arr;
                arr.push_back(Field::create_field<TYPE_STRING>("block1_data1"));
                col->insert(Field::create_field<TYPE_ARRAY>(arr));
            }
            {
                // row2: non-null, array with 1 element: "block1_data2"
                Array arr;
                arr.push_back(Field::create_field<TYPE_STRING>("block1_data2"));
                col->insert(Field::create_field<TYPE_ARRAY>(arr));
            }
            col->insert(Field()); // row3: null

            ColumnPtr column_array = std::move(col);
            ColumnWithTypeAndName type_and_name(column_array, final_type, "arr1");

            // construct Block (containing only the arr1 column)
            Block block;
            block.insert(type_and_name);

            auto st = feed_array_rows(*_inverted_index_builder, *block.get_by_position(0).column,
                                      row_num);
            EXPECT_EQ(st, Status::OK());

            // for Block1, the expected non-null behavior is row1 and row2
            ExpectedDocMap expected = {{"block1_data1", {1}}, {"block1_data2", {2}}};
            merged_expected.insert(expected.begin(), expected.end());
        }

        // --- Block 2 ---
        {
            const int row_num = 2;
            DataTypePtr inner_string =
                    std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
            DataTypePtr array_type = std::make_shared<DataTypeArray>(inner_string);
            DataTypePtr final_type = std::make_shared<DataTypeNullable>(array_type);

            MutableColumnPtr col = final_type->create_column();
            // row0: non-null, array with 1 element: "block2_data1"
            {
                Array arr;
                arr.push_back(Field::create_field<TYPE_STRING>("block2_data1"));
                col->insert(Field::create_field<TYPE_ARRAY>(arr));
            }
            // row1: null
            col->insert(Field());

            ColumnPtr column_array = std::move(col);
            ColumnWithTypeAndName type_and_name(column_array, final_type, "arr1");

            Block block;
            block.insert(type_and_name);

            auto st = feed_array_rows(*_inverted_index_builder, *block.get_by_position(0).column,
                                      row_num);
            EXPECT_EQ(st, Status::OK());

            ExpectedDocMap expected = {{"block2_data1", {4}}};
            merged_expected.insert(expected.begin(), expected.end());
        }

        // --- Block 3 ---
        {
            const int row_num = 2;
            DataTypePtr inner_string =
                    std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
            DataTypePtr array_type = std::make_shared<DataTypeArray>(inner_string);
            DataTypePtr final_type = std::make_shared<DataTypeNullable>(array_type);

            MutableColumnPtr col = final_type->create_column();
            // row0: non-null, array with 1 element: "block3_data1"
            {
                Array arr;
                arr.push_back(Field::create_field<TYPE_STRING>("block3_data1"));
                col->insert(Field::create_field<TYPE_ARRAY>(arr));
            }
            // row1: null
            col->insert(Field());

            ColumnPtr column_array = std::move(col);
            ColumnWithTypeAndName type_and_name(column_array, final_type, "arr1");

            Block block;
            block.insert(type_and_name);

            auto st = feed_array_rows(*_inverted_index_builder, *block.get_by_position(0).column,
                                      row_num);
            EXPECT_EQ(st, Status::OK());

            ExpectedDocMap expected = {{"block3_data1", {6}}};
            merged_expected.insert(expected.begin(), expected.end());
        }

        EXPECT_EQ(_inverted_index_builder->finish(), Status::OK());
        EXPECT_EQ(index_file_writer->begin_close(), Status::OK());
        EXPECT_EQ(index_file_writer->finish_close(), Status::OK());

        std::vector<int> expected_null_bitmap = {0, 3, 5, 7};
        check_terms_stats(index_path_prefix, &merged_expected, expected_null_bitmap,
                          InvertedIndexStorageFormatPB::V1, &idx_meta);
    }

    void test_array_numeric(std::string_view rowset_id, int seg_id, const TabletColumn* field) {
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
        std::unique_ptr<segment_v2::IndexColumnWriter> _inverted_index_builder = nullptr;
        EXPECT_EQ(IndexColumnWriter::create(field, &_inverted_index_builder,
                                            index_file_writer.get(), &idx_meta),
                  Status::OK());

        DataTypePtr inner_int = std::make_shared<DataTypeInt32>();
        DataTypePtr array_type = std::make_shared<DataTypeArray>(inner_int);
        DataTypePtr final_type = std::make_shared<DataTypeNullable>(array_type);

        // create a MutableColumnPtr
        MutableColumnPtr col = final_type->create_column();
        // row0: non-null, array [123, 456]
        {
            Array arr;
            arr.push_back(Field::create_field<TYPE_INT>(123));
            arr.push_back(Field::create_field<TYPE_INT>(456));
            col->insert(Field::create_field<TYPE_ARRAY>(arr));
        }
        // row1: null
        col->insert(Field());
        // row2: non-null, array [789, 101112]
        {
            Array arr;
            arr.push_back(Field::create_field<TYPE_INT>(789));
            arr.push_back(Field::create_field<TYPE_INT>(101112));
            col->insert(Field::create_field<TYPE_ARRAY>(arr));
        }
        // wrap the constructed column into a ColumnWithTypeAndName
        ColumnPtr column_array = std::move(col);
        ColumnWithTypeAndName type_and_name(column_array, final_type, "arr_num");

        // construct Block (containing only this column), with 3 rows
        Block block;
        block.insert(type_and_name);

        auto st = feed_array_rows(*_inverted_index_builder, *block.get_by_position(0).column,
                                  block.rows());
        EXPECT_EQ(st, Status::OK());
        EXPECT_EQ(_inverted_index_builder->finish(), Status::OK());
        EXPECT_EQ(index_file_writer->begin_close(), Status::OK());
        EXPECT_EQ(index_file_writer->finish_close(), Status::OK());

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

    void test_array_all_null(std::string_view rowset_id, int seg_id, const TabletColumn* field) {
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
        std::unique_ptr<segment_v2::IndexColumnWriter> _inverted_index_builder = nullptr;
        EXPECT_EQ(IndexColumnWriter::create(field, &_inverted_index_builder,
                                            index_file_writer.get(), &idx_meta),
                  Status::OK());

        // Construct inner array type: DataTypeArray(DataTypeNullable(DataTypeString))
        DataTypePtr inner_string_type =
                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        DataTypePtr array_type = std::make_shared<DataTypeArray>(inner_string_type);
        // To support outer array null values, wrap it in a Nullable type
        DataTypePtr final_type = std::make_shared<DataTypeNullable>(array_type);

        MutableColumnPtr col = final_type->create_column();
        col->insert(Field());
        col->insert(Field());

        ColumnPtr column_array = std::move(col);
        ColumnWithTypeAndName type_and_name(column_array, final_type, "arr1");

        Block block;
        block.insert(type_and_name);

        auto st = feed_array_rows(*_inverted_index_builder, *block.get_by_position(0).column,
                                  block.rows());
        EXPECT_EQ(st, Status::OK());

        EXPECT_EQ(_inverted_index_builder->finish(), Status::OK());
        EXPECT_EQ(index_file_writer->begin_close(), Status::OK());
        EXPECT_EQ(index_file_writer->finish_close(), Status::OK());

        std::vector<int> expected_null_bitmap = {0, 1};
        ExpectedDocMap expected {};
        check_terms_stats(index_path_prefix, &expected, expected_null_bitmap,
                          InvertedIndexStorageFormatPB::V1, &idx_meta);
    }

private:
    static void build_slices(PaddedPODArray<Slice>& slices, const ColumnPtr& column_array,
                             size_t num_strings) {
        const auto* col_arr = assert_cast<const ColumnArray*>(column_array.get());
        const UInt8* nested_null_map =
                assert_cast<const ColumnNullable*>(col_arr->get_data_ptr().get())
                        ->get_null_map_column()
                        .get_data()
                        .data();
        const auto* col_arr_str = assert_cast<const ColumnString*>(
                assert_cast<const ColumnNullable*>(col_arr->get_data_ptr().get())
                        ->get_nested_column_ptr()
                        .get());
        const char* char_data = (const char*)(col_arr_str->get_chars().data());
        const ColumnString::Offset* offset_cur = col_arr_str->get_offsets().data();
        const ColumnString::Offset* offset_end = offset_cur + num_strings;
        Slice* slice = slices.data();
        size_t string_offset = *(offset_cur - 1);
        const UInt8* nullmap_cur = nested_null_map;
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
    const TabletColumn* field = &(arrayTabletColumn);
    test_string("rowset_id", 0, field);
    test_non_null_string("rowset_id_non_null", 0, field);
    test_string_batch_from_first_item("rowset_id_first_item", 0, field);
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
    const TabletColumn* field = &(arrayTabletColumn);
    test_null_write("complex_null", 0, field);
    test_null_write_v2("complex_null_v2", 0, field);
    test_array_all_null("complex_null_all_null", 0, field);
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
    const TabletColumn* field = &(arrayTabletColumn);
    test_multi_block_write("multi_block", 0, field);
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
    const TabletColumn* field = &(arrayTabletColumn);
    test_array_numeric("int_test", 0, field);
}
} // namespace doris::segment_v2
