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
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <string.h>

#include <memory>
#include <string>

#include "gtest/gtest_pred_impl.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/rowset/segment_v2/inverted_index_writer.h"
#include "olap/rowset/segment_v2/zone_map_index.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_helper.h"
#include "runtime/exec_env.h"
#include "util/slice.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/olap/olap_data_convertor.h"

using namespace lucene::index;
using doris::segment_v2::InvertedIndexFileWriter;

namespace doris {
namespace segment_v2 {

class InvertedIndexArrayTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/inverted_index_array_test";

    void check_terms_stats(string file_str) {
        std::unique_ptr<DorisCompoundReader> reader;
        try {
            CLuceneError err;
            CL_NS(store)::IndexInput* index_input = nullptr;
            auto ok = DorisFSDirectory::FSIndexInput::open(
                    io::global_local_filesystem(), file_str.c_str(), index_input, err, 4096);
            if (!ok) {
                throw err;
            }
            reader = std::make_unique<DorisCompoundReader>(index_input, 4096);
        } catch (...) {
            EXPECT_TRUE(false);
        }

        std::cout << "Term statistics for " << file_str << std::endl;
        std::cout << "==================================" << std::endl;
        lucene::store::Directory* dir = reader.get();

        IndexReader* r = IndexReader::open(dir);

        printf("Max Docs: %d\n", r->maxDoc());
        printf("Num Docs: %d\n", r->numDocs());

        TermEnum* te = r->terms();
        int32_t nterms;
        for (nterms = 0; te->next(); nterms++) {
            /* empty */
            std::string token =
                    lucene_wcstoutf8string(te->term(false)->text(), te->term(false)->textLength());

            printf("Term: %s ", token.c_str());
            printf("Freq: %d\n", te->docFreq());
        }
        printf("Term count: %d\n\n", nterms);
        te->close();
        _CLLDELETE(te);

        r->close();
        _CLLDELETE(r);
        reader->close();
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
        if (!st.OK()) {
            std::cout << "init tmp file dirs error:" << st.to_string() << std::endl;
            return;
        }
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
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
        auto index_file_writer = std::make_unique<InvertedIndexFileWriter>(
                fs, index_path_prefix, std::string {rowset_id}, seg_id,
                InvertedIndexStorageFormatPB::V1);
        std::unique_ptr<segment_v2::InvertedIndexColumnWriter> _inverted_index_builder = nullptr;
        EXPECT_EQ(InvertedIndexColumnWriter::create(field, &_inverted_index_builder,
                                                    index_file_writer.get(), &idx_meta),
                  Status::OK());
        vectorized::PaddedPODArray<Slice> _slice;
        _slice.resize(5);

        vectorized::Array a1, a2;
        a1.push_back("amory");
        a1.push_back("doris");
        a2.push_back(vectorized::Null());
        a2.push_back("amory");
        a2.push_back("commiter");

        vectorized::DataTypePtr s1 = std::make_shared<vectorized::DataTypeNullable>(
                std::make_shared<vectorized::DataTypeString>());
        vectorized::DataTypePtr au = std::make_shared<vectorized::DataTypeArray>(s1);
        vectorized::MutableColumnPtr col = au->create_column();
        col->insert(a1);
        col->insert(a2);
        vectorized::ColumnPtr column_array = std::move(col);
        vectorized::ColumnWithTypeAndName type_and_name(column_array, au, "arr1");

        vectorized::PaddedPODArray<vectorized::UInt64> _offsets;
        _offsets.reserve(3);
        _offsets.emplace_back(0);
        _offsets.emplace_back(2);
        _offsets.emplace_back(5);
        const uint8_t* offsets_ptr = (const uint8_t*)(_offsets.data());

        auto* col_arr = assert_cast<const vectorized::ColumnArray*>(column_array.get());
        const vectorized::UInt8* nested_null_map =
                assert_cast<const vectorized::ColumnNullable*>(col_arr->get_data_ptr().get())
                        ->get_null_map_data()
                        .data();
        auto* col_arr_str = assert_cast<const vectorized::ColumnString*>(
                assert_cast<const vectorized::ColumnNullable*>(col_arr->get_data_ptr().get())
                        ->get_nested_column_ptr()
                        .get());
        const char* char_data = (const char*)(col_arr_str->get_chars().data());
        const vectorized::ColumnString::Offset* offset_cur = col_arr_str->get_offsets().data();
        const vectorized::ColumnString::Offset* offset_end = offset_cur + 5;

        Slice* slice = _slice.data();
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

        auto field_size = field->get_sub_field(0)->size();
        Status st = _inverted_index_builder->add_array_values(
                field_size, reinterpret_cast<const void*>(_slice.data()),
                reinterpret_cast<const uint8_t*>(nested_null_map), offsets_ptr, 2);
        EXPECT_EQ(st, Status::OK());
        EXPECT_EQ(_inverted_index_builder->finish(), Status::OK());
        EXPECT_EQ(index_file_writer->close(), Status::OK());

        check_terms_stats(index_path);
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
    delete field;
}

} // namespace segment_v2
} // namespace doris
