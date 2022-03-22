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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "exprs/anyval_util.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/segment_v2.pb.h"
#include "olap/field.h"
#include "olap/fs/block_manager.h"
#include "olap/fs/fs_util.h"
#include "olap/row_block2.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/column_writer.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"
#include "test_util/array_utils.h"
#include "testutil/desc_tbl_builder.h"
#include "util/file_utils.h"
#include "util/uid_util.h"
#include "vec/core/block.h"

namespace doris {

template <typename... Ts>
ColumnPB create_column_pb(const std::string& type, const Ts&... sub_column_types) {
    ColumnPB column;
    column.set_type(type);
    column.set_aggregation("NONE");
    column.set_is_nullable(true);
    if (type == "ARRAY") {
        column.set_length(OLAP_ARRAY_MAX_BYTES);
    }
    if constexpr (sizeof...(sub_column_types) > 0) {
        auto sub_column = create_column_pb(sub_column_types...);
        column.add_children_columns()->Swap(&sub_column);
    }
    return column;
}

std::shared_ptr<const TypeInfo> get_type_info(const ColumnPB& column_pb) {
    TabletColumn tablet_column;
    tablet_column.init_from_pb(column_pb);
    return get_type_info(&tablet_column);
}

std::unique_ptr<Field> create_field(const ColumnPB& column_pb) {
    TabletColumn column;
    column.init_from_pb(column_pb);
    return std::unique_ptr<Field>(FieldFactory::create(column));
}

TypeDescriptor get_scalar_type_desc(const TypeInfo* type_info) {
    switch (type_info->type()) {
    case OLAP_FIELD_TYPE_INT:
        return TypeDescriptor(TYPE_INT);
    case OLAP_FIELD_TYPE_VARCHAR:
        return TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    default:
        return TypeDescriptor();
    }
}

TupleDescriptor* get_tuple_descriptor(ObjectPool& object_pool, const TypeInfo* type_info) {
    DescriptorTblBuilder builder(&object_pool);
    auto& tuple_desc_builder = builder.declare_tuple();
    if (type_info->type() == OLAP_FIELD_TYPE_ARRAY) {
        TypeDescriptor type_desc(TYPE_ARRAY);
        type_desc.len = OLAP_ARRAY_MAX_BYTES;
        auto ptype = dynamic_cast<const ArrayTypeInfo*>(type_info)->item_type_info().get();
        while (ptype->type() == OLAP_FIELD_TYPE_ARRAY) {
            type_desc.children.push_back(TypeDescriptor(TYPE_ARRAY));
            ptype = dynamic_cast<const ArrayTypeInfo*>(ptype)->item_type_info().get();
        }
        type_desc.children.push_back(get_scalar_type_desc(ptype));
        tuple_desc_builder << type_desc;
    } else {
        tuple_desc_builder << get_scalar_type_desc(type_info);
    }
    return builder.build()->get_tuple_descriptor(0);
}

CollectionValue* parse(MemPool& mem_pool, FunctionContext& context, const std::string& text,
                       const ColumnPB& column_pb) {
    auto collection_value =
            reinterpret_cast<CollectionValue*>(mem_pool.allocate(sizeof(CollectionValue)));
    auto status = ArrayUtils::create_collection_value(collection_value, &context, text);
    if (!status.ok()) {
        return nullptr;
    }
    return collection_value;
}

void validate(const Field* field, const CollectionValue* expect, const CollectionValue* actual) {
    EXPECT_TRUE(field->type_info()->equal(expect, actual));
}

class ArrayTest : public ::testing::Test {
public:
    ArrayTest()
            : _mem_tracker(new MemTracker(MAX_MEMORY_BYTES, "ArrayTest")),
              _mem_pool(new MemPool(_mem_tracker.get())) {}

protected:
    void SetUp() override {
        if (FileUtils::check_exist(TEST_DIR)) {
            ASSERT_TRUE(FileUtils::remove_all(TEST_DIR).ok());
        }
        ASSERT_TRUE(FileUtils::create_dir(TEST_DIR).ok());
    }

    void TearDown() override {
        if (FileUtils::check_exist(TEST_DIR)) {
            ASSERT_TRUE(FileUtils::remove_all(TEST_DIR).ok());
        }
    }

private:
    void test_copy_array(const TupleDescriptor* tuple_desc, const Field* field,
                         const CollectionValue* array) {
        auto slot_desc = tuple_desc->slots().front();
        auto type_desc = slot_desc->type();
        auto total_size = tuple_desc->byte_size() + array->get_byte_size(type_desc);

        auto src = allocate_tuple(total_size);
        ASSERT_NE(src, nullptr);

        RawValue::write(array, src, slot_desc, _mem_pool.get());
        auto src_cv = reinterpret_cast<CollectionValue*>(src->get_slot(slot_desc->tuple_offset()));
        validate(field, array, src_cv);

        auto dst = allocate_tuple(total_size);
        ASSERT_NE(dst, nullptr);

        src->deep_copy(dst, *tuple_desc, _mem_pool.get());
        auto dst_cv = reinterpret_cast<CollectionValue*>(dst->get_slot(slot_desc->tuple_offset()));
        validate(field, src_cv, dst_cv);

        dst->init(total_size);
        int64_t offset = 0;
        char* serialized_data = reinterpret_cast<char*>(dst);
        src->deep_copy(*tuple_desc, &serialized_data, &offset, true);
        EXPECT_EQ(total_size, offset);
        EXPECT_EQ(total_size, serialized_data - reinterpret_cast<char*>(dst));
        dst_cv = reinterpret_cast<CollectionValue*>(dst->get_slot(slot_desc->tuple_offset()));
        CollectionValue::deserialize_collection(dst_cv, reinterpret_cast<char*>(dst), type_desc);
        validate(field, src_cv, dst_cv);
    }

    Tuple* allocate_tuple(size_t size) {
        auto tuple = reinterpret_cast<Tuple*>(_mem_pool->allocate(size));
        if (tuple) {
            tuple->init(size);
        }
        return tuple;
    }

    void test_direct_copy_array(const Field* field,
                                const std::vector<const CollectionValue*>& arrays) {
        CollectionValue cell;
        std::unique_ptr<char[]> variable_ptr(new char[field->length()]);
        field->allocate_memory(reinterpret_cast<char*>(&cell), variable_ptr.get());
        EXPECT_EQ(cell.null_signs(), reinterpret_cast<bool*>(variable_ptr.get()));
        for (auto array : arrays) {
            field->type_info()->direct_copy(&cell, array);
            EXPECT_EQ(cell.null_signs(), reinterpret_cast<bool*>(variable_ptr.get()));
            validate(field, array, &cell);
        }
    }

    template <segment_v2::EncodingTypePB array_encoding, segment_v2::EncodingTypePB item_encoding>
    void test_write_and_read_column(const ColumnPB& column_pb, const Field* field,
                                    const std::vector<const CollectionValue*>& arrays) {
        const std::string path = TEST_DIR + "/" + generate_uuid_string();
        LOG(INFO) << "Test directory: " << path;

        segment_v2::ColumnMetaPB meta;
        init_column_meta<array_encoding, item_encoding>(&meta, column_pb);

        TabletColumn tablet_column;
        tablet_column.init_from_pb(column_pb);
        Schema schema({tablet_column}, 0);
        {
            auto wblock = create_writable_block(path);
            ASSERT_NE(wblock, nullptr);
            auto writer = create_column_writer<array_encoding, item_encoding>(wblock.get(), meta,
                                                                              column_pb);
            ASSERT_NE(writer, nullptr);
            Status st;
            for (auto array : arrays) {
                st = writer->append(false, const_cast<CollectionValue*>(array));
                ASSERT_TRUE(st.ok());
            }
            ASSERT_TRUE(writer->finish().ok());
            ASSERT_TRUE(writer->write_data().ok());
            ASSERT_TRUE(writer->write_ordinal_index().ok());
            ASSERT_TRUE(writer->write_zone_map().ok());

            ASSERT_TRUE(wblock->close().ok());
        }
        {
            auto reader = create_column_reader(path, meta, arrays.size());
            ASSERT_NE(reader, nullptr);
            auto rblock = create_readable_block(path);
            ASSERT_NE(rblock, nullptr);
            OlapReaderStatistics stats;
            std::unique_ptr<segment_v2::ColumnIterator> iter(
                    new_iterator(rblock.get(), &stats, reader.get()));
            ASSERT_NE(iter, nullptr);
            auto st = iter->seek_to_first();
            ASSERT_TRUE(st.ok()) << st.to_string();

            RowBlockV2 block(schema, 1024);
            auto col = block.column_block(0);
            int index = 0;
            size_t rows_read = 1024;
            do {
                ColumnBlockView dst(&col);
                st = iter->next_batch(&rows_read, &dst);
                ASSERT_TRUE(st.ok());
                for (int i = 0; i < rows_read; ++i) {
                    validate(field, arrays[index++],
                             reinterpret_cast<const CollectionValue*>(col.cell_ptr(i)));
                }
                ASSERT_TRUE(st.ok());
            } while (rows_read >= 1024);

            auto tuple_desc = get_tuple_descriptor(_object_pool, get_type_info(column_pb).get());
            block.set_selected_size(rows_read);
            test_convert_to_vec_block(block, tuple_desc, field, arrays);
        }
    }

    template <segment_v2::EncodingTypePB array_encoding, segment_v2::EncodingTypePB item_encoding>
    void init_column_meta(segment_v2::ColumnMetaPB* meta, const ColumnPB& column_pb) {
        int column_id = 0;
        TabletColumn column;
        column.init_from_pb(column_pb);
        init_column_meta<array_encoding, item_encoding>(meta, &column_id, column);
    }

    template <segment_v2::EncodingTypePB array_encoding, segment_v2::EncodingTypePB item_encoding>
    void init_column_meta(segment_v2::ColumnMetaPB* meta, int* column_id,
                          const TabletColumn& column) {
        meta->set_column_id(*column_id);
        meta->set_unique_id((*column_id)++);
        meta->set_type(column.type());
        meta->set_length(column.length());
        if (column.type() == OLAP_FIELD_TYPE_ARRAY) {
            meta->set_encoding(array_encoding);
        } else {
            meta->set_encoding(item_encoding);
        }
        meta->set_compression(segment_v2::LZ4F);
        meta->set_is_nullable(true);
        for (uint32_t i = 0; i < column.get_subtype_count(); ++i) {
            init_column_meta<array_encoding, item_encoding>(meta->add_children_columns(), column_id,
                                                            column.get_sub_column(i));
        }
    }

    std::unique_ptr<fs::WritableBlock> create_writable_block(const std::string& path) {
        std::unique_ptr<fs::WritableBlock> wblock;
        fs::CreateBlockOptions fs_opts(path);
        auto st = fs::fs_util::block_manager(TStorageMedium::HDD)->create_block(fs_opts, &wblock);
        return st.ok() ? std::move(wblock) : nullptr;
    }

    template <segment_v2::EncodingTypePB array_encoding, segment_v2::EncodingTypePB item_encoding>
    std::unique_ptr<segment_v2::ColumnWriter> create_column_writer(fs::WritableBlock* wblock,
                                                                   segment_v2::ColumnMetaPB& meta,
                                                                   const ColumnPB& column_pb) {
        segment_v2::ColumnWriterOptions writer_opts = {.meta = &meta};
        TabletColumn column;
        column.init_from_pb(column_pb);
        std::unique_ptr<segment_v2::ColumnWriter> writer;
        auto st = segment_v2::ColumnWriter::create(writer_opts, &column, wblock, &writer);
        if (!st.ok()) {
            return nullptr;
        }
        st = writer->init();
        return st.ok() ? std::move(writer) : nullptr;
    }

    std::unique_ptr<segment_v2::ColumnReader> create_column_reader(
            const std::string& path, const segment_v2::ColumnMetaPB& meta, size_t num_rows) {
        segment_v2::ColumnReaderOptions reader_opts;
        FilePathDesc path_desc;
        path_desc.filepath = path;
        std::unique_ptr<segment_v2::ColumnReader> reader;
        auto st = segment_v2::ColumnReader::create(reader_opts, meta, num_rows, path_desc, &reader);
        return st.ok() ? std::move(reader) : nullptr;
    }

    std::unique_ptr<fs::ReadableBlock> create_readable_block(const std::string& path) {
        std::unique_ptr<fs::ReadableBlock> rblock;
        FilePathDesc path_desc;
        path_desc.filepath = path;
        auto block_manager = fs::fs_util::block_manager(TStorageMedium::HDD);
        auto st = block_manager->open_block(path_desc, &rblock);
        return st.ok() ? std::move(rblock) : nullptr;
    }

    segment_v2::ColumnIterator* new_iterator(fs::ReadableBlock* rblock, OlapReaderStatistics* stats,
                                             segment_v2::ColumnReader* reader) {
        segment_v2::ColumnIterator* iter = nullptr;
        auto st = reader->new_iterator(&iter);
        if (!st.ok()) {
            return nullptr;
        }
        segment_v2::ColumnIteratorOptions iter_opts;
        iter_opts.stats = stats;
        iter_opts.rblock = rblock;
        st = iter->init(iter_opts);
        return st.ok() ? iter : nullptr;
    }

    template <segment_v2::EncodingTypePB array_encoding, segment_v2::EncodingTypePB item_encoding>
    void test_array(const ColumnPB& column_pb, const Field* field,
                    const TupleDescriptor* tuple_desc, const CollectionValue* array) {
        ASSERT_NE(array, nullptr);
        test_copy_array(tuple_desc, field, array);
        test_direct_copy_array(field, {array});
        test_write_and_read_column<array_encoding, item_encoding>(column_pb, field, {array});
    }

    void test_convert_to_vec_block(RowBlockV2& row_block, const TupleDescriptor* tuple_desc,
                                   const Field* field,
                                   const std::vector<const CollectionValue*>& arrays) {
        vectorized::Block block;
        for (const auto slot_desc : tuple_desc->slots()) {
            block.insert(vectorized::ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                           slot_desc->get_data_type_ptr(),
                                                           slot_desc->col_name()));
        }

        row_block.convert_to_vec_block(&block);
        for (int i = 0; i < arrays.size(); ++i) {
            auto tuple = block.deep_copy_tuple(*tuple_desc, _mem_pool.get(), i, 0, false);
            auto actual = tuple->get_collection_slot(tuple_desc->slots().front()->tuple_offset());
            validate(field, arrays[i], actual);
        }
    }

private:
    static constexpr size_t MAX_MEMORY_BYTES = 1024 * 1024;
    static const std::string TEST_DIR;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<MemPool> _mem_pool;
    ObjectPool _object_pool;
};

const std::string ArrayTest::TEST_DIR = "./ut_dir/array_test";

TEST_F(ArrayTest, TestSimpleIntArrays) {
    auto column_pb = create_column_pb("ARRAY", "INT");
    auto type_info = get_type_info(column_pb);
    auto field = create_field(column_pb);
    auto tuple_desc = get_tuple_descriptor(_object_pool, type_info.get());
    ASSERT_EQ(tuple_desc->slots().size(), 1);
    FunctionContext context;
    ArrayUtils::prepare_context(context, *_mem_pool, column_pb);

    std::vector<const CollectionValue*> arrays = {
            parse(*_mem_pool, context, "[]", column_pb),
            parse(*_mem_pool, context, "[null]", column_pb),
            parse(*_mem_pool, context, "[1, 2, 3]", column_pb),
            parse(*_mem_pool, context, "[1, null, 3]", column_pb),
            parse(*_mem_pool, context, "[1, null, null]", column_pb),
            parse(*_mem_pool, context, "[null, null, 3]", column_pb),
            parse(*_mem_pool, context, "[null, null, null]", column_pb),
    };
    for (auto array : arrays) {
        test_array<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb, field.get(),
                                                                          tuple_desc, array);
    }
    test_direct_copy_array(field.get(), arrays);
    test_write_and_read_column<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(
            column_pb, field.get(), arrays);
}

TEST_F(ArrayTest, TestNestedIntArrays) {
    // depth 2
    auto column_pb = create_column_pb("ARRAY", "ARRAY", "INT");
    auto type_info = get_type_info(column_pb);
    auto field = create_field(column_pb);
    auto tuple_desc = get_tuple_descriptor(_object_pool, type_info.get());
    ASSERT_EQ(tuple_desc->slots().size(), 1);
    auto context = std::make_unique<FunctionContext>();
    ArrayUtils::prepare_context(*context, *_mem_pool, column_pb);

    std::vector<const CollectionValue*> arrays = {
            parse(*_mem_pool, *context, "[]", column_pb),
            parse(*_mem_pool, *context, "[[]]", column_pb),
            parse(*_mem_pool, *context, "[[1, 2, 3], [4, 5, 6]]", column_pb),
            parse(*_mem_pool, *context, "[[1, 2, 3], null, [4, 5, 6]]", column_pb),
            parse(*_mem_pool, *context, "[[1, 2, null], null, [4, null, 6], null, [null, 8, 9]]",
                  column_pb),
    };
    for (auto array : arrays) {
        test_array<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb, field.get(),
                                                                          tuple_desc, array);
    }
    test_direct_copy_array(field.get(), arrays);
    test_write_and_read_column<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(
            column_pb, field.get(), arrays);

    // depth 3
    column_pb = create_column_pb("ARRAY", "ARRAY", "ARRAY", "INT");
    type_info = get_type_info(column_pb);
    field = create_field(column_pb);
    tuple_desc = get_tuple_descriptor(_object_pool, type_info.get());
    ASSERT_EQ(tuple_desc->slots().size(), 1);
    arrays.clear();
    ASSERT_EQ(arrays.size(), 0);
    context.reset(new FunctionContext);
    ArrayUtils::prepare_context(*context, *_mem_pool, column_pb);

    arrays = {
            parse(*_mem_pool, *context, "[]", column_pb),
            parse(*_mem_pool, *context, "[[]]", column_pb),
            parse(*_mem_pool, *context, "[[[]]]", column_pb),
            parse(*_mem_pool, *context, "[[[null]], [[1], [2, 3]], [[4, 5, 6], null, null]]",
                  column_pb),
    };
    for (auto array : arrays) {
        test_array<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb, field.get(),
                                                                          tuple_desc, array);
    }
    test_direct_copy_array(field.get(), arrays);
    test_write_and_read_column<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(
            column_pb, field.get(), arrays);
}

TEST_F(ArrayTest, TestSimpleStringArrays) {
    auto column_pb = create_column_pb("ARRAY", "VARCHAR");
    auto type_info = get_type_info(column_pb);
    auto field = create_field(column_pb);
    auto tuple_desc = get_tuple_descriptor(_object_pool, type_info.get());
    ASSERT_EQ(tuple_desc->slots().size(), 1);
    FunctionContext context;
    ArrayUtils::prepare_context(context, *_mem_pool, column_pb);

    std::vector<const CollectionValue*> arrays = {
            parse(*_mem_pool, context, "[]", column_pb),
            parse(*_mem_pool, context, "[null]", column_pb),
            parse(*_mem_pool, context, "[\"a\", \"b\", \"c\"]", column_pb),
            parse(*_mem_pool, context, "[null, \"b\", \"c\"]", column_pb),
            parse(*_mem_pool, context, "[\"a\", null, \"c\"]", column_pb),
            parse(*_mem_pool, context, "[\"a\", \"b\", null]", column_pb),
            parse(*_mem_pool, context, "[null, \"b\", null]", column_pb),
            parse(*_mem_pool, context, "[null, null, null]", column_pb),
    };
    for (auto array : arrays) {
        test_array<segment_v2::DEFAULT_ENCODING, segment_v2::DICT_ENCODING>(column_pb, field.get(),
                                                                            tuple_desc, array);
    }
    test_direct_copy_array(field.get(), arrays);
    test_write_and_read_column<segment_v2::DEFAULT_ENCODING, segment_v2::DICT_ENCODING>(
            column_pb, field.get(), arrays);
}

TEST_F(ArrayTest, TestNestedStringArrays) {
    auto column_pb = create_column_pb("ARRAY", "ARRAY", "ARRAY", "VARCHAR");
    auto type_info = get_type_info(column_pb);
    auto field = create_field(column_pb);
    auto tuple_desc = get_tuple_descriptor(_object_pool, type_info.get());
    ASSERT_EQ(tuple_desc->slots().size(), 1);
    FunctionContext context;
    ArrayUtils::prepare_context(context, *_mem_pool, column_pb);

    std::vector<const CollectionValue*> arrays = {
            parse(*_mem_pool, context, "[]", column_pb),
            parse(*_mem_pool, context, "[[]]", column_pb),
            parse(*_mem_pool, context, "[[[]]]", column_pb),
            parse(*_mem_pool, context, "[null, [null], [[null]]]", column_pb),
            parse(*_mem_pool, context,
                  "[[[\"a\", null, \"c\"], [\"d\", \"e\", \"f\"]], null, [[\"g\"]]]", column_pb),
    };
    for (auto array : arrays) {
        test_array<segment_v2::DEFAULT_ENCODING, segment_v2::DICT_ENCODING>(column_pb, field.get(),
                                                                            tuple_desc, array);
    }
    test_direct_copy_array(field.get(), arrays);
    test_write_and_read_column<segment_v2::DEFAULT_ENCODING, segment_v2::DICT_ENCODING>(
            column_pb, field.get(), arrays);
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
