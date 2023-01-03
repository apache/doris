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

#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/segment_v2.pb.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/field.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/column_writer.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "runtime/collection_value.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/primitive_type.h"
#include "runtime/raw_value.h"
#include "testutil/array_utils.h"
#include "testutil/desc_tbl_builder.h"
#include "util/file_utils.h"
#include "util/uid_util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {

template <typename... Ts>
ColumnPB create_column_pb(const std::string& type, const Ts&... sub_column_types) {
    ColumnPB column;
    auto prefix = "NOT_NULL_";
    column.set_is_nullable(type.compare(0, strlen(prefix), prefix) != 0);
    column.set_type(column.is_nullable() ? type : type.substr(strlen(prefix)));
    column.set_aggregation("NONE");
    if (type == "ARRAY") {
        column.set_length(OLAP_ARRAY_MAX_BYTES);
    }
    if constexpr (sizeof...(sub_column_types) > 0) {
        auto sub_column = create_column_pb(sub_column_types...);
        column.add_children_columns()->Swap(&sub_column);
    }
    return column;
}

TypeInfoPtr get_type_info(const ColumnPB& column_pb) {
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
    case OLAP_FIELD_TYPE_BOOL:
        return TypeDescriptor(TYPE_BOOLEAN);
    case OLAP_FIELD_TYPE_TINYINT:
        return TypeDescriptor(TYPE_TINYINT);
    case OLAP_FIELD_TYPE_SMALLINT:
        return TypeDescriptor(TYPE_SMALLINT);
    case OLAP_FIELD_TYPE_INT:
        return TypeDescriptor(TYPE_INT);
    case OLAP_FIELD_TYPE_BIGINT:
        return TypeDescriptor(TYPE_BIGINT);
    case OLAP_FIELD_TYPE_LARGEINT:
        return TypeDescriptor(TYPE_LARGEINT);
    case OLAP_FIELD_TYPE_FLOAT:
        return TypeDescriptor(TYPE_FLOAT);
    case OLAP_FIELD_TYPE_DOUBLE:
        return TypeDescriptor(TYPE_DOUBLE);
    case OLAP_FIELD_TYPE_CHAR:
        return TypeDescriptor::create_char_type(TypeDescriptor::MAX_CHAR_LENGTH);
    case OLAP_FIELD_TYPE_VARCHAR:
        return TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    case OLAP_FIELD_TYPE_STRING:
        return TypeDescriptor::create_string_type();
    case OLAP_FIELD_TYPE_DATE:
        return TypeDescriptor(TYPE_DATE);
    case OLAP_FIELD_TYPE_DATETIME:
        return TypeDescriptor(TYPE_DATETIME);
    case OLAP_FIELD_TYPE_DECIMAL:
        return TypeDescriptor(TYPE_DECIMALV2);
    default:
        DCHECK(false) << "Failed to get the scalar type descriptor.";
    }
}

const TupleDescriptor* get_tuple_descriptor(ObjectPool& object_pool, const TypeInfo* type_info) {
    DescriptorTblBuilder builder(&object_pool);
    auto& tuple_desc_builder = builder.declare_tuple();
    if (type_info->type() == OLAP_FIELD_TYPE_ARRAY) {
        TypeDescriptor type_desc(TYPE_ARRAY);
        type_desc.len = OLAP_ARRAY_MAX_BYTES;
        const auto* ptype = dynamic_cast<const ArrayTypeInfo*>(type_info)->item_type_info();
        while (ptype->type() == OLAP_FIELD_TYPE_ARRAY) {
            type_desc.children.push_back(TypeDescriptor(TYPE_ARRAY));
            ptype = dynamic_cast<const ArrayTypeInfo*>(ptype)->item_type_info();
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
    ArrayTest() : _mem_pool(new MemPool()) {}

    template <segment_v2::EncodingTypePB array_encoding, segment_v2::EncodingTypePB item_encoding>
    void test(const ColumnPB& column_pb, const std::vector<std::string>& literal_arrays) {
        auto field = create_field(column_pb);
        const auto* type_info = field->type_info();
        const auto* tuple_desc = get_tuple_descriptor(_object_pool, type_info);
        EXPECT_EQ(tuple_desc->slots().size(), 1);

        FunctionContext context;
        ArrayUtils::prepare_context(context, *_mem_pool, column_pb);

        std::vector<const CollectionValue*> arrays;
        for (const auto& literal_array : literal_arrays) {
            arrays.push_back(parse(*_mem_pool, context, literal_array, column_pb));
        }

        for (auto array : arrays) {
            test_array<array_encoding, item_encoding>(column_pb, field.get(), tuple_desc, array);
        }
        test_direct_copy_array(field.get(), arrays);
        test_write_and_read_column<array_encoding, item_encoding>(column_pb, field.get(), arrays);
    }

protected:
    void SetUp() override {
        if (FileUtils::check_exist(TEST_DIR)) {
            EXPECT_TRUE(FileUtils::remove_all(TEST_DIR).ok());
        }
        EXPECT_TRUE(FileUtils::create_dir(TEST_DIR).ok());
    }

    void TearDown() override {
        if (FileUtils::check_exist(TEST_DIR)) {
            EXPECT_TRUE(FileUtils::remove_all(TEST_DIR).ok());
        }
    }

private:
    void test_copy_array(const TupleDescriptor* tuple_desc, const Field* field,
                         const CollectionValue* array) {
        auto slot_desc = tuple_desc->slots().front();
        const auto& item_type_desc = slot_desc->type().children[0];
        auto total_size = tuple_desc->byte_size() + array->get_byte_size(item_type_desc);

        auto src = allocate_tuple(total_size);
        EXPECT_NE(src, nullptr);

        RawValue::write(array, src, slot_desc, _mem_pool.get());
        auto src_cv = reinterpret_cast<CollectionValue*>(src->get_slot(slot_desc->tuple_offset()));
        validate(field, array, src_cv);

        auto dst = allocate_tuple(total_size);
        EXPECT_NE(dst, nullptr);

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
        CollectionValue::deserialize_collection(dst_cv, reinterpret_cast<char*>(dst),
                                                item_type_desc);
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
        auto filename = generate_uuid_string();
        const std::string path = TEST_DIR + "/" + filename;
        LOG(INFO) << "Test path: " << path;

        segment_v2::ColumnMetaPB meta;
        init_column_meta<array_encoding, item_encoding>(&meta, column_pb);

        TabletColumn tablet_column;
        tablet_column.init_from_pb(column_pb);
        Schema schema({tablet_column}, 0);
        {
            auto file_writer = creat_file_writer(path);
            EXPECT_NE(file_writer, nullptr);
            auto writer = create_column_writer<array_encoding, item_encoding>(file_writer.get(),
                                                                              meta, column_pb);
            EXPECT_NE(writer, nullptr);
            Status st;
            for (auto array : arrays) {
                st = writer->append(false, const_cast<CollectionValue*>(array));
                EXPECT_TRUE(st.ok());
            }
            EXPECT_TRUE(writer->finish().ok());
            EXPECT_TRUE(writer->write_data().ok());
            EXPECT_TRUE(writer->write_ordinal_index().ok());
            EXPECT_TRUE(writer->write_zone_map().ok());

            EXPECT_TRUE(file_writer->close().ok());
        }
        {
            auto type_info = get_type_info(column_pb);
            auto tuple_desc = get_tuple_descriptor(_object_pool, type_info.get());

            auto reader = create_column_reader(path, meta, arrays.size());
            EXPECT_NE(reader, nullptr);
            auto rblock = create_readable_block(path);
            EXPECT_NE(rblock, nullptr);
            OlapReaderStatistics stats;
            std::unique_ptr<segment_v2::ColumnIterator> iter(
                    new_iterator(rblock.get(), &stats, reader.get()));
            EXPECT_NE(iter, nullptr);
            auto st = iter->seek_to_first();
            EXPECT_TRUE(st.ok()) << st.to_string();

            auto data_type =
                    vectorized::DataTypeFactory::instance().create_data_type(tablet_column);
            auto column_ptr = data_type->create_column();
            size_t rows_read = 1024;
            column_ptr->reserve(rows_read);
            do {
                bool has_null = false;
                st = iter->next_batch(&rows_read, column_ptr, &has_null);
                EXPECT_TRUE(st.ok());
                vectorized::Block vblock;
                vblock.insert({const_cast<const vectorized::IColumn&>(*column_ptr).get_ptr(),
                               data_type, ""});
                for (int i = 0; i < arrays.size(); ++i) {
                    auto tuple = vblock.deep_copy_tuple(*tuple_desc, _mem_pool.get(), i, 0, false);
                    auto actual =
                            tuple->get_collection_slot(tuple_desc->slots().front()->tuple_offset());
                    validate(field, arrays[i], actual);
                }
            } while (rows_read >= 1024);
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
        meta->set_is_nullable(column.is_nullable());
        for (uint32_t i = 0; i < column.get_subtype_count(); ++i) {
            init_column_meta<array_encoding, item_encoding>(meta->add_children_columns(), column_id,
                                                            column.get_sub_column(i));
        }
    }

    io::FileWriterPtr creat_file_writer(const std::string& path) {
        io::FileWriterPtr file_writer;
        io::global_local_filesystem()->create_file(path, &file_writer);
        return file_writer;
    }

    template <segment_v2::EncodingTypePB array_encoding, segment_v2::EncodingTypePB item_encoding>
    std::unique_ptr<segment_v2::ColumnWriter> create_column_writer(io::FileWriter* file_writer,
                                                                   segment_v2::ColumnMetaPB& meta,
                                                                   const ColumnPB& column_pb) {
        segment_v2::ColumnWriterOptions writer_opts = {.meta = &meta};
        TabletColumn column;
        column.init_from_pb(column_pb);
        std::unique_ptr<segment_v2::ColumnWriter> writer;
        auto st = segment_v2::ColumnWriter::create(writer_opts, &column, file_writer, &writer);
        if (!st.ok()) {
            return nullptr;
        }
        st = writer->init();
        return st.ok() ? std::move(writer) : nullptr;
    }

    std::unique_ptr<segment_v2::ColumnReader> create_column_reader(
            const std::string& path, const segment_v2::ColumnMetaPB& meta, size_t num_rows) {
        segment_v2::ColumnReaderOptions reader_opts;
        std::unique_ptr<segment_v2::ColumnReader> reader;
        auto st = segment_v2::ColumnReader::create(reader_opts, meta, num_rows,
                                                   io::global_local_filesystem(), path, &reader);
        return st.ok() ? std::move(reader) : nullptr;
    }

    io::FileReaderSPtr create_readable_block(const std::string& path) {
        io::FileReaderSPtr reader;
        auto st = io::global_local_filesystem()->open_file(path, &reader);
        return st.ok() ? std::move(reader) : nullptr;
    }

    segment_v2::ColumnIterator* new_iterator(io::FileReader* rblock, OlapReaderStatistics* stats,
                                             segment_v2::ColumnReader* reader) {
        segment_v2::ColumnIterator* iter = nullptr;
        auto st = reader->new_iterator(&iter);
        if (!st.ok()) {
            return nullptr;
        }
        segment_v2::ColumnIteratorOptions iter_opts;
        iter_opts.stats = stats;
        iter_opts.file_reader = rblock;
        st = iter->init(iter_opts);
        return st.ok() ? iter : nullptr;
    }

    template <segment_v2::EncodingTypePB array_encoding, segment_v2::EncodingTypePB item_encoding>
    void test_array(const ColumnPB& column_pb, const Field* field,
                    const TupleDescriptor* tuple_desc, const CollectionValue* array) {
        EXPECT_NE(array, nullptr);
        test_copy_array(tuple_desc, field, array);
        test_direct_copy_array(field, {array});
        test_write_and_read_column<array_encoding, item_encoding>(column_pb, field, {array});
    }

private:
    static constexpr size_t MAX_MEMORY_BYTES = 1024 * 1024;
    static const std::string TEST_DIR;
    std::unique_ptr<MemPool> _mem_pool;
    ObjectPool _object_pool;
};

const std::string ArrayTest::TEST_DIR = "./ut_dir/array_test";

TEST_F(ArrayTest, TestBoolean) {
    // depth 1
    auto column_pb = create_column_pb("ARRAY", "BOOLEAN");
    std::vector<std::string> literal_arrays = {
            "[]",
            "[null]",
            "[true, false, false]",
            "[true, null, false]",
            "[false, null, null]",
            "[null, null, true]",
            "[null, null, null]",
    };
    test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb, literal_arrays);

    // depth 2
    column_pb = create_column_pb("ARRAY", "ARRAY", "BOOLEAN");
    literal_arrays = {
            "[]",
            "[[]]",
            "[[false, true, false], [true, false, true]]",
            "[[false, true, false], null, [true, false, true]]",
            "[[false, true, null], null, [true, null, false], null, [null, false, false]]",
    };
    test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb, literal_arrays);

    // depth 3
    column_pb = create_column_pb("ARRAY", "ARRAY", "ARRAY", "BOOLEAN");
    literal_arrays = {
            "[]",
            "[[]]",
            "[[[]]]",
            "[[[null]], [[false], [true, false]], [[false, true, false], null, null]]",
    };
    test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb, literal_arrays);
}

TEST_F(ArrayTest, TestNotNullBoolean) {
    // depth 1
    auto column_pb = create_column_pb("ARRAY", "NOT_NULL_BOOLEAN");
    std::vector<std::string> literal_arrays = {
            "[]",
            "[true, false, false]",
    };
    test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb, literal_arrays);

    // depth 2
    column_pb = create_column_pb("ARRAY", "ARRAY", "NOT_NULL_BOOLEAN");
    literal_arrays = {
            "[]",
            "[[]]",
            "[[false, true, false]]",
            "[[false, true, false], [true, false, true]]",
    };
    test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb, literal_arrays);

    // depth 3
    column_pb = create_column_pb("ARRAY", "ARRAY", "ARRAY", "NOT_NULL_BOOLEAN");
    literal_arrays = {
            "[]",
            "[[]]",
            "[[[]]]",
            "[[[]], [[false], [true, false]], [[false, true, false]]]",
    };
    test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb, literal_arrays);
}

void test_integer(const std::string& type, ArrayTest& test_suite) {
    // depth 1
    auto column_pb = create_column_pb("ARRAY", type);
    std::vector<std::string> literal_arrays = {
            "[]",
            "[null]",
            "[1, 2, 3]",
            "[1, null, 3]",
            "[1, null, null]",
            "[null, null, 3]",
            "[null, null, null]",
    };
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb,
                                                                           literal_arrays);

    // depth 2
    column_pb = create_column_pb("ARRAY", "ARRAY", type);
    literal_arrays = {
            "[]",
            "[[]]",
            "[[1, 2, 3], [4, 5, 6]]",
            "[[1, 2, 3], null, [4, 5, 6]]",
            "[[1, 2, null], null, [4, null, 6], null, [null, 8, 9]]",
    };
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb,
                                                                           literal_arrays);

    // depth 3
    column_pb = create_column_pb("ARRAY", "ARRAY", "ARRAY", type);
    literal_arrays = {
            "[]",
            "[[]]",
            "[[[]]]",
            "[[[null]], [[1], [2, 3]], [[4, 5, 6], null, null]]",
    };
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb,
                                                                           literal_arrays);
}

TEST_F(ArrayTest, TestInteger) {
    test_integer("TINYINT", *this);
    test_integer("SMALLINT", *this);
    test_integer("INT", *this);
    test_integer("BIGINT", *this);
    test_integer("LARGEINT", *this);
}

void test_not_null_integer(const std::string& type, ArrayTest& test_suite) {
    // depth 1
    auto column_pb = create_column_pb("ARRAY", type);
    std::vector<std::string> literal_arrays = {
            "[]",
            "[1, 2, 3]",
    };
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb,
                                                                           literal_arrays);

    // depth 2
    column_pb = create_column_pb("ARRAY", "ARRAY", type);
    literal_arrays = {
            "[]",
            "[[]]",
            "[[1, 2, 3]]",
            "[[1, 2, 3], [4, 5, 6]]",
    };
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb,
                                                                           literal_arrays);

    // depth 3
    column_pb = create_column_pb("ARRAY", "ARRAY", "ARRAY", type);
    literal_arrays = {
            "[]", "[[]]", "[[[]]]", "[[[1, 2, 3]]]", "[[[]], [[1], [2, 3]], [[4, 5, 6]]]",
    };
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb,
                                                                           literal_arrays);
}

TEST_F(ArrayTest, TestNotNullInteger) {
    test_not_null_integer("NOT_NULL_TINYINT", *this);
    test_not_null_integer("NOT_NULL_SMALLINT", *this);
    test_not_null_integer("NOT_NULL_INT", *this);
    test_not_null_integer("NOT_NULL_BIGINT", *this);
    test_not_null_integer("NOT_NULL_LARGEINT", *this);
}

void test_float(const std::string& type, ArrayTest& test_suite) {
    // depth 1
    auto column_pb = create_column_pb("ARRAY", type);
    std::vector<std::string> literal_arrays = {
            "[]",
            "[null]",
            "[1.5, 2.5, 3.5]",
            "[1.5, null, 3.5]",
            "[1.5, null, null]",
            "[null, null, 3.5]",
            "[null, null, null]",
    };
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb,
                                                                           literal_arrays);
    // depth 2
    column_pb = create_column_pb("ARRAY", "ARRAY", type);
    literal_arrays = {
            "[]",
            "[[]]",
            "[[1.5, 2.5, 3.5], [4.5, 5.5, 6.5]]",
            "[[1.5, 2.5, 3.5], null, [4.5, 5.5, 6.5]]",
            "[[1.5, 2.5, null], null, [4.5, null, 6.5], null, [null, 8.5, 9.5]]",
    };
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb,
                                                                           literal_arrays);

    // depth 3
    column_pb = create_column_pb("ARRAY", "ARRAY", "ARRAY", type);
    literal_arrays = {
            "[]",
            "[[]]",
            "[[[]]]",
            "[[[null]], [[1.5], [2.5, 3.5]], [[4.5, 5.5, 6.5], null, null]]",
    };
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb,
                                                                           literal_arrays);
}

TEST_F(ArrayTest, TestFloat) {
    test_float("FLOAT", *this);
    test_float("DOUBLE", *this);
}

void test_not_null_float(const std::string& type, ArrayTest& test_suite) {
    // depth 1
    auto column_pb = create_column_pb("ARRAY", type);
    std::vector<std::string> literal_arrays = {
            "[]",
            "[1.5, 2.5, 3.5]",
    };
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb,
                                                                           literal_arrays);
    // depth 2
    column_pb = create_column_pb("ARRAY", "ARRAY", type);
    literal_arrays = {
            "[]",
            "[[]]",
            "[[1.5, 2.5, 3.5]]",
            "[[1.5, 2.5, 3.5], [4.5, 5.5, 6.5]]",
    };
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb,
                                                                           literal_arrays);

    // depth 3
    column_pb = create_column_pb("ARRAY", "ARRAY", "ARRAY", type);
    literal_arrays = {
            "[]", "[[]]", "[[[]]]", "[[[1.5]]]", "[[[]], [[1.5], [2.5, 3.5]], [[4.5, 5.5, 6.5]]]",
    };
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb,
                                                                           literal_arrays);
}

TEST_F(ArrayTest, TestNotNullFloat) {
    test_not_null_float("NOT_NULL_FLOAT", *this);
    test_not_null_float("NOT_NULL_DOUBLE", *this);
}

void test_string(const std::string& type, ArrayTest& test_suite) {
    // depth 1
    auto column_pb = create_column_pb("ARRAY", type);
    std::vector<std::string> literal_arrays = {
            "[]",
            "[null]",
            "[\"a\", \"b\", \"c\"]",
            "[null, \"b\", \"c\"]",
            "[\"a\", null, \"c\"]",
            "[\"a\", \"b\", null]",
            "[null, \"b\", null]",
            "[null, null, null]",
    };
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::DICT_ENCODING>(column_pb,
                                                                             literal_arrays);

    // more depths
    column_pb = create_column_pb("ARRAY", "ARRAY", "ARRAY", type);
    literal_arrays = {
            "[]",
            "[[]]",
            "[[[]]]",
            "[null, [null], [[null]]]",
            "[[[\"a\", null, \"c\"], [\"d\", \"e\", \"f\"]], null, [[\"g\"]]]",
    };
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::DICT_ENCODING>(column_pb,
                                                                             literal_arrays);
}

TEST_F(ArrayTest, TestString) {
    test_string("CHAR", *this);
    test_string("VARCHAR", *this);
    test_string("STRING", *this);
}

void test_not_null_string(const std::string& type, ArrayTest& test_suite) {
    // depth 1
    auto column_pb = create_column_pb("ARRAY", type);
    std::vector<std::string> literal_arrays = {
            "[]",
            "[\"a\", \"b\", \"c\"]",
    };
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::DICT_ENCODING>(column_pb,
                                                                             literal_arrays);

    // more depths
    column_pb = create_column_pb("ARRAY", "ARRAY", "ARRAY", type);
    literal_arrays = {
            "[]",
            "[[]]",
            "[[[]]]",
            "[[[\"a\", \"b\", \"c\"]]]",
            "[[[\"a\", \"c\"], [\"d\", \"e\", \"f\"]], [[\"g\"]]]",
    };
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::DICT_ENCODING>(column_pb,
                                                                             literal_arrays);
}

TEST_F(ArrayTest, TestNotNullString) {
    test_not_null_string("NOT_NULL_CHAR", *this);
    test_not_null_string("NOT_NULL_VARCHAR", *this);
    test_not_null_string("NOT_NULL_STRING", *this);
}

void test_datetime(const std::string& type, ArrayTest& test_suite) {
    auto column_pb = create_column_pb("ARRAY", type);
    std::vector<std::string> literal_arrays;
    if (type == "DATE") {
        literal_arrays = {
                "[]",
                "[null]",
                "[\"2022-04-01\", \"2022-04-02\", \"2022-04-03\"]",
                "[\"2022-04-01\", null, \"2022-04-03\"]",
                "[\"2022-04-01\", null, null]",
                "[null, null, \"2022-04-03\"]",
                "[null, null, null]",
        };
    } else {
        literal_arrays = {
                "[]",
                "[null]",
                "[\"2022-04-01 19:30:40\", \"2022-04-02 19:30:40 \", \"2022-04-03 19:30:40\"]",
                "[\"2022-04-01 19:30:40\", null, \"2022-04-03 19:30:40\"]",
                "[\"2022-04-01 19:30:40\", null, null]",
                "[null, null, \"2022-04-03 19:30:40\"]",
                "[null, null, null]",
        };
    }
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb,
                                                                           literal_arrays);
    // depth 2
    column_pb = create_column_pb("ARRAY", "ARRAY", type);
    if (type == "DATE") {
        literal_arrays = {
                "[]",
                "[[]]",
                "[[\"2022-04-01\", \"2022-04-02\", \"2022-04-03\"], [\"2022-04-04\", "
                "\"2022-04-05\", "
                "\"2022-04-06\"]]",
                "[[\"2022-04-01\", \"2022-04-02\", \"2022-04-03\"], null, [\"2022-04-04\", "
                "\"2022-04-05\", \"2022-04-06\"]]",
                "[[\"2022-04-01\", \"2022-04-02\", null], null, [\"2022-04-04\", null, "
                "\"2022-04-06\"], null, [null, \"2022-04-08\", \"2022-04-09\"]]",
        };
    } else {
        literal_arrays = {
                "[]",
                "[[]]",
                "[[\"2022-04-01 19:30:40\", \"2022-04-02 19:30:40\", \"2022-04-03 19:30:40\"], "
                "[\"2022-04-04 19:30:40\", "
                "\"2022-04-05\", "
                "\"2022-04-06\"]]",
                "[[\"2022-04-01 19:30:40\", \"2022-04-02 19:30:40\", \"2022-04-03 19:30:40\"], "
                "null, [\"2022-04-04 19:30:40\", "
                "\"2022-04-05\", \"2022-04-06\"]]",
                "[[\"2022-04-01 19:30:40\", \"2022-04-02 19:30:40\", null], null, [\"2022-04-04 "
                "19:30:40\", null, "
                "\"2022-04-06 19:30:40\"], null, [null, \"2022-04-08 19:30:40\", \"2022-04-09 "
                "19:30:40\"]]",
        };
    }
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb,
                                                                           literal_arrays);

    // depth 3
    column_pb = create_column_pb("ARRAY", "ARRAY", "ARRAY", type);
    if (type == "DATE") {
        literal_arrays = {
                "[]",
                "[[]]",
                "[[[]]]",
                "[[[null]], [[\"2022-04-01\"], [\"2022-04-02\", \"2022-04-03\"]], "
                "[[\"2022-04-04\", "
                "\"2022-04-05\", \"2022-04-06\"], null, null]]",
        };
    } else {
        literal_arrays = {
                "[]",
                "[[]]",
                "[[[]]]",
                "[[[null]], [[\"2022-04-01 19:30:40\"], [\"2022-04-02 19:30:40\", \"2022-04-03 "
                "19:30:40\"]], "
                "[[\"2022-04-04 19:30:40\", "
                "\"2022-04-05 19:30:40\", \"2022-04-06 19:30:40\"], null, null]]",
        };
    }
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb,
                                                                           literal_arrays);
}

TEST_F(ArrayTest, TestDateTime) {
    test_datetime("DATE", *this);
    test_datetime("DATETIME", *this);
}

void test_not_null_datetime(const std::string& type, ArrayTest& test_suite) {
    auto column_pb = create_column_pb("ARRAY", type);
    std::vector<std::string> literal_arrays;
    if (type == "DATE") {
        literal_arrays = {
                "[]",
                "[\"2022-04-01\", \"2022-04-02\", \"2022-04-03\"]",
        };
    } else {
        literal_arrays = {
                "[]",
                "[\"2022-04-01 19:30:40\", \"2022-04-02 19:30:40 \", \"2022-04-03 19:30:40\"]",
        };
    }
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb,
                                                                           literal_arrays);
    // depth 2
    column_pb = create_column_pb("ARRAY", "ARRAY", type);
    if (type == "DATE") {
        literal_arrays = {
                "[]",
                "[[]]",
                "[[\"2022-04-01\", \"2022-04-02\", \"2022-04-03\"], [\"2022-04-04\", "
                "\"2022-04-05\", "
                "\"2022-04-06\"]]",
        };
    } else {
        literal_arrays = {
                "[]",
                "[[]]",
                "[[\"2022-04-01 19:30:40\", \"2022-04-02 19:30:40\", \"2022-04-03 19:30:40\"], "
                "[\"2022-04-04 19:30:40\", "
                "\"2022-04-05\", "
                "\"2022-04-06\"]]",
        };
    }
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb,
                                                                           literal_arrays);

    // depth 3
    column_pb = create_column_pb("ARRAY", "ARRAY", "ARRAY", type);
    if (type == "DATE") {
        literal_arrays = {
                "[]",
                "[[]]",
                "[[[]]]",
                "[[[\"2022-04-01\"]]]",
                "[[[]], [[\"2022-04-01\"], [\"2022-04-02\", \"2022-04-03\"]], "
                "[[\"2022-04-04\", "
                "\"2022-04-05\", \"2022-04-06\"]]]",
        };
    } else {
        literal_arrays = {
                "[]",
                "[[]]",
                "[[[]]]",
                "[[[\"2022-04-01 19:30:40\"]]]",
                "[[[]], [[\"2022-04-01 19:30:40\"], [\"2022-04-02 19:30:40\", \"2022-04-03 "
                "19:30:40\"]], "
                "[[\"2022-04-04 19:30:40\", "
                "\"2022-04-05 19:30:40\", \"2022-04-06 19:30:40\"]]]",
        };
    }
    test_suite.test<segment_v2::DEFAULT_ENCODING, segment_v2::BIT_SHUFFLE>(column_pb,
                                                                           literal_arrays);
}

TEST_F(ArrayTest, TestNotNullDateTime) {
    test_not_null_datetime("NOT_NULL_DATE", *this);
    test_not_null_datetime("NOT_NULL_DATETIME", *this);
}

TEST_F(ArrayTest, TestDecimal) {
    test_integer("DECIMAL", *this);
    test_not_null_integer("NOT_NULL_DECIMAL", *this);
    test_float("DECIMAL", *this);
    test_not_null_float("NOT_NULL_DECIMAL", *this);
}

} // namespace doris
