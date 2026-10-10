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

#include <gen_cpp/Descriptors_constants.h>
#include <gen_cpp/Descriptors_types.h>
#include <gtest/gtest.h>

#include <array>

#include "common/config.h"
#include "core/arena.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_file.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_varbinary.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "exprs/aggregate/aggregate_function_count.h"
#include "exprs/function/simple_function_factory.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/runtime_state.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/iterators.h"
#include "storage/merger.h"
#include "storage/predicate/null_predicate.h"
#include "storage/predicate/predicate_creator.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/schema.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/column_writer.h"
#include "storage/segment/segment.h"
#include "storage/segment/vertical_segment_writer.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {
namespace {

TabletColumn file_storage_schema(int32_t id = 17, int32_t child_id = 101) {
    TabletColumn file(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                      FieldType::OLAP_FIELD_TYPE_FILE, true, id, 0);
    file.set_name("f");
    const char* names[] = {"uri", "offset", "size", "content_type", "checksum", "inline"};
    for (size_t i = 0; i < 6; ++i) {
        const auto type = i == 1 || i == 2 ? FieldType::OLAP_FIELD_TYPE_BIGINT
                          : i == 5         ? FieldType::OLAP_FIELD_TYPE_STRING
                                           : FieldType::OLAP_FIELD_TYPE_VARCHAR;
        TabletColumn child(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, type, true);
        child.set_name(names[i]);
        child.set_unique_id(child_id + i);
        child.set_length(i == 0 ? 65533 : i == 3 || i == 4 ? 1024 : 8);
        file.add_sub_column(child);
    }
    return file;
}

void init_meta(const TabletColumn& column, ColumnMetaPB* meta,
               EncodingTypePB string_encoding = PLAIN_ENCODING) {
    meta->set_column_id(column.unique_id());
    meta->set_unique_id(column.unique_id());
    meta->set_type(static_cast<int>(column.type()));
    meta->set_length(column.length());
    meta->set_is_nullable(column.is_nullable());
    meta->set_encoding(column.type() == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
                                       column.type() == FieldType::OLAP_FIELD_TYPE_STRING
                               ? string_encoding
                               : PLAIN_ENCODING);
    meta->set_compression(CompressionTypePB::LZ4F);
    for (const auto& child : column.get_sub_columns()) {
        init_meta(*child, meta->add_children_columns(), string_encoding);
    }
}

MutableColumnPtr file_values(const std::string& first_inline = std::string("\0\xff\x80", 3)) {
    auto file = DataTypeFile().create_column();
    auto& values = assert_cast<ColumnFile&>(*file);
    for (size_t row = 0; row < 4; ++row) {
        for (size_t child = 0; child < 6; ++child) {
            auto& nullable = assert_cast<ColumnNullable&>(values.get_column(child));
            if (row == 0 && (child == 1 || child == 2)) {
                nullable.insert(Field::create_field<TYPE_BIGINT>(child == 1 ? 7 : 3));
            } else if (row == 0 && (child == 3 || child == 4)) {
                nullable.insert(Field::create_field<TYPE_STRING>(child == 3 ? "image/png"
                                                                            : "ETAG:opaque-2"));
            } else if (row == 3 || (child != 0 && child != 5) || (child == 5 && row == 2)) {
                nullable.insert_default();
            } else {
                const std::string bytes = child == 0 ? "s3://bucket/object"
                                          : row == 0 ? first_inline
                                                     : std::string();
                nullable.get_nested_column().insert_data(bytes.data(), bytes.size());
                nullable.get_null_map_data().push_back(0);
            }
        }
    }
    auto null_map = ColumnUInt8::create();
    null_map->get_data().assign({0, 0, 0, 1});
    return ColumnNullable::create(std::move(file), std::move(null_map));
}

TabletSchemaSPtr file_tablet_schema() {
    auto schema = std::make_shared<TabletSchema>();
    TabletColumn key(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                     FieldType::OLAP_FIELD_TYPE_INT, false, 1, sizeof(int32_t));
    key.set_name("k");
    key.set_is_key(true);
    key.set_index_length(sizeof(int32_t));
    schema->append_column(key);
    schema->append_column(file_storage_schema());
    TabletColumn array(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                       FieldType::OLAP_FIELD_TYPE_ARRAY, false, 18, 0);
    array.set_name("arr");
    auto nested_file = file_storage_schema(27, 201);
    array.add_sub_column(nested_file);
    schema->append_column(array);
    schema->_keys_type = DUP_KEYS;
    schema->_num_short_key_columns = 1;
    return schema;
}

Block file_tablet_block(const TabletSchemaSPtr& schema) {
    auto block = schema->create_storage_block();
    auto keys = ColumnInt32::create();
    keys->get_data().assign({0, 1, 2, 3});
    block.replace_by_position(0, std::move(keys));
    std::string bytes(256, 'x');
    bytes[0] = '\0';
    bytes[129] = '\xff';
    block.replace_by_position(1, file_values(bytes));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().assign({1, 3, 3, 4});
    block.replace_by_position(2, ColumnArray::create(file_values(bytes), std::move(offsets)));
    return block;
}

void expect_file_values_equal(const IColumn& expected, const IColumn& actual) {
    ASSERT_EQ(expected.size(), actual.size());
    const auto& before = assert_cast<const ColumnNullable&>(expected);
    const auto& after = assert_cast<const ColumnNullable&>(actual);
    const auto& before_file = assert_cast<const ColumnFile&>(before.get_nested_column());
    const auto& after_file = assert_cast<const ColumnFile&>(after.get_nested_column());
    for (size_t row = 0; row < expected.size(); ++row) {
        EXPECT_EQ(before.is_null_at(row), after.is_null_at(row));
        for (size_t child = 0; child < 6; ++child) {
            EXPECT_EQ(before_file.get_column(child)[row], after_file.get_column(child)[row]);
        }
    }
}

void expect_file_blocks_equal(const Block& expected, const Block& actual) {
    ASSERT_EQ(expected.rows(), actual.rows());
    ASSERT_EQ(3, actual.columns());
    for (size_t row = 0; row < expected.rows(); ++row) {
        EXPECT_EQ((*expected.get_by_position(0).column)[row],
                  (*actual.get_by_position(0).column)[row]);
    }
    expect_file_values_equal(*expected.get_by_position(1).column,
                             *actual.get_by_position(1).column);
    const auto& before_array = assert_cast<const ColumnArray&>(*expected.get_by_position(2).column);
    const auto& after_array = assert_cast<const ColumnArray&>(*actual.get_by_position(2).column);
    for (size_t row = 0; row < expected.rows(); ++row) {
        EXPECT_EQ(before_array.get_offsets()[row], after_array.get_offsets()[row]);
    }
    expect_file_values_equal(before_array.get_data(), after_array.get_data());
}

TColumnAccessPath access_path(TAccessPathType::type kind, std::vector<std::string> parts) {
    TColumnAccessPath path;
    path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
    path.__set_type(kind);
    if (kind == TAccessPathType::DATA) {
        TDataAccessPath data;
        data.__set_path(parts);
        path.__set_data_access_path(data);
    } else {
        TMetaAccessPath meta;
        meta.__set_path(parts);
        path.__set_meta_access_path(meta);
    }
    return path;
}

class FilePrefetchTrackingIterator final : public ColumnIterator {
public:
    explicit FilePrefetchTrackingIterator(size_t* calls) : _calls(calls) {}
    Status seek_to_ordinal(ordinal_t) override { return Status::OK(); }
    ordinal_t get_current_ordinal() const override { return 0; }
    Status init_prefetcher(const SegmentPrefetchParams&) override {
        ++*_calls;
        return Status::OK();
    }

private:
    size_t* _calls;
};

class FileColumnStorageTest : public testing::Test {
protected:
    void SetUp() override {
        _old_page_cache = config::disable_storage_page_cache;
        config::disable_storage_page_cache = true;
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_dir).ok());
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_dir).ok());
        config::disable_storage_page_cache = _old_page_cache;
    }

    Status write_and_open() {
        ColumnWithTypeAndName source {file_values(),
                                      make_nullable(std::make_shared<DataTypeFile>()), "f"};
        return write_and_open(source);
    }

    Status write_and_open(const ColumnWithTypeAndName& source, size_t batch_size = 2) {
        _meta.Clear();
        init_meta(_schema, &_meta, _string_encoding);
        io::FileWriterPtr output;
        const auto path = _dir + "/data" + std::to_string(_write_count++);
        RETURN_IF_ERROR(io::global_local_filesystem()->create_file(path, &output));
        ColumnWriterOptions options;
        options.meta = &_meta;
        options.need_zone_map = false;
        options.need_bloom_filter = false;
        std::unique_ptr<ColumnWriter> writer;
        RETURN_IF_ERROR(ColumnWriter::create(options, &_schema, output.get(), &writer));
        RETURN_IF_ERROR(writer->init());
        OlapBlockDataConvertor converter;
        converter.add_column_data_convertor(_schema);
        const auto count = source.column->size();
        for (size_t start = 0; start < count; start += batch_size) {
            const auto rows = std::min(batch_size, count - start);
            RETURN_IF_ERROR(
                    converter.set_source_content_with_specifid_column(source, start, rows, 0));
            auto [status, accessor] = converter.convert_column_data(0);
            RETURN_IF_ERROR(status);
            RETURN_IF_ERROR(writer->append(accessor->get_nullmap(), accessor->get_data(), rows));
        }
        RETURN_IF_ERROR(writer->finish());
        RETURN_IF_ERROR(writer->write_data());
        RETURN_IF_ERROR(writer->write_ordinal_index());
        RETURN_IF_ERROR(output->close());
        RETURN_IF_ERROR(io::global_local_filesystem()->open_file(path, &_input));
        ColumnReaderOptions reader_options;
        RETURN_IF_ERROR(ColumnReader::create(reader_options, _meta, count, _input, &_reader));
        return _reader->new_iterator(&_iterator, &_schema);
    }

    Status init_iterator() {
        ColumnIteratorOptions options;
        options.stats = &_stats;
        options.file_reader = _input.get();
        RETURN_IF_ERROR(_iterator->init(options));
        return _iterator->seek_to_ordinal(0);
    }

    MutableColumnPtr destination() {
        return make_nullable(std::make_shared<DataTypeFile>())->create_column();
    }

    Status write_vertical_segment(const std::string& path, const TabletSchemaSPtr& schema,
                                  const Block& block) {
        io::FileWriterPtr output;
        RETURN_IF_ERROR(io::global_local_filesystem()->create_file(path, &output));
        RowsetWriterContext context;
        context.tablet_id = 100;
        context.tablet_schema = schema;
        VerticalSegmentWriterOptions options;
        options.rowset_ctx = &context;
        options.write_type = DataWriteType::TYPE_COMPACTION;
        options.compression_type = CompressionTypePB::LZ4F;
        VerticalSegmentWriter writer(output.get(), 7, schema, nullptr, nullptr, options, nullptr);
        std::vector<std::vector<uint32_t>> groups;
        std::vector<uint32_t> cluster_keys;
        Merger::vertical_split_columns(*schema, &groups, &cluster_keys, 1);
        EXPECT_EQ((std::vector<std::vector<uint32_t>> {{0}, {1}, {2}}), groups);
        for (size_t group = 0; group < groups.size(); ++group) {
            RETURN_IF_ERROR(writer.init(groups[group], group == 0));
            Block group_block;
            for (auto column : groups[group]) {
                group_block.insert(block.get_by_position(column));
            }
            for (size_t start = 0; start < block.rows(); start += 2) {
                RETURN_IF_ERROR(writer.append_block(&group_block, start,
                                                    std::min<size_t>(2, block.rows() - start)));
            }
            uint64_t index_size = 0;
            RETURN_IF_ERROR(writer.finalize_columns(&index_size));
        }
        uint64_t file_size = 0;
        return writer.finalize_footer(&file_size);
    }

    Status read_vertical_segment(const std::string& path, const TabletSchemaSPtr& schema,
                                 Block* result) {
        StorageReadOptions options;
        options.io_ctx.reader_type = ReaderType::READER_BASE_COMPACTION;
        return read_segment(path, schema, std::move(options), result);
    }

    Status read_segment(const std::string& path, const TabletSchemaSPtr& schema,
                        StorageReadOptions options, Block* result) {
        RowsetId rowset_id;
        rowset_id.init(10002);
        std::shared_ptr<Segment> segment;
        RETURN_IF_ERROR(Segment::open(io::global_local_filesystem(), path, 100, 7, rowset_id,
                                      schema, io::FileReaderOptions {}, &segment));
        options.stats = &_stats;
        auto read_schema = std::make_shared<ReadSchema>(schema->columns());
        std::unique_ptr<RowwiseIterator> iterator;
        RETURN_IF_ERROR(segment->new_iterator(read_schema, options, &iterator));
        MutableBlock contents(schema->create_storage_block());
        for (;;) {
            Block batch = schema->create_storage_block();
            auto status = iterator->next_batch(&batch);
            if (status.is<ErrorCode::END_OF_FILE>()) break;
            RETURN_IF_ERROR(status);
            RETURN_IF_ERROR(contents.add_rows(&batch, 0, batch.rows()));
        }
        *result = contents.to_block();
        return Status::OK();
    }

    Status file_size_getter(const ColumnPtr& input, ColumnPtr* output) {
        auto selector = ColumnString::create();
        selector->insert_data("size", 4);
        const auto result_type = make_nullable(std::make_shared<DataTypeInt64>());
        Block block {{input, make_nullable(std::make_shared<DataTypeFile>()), "f"},
                     {ColumnConst::create(std::move(selector), input->size()),
                      std::make_shared<DataTypeString>(), "selector"},
                     {nullptr, result_type, "size"}};
        auto function = SimpleFunctionFactory::instance().get_function(
                "element_at", {block.get_by_position(0), block.get_by_position(1)}, result_type);
        EXPECT_NE(nullptr, function);
        RETURN_IF_ERROR(function->execute(nullptr, block, {0, 1}, 2, input->size()));
        *output = block.get_by_position(2).column;
        return Status::OK();
    }

    ColumnWithTypeAndName container_file_source(bool is_map) {
        auto file_schema = file_storage_schema();
        const std::string name = is_map ? "m" : "arr";
        TabletColumn container(
                FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                is_map ? FieldType::OLAP_FIELD_TYPE_MAP : FieldType::OLAP_FIELD_TYPE_ARRAY, true,
                20, 0);
        container.set_name(name);
        const auto file_type = make_nullable(std::make_shared<DataTypeFile>());
        const auto key_type = make_nullable(std::make_shared<DataTypeInt32>());
        if (is_map) {
            TabletColumn key(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                             FieldType::OLAP_FIELD_TYPE_INT, true, 18, sizeof(int32_t));
            key.set_name("key");
            container.add_sub_column(key);
        }
        container.add_sub_column(file_schema);
        _schema = container;

        auto files = file_values(std::string(256, '\xff'));
        for (size_t length : {513, 1024}) {
            const auto extra = file_values(std::string(length, '\0'));
            files->insert_from(*extra, 0);
        }
        auto offsets = ColumnArray::ColumnOffsets::create();
        // Long inline; empty container; NULL container; empty/NULL inline;
        // NULL FILE; a skipped long value; final long value (page-tail offset).
        offsets->get_data().assign({1, 1, 1, 3, 4, 5, 6});
        MutableColumnPtr values;
        DataTypePtr type;
        if (is_map) {
            auto keys = key_type->create_column();
            for (int32_t key = 100; key < 106; ++key) {
                keys->insert(Field::create_field<TYPE_INT>(key));
            }
            values = ColumnMap::create(std::move(keys), std::move(files), std::move(offsets));
            type = std::make_shared<DataTypeMap>(key_type, file_type);
        } else {
            values = ColumnArray::create(std::move(files), std::move(offsets));
            type = std::make_shared<DataTypeArray>(file_type);
        }
        auto nulls = ColumnUInt8::create();
        nulls->get_data().assign({0, 0, 1, 0, 0, 0, 0});
        return {ColumnNullable::create(std::move(values), std::move(nulls)), make_nullable(type),
                name};
    }

    static const IColumn& container_files(const IColumn& column, bool is_map) {
        const auto& nested = assert_cast<const ColumnNullable&>(column).get_nested_column();
        return is_map ? assert_cast<const ColumnMap&>(nested).get_values()
                      : assert_cast<const ColumnArray&>(nested).get_data();
    }

    static void expect_container_shape_equal(const IColumn& expected, const IColumn& actual,
                                             bool is_map) {
        ASSERT_EQ(expected.size(), actual.size());
        const auto& before = assert_cast<const ColumnNullable&>(expected);
        const auto& after = assert_cast<const ColumnNullable&>(actual);
        for (size_t row = 0; row < expected.size(); ++row) {
            EXPECT_EQ(before.is_null_at(row), after.is_null_at(row));
        }
        if (is_map) {
            const auto& before_map = assert_cast<const ColumnMap&>(before.get_nested_column());
            const auto& after_map = assert_cast<const ColumnMap&>(after.get_nested_column());
            ASSERT_EQ(before_map.get_keys().size(), after_map.get_keys().size());
            for (size_t row = 0; row < expected.size(); ++row) {
                EXPECT_EQ(before_map.get_offsets()[row], after_map.get_offsets()[row]);
            }
            for (size_t key = 0; key < before_map.get_keys().size(); ++key) {
                EXPECT_EQ(before_map.get_keys()[key], after_map.get_keys()[key]);
            }
        } else {
            const auto& before_array = assert_cast<const ColumnArray&>(before.get_nested_column());
            const auto& after_array = assert_cast<const ColumnArray&>(after.get_nested_column());
            for (size_t row = 0; row < expected.size(); ++row) {
                EXPECT_EQ(before_array.get_offsets()[row], after_array.get_offsets()[row]);
            }
        }
    }

    FileValueColumnIterator& container_file_iterator(bool is_map) {
        auto* child =
                is_map ? assert_cast<MapFileColumnIterator&>(*_iterator)._val_iterator.get()
                       : assert_cast<ArrayFileColumnIterator&>(*_iterator)._item_iterator.get();
        return assert_cast<FileValueColumnIterator&>(*child);
    }

    void check_container_child_projection(bool is_map) {
        const auto source = container_file_source(is_map);
        ASSERT_TRUE(write_and_open(source, 2).ok());
        _iterator->set_column_name(source.name);
        TColumnAccessPaths paths {
                access_path(TAccessPathType::DATA, {source.name, is_map ? "VALUES" : "*", "size"})};
        if (is_map) paths.push_back(access_path(TAccessPathType::DATA, {source.name, "KEYS"}));
        ASSERT_TRUE(_iterator->set_access_paths(paths, {}).ok());
        _iterator->remove_pruned_sub_iterators();
        ASSERT_TRUE(init_iterator().ok());
        auto actual = source.type->create_column();
        const rowid_t first[] = {0, 1, 2};
        const rowid_t last[] = {4, 6};
        ASSERT_TRUE(_iterator->read_by_rowids(first, 3, actual).ok());
        ASSERT_TRUE(_iterator->read_by_rowids(last, 2, actual).ok());
        auto expected = source.column->clone_resized(source.column->size());
        IColumn::Filter selected;
        selected.assign({1, 1, 1, 0, 1, 0, 1});
        ASSERT_EQ(5, expected->filter(selected));
        expect_container_shape_equal(*expected, *actual, is_map);

        const auto& before = assert_cast<const ColumnNullable&>(container_files(*expected, is_map));
        const auto& after = assert_cast<const ColumnNullable&>(container_files(*actual, is_map));
        const auto& before_file = assert_cast<const ColumnFile&>(before.get_nested_column());
        const auto& after_file = assert_cast<const ColumnFile&>(after.get_nested_column());
        ASSERT_EQ(3, after.size());
        ASSERT_EQ(6, after_file.tuple_size());
        for (size_t row = 0; row < after.size(); ++row) {
            EXPECT_EQ(before.is_null_at(row), after.is_null_at(row));
            EXPECT_EQ(before_file.get_column(2)[row], after_file.get_column(2)[row]);
            EXPECT_TRUE(after_file.get_column(0).is_null_at(row));
            EXPECT_TRUE(after_file.get_column(5).is_null_at(row));
        }
        auto& file_iterator = container_file_iterator(is_map);
        EXPECT_EQ(ColumnIterator::ReadRequirement::SKIP,
                  file_iterator._sub_column_iterators[5]->read_requirement());
        ColumnPtr sizes;
        ASSERT_TRUE(file_size_getter(after.get_ptr(), &sizes).ok());
        EXPECT_EQ(3, (*sizes)[0].get<TYPE_BIGINT>());
        EXPECT_TRUE(sizes->is_null_at(1));
        EXPECT_EQ(3, (*sizes)[2].get<TYPE_BIGINT>());
    }

    void check_container_lazy_recovery(bool is_map, bool empty_selection = false) {
        const auto source = container_file_source(is_map);
        ASSERT_TRUE(write_and_open(source, 2).ok());
        _iterator->set_column_name(source.name);
        TColumnAccessPaths predicate_paths {
                access_path(TAccessPathType::DATA, {source.name, is_map ? "VALUES" : "*", "size"})};
        if (is_map) {
            predicate_paths.push_back(access_path(TAccessPathType::DATA, {source.name, "KEYS"}));
        }
        ASSERT_TRUE(_iterator
                            ->set_access_paths({access_path(TAccessPathType::DATA, {source.name})},
                                               predicate_paths)
                            .ok());
        ASSERT_TRUE(_iterator->has_lazy_read_target());
        ASSERT_TRUE(init_iterator().ok());
        auto actual = source.type->create_column();
        _iterator->set_read_phase(ColumnIterator::ReadPhase::PREDICATE);
        size_t count = 7;
        bool has_null = false;
        ASSERT_TRUE(_iterator->next_batch(&count, actual, &has_null).ok());
        ASSERT_EQ(7, count);
        const auto& predicate_files = assert_cast<const ColumnFile&>(
                assert_cast<const ColumnNullable&>(container_files(*actual, is_map))
                        .get_nested_column());
        ASSERT_EQ(6, predicate_files.tuple_size());
        EXPECT_TRUE(predicate_files.get_column(5).is_null_at(0));
        EXPECT_EQ(3, predicate_files.get_column(2)[0].get<TYPE_BIGINT>());

        IColumn::Filter selected;
        selected.assign({1, 1, 1, 1, 1, 0, 1});
        if (empty_selection) selected.assign({0, 0, 0, 0, 0, 0, 0});
        const size_t selected_rows = empty_selection ? 0 : 6;
        ASSERT_EQ(selected_rows, actual->filter(selected));
        auto expected = source.column->clone_resized(source.column->size());
        ASSERT_EQ(selected_rows, expected->filter(selected));
        _iterator->set_read_phase(ColumnIterator::ReadPhase::LAZY);
        _iterator->finalize_lazy_phase(actual);
        const rowid_t rowids[] = {0, 1, 2, 3, 4, 6};
        ASSERT_TRUE(_iterator->read_by_rowids(rowids, selected_rows, actual).ok());
        expect_container_shape_equal(*expected, *actual, is_map);
        const auto& actual_files = container_files(*actual, is_map);
        expect_file_values_equal(container_files(*expected, is_map), actual_files);
        const auto& file = assert_cast<const ColumnFile&>(
                assert_cast<const ColumnNullable&>(actual_files).get_nested_column());
        ASSERT_EQ(6, file.tuple_size());
        for (size_t child = 0; child < 6; ++child) {
            EXPECT_EQ(empty_selection ? 0 : 5, file.get_column(child).size());
        }
    }

    void track_prefetch_initialization(std::array<size_t, 7>& calls) {
        auto& iterator = assert_cast<FileValueColumnIterator&>(*_iterator);
        for (size_t i = 0; i < 6; ++i) {
            const auto requirement = iterator._sub_column_iterators[i]->read_requirement();
            auto tracker = std::make_unique<FilePrefetchTrackingIterator>(&calls[i]);
            tracker->set_read_requirement(requirement);
            iterator._sub_column_iterators[i] = std::move(tracker);
        }
        iterator._null_iterator = std::make_unique<FilePrefetchTrackingIterator>(&calls[6]);
    }

    const std::string _dir = "./ut_dir/file_column_storage_test";
    size_t _write_count = 0;
    EncodingTypePB _string_encoding = PLAIN_ENCODING;
    bool _old_page_cache = false;
    TabletColumn _schema = file_storage_schema();
    ColumnMetaPB _meta;
    io::FileReaderSPtr _input;
    std::shared_ptr<ColumnReader> _reader;
    ColumnIteratorUPtr _iterator;
    OlapReaderStatistics _stats;
};

// Catches STRING/VARBINARY confusion, lost empty-vs-NULL bytes, and parent-null loss.
TEST_F(FileColumnStorageTest, BinaryInlineRoundTripAndSparseRead) {
    ASSERT_TRUE(write_and_open().ok());
    ASSERT_TRUE(init_iterator().ok());
    auto result = destination();
    const rowid_t rows[] = {0, 1, 2, 3};
    ASSERT_TRUE(_iterator->read_by_rowids(rows, 4, result).ok());
    const auto& parent = assert_cast<const ColumnNullable&>(*result);
    const auto& file = assert_cast<const ColumnFile&>(parent.get_nested_column());
    ASSERT_EQ(6, file.tuple_size());
    const auto& child = assert_cast<const ColumnNullable&>(file.get_column(5));
    const auto& bytes = assert_cast<const ColumnVarbinary&>(child.get_nested_column());
    EXPECT_EQ(std::string("\0\xff\x80", 3), bytes.get_data_at(0).to_string());
    EXPECT_FALSE(child.is_null_at(1));
    EXPECT_EQ(0, bytes.get_data_at(1).size);
    EXPECT_TRUE(child.is_null_at(2));
    EXPECT_TRUE(parent.is_null_at(3));
    EXPECT_EQ(7, _meta.children_columns_size());
    for (int i = 0; i < 6; ++i) {
        EXPECT_EQ(101 + i, _meta.children_columns(i).unique_id());
    }
    auto sparse = destination();
    const rowid_t sparse_rows[] = {0, 3};
    ASSERT_TRUE(_iterator->read_by_rowids(sparse_rows, 2, sparse).ok());
    EXPECT_EQ(2, sparse->size());
    EXPECT_TRUE(sparse->is_null_at(1));
}

// Catches pruning that renumbers/deletes the canonical six children.
TEST_F(FileColumnStorageTest, UriProjectionPreservesSixChildShape) {
    ASSERT_TRUE(write_and_open().ok());
    _iterator->set_column_name("f");
    ASSERT_TRUE(_iterator->set_access_paths({access_path(TAccessPathType::DATA, {"f", "uri"})}, {})
                        .ok());
    _iterator->remove_pruned_sub_iterators();
    ASSERT_TRUE(init_iterator().ok());
    auto result = destination();
    size_t count = 4;
    bool has_null = false;
    ASSERT_TRUE(_iterator->next_batch(&count, result, &has_null).ok());
    const auto& parent = assert_cast<const ColumnNullable&>(*result);
    const auto& file = assert_cast<const ColumnFile&>(parent.get_nested_column());
    EXPECT_EQ(6, file.tuple_size());
    const auto& uri = assert_cast<const ColumnNullable&>(file.get_column(0));
    EXPECT_EQ("s3://bucket/object", uri.get_nested_column().get_data_at(0).to_string());
    EXPECT_TRUE(file.get_column(5).is_null_at(0));
    EXPECT_TRUE(parent.is_null_at(3));
}

TEST_F(FileColumnStorageTest, ParentNullBitmapOnly) {
    ASSERT_TRUE(write_and_open().ok());
    _iterator->set_column_name("f");
    ASSERT_TRUE(_iterator
                        ->set_access_paths({access_path(TAccessPathType::META,
                                                        {"f", ColumnIterator::ACCESS_NULL})},
                                           {})
                        .ok());
    ASSERT_TRUE(init_iterator().ok());
    auto result = destination();
    const rowid_t rows[] = {0, 3};
    ASSERT_TRUE(_iterator->read_by_rowids(rows, 2, result).ok());
    EXPECT_FALSE(result->is_null_at(0));
    EXPECT_TRUE(result->is_null_at(1));
    const auto& file = assert_cast<const ColumnFile&>(
            assert_cast<const ColumnNullable&>(*result).get_nested_column());
    EXPECT_TRUE(file.get_column(0).is_null_at(0));
    EXPECT_EQ(6, file.tuple_size());
}

TEST_F(FileColumnStorageTest, ParentNullOnlyReadFeedsCountAndNullPredicates) {
    ASSERT_TRUE(write_and_open().ok());
    _iterator->set_column_name("f");
    ASSERT_TRUE(_iterator->set_access_paths({access_path(TAccessPathType::META, {"f", "NULL"})}, {})
                        .ok());
    ASSERT_TRUE(init_iterator().ok());
    auto values = destination();
    size_t rows = 4;
    bool has_null = false;
    ASSERT_TRUE(_iterator->next_batch(&rows, values, &has_null).ok());
    ASSERT_EQ(4, rows);
    AggregateFunctionCountNotNullUnary count({make_nullable(std::make_shared<DataTypeFile>())});
    AggregateFunctionGuard aggregate(&count);
    Arena arena;
    const IColumn* columns[] = {values.get()};
    count.add_batch_single_place(rows, aggregate.data(), columns, arena);
    auto output = ColumnInt64::create();
    count.insert_result_into(aggregate.data(), *output);
    EXPECT_EQ(3, output->get_data()[0]);
    for (bool is_null : {false, true}) {
        std::shared_ptr<ColumnPredicate> predicate =
                NullPredicate::create_shared(0, "f", is_null, TYPE_FILE);
        uint16_t selected[] = {0, 1, 2, 3};
        const auto matches = predicate->evaluate(*values, selected, 4);
        ASSERT_EQ(is_null ? 1 : 3, matches);
        EXPECT_EQ(is_null ? 3 : 0, selected[0]);
    }
    const auto& file = assert_cast<const ColumnFile&>(
            assert_cast<const ColumnNullable&>(*values).get_nested_column());
    ASSERT_EQ(6, file.tuple_size());
    for (size_t child = 0; child < 6; ++child) {
        EXPECT_TRUE(file.get_column(child).is_null_at(0));
        EXPECT_EQ(ColumnIterator::ReadRequirement::SKIP,
                  assert_cast<FileValueColumnIterator&>(*_iterator)
                          ._sub_column_iterators[child]
                          ->read_requirement());
    }
}

TEST_F(FileColumnStorageTest, ParentNullRequestDoesNotPruneCompleteFileConsumer) {
    ASSERT_TRUE(write_and_open().ok());
    _iterator->set_column_name("f");
    ASSERT_TRUE(_iterator
                        ->set_access_paths({access_path(TAccessPathType::META, {"f", "NULL"}),
                                            access_path(TAccessPathType::DATA, {"f"})},
                                           {})
                        .ok());
    ASSERT_FALSE(_iterator->read_null_map_only());
    ASSERT_TRUE(init_iterator().ok());
    auto actual = destination();
    size_t rows = 4;
    bool has_null = false;
    ASSERT_TRUE(_iterator->next_batch(&rows, actual, &has_null).ok());
    const auto expected = file_values();
    expect_file_values_equal(*expected, *actual);
}

TEST_F(FileColumnStorageTest, SelectedSizeGetterUsesScalarPredicateWithoutInlineRead) {
    ASSERT_TRUE(write_and_open().ok());
    _iterator->set_column_name("f");
    ASSERT_TRUE(_iterator->set_access_paths({access_path(TAccessPathType::DATA, {"f", "size"})}, {})
                        .ok());
    ASSERT_TRUE(init_iterator().ok());
    auto values = destination();
    const rowid_t rowids[] = {0, 2, 3};
    ASSERT_TRUE(_iterator->read_by_rowids(rowids, 3, values).ok());
    ColumnPtr sizes;
    ASSERT_TRUE(file_size_getter(values->get_ptr(), &sizes).ok());
    auto predicate = create_comparison_predicate<PredicateType::GT>(
            0, "size", make_nullable(std::make_shared<DataTypeInt64>()),
            Field::create_field<TYPE_BIGINT>(Int64 {2}), false);
    uint16_t selected[] = {0, 1, 2};
    ASSERT_EQ(1, predicate->evaluate(*sizes, selected, 3));
    EXPECT_EQ(0, selected[0]);
    EXPECT_EQ(3, (*sizes)[0].get<TYPE_BIGINT>());
    EXPECT_TRUE(sizes->is_null_at(1));
    EXPECT_TRUE(sizes->is_null_at(2));
    const auto& file = assert_cast<const ColumnFile&>(
            assert_cast<const ColumnNullable&>(*values).get_nested_column());
    ASSERT_EQ(6, file.tuple_size());
    EXPECT_TRUE(file.get_column(0).is_null_at(0));
    EXPECT_TRUE(file.get_column(5).is_null_at(0));
    EXPECT_EQ(ColumnIterator::ReadRequirement::SKIP,
              assert_cast<FileValueColumnIterator&>(*_iterator)
                      ._sub_column_iterators[5]
                      ->read_requirement());
}

TEST_F(FileColumnStorageTest, SizeNullOnlyGetterCombinesChildAndParentNulls) {
    ASSERT_TRUE(write_and_open().ok());
    _iterator->set_column_name("f");
    ASSERT_TRUE(_iterator
                        ->set_access_paths(
                                {access_path(TAccessPathType::META, {"f", "size", "NULL"})}, {})
                        .ok());
    ASSERT_TRUE(init_iterator().ok());
    auto values = destination();
    size_t rows = 4;
    bool has_null = false;
    ASSERT_TRUE(_iterator->next_batch(&rows, values, &has_null).ok());
    ColumnPtr sizes;
    ASSERT_TRUE(file_size_getter(values->get_ptr(), &sizes).ok());
    EXPECT_FALSE(sizes->is_null_at(0));
    for (size_t row = 1; row < rows; ++row) EXPECT_TRUE(sizes->is_null_at(row));
    auto& reader = assert_cast<FileValueColumnIterator&>(*_iterator);
    EXPECT_TRUE(reader._sub_column_iterators[2]->read_null_map_only());
    EXPECT_EQ(ColumnIterator::ReadRequirement::SKIP,
              reader._sub_column_iterators[5]->read_requirement());
}

TEST_F(FileColumnStorageTest, SegmentFiltersParentNullMapWithCanonicalLocalFileShape) {
    const auto schema = file_tablet_schema();
    const auto path = _dir + "/null-predicate.dat";
    ASSERT_TRUE(write_vertical_segment(path, schema, file_tablet_block(schema)).ok());
    RuntimeState state;
    TQueryOptions query_options;
    query_options.__set_enable_prune_nested_column(true);
    state.set_query_options(query_options);
    for (bool is_null : {false, true}) {
        StorageReadOptions options;
        options.io_ctx.reader_type = ReaderType::READER_QUERY;
        options.runtime_state = &state;
        options.all_access_paths[17] = {access_path(TAccessPathType::META, {"f", "NULL"})};
        options.column_predicates.push_back(
                NullPredicate::create_shared(1, "f", is_null, TYPE_FILE));
        Block actual;
        const auto status = read_segment(path, schema, std::move(options), &actual);
        ASSERT_TRUE(status.ok()) << "is_null=" << is_null << ": " << status;
        ASSERT_EQ(is_null ? 1 : 3, actual.rows());
        const auto& parent = assert_cast<const ColumnNullable&>(*actual.get_by_position(1).column);
        const auto& file = assert_cast<const ColumnFile&>(parent.get_nested_column());
        ASSERT_EQ(6, file.tuple_size());
        for (size_t row = 0; row < actual.rows(); ++row) {
            EXPECT_EQ(is_null, parent.is_null_at(row));
            EXPECT_EQ(is_null ? 3 : row, (*actual.get_by_position(0).column)[row].get<TYPE_INT>());
            for (size_t child = 0; child < 6; ++child) {
                EXPECT_TRUE(file.get_column(child).is_null_at(row));
            }
        }
    }
}

TEST_F(FileColumnStorageTest, PrefetchDoesNotInitializePrunedChildren) {
    for (const bool null_only : {false, true}) {
        ASSERT_TRUE(write_and_open().ok());
        _iterator->set_column_name("f");
        auto path = null_only
                            ? access_path(TAccessPathType::META, {"f", ColumnIterator::ACCESS_NULL})
                            : access_path(TAccessPathType::DATA, {"f", "uri"});
        ASSERT_TRUE(_iterator->set_access_paths({path}, {}).ok());
        std::array<size_t, 7> calls {};
        track_prefetch_initialization(calls);
        StorageReadOptions options;
        SegmentPrefetchParams params {.config = {4, 1024}, .read_options = options};
        ASSERT_TRUE(_iterator->init_prefetcher(params).ok());
        EXPECT_EQ(null_only ? 0 : 1, calls[0]);
        for (size_t i = 1; i < 6; ++i) {
            EXPECT_EQ(0, calls[i]) << i;
        }
        EXPECT_EQ(1, calls[6]);
    }
}

TEST_F(FileColumnStorageTest, PrefetchIncludesLazyChildrenBeforePredicateRead) {
    ASSERT_TRUE(write_and_open().ok());
    _iterator->set_column_name("f");
    ASSERT_TRUE(_iterator
                        ->set_access_paths({access_path(TAccessPathType::DATA, {"f"})},
                                           {access_path(TAccessPathType::DATA, {"f", "uri"})})
                        .ok());
    std::array<size_t, 7> calls {};
    track_prefetch_initialization(calls);
    _iterator->set_read_phase(ColumnIterator::ReadPhase::PREDICATE);
    StorageReadOptions options;
    SegmentPrefetchParams params {.config = {4, 1024}, .read_options = options};
    ASSERT_TRUE(_iterator->init_prefetcher(params).ok());
    for (size_t i = 0; i < calls.size(); ++i) {
        EXPECT_EQ(1, calls[i]) << i;
    }
}

TEST_F(FileColumnStorageTest, RejectsMalformedPersistedChildSchema) {
    ColumnPB schema;
    _schema.to_schema_pb(&schema);
    schema.mutable_children_columns(5)->set_type("VARCHAR");
    EXPECT_THROW(TabletColumn().init_from_pb(schema), Exception);
    schema.Clear();
    _schema.to_schema_pb(&schema);
    schema.mutable_children_columns(5)->set_is_nullable(false);
    EXPECT_THROW(TabletColumn().init_from_pb(schema), Exception);
    schema.Clear();
    _schema.to_schema_pb(&schema);
    schema.mutable_children_columns(0)->set_name("URI");
    EXPECT_THROW(TabletColumn().init_from_pb(schema), Exception);
}

TEST_F(FileColumnStorageTest, ArrayElementsRestoreFileAndInline) {
    TabletColumn array(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                       FieldType::OLAP_FIELD_TYPE_ARRAY, false, 18, 0);
    array.set_name("arr");
    array.add_sub_column(_schema);
    _schema = array;
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().assign({3, 4});
    auto type = std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFile>()));
    ColumnWithTypeAndName source {ColumnArray::create(file_values(), std::move(offsets)), type,
                                  "arr"};
    ASSERT_TRUE(write_and_open(source).ok());
    ASSERT_TRUE(init_iterator().ok());
    auto result = type->create_column();
    size_t count = 2;
    bool has_null = false;
    ASSERT_TRUE(_iterator->next_batch(&count, result, &has_null).ok());
    const auto& values = assert_cast<const ColumnArray&>(*result);
    EXPECT_EQ(3, values.get_offsets()[0]);
    EXPECT_EQ(4, values.get_offsets()[1]);
    const auto& nullable_file = assert_cast<const ColumnNullable&>(values.get_data());
    const auto& file = assert_cast<const ColumnFile&>(nullable_file.get_nested_column());
    const auto& bytes = assert_cast<const ColumnNullable&>(file.get_column(5));
    EXPECT_EQ(std::string("\0\xff\x80", 3), bytes.get_nested_column().get_data_at(0).to_string());
    EXPECT_TRUE(nullable_file.is_null_at(3));
}

TEST_F(FileColumnStorageTest, ArrayFileChildProjectionAcrossSparseRows) {
    check_container_child_projection(false);
}

TEST_F(FileColumnStorageTest, MapFileChildProjectionPreservesKeysAcrossSparseRows) {
    check_container_child_projection(true);
}

TEST_F(FileColumnStorageTest, ArrayFileLazyRecoveryPreservesFilteredOffsetsAndNulls) {
    check_container_lazy_recovery(false);
}

TEST_F(FileColumnStorageTest, MapFileLazyRecoveryPreservesFilteredKeysOffsetsAndNulls) {
    check_container_lazy_recovery(true);
}

TEST_F(FileColumnStorageTest, NestedFileLazyRecoveryHandlesEmptySelection) {
    check_container_lazy_recovery(false, true);
    check_container_lazy_recovery(true, true);
}

TEST_F(FileColumnStorageTest, EmptyArraysDoNotSeekEmptyFileStreams) {
    TabletColumn array(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                       FieldType::OLAP_FIELD_TYPE_ARRAY, false, 18, 0);
    array.set_name("arr");
    array.add_sub_column(_schema);
    _schema = array;
    auto type = std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFile>()));
    auto values = type->create_column();
    values->insert_many_defaults(2);
    ColumnWithTypeAndName source {std::move(values), type, "arr"};
    ASSERT_TRUE(write_and_open(source).ok());
    ASSERT_TRUE(init_iterator().ok());
    auto result = type->create_column();
    const rowid_t rows[] = {0, 1};
    ASSERT_TRUE(_iterator->read_by_rowids(rows, 2, result).ok());
    const auto& restored = assert_cast<const ColumnArray&>(*result);
    EXPECT_EQ(2, restored.size());
    EXPECT_EQ(0, restored.get_data().size());
    EXPECT_EQ(0, restored.get_offsets()[0]);
    EXPECT_EQ(0, restored.get_offsets()[1]);
}

TEST_F(FileColumnStorageTest, LazyInlineRecoveryAfterFiltering) {
    ASSERT_TRUE(write_and_open().ok());
    _iterator->set_column_name("f");
    ASSERT_TRUE(_iterator
                        ->set_access_paths({access_path(TAccessPathType::DATA, {"f"})},
                                           {access_path(TAccessPathType::DATA, {"f", "uri"})})
                        .ok());
    ASSERT_TRUE(init_iterator().ok());
    auto result = destination();
    _iterator->set_read_phase(ColumnIterator::ReadPhase::PREDICATE);
    size_t count = 4;
    bool has_null = false;
    ASSERT_TRUE(_iterator->next_batch(&count, result, &has_null).ok());
    IColumn::Filter filter;
    filter.assign({1, 0, 0, 1});
    ASSERT_EQ(2, result->filter(filter));
    _iterator->set_read_phase(ColumnIterator::ReadPhase::LAZY);
    _iterator->finalize_lazy_phase(result);
    const rowid_t rows[] = {0, 3};
    ASSERT_TRUE(_iterator->read_by_rowids(rows, 2, result).ok());
    const auto& parent = assert_cast<const ColumnNullable&>(*result);
    const auto& file = assert_cast<const ColumnFile&>(parent.get_nested_column());
    for (size_t i = 0; i < 6; ++i) {
        EXPECT_EQ(2, file.get_column(i).size());
    }
    const auto& bytes = assert_cast<const ColumnNullable&>(file.get_column(5));
    EXPECT_EQ(std::string("\0\xff\x80", 3), bytes.get_nested_column().get_data_at(0).to_string());
    EXPECT_TRUE(parent.is_null_at(1));
}

TEST_F(FileColumnStorageTest, NonNullSegmentCanBeReadAfterNullablePromotion) {
    _schema.set_is_nullable(false);
    auto parent = file_values();
    auto values = assert_cast<const ColumnNullable&>(*parent).get_nested_column().clone_resized(3);
    ColumnWithTypeAndName source {std::move(values), std::make_shared<DataTypeFile>(), "f"};
    ASSERT_TRUE(write_and_open(source).ok());
    ASSERT_EQ(6, _meta.children_columns_size());
    ASSERT_TRUE(init_iterator().ok());
    auto result = destination();
    size_t count = 3;
    bool has_null = false;
    ASSERT_TRUE(_iterator->next_batch(&count, result, &has_null).ok());
    EXPECT_EQ(3, result->size());
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_FALSE(result->is_null_at(i));
    }
}

TEST_F(FileColumnStorageTest, RejectsInvalidValueBeforeWritingChildren) {
    auto values = file_values();
    auto& file =
            assert_cast<ColumnFile&>(assert_cast<ColumnNullable&>(*values).get_nested_column());
    assert_cast<ColumnNullable&>(file.get_column(0)).get_null_map_data()[0] = 1;
    OlapBlockDataConvertor converter;
    converter.add_column_data_convertor(_schema);
    ColumnWithTypeAndName source {std::move(values),
                                  make_nullable(std::make_shared<DataTypeFile>()), "f"};
    ASSERT_TRUE(converter.set_source_content_with_specifid_column(source, 0, 4, 0).ok());
    EXPECT_FALSE(converter.convert_column_data(0).first.ok());
}

TEST_F(FileColumnStorageTest, NullStructAncestorHidesDefaultFileChildren) {
    auto file_schema = _schema;
    file_schema.set_is_nullable(false);
    TabletColumn struct_schema(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                               FieldType::OLAP_FIELD_TYPE_STRUCT);
    struct_schema.set_is_nullable(true);
    struct_schema.add_sub_column(file_schema);
    auto type = std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeFile>()},
                                                 Strings {"f"});
    auto nested = type->create_column();
    nested->insert_default();
    auto nulls = ColumnUInt8::create();
    nulls->get_data().push_back(1);
    ColumnWithTypeAndName source {ColumnNullable::create(std::move(nested), std::move(nulls)),
                                  make_nullable(type), "s"};
    OlapBlockDataConvertor converter;
    converter.add_column_data_convertor(struct_schema);
    ASSERT_TRUE(converter.set_source_content_with_specifid_column(source, 0, 1, 0).ok());
    auto [status, accessor] = converter.convert_column_data(0);
    EXPECT_TRUE(status.ok()) << status;
    ASSERT_NE(nullptr, accessor);
    EXPECT_EQ(1, accessor->get_nullmap()[0]);
}

// The same complete values go through the read -> convert -> write path used by rewrites.
TEST_F(FileColumnStorageTest, RewritePreservesAllChildrenAndNullLevels) {
    _string_encoding = DICT_ENCODING;
    ASSERT_TRUE(write_and_open().ok());
    ASSERT_TRUE(init_iterator().ok());
    auto first = destination();
    size_t count = 4;
    bool has_null = false;
    ASSERT_TRUE(_iterator->next_batch(&count, first, &has_null).ok());
    ASSERT_EQ(4, count);
    ColumnWithTypeAndName source {first->get_ptr(), make_nullable(std::make_shared<DataTypeFile>()),
                                  "f"};
    ASSERT_TRUE(write_and_open(source, 1).ok());
    ASSERT_TRUE(init_iterator().ok());
    auto second = destination();
    count = 8;
    ASSERT_TRUE(_iterator->next_batch(&count, second, &has_null).ok());
    ASSERT_EQ(4, count);
    const auto& before = assert_cast<const ColumnNullable&>(*first);
    const auto& after = assert_cast<const ColumnNullable&>(*second);
    const auto& before_file = assert_cast<const ColumnFile&>(before.get_nested_column());
    const auto& after_file = assert_cast<const ColumnFile&>(after.get_nested_column());
    for (size_t row = 0; row < count; ++row) {
        EXPECT_EQ(before.is_null_at(row), after.is_null_at(row));
        for (size_t child = 0; child < 6; ++child) {
            EXPECT_EQ(before_file.get_column(child)[row], after_file.get_column(child)[row]);
        }
    }
    count = 1;
    ASSERT_TRUE(_iterator->next_batch(&count, second, &has_null).ok());
    EXPECT_EQ(0, count);
}

TEST_F(FileColumnStorageTest, MapValuesRestoreFileAndInline) {
    TabletColumn key(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                     FieldType::OLAP_FIELD_TYPE_INT, true);
    key.set_name("key");
    key.set_unique_id(18);
    TabletColumn map(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                     FieldType::OLAP_FIELD_TYPE_MAP, false, 19, 0);
    map.set_name("m");
    map.add_sub_column(key);
    map.add_sub_column(_schema);
    _schema = map;
    auto key_type = make_nullable(std::make_shared<DataTypeInt32>());
    auto keys = key_type->create_column();
    for (int i = 0; i < 4; ++i) {
        keys->insert(Field::create_field<TYPE_INT>(i));
    }
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().assign({3, 4});
    auto type = std::make_shared<DataTypeMap>(key_type,
                                              make_nullable(std::make_shared<DataTypeFile>()));
    ColumnWithTypeAndName source {
            ColumnMap::create(std::move(keys), file_values(), std::move(offsets)), type, "m"};
    ASSERT_TRUE(write_and_open(source, 1).ok());
    ASSERT_TRUE(init_iterator().ok());
    auto result = type->create_column();
    const rowid_t rows[] = {0, 1};
    ASSERT_TRUE(_iterator->read_by_rowids(rows, 2, result).ok());
    const auto& values = assert_cast<const ColumnMap&>(*result);
    EXPECT_EQ(3, values.get_offsets()[0]);
    EXPECT_EQ(4, values.get_offsets()[1]);
    const auto& nullable_file = assert_cast<const ColumnNullable&>(values.get_values());
    const auto& file = assert_cast<const ColumnFile&>(nullable_file.get_nested_column());
    const auto& bytes = assert_cast<const ColumnNullable&>(file.get_column(5));
    EXPECT_EQ(std::string("\0\xff\x80", 3), bytes.get_nested_column().get_data_at(0).to_string());
    EXPECT_FALSE(bytes.is_null_at(1));
    EXPECT_EQ(0, bytes.get_nested_column().get_data_at(1).size);
    EXPECT_TRUE(bytes.is_null_at(2));
    EXPECT_TRUE(nullable_file.is_null_at(3));
}

TEST_F(FileColumnStorageTest, StructAncestorNullRoundTripAndLocalProjection) {
    auto file_schema = _schema;
    file_schema.set_is_nullable(false);
    TabletColumn structure(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                           FieldType::OLAP_FIELD_TYPE_STRUCT, true, 18, 0);
    structure.set_name("s");
    structure.add_sub_column(file_schema);
    _schema = structure;
    auto nullable_file = file_values();
    const auto& file = assert_cast<const ColumnNullable&>(*nullable_file);
    MutableColumns children;
    children.push_back(file.get_nested_column().clone_resized(4));
    auto type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeFile>()}, Strings {"f"}));
    ColumnWithTypeAndName source {
            ColumnNullable::create(ColumnStruct::create(std::move(children)),
                                   file.get_null_map_column().clone_resized(4)),
            type, "s"};
    ASSERT_TRUE(write_and_open(source).ok());
    _iterator->set_column_name("s");
    ASSERT_TRUE(
            _iterator->set_access_paths({access_path(TAccessPathType::DATA, {"s", "f", "uri"})}, {})
                    .ok());
    _iterator->remove_pruned_sub_iterators();
    ASSERT_TRUE(init_iterator().ok());
    auto result = type->create_column();
    size_t count = 4;
    bool has_null = false;
    ASSERT_TRUE(_iterator->next_batch(&count, result, &has_null).ok());
    EXPECT_TRUE(result->is_null_at(3));
    const auto& structure_result = assert_cast<const ColumnStruct&>(
            assert_cast<const ColumnNullable&>(*result).get_nested_column());
    const auto& file_result = assert_cast<const ColumnFile&>(structure_result.get_column(0));
    EXPECT_EQ(6, file_result.tuple_size());
    EXPECT_EQ("s3://bucket/object", assert_cast<const ColumnNullable&>(file_result.get_column(0))
                                            .get_nested_column()
                                            .get_data_at(0)
                                            .to_string());
}

TEST_F(FileColumnStorageTest, RejectsMalformedSegmentChildren) {
    ASSERT_TRUE(write_and_open().ok());
    const auto valid_meta = _meta;
    auto check_rejected = [&] {
        std::shared_ptr<ColumnReader> reader;
        EXPECT_FALSE(ColumnReader::create(ColumnReaderOptions {}, _meta, 4, _input, &reader).ok());
        _meta = valid_meta;
    };
    _meta.mutable_children_columns()->RemoveLast();
    check_rejected();
    _meta.mutable_children_columns(5)->set_type(
            static_cast<int>(FieldType::OLAP_FIELD_TYPE_VARCHAR));
    check_rejected();
    _meta.mutable_children_columns(0)->set_is_nullable(false);
    check_rejected();
    _meta.mutable_children_columns(1)->set_num_rows(3);
    check_rejected();
    _meta.mutable_children_columns(6)->set_is_nullable(true);
    check_rejected();
}

TEST_F(FileColumnStorageTest, WriteValidationUsesSelectedRowsAndAncestorNulls) {
    auto values = file_values();
    auto& parent = assert_cast<ColumnNullable&>(*values);
    auto& file = assert_cast<ColumnFile&>(parent.get_nested_column());
    assert_cast<ColumnNullable&>(file.get_column(0)).get_null_map_data()[0] = 1;
    OlapBlockDataConvertor converter;
    converter.add_column_data_convertor(_schema);
    ColumnWithTypeAndName source {values->get_ptr(),
                                  make_nullable(std::make_shared<DataTypeFile>()), "f"};
    ASSERT_TRUE(converter.set_source_content_with_specifid_column(source, 1, 3, 0).ok());
    EXPECT_TRUE(converter.convert_column_data(0).first.ok());
    ASSERT_TRUE(converter.set_source_content_with_specifid_column(source, 0, 4, 0).ok());
    EXPECT_FALSE(converter.convert_column_data(0).first.ok());
    parent.get_null_map_data()[0] = 1;
    EXPECT_TRUE(converter.convert_column_data(0).first.ok());
}

TEST_F(FileColumnStorageTest, AddedNullableFileDefaultsToParentNull) {
    DefaultValueColumnIterator iterator(false, "", true, FieldType::OLAP_FIELD_TYPE_FILE, 0, 0, 0);
    ColumnIteratorOptions options;
    ASSERT_TRUE(iterator.init(options).ok());
    auto result = destination();
    size_t count = 3;
    bool has_null = false;
    ASSERT_TRUE(iterator.next_batch(&count, result, &has_null).ok());
    EXPECT_TRUE(has_null);
    ASSERT_EQ(3, result->size());
    for (size_t row = 0; row < 3; ++row) {
        EXPECT_TRUE(result->is_null_at(row));
    }
    EXPECT_TRUE(
            validate_file_column(*result, make_nullable(std::make_shared<DataTypeFile>())).ok());
}

TEST_F(FileColumnStorageTest, ThriftMetadataUsesFileOnlyBinaryCarrier) {
    TColumn column;
    column.__set_column_name("f");
    column.column_type.__set_type(TPrimitiveType::FILE);
    column.__set_is_allow_null(true);
    column.__set_aggregation_type(TAggregationType::NONE);
    const TPrimitiveType::type types[] = {TPrimitiveType::VARCHAR, TPrimitiveType::BIGINT,
                                          TPrimitiveType::BIGINT,  TPrimitiveType::VARCHAR,
                                          TPrimitiveType::VARCHAR, TPrimitiveType::VARBINARY};
    for (int i = 0; i < 6; ++i) {
        TColumn child;
        child.__set_column_name(_schema.get_sub_column(i).name());
        child.column_type.__set_type(types[i]);
        child.column_type.__set_len(i == 0 ? 65533 : 1024);
        child.__set_is_allow_null(true);
        child.__set_col_unique_id(101 + i);
        child.__set_aggregation_type(TAggregationType::NONE);
        column.children_column.push_back(child);
    }
    ColumnPB pb;
    TabletMeta::init_column_from_tcolumn(17, column, &pb);
    EXPECT_EQ("FILE", pb.type());
    ASSERT_EQ(6, pb.children_columns_size());
    EXPECT_EQ("STRING", pb.children_columns(5).type());
    TabletColumn restored;
    restored.init_from_pb(pb);
    // The FE length includes the VARCHAR storage prefix. Logical FILE metadata must
    // restore its canonical lengths instead of using the generic VARCHAR(-1) type.
    EXPECT_EQ(TabletColumn::get_field_length_by_type(TPrimitiveType::VARCHAR, 65533),
              restored.get_sub_column(0).length());
    const auto logical_type = remove_nullable(restored.get_vec_type());
    const auto& logical_file = assert_cast<const DataTypeFile&>(*logical_type);
    for (size_t child : {0, 3, 4}) {
        const auto type = remove_nullable(logical_file.get_element(child));
        EXPECT_EQ(TYPE_VARCHAR, type->get_primitive_type());
        EXPECT_EQ(child == 0 ? 65533 : 1024, assert_cast<const DataTypeString&>(*type).len());
    }
    EXPECT_EQ(TYPE_VARBINARY, remove_nullable(logical_file.get_element(5))->get_primitive_type());
    for (int i = 0; i < 6; ++i) {
        EXPECT_EQ(101 + i, restored.get_sub_column(i).unique_id());
    }
    column.children_column[5].column_type.__set_type(TPrimitiveType::STRING);
    EXPECT_THROW(TabletMeta::init_column_from_tcolumn(17, column, &pb), Exception);
}

TEST_F(FileColumnStorageTest, TabletSchemaRoundTripPreservesIdentityAndChildIds) {
    ColumnPB schema;
    _schema.to_schema_pb(&schema);
    TabletColumn restored;
    restored.init_from_pb(schema);
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_FILE, restored.type());
    ASSERT_EQ(6, restored.get_subtype_count());
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_STRING, restored.get_sub_column(5).type());
    const auto logical = remove_nullable(restored.get_vec_type());
    EXPECT_EQ(TYPE_FILE, logical->get_primitive_type());
    EXPECT_EQ(TYPE_VARBINARY,
              remove_nullable(assert_cast<const DataTypeFile&>(*logical).get_element(5))
                      ->get_primitive_type());
    for (int i = 0; i < 6; ++i) {
        EXPECT_EQ(101 + i, restored.get_sub_column(i).unique_id());
    }
}

TEST_F(FileColumnStorageTest, LogicalFileTypeRestoresCanonicalChildrenFromStorageCarriers) {
    for (bool nullable : {false, true}) {
        auto schema = file_storage_schema();
        schema.set_is_nullable(nullable);
        for (size_t child : {0, 3, 4}) {
            schema.get_sub_columns()[child]->set_length(TabletColumn::get_field_length_by_type(
                    TPrimitiveType::VARCHAR, child == 0 ? 65533 : 1024));
        }
        const auto type = schema.get_vec_type();
        EXPECT_EQ(nullable, type->is_nullable());
        const auto nested = remove_nullable(type);
        ASSERT_EQ(TYPE_FILE, nested->get_primitive_type());
        const auto& file = assert_cast<const DataTypeFile&>(*nested);
        const DataTypeFile canonical;
        ASSERT_EQ(6, file.get_elements().size());
        EXPECT_EQ(canonical.get_element_names(), file.get_element_names());
        for (size_t child = 0; child < 6; ++child) {
            EXPECT_TRUE(file.get_element(child)->is_nullable());
            const auto actual = remove_nullable(file.get_element(child));
            const auto expected = remove_nullable(canonical.get_element(child));
            EXPECT_EQ(expected->get_primitive_type(), actual->get_primitive_type());
            if (child == 0 || child == 3 || child == 4) {
                EXPECT_EQ(child == 0 ? 65533 : 1024,
                          assert_cast<const DataTypeString&>(*actual).len());
            }
        }
        // Ordinary storage VARCHAR and STRING retain their existing scalar mappings.
        const auto varchar = remove_nullable(schema.get_sub_column(0).get_vec_type());
        EXPECT_EQ(TYPE_VARCHAR, varchar->get_primitive_type());
        EXPECT_EQ(-1, assert_cast<const DataTypeString&>(*varchar).len());
        EXPECT_EQ(TYPE_STRING,
                  remove_nullable(schema.get_sub_column(5).get_vec_type())->get_primitive_type());
    }
}

TEST_F(FileColumnStorageTest, LogicalFileTypeRejectsMalformedPhysicalChildren) {
    for (size_t child = 0; child < 6; ++child) {
        SCOPED_TRACE(child);
        auto wrong_type = file_storage_schema();
        wrong_type.get_sub_columns()[child]->set_type(FieldType::OLAP_FIELD_TYPE_BOOL);
        EXPECT_THROW(wrong_type.get_vec_type(), Exception);
        auto wrong_name = file_storage_schema();
        wrong_name.get_sub_columns()[child]->set_name("unexpected");
        EXPECT_THROW(wrong_name.get_vec_type(), Exception);
        auto nonnullable = file_storage_schema();
        nonnullable.get_sub_columns()[child]->set_is_nullable(false);
        EXPECT_THROW(nonnullable.get_vec_type(), Exception);
    }
    TabletColumn missing_children(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                                  FieldType::OLAP_FIELD_TYPE_FILE, true, 17, 0);
    EXPECT_THROW(missing_children.get_vec_type(), Exception);
}

TEST_F(FileColumnStorageTest, SnapshotHeaderRestoreReopensCompleteNestedFileSegment) {
    auto schema = file_tablet_schema();
    auto source = file_tablet_block(schema);
    const auto source_path = _dir + "/source.dat";
    ASSERT_TRUE(write_vertical_segment(source_path, schema, source).ok());
    TabletMetaPB metadata;
    metadata.set_tablet_id(100);
    metadata.set_tablet_state(PB_RUNNING);
    schema->to_schema_pb(metadata.mutable_schema());
    const auto header = _dir + "/100.hdr";
    ASSERT_TRUE(TabletMeta::save(header, metadata).ok());
    const auto restored_path = _dir + "/restored.dat";
    ASSERT_TRUE(io::global_local_filesystem()->link_file(source_path, restored_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->delete_file(source_path).ok());
    schema.reset();

    TabletMeta restored;
    ASSERT_TRUE(restored.create_from_file(header).ok());
    const auto& restored_schema = restored.tablet_schema();
    const auto& file = restored_schema->column(1);
    const auto& nested_file = restored_schema->column(2).get_sub_column(0);
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_FILE, file.type());
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_FILE, nested_file.type());
    EXPECT_EQ(17, file.unique_id());
    EXPECT_EQ(27, nested_file.unique_id());
    ASSERT_EQ(6, file.get_subtype_count());
    ASSERT_EQ(6, nested_file.get_subtype_count());
    for (size_t child = 0; child < 6; ++child) {
        EXPECT_EQ(101 + child, file.get_sub_column(child).unique_id());
        EXPECT_EQ(201 + child, nested_file.get_sub_column(child).unique_id());
    }
    Block actual;
    ASSERT_TRUE(read_vertical_segment(restored_path, restored_schema, &actual).ok());
    expect_file_blocks_equal(source, actual);
}

TEST_F(FileColumnStorageTest, VerticalCompactionWriterRewritesAllFileChildren) {
    const auto schema = file_tablet_schema();
    const auto expected = file_tablet_block(schema);
    const auto first_path = _dir + "/before-compaction.dat";
    ASSERT_TRUE(write_vertical_segment(first_path, schema, expected).ok());
    Block input;
    ASSERT_TRUE(read_vertical_segment(first_path, schema, &input).ok());
    expect_file_blocks_equal(expected, input);
    const auto second_path = _dir + "/after-compaction.dat";
    ASSERT_TRUE(write_vertical_segment(second_path, schema, input).ok());
    input.clear();
    ASSERT_TRUE(io::global_local_filesystem()->delete_file(first_path).ok());
    Block actual;
    ASSERT_TRUE(read_vertical_segment(second_path, schema, &actual).ok());
    expect_file_blocks_equal(expected, actual);
}

} // namespace
} // namespace doris::segment_v2
