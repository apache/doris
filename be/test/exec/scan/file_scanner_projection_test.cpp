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
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "core/arena.h"
#include "core/column/column_file.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exec/operator/olap_scan_operator.h"
#include "exec/scan/scanner.h"
#include "exprs/aggregate/aggregate_function_count.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/column_writer.h"
#include "storage/tablet/tablet_schema.h"
#include "testutil/desc_tbl_builder.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {
namespace {

using namespace segment_v2;

// Observe physical I/O, not calls to next_batch: skipped scalar iterators still receive
// next_batch calls to append local NULL placeholders.
class ProjectionFileReader final : public io::FileReader {
public:
    explicit ProjectionFileReader(io::FileReaderSPtr reader) : _reader(std::move(reader)) {}
    Status close() override { return _reader->close(); }
    const io::Path& path() const override { return _reader->path(); }
    size_t size() const override { return _reader->size(); }
    bool closed() const override { return _reader->closed(); }
    int64_t mtime() const override { return _reader->mtime(); }

    bool touched(const PagePointerPB& page) const {
        return std::any_of(_reads.begin(), _reads.end(), [&](const auto& read) {
            return read.first < page.offset() + page.size() &&
                   page.offset() < read.first + read.second;
        });
    }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override {
        RETURN_IF_ERROR(_reader->read_at(offset, result, bytes_read, io_ctx));
        _reads.emplace_back(offset, *bytes_read);
        return Status::OK();
    }

private:
    io::FileReaderSPtr _reader;
    std::vector<std::pair<size_t, size_t>> _reads;
};

// Only the source adapter is test-specific. Filtering, padding, expression cloning,
// projection, and release of the original FILE block use the production Scanner.
class FileProjectionScanner final : public Scanner {
public:
    FileProjectionScanner(RuntimeState* state, ScanLocalStateBase* local_state,
                          RuntimeProfile* profile, ColumnIterator* iterator)
            : Scanner(state, local_state, -1, profile), _iterator(iterator) {}

    const Block& origin_block() const { return _origin_block; }

protected:
    Status _get_block_impl(RuntimeState*, Block* block, bool* eos) override {
        auto column = IColumn::mutate(block->get_by_position(0).column);
        size_t rows = 2;
        RETURN_IF_ERROR(_iterator->next_batch(&rows, column));
        block->replace_by_position(0, std::move(column));
        *eos = rows == 0;
        return Status::OK();
    }

private:
    ColumnIterator* _iterator;
};

VExprSPtr function(const std::string& name, const DataTypePtr& type, VExprSPtrs children) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    node.__set_type(type->to_thrift());
    node.__set_is_nullable(type->is_nullable());
    node.__set_num_children(children.size());
    TFunction fn;
    TFunctionName function_name;
    function_name.__set_function_name(name);
    fn.__set_name(function_name);
    fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
    node.__set_fn(fn);
    VExprSPtr expression;
    THROW_IF_ERROR(VExpr::create_expr(node, expression));
    for (auto& child : children) expression->add_child(child);
    return expression;
}

TColumnAccessPath access_path(bool null_only) {
    TColumnAccessPath path;
    path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
    path.__set_type(null_only ? TAccessPathType::META : TAccessPathType::DATA);
    if (null_only) {
        TMetaAccessPath meta;
        meta.__set_path({"f", "NULL"});
        path.__set_meta_access_path(meta);
    } else {
        TDataAccessPath data;
        data.__set_path({"f", "size"});
        path.__set_data_access_path(data);
    }
    return path;
}

class FileScannerProjectionTest : public testing::Test {
protected:
    enum class ProjectionKind { SIZE, IS_NULL, COUNT_MARKER };

    void SetUp() override {
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_dir).ok());
    }

    void TearDown() override {
        _iterator.reset();
        _reader.reset();
        _input.reset();
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_dir).ok());
    }

    Status write_and_open(bool null_only) {
        TabletColumn schema(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                            FieldType::OLAP_FIELD_TYPE_FILE, true, 17, 0);
        schema.set_name("f");
        const char* names[] = {"uri", "offset", "size", "content_type", "checksum", "inline"};
        for (size_t i = 0; i < 6; ++i) {
            const auto type = i == 1 || i == 2 ? FieldType::OLAP_FIELD_TYPE_BIGINT
                              : i == 5         ? FieldType::OLAP_FIELD_TYPE_STRING
                                               : FieldType::OLAP_FIELD_TYPE_VARCHAR;
            TabletColumn child(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, type, true);
            child.set_name(names[i]);
            child.set_unique_id(101 + i);
            child.set_length(i == 0 ? 65533 : 1024);
            schema.add_sub_column(child);
        }
        init_meta(schema, &_meta);
        auto source = _file_type->create_column();
        auto& nullable = assert_cast<ColumnNullable&>(*source);
        auto& file = assert_cast<ColumnFile&>(nullable.get_nested_column());
        for (size_t row = 0; row < 4; ++row) {
            for (size_t child = 0; child < 6; ++child) {
                auto& field = assert_cast<ColumnNullable&>(file.get_column(child));
                if (row == 3 || (row == 1 && (child == 1 || child == 2))) {
                    field.insert_default();
                } else if (child == 1 || child == 2) {
                    field.insert(Field::create_field<TYPE_BIGINT>(child == 2 && row == 0 ? 3 : 0));
                } else {
                    const std::string value = child == 0   ? "s3://bucket/never-on-wire.txt"
                                              : child == 3 ? "text/plain"
                                              : child == 4 ? "ETAG:opaque-2"
                                                           : std::string(256, 'x');
                    field.get_nested_column().insert_data(value.data(), value.size());
                    field.get_null_map_data().push_back(0);
                }
            }
            nullable.get_null_map_data().push_back(row == 3);
        }
        ColumnWithTypeAndName input {std::move(source), _file_type, "f"};
        io::FileWriterPtr output;
        RETURN_IF_ERROR(io::global_local_filesystem()->create_file(_dir + "/data", &output));
        ColumnWriterOptions options;
        options.meta = &_meta;
        options.need_zone_map = false;
        options.need_bloom_filter = false;
        std::unique_ptr<ColumnWriter> writer;
        RETURN_IF_ERROR(ColumnWriter::create(options, &schema, output.get(), &writer));
        RETURN_IF_ERROR(writer->init());
        OlapBlockDataConvertor converter;
        converter.add_column_data_convertor(schema);
        RETURN_IF_ERROR(converter.set_source_content_with_specifid_column(input, 0, 4, 0));
        auto [status, accessor] = converter.convert_column_data(0);
        RETURN_IF_ERROR(status);
        RETURN_IF_ERROR(writer->append(accessor->get_nullmap(), accessor->get_data(), 4));
        RETURN_IF_ERROR(writer->finish());
        RETURN_IF_ERROR(writer->write_data());
        RETURN_IF_ERROR(writer->write_ordinal_index());
        RETURN_IF_ERROR(output->close());
        io::FileReaderSPtr input_file;
        RETURN_IF_ERROR(io::global_local_filesystem()->open_file(_dir + "/data", &input_file));
        _input = std::make_shared<ProjectionFileReader>(std::move(input_file));
        RETURN_IF_ERROR(ColumnReader::create({}, _meta, 4, _input, &_reader));
        RETURN_IF_ERROR(_reader->new_iterator(&_iterator, &schema));
        _iterator->set_column_name("f");
        RETURN_IF_ERROR(_iterator->set_access_paths({access_path(null_only)}, {}));
        ColumnIteratorOptions read_options;
        read_options.use_page_cache = false;
        read_options.file_reader = _input.get();
        read_options.stats = &_stats;
        RETURN_IF_ERROR(_iterator->init(read_options));
        return _iterator->seek_to_ordinal(0);
    }

    static void init_meta(const TabletColumn& schema, ColumnMetaPB* meta) {
        meta->set_column_id(schema.unique_id());
        meta->set_unique_id(schema.unique_id());
        meta->set_type(static_cast<int>(schema.type()));
        meta->set_length(schema.length());
        meta->set_is_nullable(schema.is_nullable());
        meta->set_encoding(PLAIN_ENCODING);
        meta->set_compression(CompressionTypePB::LZ4F);
        for (const auto& child : schema.get_sub_columns()) {
            init_meta(*child, meta->add_children_columns());
        }
    }

    void expect_physical_reads(bool null_only) {
        ASSERT_EQ(_meta.children_columns_size(), 7);
        for (int child = 0; child < 7; ++child) {
            const auto& meta = _meta.children_columns(child);
            const auto index =
                    std::find_if(meta.indexes().begin(), meta.indexes().end(),
                                 [](const auto& value) { return value.type() == ORDINAL_INDEX; });
            ASSERT_NE(index, meta.indexes().end());
            const auto& root = index->ordinal_index().root_page();
            // Four rows fit in one page. This gives the exact physical data range without
            // loading any pruned child's index or pages to instrument the test.
            ASSERT_TRUE(root.is_root_data_page());
            EXPECT_EQ(_input->touched(root.root_page()), child == 6 || (!null_only && child == 2))
                    << "physical child " << child;
        }
    }

    void run_projection(ProjectionKind projection, int batch_size) {
        const bool null_only = projection != ProjectionKind::SIZE;
        const bool count_marker = projection == ProjectionKind::COUNT_MARKER;
        auto status = write_and_open(null_only);
        ASSERT_TRUE(status.ok()) << status;
        DataTypePtr scalar_type = make_nullable(std::make_shared<DataTypeInt64>());
        PGenericType::TypeId wire_type = PGenericType::INT64;
        if (projection == ProjectionKind::IS_NULL) {
            scalar_type = std::make_shared<DataTypeUInt8>();
            wire_type = PGenericType::UINT8;
        } else if (count_marker) {
            scalar_type = make_nullable(std::make_shared<DataTypeInt8>());
            wire_type = PGenericType::INT8;
        }
        ObjectPool pool;
        DescriptorTblBuilder builder(&pool);
        builder.declare_tuple() << TupleDescBuilder::SlotType {_file_type, "f"};
        builder.declare_tuple() << TupleDescBuilder::SlotType {scalar_type, "value"};
        auto* descriptors = builder.build();
        ASSERT_NE(descriptors, nullptr);
        // DescriptorTblBuilder makes every slot nullable; mirror the actual scalar
        // projection's nullability (IS NULL itself is not nullable).
        descriptors->get_tuple_descriptor(1)->slots()[0]->_type = scalar_type;
        MockRuntimeState state;
        state._batch_size = batch_size;
        state.set_desc_tbl(descriptors);
        TOlapScanNode scan_node;
        scan_node.__set_tuple_id(0);
        scan_node.__set_keyType(TKeysType::DUP_KEYS);
        TPlanNode plan;
        plan.__set_node_id(0);
        plan.__set_node_type(TPlanNodeType::OLAP_SCAN_NODE);
        plan.__set_num_children(0);
        plan.__set_limit(-1);
        plan.__set_row_tuples({0});
        plan.__set_olap_scan_node(scan_node);
        auto op = std::make_shared<OlapScanOperatorX>(&pool, plan, 0, *descriptors, 1,
                                                      TQueryCacheParam {});
        op->_row_descriptor = RowDescriptor(*descriptors, {0});
        op->_output_tuple_desc = descriptors->get_tuple_descriptor(0);
        op->set_projection_for_test(RowDescriptor(*descriptors, {1}));
        auto local_state = OlapScanLocalState::create_shared(&state, op.get());
        auto slot = VSlotRef::create_shared(op->_output_tuple_desc->slots()[0]);
        VExprSPtr expression =
                null_only ? function("is_null_pred", std::make_shared<DataTypeUInt8>(), {slot})
                          : function("element_at", scalar_type,
                                     {slot, VLiteral::create_shared(
                                                    std::make_shared<DataTypeString>(),
                                                    Field::create_field<TYPE_STRING>("size"))});
        if (count_marker) {
            // Match ProjectFileCount exactly: IF(IsNull(value), NULL::TINYINT, 1::TINYINT).
            // create_expr selects the production VectorizedIfExpr, not a generic fn call.
            TExprNode null_literal;
            null_literal.__set_node_type(TExprNodeType::NULL_LITERAL);
            null_literal.__set_type(scalar_type->to_thrift());
            null_literal.__set_is_nullable(true);
            null_literal.__set_num_children(0);
            auto one = VLiteral::create_shared(std::make_shared<DataTypeInt8>(),
                                               Field::create_field<TYPE_TINYINT>(1));
            expression = function("if", scalar_type,
                                  {expression, VLiteral::create_shared(null_literal), one});
        }
        auto context = std::make_shared<VExprContext>(expression);
        status = context->prepare(&state, op->_row_descriptor);
        ASSERT_TRUE(status.ok()) << status;
        status = context->open(&state);
        ASSERT_TRUE(status.ok()) << status;
        local_state->_projections = {context};
        RuntimeProfile profile("FILE scalar projection");
        FileProjectionScanner scanner(&state, local_state.get(), &profile, _iterator.get());
        status = scanner.init(&state, {});
        ASSERT_TRUE(status.ok()) << status;
        std::array<std::optional<int64_t>, 4> expected =
                null_only
                        ? std::array<std::optional<int64_t>, 4> {0, 0, 0, 1}
                        : std::array<std::optional<int64_t>, 4> {3, std::nullopt, 0, std::nullopt};
        if (count_marker) expected = {1, 1, 1, std::nullopt};
        auto received_markers = scalar_type->create_column();
        Block output;
        bool eos = false;
        size_t row = 0;
        size_t batches = 0;
        while (!eos) {
            output.clear_column_data();
            status = scanner.get_block_after_projects(&state, &output, &eos);
            ASSERT_TRUE(status.ok()) << status;
            if (output.rows() == 0) continue;
            ++batches;
            ASSERT_EQ(output.columns(), 1);
            EXPECT_TRUE(output.get_by_position(0).type->equals(*scalar_type));
            // The local FILE is still six children, but has been emptied after projection.
            ASSERT_EQ(scanner.origin_block().rows(), 0);
            const auto& local_file = assert_cast<const ColumnNullable&>(
                    *scanner.origin_block().get_by_position(0).column);
            EXPECT_EQ(assert_cast<const ColumnFile&>(local_file.get_nested_column()).tuple_size(),
                      6);
            PBlock wire;
            size_t uncompressed = 0;
            size_t compressed = 0;
            int64_t time = 0;
            status = output.serialize(BeExecVersionManager::get_newest_version(), &wire,
                                      &uncompressed, &compressed, &time, NO_COMPRESSION);
            ASSERT_TRUE(status.ok()) << status;
            ASSERT_EQ(wire.column_metas_size(), 1);
            EXPECT_EQ(wire.column_metas(0).type(), wire_type);
            EXPECT_EQ(wire.column_metas(0).is_nullable(), scalar_type->is_nullable());
            EXPECT_EQ(wire.column_metas(0).children_size(), 0);
            EXPECT_EQ(wire.column_values().find("never-on-wire"), std::string::npos);
            EXPECT_EQ(wire.column_values().find(std::string(32, 'x')), std::string::npos);
            Block restored;
            status = restored.deserialize(wire, &uncompressed, &time);
            ASSERT_TRUE(status.ok()) << status;
            ASSERT_EQ(restored.columns(), 1);
            ASSERT_EQ(restored.rows(), output.rows());
            EXPECT_TRUE(restored.get_by_position(0).type->equals(*scalar_type));
            const auto& values = *restored.get_by_position(0).column;
            if (count_marker) {
                const auto& nullable = assert_cast<const ColumnNullable&>(values);
                ASSERT_NE(check_and_get_column<ColumnInt8>(&nullable.get_nested_column()), nullptr);
                received_markers->insert_range_from(values, 0, values.size());
            }
            for (size_t i = 0; i < restored.rows(); ++i, ++row) {
                ASSERT_LT(row, expected.size());
                EXPECT_EQ(values.is_null_at(i), !expected[row].has_value());
                if (expected[row]) {
                    ASSERT_FALSE(values.is_null_at(i));
                    const auto& nested =
                            values.is_nullable()
                                    ? assert_cast<const ColumnNullable&>(values).get_nested_column()
                                    : values;
                    EXPECT_EQ(nested.get_int(i), *expected[row]);
                }
            }
        }
        EXPECT_EQ(row, expected.size());
        EXPECT_EQ(batches, batch_size == 4 ? 2 : 1);
        expect_physical_reads(null_only);
        if (count_marker) {
            // Aggregate only the scalar markers received from PBlock. In particular, a
            // non-NULL FILE whose size is NULL must still count, while a NULL FILE must not.
            AggregateFunctionCountNotNullUnary count({scalar_type});
            AggregateFunctionGuard aggregate(&count);
            Arena arena;
            const IColumn* columns[] = {received_markers.get()};
            count.add_batch_single_place(received_markers->size(), aggregate.data(), columns,
                                         arena);
            auto result = ColumnInt64::create();
            count.insert_result_into(aggregate.data(), *result);
            ASSERT_EQ(result->size(), 1);
            EXPECT_EQ(result->get_data()[0], 3);
        }
    }

    const std::string _dir = "./ut_dir/file_scanner_projection_test";
    const DataTypePtr _file_type = make_nullable(std::make_shared<DataTypeFile>());
    ColumnMetaPB _meta;
    OlapReaderStatistics _stats;
    std::shared_ptr<ProjectionFileReader> _input;
    std::shared_ptr<ColumnReader> _reader;
    ColumnIteratorUPtr _iterator;
};

TEST_F(FileScannerProjectionTest, SelectedSizeCrossesWireAsScalarAndReusesOutput) {
    run_projection(ProjectionKind::SIZE, 4);
}

TEST_F(FileScannerProjectionTest, SelectedSizeSurvivesScannerPaddingBeforeScalarSerialization) {
    run_projection(ProjectionKind::SIZE, 16);
}

TEST_F(FileScannerProjectionTest, ParentNullOnlyCrossesWireAsScalar) {
    run_projection(ProjectionKind::IS_NULL, 4);
}

TEST_F(FileScannerProjectionTest, CountFileMarkerCrossesWireAsNullableTinyIntBeforeAggregate) {
    run_projection(ProjectionKind::COUNT_MARKER, 4);
}

TEST_F(FileScannerProjectionTest, CountFileMarkerSurvivesScannerPaddingBeforeAggregate) {
    run_projection(ProjectionKind::COUNT_MARKER, 16);
}

} // namespace
} // namespace doris
