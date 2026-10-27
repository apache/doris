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

#include "format_v2/table/load_reader.h"

#include <arrow/api.h>
#include <arrow/extension/parquet_variant.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <arrow/util/compression.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <limits>

#include "core/column/column_array.h"
#include "core/column/column_complex.h"
#include "core/column/column_decimal.h"
#include "core/column/column_fixed_length_object.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_agg_state.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_ipv4.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_variant_v2.h"
#include "format/arrow/arrow_block_convertor.h"
#include "format/arrow/arrow_row_batch.h"
#include "format_v2/schema_reader.h"
#include "io/fs/kafka_consumer_pipe.h"
#include "load/stream_load/new_load_stream_mgr.h"
#include "load/stream_load/stream_load_context.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace doris::format {
namespace {

TTypeDesc scalar_type(const DataTypePtr& type) {
    TScalarType scalar;
    scalar.__set_type(doris::to_thrift(type->get_primitive_type()));
    TTypeNode node;
    node.__set_type(TTypeNodeType::SCALAR);
    node.__set_scalar_type(scalar);
    TTypeDesc result;
    result.__set_types({node});
    return result;
}

struct SourceReader {
    std::vector<ColumnDefinition> columns;
    bool all_null = false;
    bool missing_first = false;
    bool omit_first_column = false;
    bool eof_with_rows = false;
    int opens = 0;
    int closes = 0;
};

class SourceFileReader final : public FileReader {
public:
    SourceFileReader(std::shared_ptr<io::FileSystemProperties>& properties,
                     std::unique_ptr<io::FileDescription>& file, std::shared_ptr<io::IOContext> io,
                     RuntimeProfile* profile, SourceReader& source)
            : FileReader(properties, file, io, profile), _source(source) {}
    Status init(RuntimeState*) override { return Status::OK(); }
    Status get_schema(std::vector<ColumnDefinition>* schema) const override {
        *schema = _source.columns;
        if (_source.omit_first_column) {
            schema->erase(schema->begin());
        }
        for (size_t i = 0; i < schema->size(); ++i) {
            (*schema)[i].local_id = cast_set<int32_t>(i);
            (*schema)[i].default_expr.reset();
        }
        return Status::OK();
    }
    Status open(std::shared_ptr<FileScanRequest> request) override {
        EXPECT_TRUE(request->reject_invalid_rows);
        EXPECT_TRUE(request->conjuncts.empty());
        ++_source.opens;
        return FileReader::open(std::move(request));
    }
    Status get_block(Block* block, size_t* rows, bool* eos) override {
        block->clear_column_data();
        _missing_fields.clear();
        *rows = 0;
        *eos = _calls++ != 0;
        if (*eos) {
            return Status::OK();
        }
        auto column = IColumn::mutate(block->get_by_position(0).column);
        auto& nullable = assert_cast<ColumnNullable&>(*column);
        if (_source.all_null) {
            nullable.insert_default();
        } else {
            nullable.get_nested_column().insert_data("value", 5);
            nullable.get_null_map_data().push_back(0);
        }
        nullable.insert_default();
        block->replace_by_position(0, std::move(column));
        for (size_t i = 1; i < block->columns(); ++i) {
            auto other = block->get_by_position(i).type->create_column();
            other->insert_many_defaults(2);
            block->replace_by_position(i, std::move(other));
        }
        if (_source.missing_first) {
            _missing_fields.emplace(LocalColumnId(0), std::vector<uint8_t> {1, 0});
        }
        *rows = 2;
        *eos = _source.eof_with_rows;
        return Status::OK();
    }
    Status close() override {
        ++_source.closes;
        return Status::OK();
    }

private:
    SourceReader& _source;
    int _calls = 0;
};

class TestLoadTableReader final : public LoadTableReader {
public:
    TestLoadTableReader(std::unique_ptr<SourceReader> source_reader, TupleDescriptor* source,
                        TupleDescriptor* destination)
            : LoadTableReader(source, destination), _input(std::move(source_reader)) {}
    Status init(TableReadOptions&& options) override {
        RETURN_IF_ERROR(LoadTableReader::init(std::move(options)));
        _input->columns = _projected_columns;
        return Status::OK();
    }
    Status prepare_split(const SplitReadOptions& options) override {
        auto split = options;
        split.current_range.__set_path("load-reader-test.csv");
        split.current_range.__set_file_type(TFileType::FILE_LOCAL);
        split.current_range.__set_format_type(TFileFormatType::FORMAT_CSV_PLAIN);
        split.current_range.__set_size(10);
        split.current_range.__set_file_size(10);
        split.current_split_format = FileFormat::CSV;
        return LoadTableReader::prepare_split(split);
    }

protected:
    Status create_file_reader(std::unique_ptr<FileReader>* reader) override {
        *reader = std::make_unique<SourceFileReader>(_system_properties, _current_task->data_file,
                                                     _io_ctx, _scanner_profile, *_input);
        return Status::OK();
    }

private:
    std::unique_ptr<SourceReader> _input;
};

class LoadReaderTest : public testing::Test {
protected:
    void SetUp() override {
        TTupleDescriptor tuple;
        tuple.__set_id(0);
        tuple.__set_byteSize(0);
        tuple.__set_numNullBytes(1);
        source = std::make_unique<TupleDescriptor>(tuple);
        tuple.__set_id(1);
        destination = std::make_unique<TupleDescriptor>(tuple);
        TSlotDescriptor slot;
        slot.__set_id(10);
        slot.__set_parent(0);
        slot.__set_slotType(scalar_type(make_nullable(std::make_shared<DataTypeString>())));
        slot.__set_columnPos(0);
        slot.__set_slotIdx(0);
        slot.__set_byteOffset(0);
        slot.__set_nullIndicatorByte(0);
        slot.__set_nullIndicatorBit(0);
        slot.__set_isMaterialized(true);
        slot.__set_colName("raw_value");
        source_definition = slot;
        source_slot = std::make_unique<SlotDescriptor>(slot);
        source->add_slot(source_slot.get());
        slot.__set_id(20);
        slot.__set_parent(1);
        slot.__set_colName("value");
        destination_slot = std::make_unique<SlotDescriptor>(slot);
        destination->add_slot(destination_slot.get());
        params.__set_src_tuple_id(0);
        params.__set_dest_tuple_id(1);
        params.__set_num_of_columns_from_file(1);
        TFileScanSlotInfo required;
        required.__set_slot_id(10);
        required.__set_is_file_slot(true);
        params.__set_required_slots({required});
        TExprNode node;
        node.__set_node_type(TExprNodeType::SLOT_REF);
        node.__set_type(scalar_type(source_slot->type()));
        node.__set_num_children(0);
        node.__set_is_nullable(true);
        TSlotRef ref;
        ref.__set_slot_id(10);
        ref.__set_tuple_id(0);
        node.__set_slot_ref(ref);
        TExpr expression;
        expression.__set_nodes({node});
        params.__set_expr_of_dest_slot({{20, expression}});
        params.__set_dest_sid_to_src_sid_without_trans({{20, 10}});
    }
    void set_destination_type(DataTypePtr type) {
        TTupleDescriptor tuple;
        tuple.__set_id(1);
        tuple.__set_byteSize(0);
        tuple.__set_numNullBytes(1);
        destination = std::make_unique<TupleDescriptor>(tuple);
        TSlotDescriptor slot;
        slot.__set_id(20);
        slot.__set_parent(1);
        slot.__set_slotType(scalar_type(type));
        slot.__set_columnPos(0);
        slot.__set_slotIdx(0);
        slot.__set_byteOffset(0);
        slot.__set_nullIndicatorByte(0);
        slot.__set_nullIndicatorBit(type->is_nullable() ? 0 : -1);
        slot.__set_isMaterialized(true);
        slot.__set_colName("value");
        destination_slot = std::make_unique<SlotDescriptor>(slot);
        destination->add_slot(destination_slot.get());
    }
    void cast_destination_to_int() {
        set_destination_type(make_nullable(std::make_shared<DataTypeInt32>()));
        TExprNode cast;
        cast.__set_node_type(TExprNodeType::CAST_EXPR);
        cast.__set_type(scalar_type(destination_slot->type()));
        cast.__set_is_nullable(true);
        cast.__set_num_children(1);
        params.expr_of_dest_slot[20].nodes.insert(params.expr_of_dest_slot[20].nodes.begin(), cast);
    }
    Status init(LoadTableReader& reader, FileFormat format = FileFormat::CSV) {
        TTupleDescriptor tuple;
        tuple.__set_id(0);
        tuple.__set_byteSize(0);
        tuple.__set_numNullBytes(1);
        TDescriptorTable descriptors;
        descriptors.__set_tupleDescriptors({tuple});
        descriptors.__set_slotDescriptors({source_definition});
        if (source_bitmap) {
            descriptors.slotDescriptors.push_back(bitmap_definition);
        }
        if (anchor_slot) {
            descriptors.slotDescriptors.push_back(anchor_definition);
        }
        DescriptorTbl* table = nullptr;
        RETURN_IF_ERROR(DescriptorTbl::create(&pool, descriptors, &table));
        state.set_desc_tbl(table);
        return reader.init({.projected_columns = {},
                            .conjuncts = {},
                            .format = format,
                            .scan_params = &params,
                            .io_ctx = io,
                            .runtime_state = &state,
                            .scanner_profile = &profile});
    }
    void add_partial_update_columns(bool key) {
        TTupleDescriptor tuple;
        tuple.__set_id(0);
        tuple.__set_byteSize(0);
        tuple.__set_numNullBytes(1);
        source = std::make_unique<TupleDescriptor>(tuple);
        source_definition.__set_col_unique_id(7);
        source_definition.__set_is_key(key);
        source_slot = std::make_unique<SlotDescriptor>(source_definition);
        source->add_slot(source_slot.get());
        TSlotDescriptor bitmap = source_definition;
        bitmap.__set_id(11);
        bitmap.__set_slotIdx(1);
        bitmap.__set_columnPos(1);
        bitmap.__set_is_key(false);
        bitmap.__set_col_unique_id(9);
        bitmap.__set_colName("__DORIS_SKIP_BITMAP_COL__");
        bitmap.__set_slotType(scalar_type(make_nullable(std::make_shared<DataTypeBitMap>())));
        bitmap_definition = bitmap;
        source_bitmap = std::make_unique<SlotDescriptor>(bitmap);
        source->add_slot(source_bitmap.get());
        TFileScanSlotInfo required;
        required.__set_slot_id(11);
        required.__set_is_file_slot(true);
        params.required_slots.push_back(required);
        params.__set_num_of_columns_from_file(2);
        params.__set_sequence_map_col("raw_value");
        bitmap.__set_id(21);
        bitmap.__set_parent(1);
        destination_bitmap = std::make_unique<SlotDescriptor>(bitmap);
        destination->add_slot(destination_bitmap.get());
        TExpr bitmap_expr = params.expr_of_dest_slot[20];
        bitmap_expr.nodes[0].__set_type(bitmap.slotType);
        bitmap_expr.nodes[0].slot_ref.__set_slot_id(11);
        params.expr_of_dest_slot.emplace(21, bitmap_expr);
        TSlotDescriptor sequence = source_definition;
        sequence.__set_id(22);
        sequence.__set_parent(1);
        sequence.__set_slotIdx(2);
        sequence.__set_columnPos(2);
        sequence.__set_is_key(false);
        sequence.__set_col_unique_id(8);
        sequence.__set_colName("__DORIS_SEQUENCE_COL__");
        destination_sequence = std::make_unique<SlotDescriptor>(sequence);
        destination->add_slot(destination_sequence.get());
        params.expr_of_dest_slot.emplace(22, params.expr_of_dest_slot[20]);
    }
    void add_anchor_source_column() {
        anchor_definition = source_definition;
        anchor_definition.__set_id(12);
        anchor_definition.__set_slotIdx(1);
        anchor_definition.__set_columnPos(1);
        anchor_definition.__set_colName("anchor");
        anchor_slot = std::make_unique<SlotDescriptor>(anchor_definition);
        source->add_slot(anchor_slot.get());
        TFileScanSlotInfo required;
        required.__set_slot_id(12);
        required.__set_is_file_slot(true);
        params.required_slots.push_back(required);
        params.__set_num_of_columns_from_file(2);
    }
    void TearDown() override {
        if (!input_path.empty()) {
            std::filesystem::remove(input_path);
        }
    }
    std::filesystem::path input_path;
    ObjectPool pool;
    TSlotDescriptor source_definition, bitmap_definition, anchor_definition;
    std::unique_ptr<SlotDescriptor> anchor_slot;
    std::unique_ptr<SlotDescriptor> source_bitmap, destination_bitmap, destination_sequence;
    RuntimeState state;
    RuntimeProfile profile {"load"};
    std::shared_ptr<io::IOContext> io = std::make_shared<io::IOContext>();
    TFileScanRangeParams params;
    std::unique_ptr<TupleDescriptor> source, destination;
    std::unique_ptr<SlotDescriptor> source_slot, destination_slot;
};

class LoadJsonPipeTest : public LoadReaderTest {
protected:
    void SetUp() override {
        LoadReaderTest::SetUp();
        auto* env = ExecEnv::GetInstance();
        if (!env->new_load_stream_mgr()) {
            env->_new_load_stream_mgr = std::make_unique<NewLoadStreamMgr>();
            _owns_stream_manager = true;
        }
        _context = std::make_shared<StreamLoadContext>(env);
        _pipe = std::make_shared<io::KafkaConsumerPipe>();
        _context->pipe = _pipe;
        ASSERT_TRUE(env->new_load_stream_mgr()->put(_context->id, _context).ok());
        params.__set_file_type(TFileType::FILE_STREAM);
        params.__set_format_type(TFileFormatType::FORMAT_JSON);
        params.__set_compress_type(TFileCompressType::PLAIN);
        TFileAttributes attributes;
        attributes.__set_read_json_by_line(false);
        params.__set_file_attributes(attributes);
    }
    void TearDown() override {
        ExecEnv::GetInstance()->new_load_stream_mgr()->remove(_context->id);
        _context.reset();
        _pipe.reset();
        if (_owns_stream_manager) {
            ExecEnv::GetInstance()->_new_load_stream_mgr.reset();
        }
        LoadReaderTest::TearDown();
    }
    Status prepare(LoadTableReader& reader, FileFormat format = FileFormat::JSON) {
        RETURN_IF_ERROR(init(reader, format));
        SplitReadOptions split;
        split.current_split_format = format;
        split.current_range.__set_file_type(TFileType::FILE_STREAM);
        split.current_range.__set_format_type(params.format_type);
        split.current_range.__set_load_id(_context->id.to_thrift());
        split.current_range.__set_size(-1);
        return reader.prepare_split(split);
    }
    void append(const std::string& document) {
        ASSERT_TRUE(_pipe->append_json(document.data(), document.size()).ok());
    }
    std::shared_ptr<io::KafkaConsumerPipe> _pipe;
    std::shared_ptr<StreamLoadContext> _context;
    bool _owns_stream_manager = false;
};

class LoadArrowPipeTest : public LoadJsonPipeTest {
protected:
    void SetUp() override {
        LoadJsonPipeTest::SetUp();
        params.__set_format_type(TFileFormatType::FORMAT_ARROW);
    }
    void set_column_type(const DataTypePtr& type) {
        auto descriptor = type->to_thrift();
        if (remove_nullable(type)->get_primitive_type() == TYPE_AGG_STATE) {
            // AGG_STATE's function signature lives on TTypeDesc, outside its scalar type node.
            const auto& state_type = assert_cast<const DataTypeAggState&>(*remove_nullable(type));
            std::vector<TTypeDesc> arguments;
            for (const auto& argument : state_type.get_sub_types()) {
                auto argument_descriptor = argument->to_thrift();
                argument_descriptor.__set_is_nullable(argument->is_nullable());
                arguments.push_back(std::move(argument_descriptor));
            }
            descriptor.__set_sub_types(arguments);
            descriptor.__set_function_name(state_type.get_function_name());
            descriptor.__set_result_is_nullable(false);
            descriptor.__set_be_exec_version(BeExecVersionManager::get_newest_version());
        }
        source_definition.__set_slotType(descriptor);
        source_slot = std::make_unique<SlotDescriptor>(source_definition);
        TTupleDescriptor tuple;
        tuple.__set_id(0);
        tuple.__set_numNullBytes(1);
        source = std::make_unique<TupleDescriptor>(tuple);
        source->add_slot(source_slot.get());
        tuple.__set_id(1);
        destination = std::make_unique<TupleDescriptor>(tuple);
        auto definition = source_definition;
        definition.__set_id(20);
        definition.__set_parent(1);
        definition.__set_colName("value");
        destination_slot = std::make_unique<SlotDescriptor>(definition);
        destination->add_slot(destination_slot.get());
        params.expr_of_dest_slot[20].nodes[0].__set_type(descriptor);
    }
    void check_timestamp_rounding(bool nested) {
        const auto time_type = make_nullable(std::make_shared<DataTypeDateTimeV2>(3));
        set_column_type(nested ? make_nullable(std::make_shared<DataTypeArray>(time_type))
                               : time_type);
        const auto wire_type = arrow::timestamp(arrow::TimeUnit::MICRO);
        arrow::TimestampBuilder builder(wire_type, arrow::default_memory_pool());
        ASSERT_TRUE(builder.Append(123789).ok());
        auto timestamps = builder.Finish();
        ASSERT_TRUE(timestamps.ok());
        std::shared_ptr<arrow::Array> values = *timestamps;
        if (nested) {
            arrow::Int32Builder offsets;
            ASSERT_TRUE(offsets.AppendValues({0, 1}).ok());
            auto offset_values = offsets.Finish();
            ASSERT_TRUE(offset_values.ok());
            auto lists = arrow::ListArray::FromArrays(**offset_values, *values);
            ASSERT_TRUE(lists.ok());
            values = *lists;
        }
        auto schema = arrow::schema({arrow::field("raw_value", values->type())});
        append_batch(arrow::RecordBatch::Make(schema, 1, {values}));
        ASSERT_TRUE(_pipe->finish().ok());
        LoadTableReader reader(source.get(), destination.get());
        ASSERT_TRUE(prepare(reader, FileFormat::ARROW).ok());
        Block output(destination->slots(), 0);
        bool eos = false;
        const auto status = reader.get_block(&output, &eos);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(output.rows(), 1);
        const IColumn* value_column = output.get_by_position(0).column.get();
        if (nested) {
            const auto& nullable = assert_cast<const ColumnNullable&>(*value_column);
            value_column =
                    &assert_cast<const ColumnArray&>(nullable.get_nested_column()).get_data();
        }
        EXPECT_EQ(time_type->to_string(*value_column, 0), "1970-01-01 00:00:00.124");
        ASSERT_TRUE(reader.close().ok());
    }
    void append_stream(const std::vector<std::optional<std::string>>& values,
                       const std::string& name = "raw_value") {
        arrow::StringBuilder builder;
        for (const auto& value : values) {
            ASSERT_TRUE((value ? builder.Append(*value) : builder.AppendNull()).ok());
        }
        auto array = builder.Finish();
        ASSERT_TRUE(array.ok());
        auto schema = arrow::schema({arrow::field(name, arrow::utf8())});
        append_batch(arrow::RecordBatch::Make(schema, values.size(), {*array}));
    }
    void check_hidden_container_values(int container) {
        const auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
        set_column_type(make_nullable(
                container == 2 ? DataTypePtr(std::make_shared<DataTypeMap>(
                                         std::make_shared<DataTypeInt32>(), int_type))
                               : DataTypePtr(std::make_shared<DataTypeArray>(int_type))));
        arrow::Int64Builder builder;
        ASSERT_TRUE(builder.AppendValues({std::numeric_limits<int64_t>::max(), 7}).ok());
        auto values = builder.Finish();
        ASSERT_TRUE(values.ok());
        arrow::BooleanBuilder validity;
        ASSERT_TRUE(validity.AppendValues(std::vector<bool> {false, true}).ok());
        auto valid = validity.Finish();
        ASSERT_TRUE(valid.ok());
        const auto bitmap = (*valid)->data()->buffers[1];
        std::shared_ptr<arrow::Array> input;
        if (container == 1) {
            input = std::make_shared<arrow::FixedSizeListArray>(
                    arrow::fixed_size_list(arrow::int64(), 1), 2, *values, bitmap, 1);
        } else {
            arrow::Int32Builder offsets;
            ASSERT_TRUE(offsets.AppendValues({0, 1, 2}).ok());
            auto positions = offsets.Finish();
            ASSERT_TRUE(positions.ok());
            if (container == 2) {
                input = std::make_shared<arrow::MapArray>(
                        arrow::map(arrow::int64(), arrow::int64()), 2,
                        (*positions)->data()->buffers[1], *values, *values, bitmap, 1);
            } else {
                auto lists = arrow::ListArray::FromArrays(**positions, **values,
                                                          arrow::default_memory_pool(), bitmap, 1);
                ASSERT_TRUE(lists.ok());
                input = *lists;
            }
        }
        append_batch(arrow::RecordBatch::Make(
                arrow::schema({arrow::field("raw_value", input->type())}), 2, {input}));
        ASSERT_TRUE(_pipe->finish().ok());
        LoadTableReader reader(source.get(), destination.get());
        ASSERT_TRUE(prepare(reader, FileFormat::ARROW).ok());
        Block output(destination->slots(), 0);
        bool eos = false;
        const auto status = reader.get_block(&output, &eos);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(output.rows(), 2);
        EXPECT_TRUE(output.get_by_position(0).column->is_null_at(0));
        EXPECT_FALSE(output.get_by_position(0).column->is_null_at(1));
        const auto& column = assert_cast<const ColumnNullable&>(*output.get_by_position(0).column);
        if (container == 2) {
            const auto& map = assert_cast<const ColumnMap&>(column.get_nested_column());
            EXPECT_EQ(map.get_keys().get_int(0), 7);
        } else {
            const auto& array = assert_cast<const ColumnArray&>(column.get_nested_column());
            const auto& elements = assert_cast<const ColumnNullable&>(array.get_data());
            EXPECT_EQ(elements.get_nested_column().get_int(array.get_offsets()[0]), 7);
        }
        ASSERT_TRUE(reader.close().ok());
    }
    void append_batch(const std::shared_ptr<arrow::RecordBatch>& batch, bool compressed = false) {
        auto sink = arrow::io::BufferOutputStream::Create();
        ASSERT_TRUE(sink.ok());
        auto options = arrow::ipc::IpcWriteOptions::Defaults();
        if (compressed) {
            auto codec = arrow::util::Codec::Create(arrow::Compression::ZSTD);
            ASSERT_TRUE(codec.ok());
            options.codec = std::move(*codec);
        }
        auto writer = arrow::ipc::MakeStreamWriter(*sink, batch->schema(), options);
        ASSERT_TRUE(writer.ok());
        ASSERT_TRUE((*writer)->WriteRecordBatch(*batch).ok());
        ASSERT_TRUE((*writer)->Close().ok());
        auto buffer = (*sink)->Finish();
        ASSERT_TRUE(buffer.ok());
        // HTTP chunks need not coincide with IPC message boundaries.
        for (int64_t pos = 0; pos < (*buffer)->size(); pos += 7) {
            const auto length = std::min<int64_t>(7, (*buffer)->size() - pos);
            ASSERT_TRUE(_pipe->append_json(reinterpret_cast<const char*>((*buffer)->data() + pos),
                                           length)
                                .ok());
        }
    }
    void make_native_variant_array(bool extension, std::shared_ptr<arrow::Array>* array) {
        ASSERT_TRUE(register_arrow_variant_extension().ok());
        auto type = std::make_shared<DataTypeVariantV2>();
        auto values = type->create_column();
        auto serde = type->get_serde();
        DataTypeSerDe::FormatOptions options;
        for (std::string json : {"42", "null", "null"}) {
            Slice slice(json.data(), json.size());
            ASSERT_TRUE(serde->deserialize_one_cell_from_json(*values, slice, options).ok());
        }
        auto nulls = ColumnUInt8::create();
        nulls->get_data().assign({0, 0, 1});
        Block block {{ColumnNullable::create(std::move(values), std::move(nulls)),
                      make_nullable(type), "raw_value"}};
        auto wire_type = arrow::extension::variant(
                arrow::struct_({arrow::field("metadata", arrow::binary(), false),
                                arrow::field("value", arrow::binary(), false)}));
        ArrowFlightArrowBlockConvertor converter(
                arrow::schema({arrow::field("raw_value", wire_type)}), cctz::utc_time_zone());
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_TRUE(converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch).ok());
        *array = extension ? batch->column(0)
                           : static_cast<const arrow::ExtensionArray&>(*batch->column(0)).storage();
    }
    void wrap_native_variant(int container, std::shared_ptr<arrow::Array>* values) {
        const auto variant_type = make_nullable(std::make_shared<DataTypeVariantV2>());
        arrow::Int64Builder integers;
        ASSERT_TRUE(integers.AppendValues({7, 8, 9}).ok());
        auto numbers = integers.Finish();
        ASSERT_TRUE(numbers.ok());
        if (container == 3) {
            set_column_type(make_nullable(std::make_shared<DataTypeStruct>(
                    DataTypes {variant_type, make_nullable(std::make_shared<DataTypeInt32>())},
                    Strings {"payload", "number"})));
            auto structs = arrow::StructArray::Make(
                    {*values, *numbers}, std::vector<std::string> {"WIRE_PAYLOAD", "WIRE_NUMBER"});
            ASSERT_TRUE(structs.ok());
            *values = *structs;
            return;
        }
        arrow::Int32Builder offsets;
        ASSERT_TRUE(offsets.AppendValues({0, 1, 2, 3}).ok());
        auto positions = offsets.Finish();
        ASSERT_TRUE(positions.ok());
        if (container == 2) {
            set_column_type(make_nullable(std::make_shared<DataTypeMap>(
                    std::make_shared<DataTypeInt32>(), variant_type)));
            *values = std::make_shared<arrow::MapArray>(
                    arrow::map(arrow::int64(), (*values)->type()), 3,
                    (*positions)->data()->buffers[1], *numbers, *values);
        } else {
            set_column_type(make_nullable(std::make_shared<DataTypeArray>(variant_type)));
            auto lists = arrow::ListArray::FromArrays(**positions, **values);
            ASSERT_TRUE(lists.ok());
            *values = *lists;
        }
    }
    void check_native_variant(bool extension, int container = 0) {
        set_column_type(make_nullable(std::make_shared<DataTypeVariantV2>()));
        std::shared_ptr<arrow::Array> values;
        make_native_variant_array(extension, &values);
        ASSERT_NE(values, nullptr);
        if (container != 0) {
            wrap_native_variant(container, &values);
        }
        append_batch(arrow::RecordBatch::Make(
                arrow::schema({arrow::field("raw_value", values->type())}), 3, {values}));
        ASSERT_TRUE(_pipe->finish().ok());
        LoadTableReader reader(source.get(), destination.get());
        ASSERT_TRUE(prepare(reader, FileFormat::ARROW).ok());
        Block output(destination->slots(), 0);
        bool eos = false;
        const auto status = reader.get_block(&output, &eos);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(output.rows(), 3);
        const IColumn* result_column = output.get_by_position(0).column.get();
        if (container != 0) {
            const auto& outer =
                    assert_cast<const ColumnNullable&>(*result_column).get_nested_column();
            if (container == 1) {
                result_column = &assert_cast<const ColumnArray&>(outer).get_data();
            } else if (container == 2) {
                result_column = &assert_cast<const ColumnMap&>(outer).get_values();
            } else {
                result_column = &assert_cast<const ColumnStruct&>(outer).get_column(0);
            }
        }
        const auto& nullable = assert_cast<const ColumnNullable&>(*result_column);
        const auto& result = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
        EXPECT_EQ(result.get_value_ref(0).get_int(), 42);
        EXPECT_FALSE(nullable.is_null_at(1));
        EXPECT_TRUE(result.get_value_ref(1).is_null());
        EXPECT_TRUE(nullable.is_null_at(2));
        ASSERT_TRUE(reader.close().ok());
    }
    void check_count_state(bool variable_binary, int width = 8) {
        set_column_type(make_nullable(std::make_shared<DataTypeAggState>(
                DataTypes {std::make_shared<DataTypeInt32>()}, false, "count",
                BeExecVersionManager::get_newest_version())));
        std::unique_ptr<arrow::ArrayBuilder> builder;
        const auto wire_type = variable_binary ? arrow::binary() : arrow::fixed_size_binary(width);
        ASSERT_TRUE(arrow::MakeBuilder(arrow::default_memory_pool(), wire_type, &builder).ok());
        const uint64_t counts[] = {0, 7};
        for (const auto& count : counts) {
            const std::string_view bytes(reinterpret_cast<const char*>(&count), width);
            const auto status =
                    variable_binary
                            ? static_cast<arrow::BinaryBuilder&>(*builder).Append(bytes)
                            : static_cast<arrow::FixedSizeBinaryBuilder&>(*builder).Append(bytes);
            ASSERT_TRUE(status.ok()) << status;
        }
        ASSERT_TRUE(builder->AppendNull().ok());
        auto values = builder->Finish();
        ASSERT_TRUE(values.ok());
        append_batch(arrow::RecordBatch::Make(arrow::schema({arrow::field("raw_value", wire_type)}),
                                              3, {*values}));
        ASSERT_TRUE(_pipe->finish().ok());
        LoadTableReader reader(source.get(), destination.get());
        const auto prepared = prepare(reader, FileFormat::ARROW);
        ASSERT_TRUE(prepared.ok()) << prepared;
        Block output(destination->slots(), 0);
        bool eos = false;
        const auto status = reader.get_block(&output, &eos);
        if (width != sizeof(uint64_t)) {
            EXPECT_FALSE(status.ok());
            EXPECT_NE(status.to_string().find("Cannot decode Arrow column"), std::string::npos)
                    << status;
        } else {
            ASSERT_TRUE(status.ok()) << status;
            ASSERT_EQ(output.rows(), 3);
            const auto& nullable =
                    assert_cast<const ColumnNullable&>(*output.get_by_position(0).column);
            const auto& result =
                    assert_cast<const ColumnFixedLengthObject&>(nullable.get_nested_column());
            EXPECT_TRUE(nullable.is_null_at(2));
            EXPECT_EQ(result.item_size(), sizeof(uint64_t));
            EXPECT_EQ(result.get_data_at(0),
                      StringRef(reinterpret_cast<const char*>(&counts[0]), 8));
            EXPECT_EQ(result.get_data_at(1),
                      StringRef(reinterpret_cast<const char*>(&counts[1]), 8));
        }
        ASSERT_TRUE(reader.close().ok());
    }
    void check_null_fixed_list_struct(bool narrow, bool all_null = false, int width = 1) {
        const auto structure = make_nullable(std::make_shared<DataTypeStruct>(
                DataTypes {make_nullable(std::make_shared<DataTypeInt32>())}, Strings {"a"}));
        set_column_type(make_nullable(std::make_shared<DataTypeArray>(structure)));
        const auto wire_type = narrow ? arrow::int64() : arrow::int32();
        std::unique_ptr<arrow::ArrayBuilder> builder;
        ASSERT_TRUE(arrow::MakeBuilder(arrow::default_memory_pool(), wire_type, &builder).ok());
        for (int i = 0; i < 2 * width; ++i) {
            const auto status = narrow ? static_cast<arrow::Int64Builder&>(*builder).Append(
                                                 i < width ? std::numeric_limits<int64_t>::max()
                                                           : 7 + i - width)
                                       : static_cast<arrow::Int32Builder&>(*builder).Append(
                                                 i < width ? 1 + i : 7 + i - width);
            ASSERT_TRUE(status.ok()) << status;
        }
        auto values = builder->Finish();
        ASSERT_TRUE(values.ok());
        auto structs = arrow::StructArray::Make(
                {*values}, arrow::FieldVector {arrow::field("a", wire_type, false)});
        ASSERT_TRUE(structs.ok());
        arrow::BooleanBuilder validity;
        ASSERT_TRUE(validity.AppendValues(std::vector<bool> {false, !all_null}).ok());
        auto valid = validity.Finish();
        ASSERT_TRUE(valid.ok());
        auto lists = std::make_shared<arrow::FixedSizeListArray>(
                arrow::fixed_size_list((*structs)->type(), width), 2, *structs,
                (*valid)->data()->buffers[1], all_null ? 2 : 1);
        append_batch(arrow::RecordBatch::Make(
                arrow::schema({arrow::field("raw_value", lists->type())}), 2, {lists}));
        ASSERT_TRUE(_pipe->finish().ok());
        LoadTableReader reader(source.get(), destination.get());
        ASSERT_TRUE(prepare(reader, FileFormat::ARROW).ok());
        Block output(destination->slots(), 0);
        bool eos = false;
        const auto status = reader.get_block(&output, &eos);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(output.rows(), 2);
        const auto& outer = assert_cast<const ColumnNullable&>(*output.get_by_position(0).column);
        EXPECT_TRUE(outer.is_null_at(0));
        EXPECT_EQ(outer.is_null_at(1), all_null);
        const auto& array = assert_cast<const ColumnArray&>(outer.get_nested_column());
        EXPECT_EQ(array.get_offsets()[0], 0);
        EXPECT_EQ(array.get_offsets()[1], all_null ? 0 : width);
        const auto& elements = assert_cast<const ColumnNullable&>(array.get_data());
        const auto& fields = assert_cast<const ColumnStruct&>(elements.get_nested_column());
        if (!all_null) {
            const auto& numbers = assert_cast<const ColumnNullable&>(fields.get_column(0));
            for (int i = 0; i < width; ++i) {
                EXPECT_FALSE(numbers.is_null_at(i));
                EXPECT_EQ(numbers.get_nested_column().get_int(i), 7 + i);
            }
        }
        ASSERT_TRUE(reader.close().ok());
    }
    void check_null_fixed_list_variant(bool extension) {
        set_column_type(make_nullable(std::make_shared<DataTypeArray>(
                make_nullable(std::make_shared<DataTypeVariantV2>()))));
        std::shared_ptr<arrow::Array> values;
        make_native_variant_array(extension, &values);
        ASSERT_NE(values, nullptr);
        arrow::BooleanBuilder validity;
        ASSERT_TRUE(validity.AppendValues(std::vector<bool> {true, false, true}).ok());
        auto valid = validity.Finish();
        ASSERT_TRUE(valid.ok());
        auto lists = std::make_shared<arrow::FixedSizeListArray>(
                arrow::fixed_size_list(values->type(), 1), 3, values, (*valid)->data()->buffers[1],
                1);
        append_batch(arrow::RecordBatch::Make(
                arrow::schema({arrow::field("raw_value", lists->type())}), 3, {lists}));
        ASSERT_TRUE(_pipe->finish().ok());
        LoadTableReader reader(source.get(), destination.get());
        ASSERT_TRUE(prepare(reader, FileFormat::ARROW).ok());
        Block output(destination->slots(), 0);
        bool eos = false;
        const auto status = reader.get_block(&output, &eos);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(output.rows(), 3);
        const auto& outer = assert_cast<const ColumnNullable&>(*output.get_by_position(0).column);
        EXPECT_TRUE(outer.is_null_at(1));
        const auto& array = assert_cast<const ColumnArray&>(outer.get_nested_column());
        EXPECT_EQ(array.get_offsets()[0], 1);
        EXPECT_EQ(array.get_offsets()[1], 1);
        EXPECT_EQ(array.get_offsets()[2], 2);
        const auto& elements = assert_cast<const ColumnNullable&>(array.get_data());
        const auto& variants = assert_cast<const ColumnVariantV2&>(elements.get_nested_column());
        EXPECT_EQ(variants.get_value_ref(0).get_int(), 42);
        EXPECT_TRUE(elements.is_null_at(1));
        ASSERT_TRUE(reader.close().ok());
    }
    void check_decimal_rescaling(bool legacy, bool nested = false, int scale = 10) {
        const auto decimal_type =
                make_nullable(legacy ? DataTypePtr(std::make_shared<DataTypeDecimalV2>(27, 9))
                                     : DataTypePtr(std::make_shared<DataTypeDecimal128>(27, 9)));
        set_column_type(nested ? make_nullable(std::make_shared<DataTypeArray>(decimal_type))
                               : decimal_type);
        const int64_t unscaled = scale == 10 ? 12345678901LL : 1234567;
        arrow::Decimal128Builder builder(arrow::decimal128(20, scale),
                                         arrow::default_memory_pool());
        ASSERT_TRUE(builder.Append(arrow::Decimal128(unscaled)).ok());
        ASSERT_TRUE(builder.Append(arrow::Decimal128(-unscaled)).ok());
        ASSERT_TRUE(builder.AppendNull().ok());
        auto decimals = builder.Finish();
        ASSERT_TRUE(decimals.ok());
        std::shared_ptr<arrow::Array> values = *decimals;
        if (nested) {
            arrow::Int32Builder offsets;
            ASSERT_TRUE(offsets.AppendValues({0, 3}).ok());
            auto positions = offsets.Finish();
            ASSERT_TRUE(positions.ok());
            auto lists = arrow::ListArray::FromArrays(**positions, *values);
            ASSERT_TRUE(lists.ok());
            values = *lists;
        }
        append_batch(
                arrow::RecordBatch::Make(arrow::schema({arrow::field("raw_value", values->type())}),
                                         nested ? 1 : 3, {values}));
        ASSERT_TRUE(_pipe->finish().ok());
        LoadTableReader reader(source.get(), destination.get());
        ASSERT_TRUE(prepare(reader, FileFormat::ARROW).ok());
        Block output(destination->slots(), 0);
        bool eos = false;
        const auto status = reader.get_block(&output, &eos);
        if (!legacy) {
            EXPECT_FALSE(status.ok());
            EXPECT_NE(status.to_string().find("Cannot decode Arrow column"), std::string::npos)
                    << status;
        } else {
            ASSERT_TRUE(status.ok()) << status;
            ASSERT_EQ(output.rows(), nested ? 1 : 3);
            const IColumn* result = output.get_by_position(0).column.get();
            if (nested) {
                const auto& outer = assert_cast<const ColumnNullable&>(*result).get_nested_column();
                result = &assert_cast<const ColumnArray&>(outer).get_data();
            }
            const auto& nullable = assert_cast<const ColumnNullable&>(*result);
            const auto& numbers =
                    assert_cast<const ColumnDecimal<TYPE_DECIMALV2>&>(nullable.get_nested_column());
            const int64_t expected = scale == 10 ? 1234567890 : 1234567000;
            EXPECT_EQ(numbers.get_element(0).value(), expected);
            EXPECT_EQ(numbers.get_element(1).value(), -expected);
            EXPECT_TRUE(nullable.is_null_at(2));
        }
        ASSERT_TRUE(reader.close().ok());
    }
};

TEST_F(LoadArrowPipeTest, NullFixedSizeListPreservesNonNullableStructDescendants) {
    check_null_fixed_list_struct(false);
}

TEST_F(LoadArrowPipeTest, NullFixedSizeListCompactsStructsBeforeCheckedNarrowing) {
    check_null_fixed_list_struct(true, false, 2);
}

TEST_F(LoadArrowPipeTest, AllNullFixedSizeListCompactsNonNullableStructDescendants) {
    check_null_fixed_list_struct(false, true);
}

TEST_F(LoadArrowPipeTest, NullFixedSizeListPreservesNativeVariantStorage) {
    check_null_fixed_list_variant(false);
}

TEST_F(LoadArrowPipeTest, NullFixedSizeListPreservesNativeVariantExtension) {
    check_null_fixed_list_variant(true);
}

TEST_F(LoadArrowPipeTest, DecimalV2RetainsHigherScaleInputTruncation) {
    check_decimal_rescaling(true);
}

TEST_F(LoadArrowPipeTest, NestedDecimalV2RetainsHigherScaleInputTruncation) {
    check_decimal_rescaling(true, true);
}

TEST_F(LoadArrowPipeTest, DecimalV2RetainsLowerScaleInputExpansion) {
    check_decimal_rescaling(true, false, 6);
}

TEST_F(LoadArrowPipeTest, DecimalV3StillRejectsLossyScaleReduction) {
    check_decimal_rescaling(false);
}

TEST_F(LoadArrowPipeTest, NativeVariantStoragePreservesValuesAndSqlNulls) {
    check_native_variant(false);
}

TEST_F(LoadArrowPipeTest, NativeVariantExtensionPreservesValuesAndSqlNulls) {
    check_native_variant(true);
}

TEST_F(LoadArrowPipeTest, NativeVariantInsideArrayPreservesValuesAndSqlNulls) {
    check_native_variant(false, 1);
}

TEST_F(LoadArrowPipeTest, NativeVariantInsideMapSurvivesKeyNarrowing) {
    check_native_variant(false, 2);
}

TEST_F(LoadArrowPipeTest, NativeVariantInsideStructSurvivesSiblingNarrowing) {
    check_native_variant(false, 3);
}

TEST_F(LoadArrowPipeTest, NativeVariantExtensionInsideArrayPreservesValuesAndSqlNulls) {
    check_native_variant(true, 1);
}

TEST_F(LoadArrowPipeTest, NativeVariantExtensionInsideMapSurvivesKeyNarrowing) {
    check_native_variant(true, 2);
}

TEST_F(LoadArrowPipeTest, NativeVariantExtensionInsideStructSurvivesSiblingNarrowing) {
    check_native_variant(true, 3);
}

TEST_F(LoadArrowPipeTest, CountStateUsesFixedSizeBinarySerialization) {
    check_count_state(false);
}

TEST_F(LoadArrowPipeTest, CountStateAcceptsBinarySerialization) {
    check_count_state(true);
}

TEST_F(LoadArrowPipeTest, CountStateRejectsWrongFixedSizeBinaryWidth) {
    check_count_state(false, 4);
}

TEST_F(LoadArrowPipeTest, CountStateRejectsWrongBinaryWidth) {
    check_count_state(true, 4);
}

TEST_F(LoadArrowPipeTest, StructInputForAStringSlotReturnsAnError) {
    arrow::Int32Builder builder;
    ASSERT_TRUE(builder.Append(7).ok());
    auto values = builder.Finish();
    ASSERT_TRUE(values.ok());
    auto structs = arrow::StructArray::Make({*values}, std::vector<std::string> {"value"});
    ASSERT_TRUE(structs.ok());
    append_batch(arrow::RecordBatch::Make(
            arrow::schema({arrow::field("raw_value", (*structs)->type())}), 1, {*structs}));
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    ASSERT_TRUE(prepare(reader, FileFormat::ARROW).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    const auto status = reader.get_block(&output, &eos);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Cannot decode Arrow column"), std::string::npos) << status;
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadArrowPipeTest, ConcatenatedStreamsRespectBatchBoundariesAndNulls) {
    auto options = state.query_options();
    options.__set_batch_size(1);
    state.set_query_options(options);
    append_stream({});
    append_stream({"first", std::nullopt, "third"});
    append_stream({"last"});
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    const auto status = prepare(reader, FileFormat::ARROW);
    ASSERT_TRUE(status.ok()) << status;
    Block output(destination->slots(), 0);
    bool eos = false;
    const std::vector<std::optional<std::string>> expected {"first", std::nullopt, "third", "last"};
    for (const auto& value : expected) {
        const auto read_status = reader.get_block(&output, &eos);
        ASSERT_TRUE(read_status.ok()) << read_status;
        ASSERT_EQ(output.rows(), 1);
        const auto& column = assert_cast<const ColumnNullable&>(*output.get_by_position(0).column);
        EXPECT_EQ(column.is_null_at(0), !value.has_value());
        if (value) {
            EXPECT_EQ(column.get_data_at(0).to_string(), *value);
        }
    }
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(output.rows(), 0);
    EXPECT_EQ(_pipe->get_queue_size(), 0);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadArrowPipeTest, ReorderedFieldsAndDictionaryStringsUseFileLocalMapping) {
    arrow::StringDictionaryBuilder builder;
    ASSERT_TRUE(builder.Append("dictionary").ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    auto values = builder.Finish();
    ASSERT_TRUE(values.ok());
    arrow::Int32Builder unused;
    ASSERT_TRUE(unused.AppendValues({1, 2}).ok());
    auto ignored = unused.Finish();
    ASSERT_TRUE(ignored.ok());
    auto schema = arrow::schema({arrow::field("ignored", arrow::int32()),
                                 arrow::field("raw_value", (*values)->type())});
    append_batch(arrow::RecordBatch::Make(schema, 2, {*ignored, *values}));
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    ASSERT_TRUE(prepare(reader, FileFormat::ARROW).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    const auto status = reader.get_block(&output, &eos);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(output.rows(), 2);
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(0).to_string(), "dictionary");
    EXPECT_TRUE(output.get_by_position(0).column->is_null_at(1));
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadArrowPipeTest, NestedArraysKeepNullsAcrossBatchBoundaries) {
    set_column_type(make_nullable(
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt32>()))));
    auto options = state.query_options();
    options.__set_batch_size(1);
    state.set_query_options(options);
    auto values = std::make_shared<arrow::Int32Builder>();
    arrow::ListBuilder lists(arrow::default_memory_pool(), values);
    ASSERT_TRUE(lists.Append().ok());
    ASSERT_TRUE(values->Append(1).ok());
    ASSERT_TRUE(values->AppendNull().ok());
    ASSERT_TRUE(lists.AppendNull().ok());
    ASSERT_TRUE(lists.Append().ok());
    auto array = lists.Finish();
    ASSERT_TRUE(array.ok());
    auto schema = arrow::schema({arrow::field("raw_value", (*array)->type())});
    append_batch(arrow::RecordBatch::Make(schema, 3, {*array}));
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    ASSERT_TRUE(prepare(reader, FileFormat::ARROW).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    for (int row = 0; row < 3; ++row) {
        const auto status = reader.get_block(&output, &eos);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(output.rows(), 1);
        const auto& nullable =
                assert_cast<const ColumnNullable&>(*output.get_by_position(0).column);
        EXPECT_EQ(nullable.is_null_at(0), row == 1);
        const auto& array_column = assert_cast<const ColumnArray&>(nullable.get_nested_column());
        EXPECT_EQ(array_column.get_offsets()[0], row == 0 ? 2 : 0);
        if (row == 0) {
            const auto& elements = assert_cast<const ColumnNullable&>(array_column.get_data());
            EXPECT_EQ(elements.get_nested_column().get_int(0), 1);
            EXPECT_TRUE(array_column.get_data().is_null_at(1));
        }
    }
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadArrowPipeTest, BinaryStringPayloadPreservesItsBytes) {
    arrow::BinaryBuilder builder;
    ASSERT_TRUE(builder.Append("\xFF", 1).ok());
    auto values = builder.Finish();
    ASSERT_TRUE(values.ok());
    auto schema = arrow::schema({arrow::field("raw_value", arrow::binary())});
    append_batch(arrow::RecordBatch::Make(schema, 1, {*values}));
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    ASSERT_TRUE(prepare(reader, FileFormat::ARROW).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    const auto status = reader.get_block(&output, &eos);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(output.rows(), 1);
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(0).to_string(), "\xFF");
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadArrowPipeTest, StructChildrenBindPositionallyInsideAnArray) {
    const auto structure = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(std::make_shared<DataTypeInt32>()),
                       make_nullable(std::make_shared<DataTypeInt32>())},
            Strings {"a", "b"}));
    set_column_type(make_nullable(std::make_shared<DataTypeArray>(structure)));
    arrow::Int64Builder builder;
    ASSERT_TRUE(builder.AppendValues({7, 9}).ok());
    auto values = builder.Finish();
    ASSERT_TRUE(values.ok());
    auto structs = arrow::StructArray::Make(
            {*values, *values}, arrow::FieldVector {arrow::field("A", arrow::int64()),
                                                    arrow::field("different", arrow::int64())});
    ASSERT_TRUE(structs.ok());
    arrow::Int32Builder offsets;
    ASSERT_TRUE(offsets.AppendValues({0, 2}).ok());
    auto positions = offsets.Finish();
    ASSERT_TRUE(positions.ok());
    auto lists = arrow::ListArray::FromArrays(**positions, **structs);
    ASSERT_TRUE(lists.ok());
    append_batch(arrow::RecordBatch::Make(
            arrow::schema({arrow::field("raw_value", (*lists)->type())}), 1, {*lists}));
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    ASSERT_TRUE(prepare(reader, FileFormat::ARROW).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    const auto status = reader.get_block(&output, &eos);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(output.rows(), 1);
    const auto& outer = assert_cast<const ColumnNullable&>(*output.get_by_position(0).column);
    const auto& array = assert_cast<const ColumnArray&>(outer.get_nested_column());
    const auto& elements = assert_cast<const ColumnNullable&>(array.get_data());
    const auto& fields = assert_cast<const ColumnStruct&>(elements.get_nested_column());
    EXPECT_FALSE(fields.get_column(0).is_null_at(0));
    EXPECT_EQ(
            assert_cast<const ColumnNullable&>(fields.get_column(0)).get_nested_column().get_int(0),
            7);
    EXPECT_FALSE(fields.get_column(1).is_null_at(1));
    EXPECT_EQ(
            assert_cast<const ColumnNullable&>(fields.get_column(1)).get_nested_column().get_int(1),
            9);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadArrowPipeTest, Date64RetainsDorisRangeValidation) {
    set_column_type(make_nullable(std::make_shared<DataTypeDateV2>()));
    arrow::Date64Builder builder;
    ASSERT_TRUE(builder.Append(253402300800000LL).ok());
    auto values = builder.Finish();
    ASSERT_TRUE(values.ok());
    append_batch(arrow::RecordBatch::Make(
            arrow::schema({arrow::field("raw_value", arrow::date64())}), 1, {*values}));
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    ASSERT_TRUE(prepare(reader, FileFormat::ARROW).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    const auto status = reader.get_block(&output, &eos);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Doris DATE range"), std::string::npos) << status;
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadArrowPipeTest, UInt32Ipv4PreservesTheUnsignedAddressBits) {
    set_column_type(make_nullable(std::make_shared<DataTypeIPv4>()));
    arrow::UInt32Builder builder;
    ASSERT_TRUE(builder.AppendValues({0x7fffffffU, 0x80000000U, 0xffffffffU}).ok());
    auto values = builder.Finish();
    ASSERT_TRUE(values.ok());
    append_batch(arrow::RecordBatch::Make(
            arrow::schema({arrow::field("raw_value", arrow::uint32())}), 3, {*values}));
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    ASSERT_TRUE(prepare(reader, FileFormat::ARROW).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    const auto status = reader.get_block(&output, &eos);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(output.rows(), 3);
    const auto type = destination_slot->type();
    EXPECT_EQ(type->to_string(*output.get_by_position(0).column, 0), "127.255.255.255");
    EXPECT_EQ(type->to_string(*output.get_by_position(0).column, 1), "128.0.0.0");
    EXPECT_EQ(type->to_string(*output.get_by_position(0).column, 2), "255.255.255.255");
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadArrowPipeTest, NullStructIgnoresAnOverflowingHiddenChild) {
    set_column_type(make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(std::make_shared<DataTypeInt32>())}, Strings {"a"})));
    arrow::Int64Builder builder;
    ASSERT_TRUE(builder.AppendValues({std::numeric_limits<int64_t>::max(), 7}).ok());
    auto values = builder.Finish();
    ASSERT_TRUE(values.ok());
    arrow::BooleanBuilder validity;
    ASSERT_TRUE(validity.AppendValues(std::vector<bool> {false, true}).ok());
    auto valid = validity.Finish();
    ASSERT_TRUE(valid.ok());
    auto structs = arrow::StructArray::Make({*values},
                                            arrow::FieldVector {arrow::field("a", arrow::int64())},
                                            (*valid)->data()->buffers[1], 1);
    ASSERT_TRUE(structs.ok());
    append_batch(arrow::RecordBatch::Make(
            arrow::schema({arrow::field("raw_value", (*structs)->type())}), 2, {*structs}));
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    ASSERT_TRUE(prepare(reader, FileFormat::ARROW).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    const auto status = reader.get_block(&output, &eos);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(output.rows(), 2);
    EXPECT_TRUE(output.get_by_position(0).column->is_null_at(0));
    const auto& nullable = assert_cast<const ColumnNullable&>(*output.get_by_position(0).column);
    const auto& fields = assert_cast<const ColumnStruct&>(nullable.get_nested_column());
    EXPECT_EQ(
            assert_cast<const ColumnNullable&>(fields.get_column(0)).get_nested_column().get_int(1),
            7);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadArrowPipeTest, NullListIgnoresAnOverflowingHiddenChild) {
    check_hidden_container_values(0);
}

TEST_F(LoadArrowPipeTest, NullFixedSizeListIgnoresAnOverflowingHiddenChild) {
    check_hidden_container_values(1);
}

TEST_F(LoadArrowPipeTest, NullMapIgnoresOverflowingHiddenKeysAndValues) {
    check_hidden_container_values(2);
}

TEST_F(LoadArrowPipeTest, NumericIntStillRejectsAnOverflowingUInt32) {
    set_column_type(make_nullable(std::make_shared<DataTypeInt32>()));
    arrow::UInt32Builder builder;
    ASSERT_TRUE(builder.Append(0xffffffffU).ok());
    auto values = builder.Finish();
    ASSERT_TRUE(values.ok());
    append_batch(arrow::RecordBatch::Make(
            arrow::schema({arrow::field("raw_value", arrow::uint32())}), 1, {*values}));
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    ASSERT_TRUE(prepare(reader, FileFormat::ARROW).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    EXPECT_FALSE(reader.get_block(&output, &eos).ok());
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadArrowPipeTest, ReadsCompressedIpcBatches) {
    arrow::StringBuilder builder;
    ASSERT_TRUE(builder.Append("compressed").ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    auto values = builder.Finish();
    ASSERT_TRUE(values.ok());
    auto schema = arrow::schema({arrow::field("raw_value", arrow::utf8())});
    append_batch(arrow::RecordBatch::Make(schema, 2, {*values}), true);
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    ASSERT_TRUE(prepare(reader, FileFormat::ARROW).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    const auto status = reader.get_block(&output, &eos);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(output.rows(), 2);
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(0).to_string(), "compressed");
    EXPECT_TRUE(output.get_by_position(0).column->is_null_at(1));
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadArrowPipeTest, TimestampRoundingUsesTheDorisSerde) {
    check_timestamp_rounding(false);
}

TEST_F(LoadArrowPipeTest, NestedTimestampRoundingUsesTheDorisSerde) {
    check_timestamp_rounding(true);
}

TEST_F(LoadArrowPipeTest, MalformedIpcReturnsAnError) {
    append("invalid IPC");
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    ASSERT_TRUE(prepare(reader, FileFormat::ARROW).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    EXPECT_FALSE(reader.get_block(&output, &eos).ok());
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadArrowPipeTest, EmptyFinishedPipeReturnsEof) {
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    const auto status = prepare(reader, FileFormat::ARROW);
    ASSERT_TRUE(status.ok()) << status;
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(output.rows(), 0);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadArrowPipeTest, RejectsMissingRequestedColumn) {
    append_stream({"value"}, "other");
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    const auto status = prepare(reader, FileFormat::ARROW);
    ASSERT_TRUE(status.ok()) << status;
    Block output(destination->slots(), 0);
    bool eos = false;
    EXPECT_FALSE(reader.get_block(&output, &eos).ok());
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadJsonPipeTest, SchemaProbeReadsTheSampleWithoutConsumingTheLoadPipe) {
    const std::string document = R"({"raw_value":"payload"})";
    append(document);
    ASSERT_TRUE(_pipe->finish().ok());
    ASSERT_TRUE(_context->allocate_schema_buffer().ok());
    _context->schema_buffer()->put_bytes(document.data(), document.size());
    TFileRangeDesc range;
    range.__set_file_type(TFileType::FILE_STREAM);
    range.__set_load_id(_context->id.to_thrift());
    range.__set_start_offset(0);
    range.__set_size(-1);
    std::vector<ColumnDefinition> schema;
    const auto status = fetch_file_schema(params, range, io, &profile, &schema);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(schema.size(), 1);
    EXPECT_EQ(schema[0].name, "raw_value");
    LoadTableReader reader(source.get(), destination.get());
    ASSERT_TRUE(prepare(reader).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    ASSERT_EQ(output.rows(), 1);
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(0).to_string(), "payload");
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadJsonPipeTest, ReadsEveryKafkaMessageBeforePipeEof) {
    append(R"({"raw_value":"first"})");
    append(R"({"raw_value":"second"})");
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    const auto status = prepare(reader);
    ASSERT_TRUE(status.ok()) << status;
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    ASSERT_EQ(output.rows(), 2);
    EXPECT_FALSE(eos);
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(0).to_string(), "first");
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(1).to_string(), "second");
    EXPECT_EQ(_pipe->get_queue_size(), 0);
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(output.rows(), 0);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadJsonPipeTest, MessagesContinueAcrossBatchBoundaries) {
    auto options = state.query_options();
    options.__set_batch_size(1);
    state.set_query_options(options);
    append(R"({"raw_value":"first"})");
    append(R"({"raw_value":"second"})");
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    const auto status = prepare(reader);
    ASSERT_TRUE(status.ok()) << status;
    Block output(destination->slots(), 0);
    bool eos = false;
    for (const auto* expected : {"first", "second"}) {
        ASSERT_TRUE(reader.get_block(&output, &eos).ok());
        EXPECT_FALSE(eos);
        ASSERT_EQ(output.rows(), 1);
        EXPECT_EQ(output.get_by_position(0).column->get_data_at(0).to_string(), expected);
    }
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(output.rows(), 0);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadJsonPipeTest, FixedLengthStreamEndsAfterItsDocument) {
    const std::string document = R"({"raw_value":"fixed"})";
    auto pipe = std::make_shared<io::StreamLoadPipe>(1024, 1024, document.size());
    _context->pipe = pipe;
    ASSERT_TRUE(pipe->append_and_flush(document.data(), document.size()).ok());
    ASSERT_TRUE(pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    const auto status = prepare(reader);
    ASSERT_TRUE(status.ok()) << status;
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    ASSERT_EQ(output.rows(), 1);
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(0).to_string(), "fixed");
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(output.rows(), 0);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadJsonPipeTest, RejectedMessageDoesNotHideLaterMessages) {
    append(R"({"raw_value":"first"})");
    append("{");
    append(R"({"raw_value":"last"})");
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    const auto status = prepare(reader);
    ASSERT_TRUE(status.ok()) << status;
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    ASSERT_EQ(output.rows(), 2);
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(1).to_string(), "last");
    EXPECT_EQ(io->load_filtered_rows, 1);
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_TRUE(eos);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadJsonPipeTest, ChunkedTransferRemainsOneDocument) {
    _pipe->set_is_chunked_transfer(true);
    append(R"({"raw_)");
    append(R"(value":"joined"})");
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    const auto status = prepare(reader);
    ASSERT_TRUE(status.ok()) << status;
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    ASSERT_EQ(output.rows(), 1);
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(0).to_string(), "joined");
    EXPECT_EQ(io->load_filtered_rows, 0);
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_TRUE(eos);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadJsonPipeTest, EmptyFinishedPipeReturnsEof) {
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    const auto status = prepare(reader);
    ASSERT_TRUE(status.ok()) << status;
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(output.rows(), 0);
    EXPECT_EQ(io->load_filtered_rows, 0);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadReaderTest, MissingPhysicalSourceAliasRequiresDeclaredDefault) {
    add_anchor_source_column();
    params.__set_strict_mode(true);
    auto input = std::make_unique<SourceReader>();
    input->omit_first_column = true;
    auto* physical = input.get();
    TestLoadTableReader reader(std::move(input), source.get(), destination.get());
    ASSERT_TRUE(init(reader).ok());
    ASSERT_TRUE(reader.prepare_split({}).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    const auto status = reader.get_block(&output, &eos);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("raw_value"), std::string::npos);
    EXPECT_EQ(physical->opens, 0);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadReaderTest, MissingPhysicalSourceAcceptsExplicitNullDefault) {
    add_anchor_source_column();
    params.__set_strict_mode(true);
    // An empty expression is FE's explicit NULL default, not an absent map entry.
    params.__set_default_value_of_src_slot({{10, TExpr {}}});
    auto input = std::make_unique<SourceReader>();
    input->omit_first_column = true;
    TestLoadTableReader reader(std::move(input), source.get(), destination.get());
    ASSERT_TRUE(init(reader).ok());
    ASSERT_TRUE(reader.prepare_split({}).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    ASSERT_EQ(output.rows(), 2);
    EXPECT_TRUE(output.get_by_position(0).column->is_null_at(0));
    EXPECT_TRUE(output.get_by_position(0).column->is_null_at(1));
    EXPECT_EQ(io->load_filtered_rows, 0);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadReaderTest, PartitionSourceDoesNotRequireFileDefault) {
    add_anchor_source_column();
    params.required_slots[0].__set_is_file_slot(false);
    params.__set_num_of_columns_from_file(1);
    auto input = std::make_unique<SourceReader>();
    input->omit_first_column = true;
    TestLoadTableReader reader(std::move(input), source.get(), destination.get());
    ASSERT_TRUE(init(reader).ok());
    SplitReadOptions split;
    split.current_range.__set_columns_from_path({"partition-value"});
    ASSERT_TRUE(reader.prepare_split(split).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    ASSERT_EQ(output.rows(), 2);
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(0).to_string(), "partition-value");
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadJsonPipeTest, EmptyOuterArrayIsRejectedBeforeFollowingMessage) {
    params.file_attributes.__set_strip_outer_array(true);
    append("[]");
    append(R"([{"raw_value":"valid"}])");
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    const auto status = prepare(reader);
    ASSERT_TRUE(status.ok()) << status;
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    ASSERT_EQ(output.rows(), 1);
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(0).to_string(), "valid");
    EXPECT_EQ(io->load_filtered_rows, 1);
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(io->load_filtered_rows, 1);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadJsonPipeTest, ConfiguredRootCountsMissingAndNullElements) {
    params.file_attributes.__set_strip_outer_array(true);
    params.file_attributes.__set_json_root("$.payload");
    params.file_attributes.__set_jsonpaths(R"(["$.k"])");
    append(R"([{"other":1},{"payload":null},{"payload":{"k":2}}])");
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    const auto status = prepare(reader);
    ASSERT_TRUE(status.ok()) << status;
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    ASSERT_EQ(output.rows(), 1);
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(0).to_string(), "2");
    EXPECT_EQ(io->load_filtered_rows, 2);
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(io->load_filtered_rows, 2);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadJsonPipeTest, RootRejectionPreservesEarlierAndLaterSiblings) {
    params.file_attributes.__set_strip_outer_array(true);
    params.file_attributes.__set_json_root("$.payload");
    params.file_attributes.__set_jsonpaths(R"(["$.k"])");
    append(R"([{"payload":{"k":1}},{"other":1},{"payload":null},
               {"payload":3},{"payload":[]},{"payload":{"k":2}}])");
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    const auto status = prepare(reader);
    ASSERT_TRUE(status.ok()) << status;
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    ASSERT_EQ(output.rows(), 2);
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(0).to_string(), "1");
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(1).to_string(), "2");
    EXPECT_EQ(io->load_filtered_rows, 4);
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_TRUE(eos);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadJsonPipeTest, RejectedRootsDoNotHideFollowingMessage) {
    params.file_attributes.__set_strip_outer_array(true);
    params.file_attributes.__set_json_root("$.payload");
    params.file_attributes.__set_jsonpaths(R"(["$.k"])");
    append(R"([{"other":1},{"payload":null}])");
    append(R"([{"payload":{"k":2}}])");
    ASSERT_TRUE(_pipe->finish().ok());
    LoadTableReader reader(source.get(), destination.get());
    const auto status = prepare(reader);
    ASSERT_TRUE(status.ok()) << status;
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    ASSERT_EQ(output.rows(), 1);
    EXPECT_FALSE(eos);
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(0).to_string(), "2");
    EXPECT_EQ(io->load_filtered_rows, 2);
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_TRUE(eos);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadReaderTest, SourceSlotsAreConvertedBeforeDestinationFiltering) {
    auto child = std::make_unique<SourceReader>();
    auto* input = child.get();
    TestLoadTableReader reader(std::move(child), source.get(), destination.get());
    auto status = init(reader);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(input->columns.size(), 1);
    EXPECT_EQ(input->columns[0].name, "raw_value");
    ASSERT_TRUE(reader.prepare_split({}).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(output.rows(), 2);
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(0).to_string(), "value");
    EXPECT_TRUE(output.get_by_position(0).column->is_null_at(1));
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(output.rows(), 0);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadReaderTest, StrictCastRejectsInvalidNonNullSourceOnly) {
    cast_destination_to_int();
    params.__set_strict_mode(true);
    TestLoadTableReader reader(std::make_unique<SourceReader>(), source.get(), destination.get());
    const auto init_status = init(reader);
    ASSERT_TRUE(init_status.ok()) << init_status;
    ASSERT_TRUE(reader.prepare_split({}).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    auto status = reader.get_block(&output, &eos);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_FALSE(eos);
    ASSERT_EQ(output.rows(), 1);
    EXPECT_TRUE(output.get_by_position(0).column->is_null_at(0));
    EXPECT_EQ(io->load_filtered_rows, 1);
    EXPECT_EQ(io->load_unselected_rows, 0);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadReaderTest, NonStrictCastPreservesNullableRows) {
    cast_destination_to_int();
    params.__set_strict_mode(false);
    TestLoadTableReader reader(std::make_unique<SourceReader>(), source.get(), destination.get());
    const auto init_status = init(reader);
    ASSERT_TRUE(init_status.ok()) << init_status;
    ASSERT_TRUE(reader.prepare_split({}).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    ASSERT_EQ(output.rows(), 2);
    EXPECT_TRUE(output.get_by_position(0).column->is_null_at(0));
    EXPECT_TRUE(output.get_by_position(0).column->is_null_at(1));
    EXPECT_EQ(io->load_filtered_rows, 0);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadReaderTest, PreFilterRunsBeforeStrictConversion) {
    cast_destination_to_int();
    params.__set_strict_mode(true);
    TExprNode predicate;
    predicate.__set_node_type(TExprNodeType::BOOL_LITERAL);
    predicate.__set_type(scalar_type(std::make_shared<DataTypeUInt8>()));
    predicate.__set_num_children(0);
    predicate.__set_is_nullable(false);
    TBoolLiteral literal;
    literal.__set_value(false);
    predicate.__set_bool_literal(literal);
    TExpr expression;
    expression.__set_nodes({predicate});
    params.__set_pre_filter_exprs_list({expression});
    TestLoadTableReader reader(std::make_unique<SourceReader>(), source.get(), destination.get());
    const auto init_status = init(reader);
    ASSERT_TRUE(init_status.ok()) << init_status;
    ASSERT_TRUE(reader.prepare_split({}).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_FALSE(eos);
    EXPECT_EQ(output.rows(), 0);
    EXPECT_EQ(io->load_filtered_rows, 0);
    EXPECT_EQ(io->load_unselected_rows, 2);
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_TRUE(eos);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadReaderTest, MissingDefaultDoesNotReplaceExplicitNull) {
    TExprNode literal;
    literal.__set_node_type(TExprNodeType::STRING_LITERAL);
    literal.__set_type(scalar_type(source_slot->type()));
    literal.__set_num_children(0);
    literal.__set_is_nullable(true);
    TStringLiteral value;
    value.__set_value("fallback");
    literal.__set_string_literal(value);
    TExpr expression;
    expression.__set_nodes({literal});
    params.__set_default_value_of_src_slot({{10, expression}});
    auto child = std::make_unique<SourceReader>();
    child->missing_first = true;
    TestLoadTableReader reader(std::move(child), source.get(), destination.get());
    const auto init_status = init(reader);
    ASSERT_TRUE(init_status.ok()) << init_status;
    ASSERT_TRUE(reader.prepare_split({}).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    ASSERT_EQ(output.rows(), 2);
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(0).to_string(), "fallback");
    EXPECT_TRUE(output.get_by_position(0).column->is_null_at(1));
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadReaderTest, NonNullableDestinationFiltersNulls) {
    set_destination_type(std::make_shared<DataTypeString>());
    TestLoadTableReader reader(std::make_unique<SourceReader>(), source.get(), destination.get());
    const auto init_status = init(reader);
    ASSERT_TRUE(init_status.ok()) << init_status;
    ASSERT_TRUE(reader.prepare_split({}).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    ASSERT_EQ(output.rows(), 1);
    EXPECT_FALSE(output.get_by_position(0).column->is_nullable());
    EXPECT_EQ(io->load_filtered_rows, 1);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadReaderTest, FullyRejectedBatchIsNotEofAndNextSplitStillReads) {
    set_destination_type(std::make_shared<DataTypeString>());
    auto child = std::make_unique<SourceReader>();
    auto* input = child.get();
    input->all_null = true;
    input->eof_with_rows = true;
    TestLoadTableReader reader(std::move(child), source.get(), destination.get());
    const auto init_status = init(reader);
    ASSERT_TRUE(init_status.ok()) << init_status;
    ASSERT_TRUE(reader.prepare_split({}).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_FALSE(eos);
    EXPECT_EQ(output.rows(), 0);
    EXPECT_EQ(io->load_filtered_rows, 2);
    EXPECT_EQ(input->opens, 1);
    EXPECT_EQ(input->closes, 1);
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_TRUE(eos);
    input->all_null = false;
    ASSERT_TRUE(reader.prepare_split({}).ok());
    eos = false;
    ASSERT_TRUE(reader.get_block(&output, &eos).ok());
    EXPECT_FALSE(eos);
    EXPECT_EQ(output.rows(), 1);
    EXPECT_EQ(io->load_filtered_rows, 3);
    EXPECT_EQ(input->opens, 2);
    EXPECT_EQ(input->closes, 2);
    ASSERT_TRUE(reader.close().ok());
    ASSERT_TRUE(reader.close().ok());
    EXPECT_EQ(input->closes, 2);
}

TEST_F(LoadReaderTest, FlexibleUpdatePreservesOmittedValueAndSequenceMarks) {
    add_partial_update_columns(false);
    auto child = std::make_unique<SourceReader>();
    child->missing_first = true;
    TestLoadTableReader reader(std::move(child), source.get(), destination.get());
    const auto init_status = init(reader);
    ASSERT_TRUE(init_status.ok()) << init_status;
    ASSERT_TRUE(reader.prepare_split({}).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    auto status = reader.get_block(&output, &eos);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(output.rows(), 2);
    const auto& nullable = assert_cast<const ColumnNullable&>(*output.get_by_position(1).column);
    const auto& bitmaps = assert_cast<const ColumnBitmap&>(nullable.get_nested_column()).get_data();
    EXPECT_TRUE(bitmaps[0].contains(7));
    EXPECT_TRUE(bitmaps[0].contains(8));
    EXPECT_FALSE(bitmaps[1].contains(7));
    EXPECT_FALSE(bitmaps[1].contains(8));
    EXPECT_EQ(io->load_filtered_rows, 0);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadReaderTest, FlexibleUpdateRejectsMissingKeyBeforeExpressions) {
    add_partial_update_columns(true);
    auto child = std::make_unique<SourceReader>();
    child->missing_first = true;
    TestLoadTableReader reader(std::move(child), source.get(), destination.get());
    const auto init_status = init(reader);
    ASSERT_TRUE(init_status.ok()) << init_status;
    ASSERT_TRUE(reader.prepare_split({}).ok());
    Block output(destination->slots(), 0);
    bool eos = false;
    auto status = reader.get_block(&output, &eos);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(output.rows(), 1);
    EXPECT_EQ(io->load_filtered_rows, 1);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(LoadReaderTest, NativeReadersPreserveValidRowsAroundMalformedRecords) {
    cast_destination_to_int();
    params.__set_strict_mode(true);
    params.__set_file_type(TFileType::FILE_LOCAL);
    params.__set_compress_type(TFileCompressType::PLAIN);
    TFileTextScanRangeParams text;
    text.__set_column_separator(",");
    text.__set_line_delimiter("\n");
    TFileAttributes attributes;
    attributes.__set_text_params(text);
    attributes.__set_read_json_by_line(true);
    params.__set_file_attributes(attributes);
    for (const bool json : {false, true}) {
        SCOPED_TRACE(json ? "JSON" : "CSV");
        params.__set_format_type(json ? TFileFormatType::FORMAT_JSON
                                      : TFileFormatType::FORMAT_CSV_PLAIN);
        input_path = std::filesystem::temp_directory_path() /
                     ("v2_load_reader_" + std::to_string(getpid()));
        {
            std::ofstream file(input_path);
            file << (json ? "{\"raw_value\":1}\n{\"raw_value\":\"bad\"}\nnot-json\n"
                            "{\"raw_value\":null}\n{\"raw_value\":3}\n"
                          : "1\nbad\n2,extra\n\\N\n3\n");
        }
        io->load_filtered_rows = 0;
        const auto format = json ? FileFormat::JSON : FileFormat::CSV;
        LoadTableReader reader(source.get(), destination.get());
        auto status = init(reader, format);
        ASSERT_TRUE(status.ok()) << status;
        TFileRangeDesc range;
        range.__set_path(input_path.string());
        range.__set_start_offset(0);
        range.__set_size(std::filesystem::file_size(input_path));
        range.__set_file_size(range.size);
        range.__set_file_type(TFileType::FILE_LOCAL);
        range.__set_format_type(params.format_type);
        SplitReadOptions split;
        split.current_range = range;
        split.current_split_format = format;
        status = reader.prepare_split(split);
        ASSERT_TRUE(status.ok()) << status;
        Block output(destination->slots(), 0);
        bool eos = false;
        status = reader.get_block(&output, &eos);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_FALSE(eos);
        ASSERT_EQ(output.rows(), 3);
        EXPECT_EQ(assert_cast<const ColumnInt32&>(
                          assert_cast<const ColumnNullable&>(*output.get_by_position(0).column)
                                  .get_nested_column())
                          .get_element(0),
                  1);
        EXPECT_TRUE(output.get_by_position(0).column->is_null_at(1));
        EXPECT_EQ(assert_cast<const ColumnInt32&>(
                          assert_cast<const ColumnNullable&>(*output.get_by_position(0).column)
                                  .get_nested_column())
                          .get_element(2),
                  3);
        EXPECT_EQ(io->load_filtered_rows, 2);
        ASSERT_TRUE(reader.get_block(&output, &eos).ok());
        EXPECT_TRUE(eos);
        ASSERT_TRUE(reader.close().ok());
    }
}

TEST_F(LoadReaderTest, NativeJsonMissingFieldUsesDefaultButExplicitNullDoesNot) {
    add_anchor_source_column();
    params.__set_format_type(TFileFormatType::FORMAT_JSON);
    params.__set_file_type(TFileType::FILE_LOCAL);
    params.__set_compress_type(TFileCompressType::PLAIN);
    TFileTextScanRangeParams text;
    text.__set_line_delimiter("\n");
    TFileAttributes attributes;
    attributes.__set_text_params(text);
    attributes.__set_read_json_by_line(true);
    params.__set_file_attributes(attributes);
    TExprNode literal;
    literal.__set_node_type(TExprNodeType::STRING_LITERAL);
    literal.__set_type(scalar_type(source_slot->type()));
    literal.__set_num_children(0);
    literal.__set_is_nullable(true);
    TStringLiteral value;
    value.__set_value("fallback");
    literal.__set_string_literal(value);
    TExpr expression;
    expression.__set_nodes({literal});
    params.__set_default_value_of_src_slot({{10, expression}});
    input_path = std::filesystem::temp_directory_path() /
                 ("v2_load_defaults_" + std::to_string(getpid()));
    {
        std::ofstream file(input_path);
        file << "{\"anchor\":\"a\"}\n{\"anchor\":\"b\",\"raw_value\":null}\n";
    }
    LoadTableReader reader(source.get(), destination.get());
    auto status = init(reader, FileFormat::JSON);
    ASSERT_TRUE(status.ok()) << status;
    SplitReadOptions split;
    split.current_split_format = FileFormat::JSON;
    split.current_range.__set_path(input_path.string());
    split.current_range.__set_start_offset(0);
    split.current_range.__set_size(std::filesystem::file_size(input_path));
    split.current_range.__set_file_size(split.current_range.size);
    split.current_range.__set_file_type(TFileType::FILE_LOCAL);
    split.current_range.__set_format_type(params.format_type);
    status = reader.prepare_split(split);
    ASSERT_TRUE(status.ok()) << status;
    Block output(destination->slots(), 0);
    bool eos = false;
    status = reader.get_block(&output, &eos);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(output.rows(), 2);
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(0).to_string(), "fallback");
    EXPECT_TRUE(output.get_by_position(0).column->is_null_at(1));
    EXPECT_EQ(io->load_filtered_rows, 0);
    ASSERT_TRUE(reader.close().ok());
}

} // namespace
} // namespace doris::format
