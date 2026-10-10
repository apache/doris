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

#include <arrow/memory_pool.h>
#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "core/block/block.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "format/parquet/vparquet_reader.h"
#include "format/transformer/vparquet_writer.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/uid_util.h"

namespace doris {

class ParquetFileInputTest : public testing::Test {
protected:
    void SetUp() override {
        _path = "./parquet_file_input_" + UniqueId::gen_uid().to_string() + ".parquet";
        _previous_pool = ExecEnv::GetInstance()->_arrow_memory_pool;
        ExecEnv::GetInstance()->_arrow_memory_pool = arrow::default_memory_pool();
    }

    void TearDown() override {
        ExecEnv::GetInstance()->_arrow_memory_pool = _previous_pool;
        static_cast<void>(io::global_local_filesystem()->delete_file(_path));
    }

    std::string _path;
    arrow::MemoryPool* _previous_pool = nullptr;
};

TEST_F(ParquetFileInputTest, RejectsFileLoadSlotsFromOrdinaryParquetStructs) {
    const auto string = make_nullable(std::make_shared<DataTypeString>());
    const auto physical_file =
            make_nullable(std::make_shared<DataTypeStruct>(DataTypes {string}, Strings {"uri"}));
    const auto file = make_nullable(std::make_shared<DataTypeFile>());
    const auto wrap_types = [&](const DataTypePtr& value) -> DataTypes {
        return {value, make_nullable(std::make_shared<DataTypeArray>(value)),
                make_nullable(
                        std::make_shared<DataTypeStruct>(DataTypes {value}, Strings {"payload"})),
                make_nullable(std::make_shared<DataTypeMap>(string, value))};
    };
    const auto source_types = wrap_types(physical_file);
    const auto destination_types = wrap_types(file);
    const std::vector<std::string> names {"f", "a", "s", "m"};
    RuntimeState state;
    state.set_timezone("UTC");
    io::FileWriterPtr file_writer;
    ASSERT_TRUE(io::global_local_filesystem()->create_file(_path, &file_writer).ok());
    const auto contexts = MockSlotRef::create_mock_contexts(source_types);
    VParquetWriter writer(&state, file_writer.get(), contexts, names, false, ParquetFileOptions {});
    ASSERT_TRUE(writer.open().ok());
    Block block;
    for (size_t i = 0; i < source_types.size(); ++i) {
        auto column = source_types[i]->create_column();
        column->insert_default();
        block.insert({std::move(column), source_types[i], names[i]});
    }
    ASSERT_TRUE(writer.write(block).ok());
    ASSERT_TRUE(writer.close().ok());

    for (size_t i = 0; i < destination_types.size(); ++i) {
        SCOPED_TRACE(destination_types[i]->get_name());
        // Ordinary STRUCT slots still initialize; direct load FILE slots must fail at init,
        // before FileScanner replaces their block types with the physical Parquet schema.
        for (const bool file_destination : {false, true}) {
            SCOPED_TRACE(file_destination);
            const auto& type = file_destination ? destination_types[i] : source_types[i];
            TSlotDescriptor slot;
            slot.__set_id(0);
            slot.__set_parent(0);
            slot.__set_slotType(type->to_thrift());
            slot.__set_colName(names[i]);
            slot.__set_isMaterialized(true);
            slot.__set_nullIndicatorBit(0);
            TTupleDescriptor tuple;
            tuple.__set_id(0);
            tuple.__set_byteSize(16);
            tuple.__set_numNullBytes(1);
            TDescriptorTable thrift_descriptors;
            thrift_descriptors.__set_slotDescriptors({slot});
            thrift_descriptors.__set_tupleDescriptors({tuple});
            ObjectPool pool;
            DescriptorTbl* descriptors = nullptr;
            ASSERT_TRUE(DescriptorTbl::create(&pool, thrift_descriptors, &descriptors).ok());
            const auto* tuple_descriptor = descriptors->get_tuple_descriptor(0);
            std::vector<ColumnDescriptor> columns {
                    {names[i], tuple_descriptor->slots()[0], ColumnCategory::REGULAR, nullptr}};
            std::unordered_map<std::string, uint32_t> positions {{names[i], 0}};
            TFileScanRangeParams params;
            TFileRangeDesc range;
            range.__set_path(_path);
            range.__set_start_offset(0);
            io::FileReaderSPtr input;
            ASSERT_TRUE(io::global_local_filesystem()->open_file(_path, &input).ok());
            range.__set_size(input->size());
            auto reader = ParquetReader::create_unique(nullptr, params, range, 1024,
                                                       &state.timezone_obj(), nullptr, &state,
                                                       nullptr, false);
            reader->set_file_reader(input);
            ParquetInitContext init;
            init.state = &state;
            init.column_descs = &columns;
            init.col_name_to_block_idx = &positions;
            init.tuple_descriptor = tuple_descriptor;
            init.params = &params;
            init.range = &range;
            const auto status = reader->init_reader(&init);
            if (file_destination) {
                EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
                EXPECT_NE(status.to_string().find("Parquet input does not support type"),
                          std::string::npos);
            } else {
                EXPECT_TRUE(status.ok()) << status;
            }
        }
    }
}

} // namespace doris
