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

#pragma once

#include <functional>
#include <memory>
#include <vector>

#include "format_v2/file_reader.h"
#include "format_v2/table_reader.h"

struct _bson_t;
namespace doris {
struct BsonBinding;
class DataTypeSerDe;
class SlotDescriptor;
} // namespace doris

namespace doris::format::mongodb {

// A cursor owns the borrowed BSON document until next() or close(). No cursor crosses a split boundary.
class MongoCursorStream {
public:
    virtual ~MongoCursorStream() = default;
    virtual Status open(const TMongoScanDesc& descriptor, std::function<bool()> cancelled) = 0;
    virtual Status next(const _bson_t** document, bool* eof) = 0;
    virtual void close() = 0;
};
std::unique_ptr<MongoCursorStream> make_native_mongo_stream();
using MongoStreamFactory = std::function<std::unique_ptr<MongoCursorStream>()>;
Status validate_mongodb_range(const TFileRangeDesc& range);

class MongoFileReader final : public FileReader {
public:
    MongoFileReader(std::shared_ptr<io::FileSystemProperties>& properties,
                    std::unique_ptr<io::FileDescription>& description,
                    std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                    TFileRangeDesc range, std::vector<SlotDescriptor*> slots,
                    MongoStreamFactory factory = {});
    ~MongoFileReader() override;
    Status init(RuntimeState* state) override;
    Status get_schema(std::vector<ColumnDefinition>* schema) const override;
    std::unique_ptr<TableColumnMapper> create_column_mapper(
            TableColumnMapperOptions options) const override;
    void set_batch_size(size_t size) override;
    Status open(std::shared_ptr<FileScanRequest> request) override;
    Status get_block(Block* block, size_t* rows, bool* eof) override;
    Status close() override;

private:
    void _init_profile() override;
    Status _read_block(Block* block, size_t* rows, bool* eof);
    const TFileRangeDesc _range;
    const std::vector<SlotDescriptor*> _slots;
    MongoStreamFactory _factory;
    RuntimeState* _state = nullptr;
    size_t _batch_size = 1024;
    std::unique_ptr<MongoCursorStream> _stream;
    std::vector<BsonBinding> _bindings;
    std::vector<const BsonBinding*> _read_bindings;
    std::vector<size_t> _positions;
    std::vector<std::shared_ptr<DataTypeSerDe>> _serdes;
    RuntimeProfile::Counter* _total_time = nullptr;
    RuntimeProfile::Counter* _io_time = nullptr;
    RuntimeProfile::Counter* _filter_time = nullptr;
    RuntimeProfile::Counter* _read_time = nullptr;
    RuntimeProfile::Counter* _decode_time = nullptr;
    RuntimeProfile::Counter* _rows_read = nullptr;
    RuntimeProfile::Counter* _bytes_read = nullptr;
};

class MongoReader final : public TableReader {
public:
    explicit MongoReader(MongoStreamFactory factory = {});
    Status prepare_split(const SplitReadOptions& options) override;

protected:
    Status create_file_reader(std::unique_ptr<FileReader>* reader) override;

private:
    MongoStreamFactory _factory;
};

} // namespace doris::format::mongodb
