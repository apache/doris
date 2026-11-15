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
struct _mongoc_collection_t;
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
    // Open the logical namespace with a private client lease. The callback stays valid until close;
    // callers must close even after a partially successful open returns an error.
    virtual Status open(const TMongoScanDesc& descriptor, std::function<bool()> cancelled) = 0;
    // On success, either return one borrowed document with eof=false, or eof=true with no document.
    // A command/cancellation error is a failing Status, never normal EOF.
    virtual Status next(const _bson_t** document, bool* eof) = 0;
    // Idempotently destroy the cursor before returning its client lease to the pool.
    virtual void close() = 0;
};
// Production libmongoc transport; a factory can inject a deterministic stream into reader tests.
std::unique_ptr<MongoCursorStream> make_native_mongo_stream();
using MongoStreamFactory = std::function<std::unique_ptr<MongoCursorStream>()>;
// Validate the typed single-cursor contract and resource bounds before creating driver objects.
Status validate_mongodb_range(const TFileRangeDesc& range);
// Apply the effective catalog mode while preserving URI tag sets and max staleness on the collection.
Status configure_mongodb_read_preference(_mongoc_collection_t* collection,
                                         const std::string& preference);

// FileScannerV2 adapter for one MongoDB logical namespace. init parses the pinned schema without
// network I/O; open binds a file-local request and creates its stream; get_block copies BSON values
// into Doris columns through their SerDes. EOF/error/early termination all end in close.
// Scanner execution is serial for this reader; RuntimeState and slot descriptors outlive it.
class MongoFileReader final : public FileReader {
public:
    MongoFileReader(std::shared_ptr<io::FileSystemProperties>& properties,
                    std::unique_ptr<io::FileDescription>& description,
                    std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                    TFileRangeDesc range, std::vector<SlotDescriptor*> slots,
                    MongoStreamFactory factory = {});
    ~MongoFileReader() override;
    // Freeze bindings from the FE descriptor and initialize session batch limits/profile counters.
    Status init(RuntimeState* state) override;
    // Expose projected slot types as the materialized file schema, with slot ordinals as local IDs.
    Status get_schema(std::vector<ColumnDefinition>* schema) const override;
    // Keep table-to-local identity translation in the mapper; the reader consumes its local request.
    std::unique_ptr<TableColumnMapper> create_column_mapper(
            TableColumnMapperOptions options) const override;
    // Override the Doris output row limit; MongoDB's driver batch_size remains in the descriptor.
    void set_batch_size(size_t size) override;
    // Resolve local column bindings and open a fresh cursor. A second open needs an intervening close.
    Status open(std::shared_ptr<FileScanRequest> request) override;
    // Produce locally filtered rows, releasing the cursor on EOF/error. An empty block can still
    // have eof=false when local predicates remove the entire source batch.
    Status get_block(Block* block, size_t* rows, bool* eof) override;
    // Release the cursor/request and mark EOF. Repeated close and destructor cleanup are safe.
    Status close() override;

private:
    // Attribute native transport time to the common FileReader I/O hierarchy as well as Mongo timers.
    void _init_profile() override;
    // Append whole rows until the row/BSON-byte budget is reached, then evaluate localized filters.
    // SerDes roll back partial cells; this method also rolls back earlier columns in a failed row.
    Status _read_block(Block* block, size_t* rows, bool* eof);
    const TFileRangeDesc _range;
    const std::vector<SlotDescriptor*> _slots;
    MongoStreamFactory _factory;
    RuntimeState* _state = nullptr;
    size_t _batch_size = 1024;
    std::unique_ptr<MongoCursorStream> _stream;
    // Owned by the reader and immutable after init, keeping _read_bindings pointers stable.
    std::vector<BsonBinding> _bindings;
    // Parallel vectors indexed by projected field: binding, file-local block position and target SerDe.
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

// TableReader integration retains generic split orchestration and final table-block materialization.
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
