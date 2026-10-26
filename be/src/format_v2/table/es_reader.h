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
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "format_v2/file_reader.h"
#include "format_v2/table_reader.h"

namespace doris {
class ScrollParser;
class SlotDescriptor;
} // namespace doris

namespace doris::format::es {

// The transport boundary allows reader tests to use real ES responses without a running server.
class EsScrollStream {
public:
    virtual ~EsScrollStream() = default;
    virtual Status open() = 0;
    virtual Status next(bool* eof, std::unique_ptr<ScrollParser>& parser) = 0;
    virtual Status close() = 0;
};
using EsScrollStreamFactory = std::function<std::unique_ptr<EsScrollStream>(
        const std::string&, const std::map<std::string, std::string>&, bool)>;

class EsFileReader final : public FileReader {
public:
    EsFileReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                 std::unique_ptr<io::FileDescription>& file_description,
                 std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                 TFileRangeDesc range, const TFileScanRangeParams& params,
                 std::vector<SlotDescriptor*> file_slot_descs,
                 EsScrollStreamFactory stream_factory = {});
    ~EsFileReader() override;

    Status init(RuntimeState* state) override;
    Status get_schema(std::vector<ColumnDefinition>* schema) const override;
    std::unique_ptr<TableColumnMapper> create_column_mapper(
            TableColumnMapperOptions options) const override;
    void set_batch_size(size_t batch_size) override;
    Status open(std::shared_ptr<FileScanRequest> request) override;
    Status get_block(Block* block, size_t* rows, bool* eof) override;
    Status close() override;

private:
    void _init_profile() override;
    Status _open_stream();
    Status _next_page();
    Status _read_block(Block* block, size_t* rows, bool* eof);

    const TFileRangeDesc _range;
    const TFileScanRangeParams& _params;
    const std::vector<SlotDescriptor*> _file_slots;
    EsScrollStreamFactory _stream_factory;
    RuntimeState* _state = nullptr;
    size_t _batch_size = 1024;
    std::unique_ptr<EsScrollStream> _stream;
    std::unique_ptr<ScrollParser> _parser;
    std::vector<SlotDescriptor*> _read_slots;
    std::vector<size_t> _read_positions;
    std::map<std::string, std::string> _docvalue_context;
    size_t _page_rows_remaining = 0;
    bool _source_eof = false;
    RuntimeProfile::Counter* _total_time = nullptr;
    RuntimeProfile::Counter* _read_time = nullptr;
    RuntimeProfile::Counter* _materialize_time = nullptr;
    RuntimeProfile::Counter* _filter_time = nullptr;
    RuntimeProfile::Counter* _io_time = nullptr;
    RuntimeProfile::Counter* _batches_read = nullptr;
    RuntimeProfile::Counter* _rows_read = nullptr;
};

class EsReader final : public TableReader {
public:
    explicit EsReader(EsScrollStreamFactory stream_factory = {});
    Status init(TableReadOptions&& options) override;
    Status prepare_split(const SplitReadOptions& options) override;

protected:
    Status create_file_reader(std::unique_ptr<FileReader>* reader) override;

private:
    EsScrollStreamFactory _stream_factory;
};

} // namespace doris::format::es
