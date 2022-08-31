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

#include <map>
#include <string>
#include <vector>

#include "exec/base_scanner.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "olap/file_helper.h"
#include "olap/merger.h"
#include "olap/olap_common.h"
#include "olap/row_cursor.h"
#include "olap/rowset/rowset.h"
#include "olap/tablet_schema.h"

namespace doris {

class BinaryFile;
class BinaryReader;
struct ColumnMapping;
class RowCursor;

class PushHandler {
public:
    using SchemaMapping = std::vector<ColumnMapping>;

    PushHandler() = default;
    ~PushHandler() = default;

    // Load local data file into specified tablet.
    Status process_streaming_ingestion(TabletSharedPtr tablet, const TPushReq& request,
                                       PushType push_type,
                                       std::vector<TTabletInfo>* tablet_info_vec);

    int64_t write_bytes() const { return _write_bytes; }
    int64_t write_rows() const { return _write_rows; }

private:
    Status _convert_v2(TabletSharedPtr cur_tablet, RowsetSharedPtr* cur_rowset,
                       TabletSchemaSPtr tablet_schema);
    // Convert local data file to internal formatted delta,
    // return new delta's SegmentGroup
    Status _convert(TabletSharedPtr cur_tablet, RowsetSharedPtr* cur_rowset,
                    TabletSchemaSPtr tablet_schema);

    // Only for debug
    std::string _debug_version_list(const Versions& versions) const;

    Status _do_streaming_ingestion(TabletSharedPtr tablet, const TPushReq& request,
                                   PushType push_type, std::vector<TTabletInfo>* tablet_info_vec);

private:
    // mainly tablet_id, version and delta file path
    TPushReq _request;

    ObjectPool _pool;
    DescriptorTbl* _desc_tbl = nullptr;

    int64_t _write_bytes = 0;
    int64_t _write_rows = 0;
    DISALLOW_COPY_AND_ASSIGN(PushHandler);
};

// package FileHandlerWithBuf to read header of dpp output file
class BinaryFile : public FileHandlerWithBuf {
public:
    BinaryFile() = default;
    virtual ~BinaryFile() { close(); }

    Status init(const char* path);

    size_t header_size() const { return _header.size(); }
    size_t file_length() const { return _header.file_length(); }
    uint32_t checksum() const { return _header.checksum(); }
    SchemaHash schema_hash() const { return _header.message().schema_hash(); }

private:
    FileHeader<OLAPRawDeltaHeaderMessage, int32_t, FileHandlerWithBuf> _header;

    DISALLOW_COPY_AND_ASSIGN(BinaryFile);
};

class IBinaryReader {
public:
    static IBinaryReader* create(bool need_decompress);
    virtual ~IBinaryReader() = default;

    virtual Status init(TabletSchemaSPtr tablet_schema, BinaryFile* file) = 0;
    virtual Status finalize() = 0;

    virtual Status next(RowCursor* row) = 0;

    virtual bool eof() = 0;

    // call this function after finalize()
    bool validate_checksum() { return _adler_checksum == _file->checksum(); }

protected:
    IBinaryReader()
            : _file(nullptr),
              _content_len(0),
              _curr(0),
              _adler_checksum(ADLER32_INIT),
              _ready(false) {}

    BinaryFile* _file;
    TabletSchemaSPtr _tablet_schema;
    size_t _content_len;
    size_t _curr;
    uint32_t _adler_checksum;
    bool _ready;
};

// input file reader for Protobuffer format
class BinaryReader : public IBinaryReader {
public:
    explicit BinaryReader();
    ~BinaryReader() override { finalize(); }

    Status init(TabletSchemaSPtr tablet_schema, BinaryFile* file) override;
    Status finalize() override;

    Status next(RowCursor* row) override;

    bool eof() override { return _curr >= _content_len; }

private:
    char* _row_buf;
    size_t _row_buf_size;
};

class LzoBinaryReader : public IBinaryReader {
public:
    explicit LzoBinaryReader();
    ~LzoBinaryReader() override { finalize(); }

    Status init(TabletSchemaSPtr tablet_schema, BinaryFile* file) override;
    Status finalize() override;

    Status next(RowCursor* row) override;

    bool eof() override { return _curr >= _content_len && _row_num == 0; }

private:
    Status _next_block();

    using RowNumType = uint32_t;
    using CompressedSizeType = uint64_t;

    char* _row_buf;
    char* _row_compressed_buf;
    char* _row_info_buf;
    size_t _max_row_num;
    size_t _max_row_buf_size;
    size_t _max_compressed_buf_size;
    size_t _row_num;
    size_t _next_row_start;
};

class PushBrokerReader {
public:
    PushBrokerReader() : _ready(false), _eof(false), _fill_tuple(false) {}
    ~PushBrokerReader() = default;

    Status init(const Schema* schema, const TBrokerScanRange& t_scan_range,
                const TDescriptorTable& t_desc_tbl);
    Status next(ContiguousRow* row);
    void print_profile();

    Status close() {
        _ready = false;
        return Status::OK();
    }
    bool eof() const { return _eof; }
    bool is_fill_tuple() const { return _fill_tuple; }
    MemPool* mem_pool() { return _mem_pool.get(); }

private:
    Status fill_field_row(RowCursorCell* dst, const char* src, bool src_null, MemPool* mem_pool,
                          FieldType type);
    bool _ready;
    bool _eof;
    bool _fill_tuple;
    TupleDescriptor* _tuple_desc;
    Tuple* _tuple;
    const Schema* _schema;
    std::unique_ptr<RuntimeState> _runtime_state;
    RuntimeProfile* _runtime_profile;
    std::unique_ptr<MemPool> _mem_pool;
    std::unique_ptr<ScannerCounter> _counter;
    std::unique_ptr<BaseScanner> _scanner;
    // Not used, just for placeholding
    std::vector<TExpr> _pre_filter_texprs;
};

} // namespace doris
