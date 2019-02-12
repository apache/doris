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

#ifndef DORIS_BE_SRC_OLAP_PUSH_HANDLER_H
#define DORIS_BE_SRC_OLAP_PUSH_HANDLER_H

#include <map>
#include <string>
#include <vector>

#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "olap/file_helper.h"
#include "olap/merger.h"
#include "olap/olap_common.h"
#include "olap/segment_group.h"
#include "olap/row_cursor.h"
#include "olap/data_writer.h"

namespace doris {

typedef std::vector<ColumnData*> DataSources;
typedef std::vector<SegmentGroup*> Indices;

class BinaryFile;
class BinaryReader;
class ColumnMapping;
class RowCursor;

struct TableVars {
    OLAPTablePtr olap_table;
    Versions unused_versions;
    Indices unused_indices;
    Indices added_indices;
};

class PushHandler {
public:
    typedef std::vector<ColumnMapping> SchemaMapping;

    PushHandler() : _header_locked(false) {}
    ~PushHandler() {}

    // Load local data file into specified tablet.
    OLAPStatus process(
            OLAPTablePtr olap_table,
            const TPushReq& request,
            PushType push_type,
            std::vector<TTabletInfo>* tablet_info_vec);

    OLAPStatus process_realtime_push(
            OLAPTablePtr olap_table,
            const TPushReq& request,
            PushType push_type,
            std::vector<TTabletInfo>* tablet_info_vec);

    int64_t write_bytes() const { return _write_bytes; }
    int64_t write_rows() const { return _write_rows; }
private:
    // Validate request, mainly data version check.
    OLAPStatus _validate_request(
            OLAPTablePtr olap_table_for_raw,
            OLAPTablePtr olap_table_for_schema_change,
            bool is_rollup_new_table,
            PushType push_type);

    // The latest version can be reverted for following scene:
    // user submit a push job and cancel it soon, but some 
    // tablets already push success.
    OLAPStatus _get_versions_reverted(
            OLAPTablePtr olap_table,
            bool is_schema_change_tablet,
            PushType push_type,
            Versions* unused_versions);

    // Convert local data file to internal formatted delta,
    // return new delta's SegmentGroup
    OLAPStatus _convert(
            OLAPTablePtr curr_olap_table,
            OLAPTablePtr new_olap_table_vec,
            Indices* curr_olap_indices,
            Indices* new_olap_indices,
            AlterTabletType alter_table_type);

    // Update header info when new version add or dirty version removed.
    OLAPStatus _update_header(
            OLAPTablePtr olap_table,
            Versions* unused_versions,
            Indices* new_indices,
            Indices* unused_indices);

    // remove all old file of cumulatives versions
    void _delete_old_indices(Indices* indices);

    // Clear schema change information.
    OLAPStatus _clear_alter_table_info(
            OLAPTablePtr olap_table,
            OLAPTablePtr related_olap_table);

    // Only for debug
    std::string _debug_version_list(const Versions& versions) const;

    // Lock tablet header before read header info.
    void _obtain_header_rdlock() {
        for (std::list<OLAPTablePtr>::iterator it = _olap_table_arr.begin();
                it != _olap_table_arr.end(); ++it) {
            VLOG(3) << "obtain all header locks rd. tablet=" << (*it)->full_name();
            (*it)->obtain_header_rdlock();
        }

        _header_locked = true;
    }

    // Locak tablet header before write header info.
    void _obtain_header_wrlock() {
        for (std::list<OLAPTablePtr>::iterator it = _olap_table_arr.begin();
                it != _olap_table_arr.end(); ++it) {
            VLOG(3) << "obtain all header locks wr. tablet=" << (*it)->full_name();
            (*it)->obtain_header_wrlock();
        }

        _header_locked = true;
    }

    // Release tablet header lock.
    void _release_header_lock() {
        if (_header_locked) {
            for (std::list<OLAPTablePtr>::reverse_iterator it = _olap_table_arr.rbegin();
                    it != _olap_table_arr.rend(); ++it) {
                VLOG(3) << "release all header locks. tablet=" << (*it)->full_name();
                (*it)->release_header_lock();
            }

            _header_locked = false;
        }
    }

    void _get_tablet_infos(
            const std::vector<TableVars>& table_infoes,
            std::vector<TTabletInfo>* tablet_info_vec);

    // mainly tablet_id, version and delta file path
    TPushReq _request;

    // maily contains specified tablet object
    // contains related tables also if in schema change, tablet split or rollup
    std::list<OLAPTablePtr> _olap_table_arr;

    // lock tablet header before modify tabelt header
    bool _header_locked;

    int64_t _write_bytes = 0;
    int64_t _write_rows = 0;
    DISALLOW_COPY_AND_ASSIGN(PushHandler);
};

// package FileHandlerWithBuf to read header of dpp output file
class BinaryFile : public FileHandlerWithBuf {
public:
    BinaryFile() {}
    virtual ~BinaryFile() {
        close();
    }

    OLAPStatus init(const char* path);

    size_t header_size() const {
        return _header.size();
    }
    size_t file_length() const {
        return _header.file_length();
    }
    uint32_t checksum() const {
        return _header.checksum();
    }
    SchemaHash schema_hash() const {
        return _header.message().schema_hash();
    }

private:
    FileHeader<OLAPRawDeltaHeaderMessage, int32_t, FileHandlerWithBuf> _header;

    DISALLOW_COPY_AND_ASSIGN(BinaryFile);
};

class IBinaryReader {
public:
    static IBinaryReader* create(bool need_decompress);
    virtual ~IBinaryReader() {}

    virtual OLAPStatus init(OLAPTablePtr table, BinaryFile* file) = 0;
    virtual OLAPStatus finalize() = 0;

    virtual OLAPStatus next(RowCursor* row, MemPool* mem_pool) = 0;

    virtual bool eof() = 0;

    // call this function after finalize()
    bool validate_checksum() {
        return _adler_checksum == _file->checksum();
    }

protected:
    IBinaryReader()
        : _file(NULL),
          _content_len(0),
          _curr(0),
          _adler_checksum(ADLER32_INIT),
          _ready(false) {
    }

    BinaryFile* _file;
    OLAPTablePtr _table;
    size_t _content_len;
    size_t _curr;
    uint32_t _adler_checksum;
    bool _ready;
};

// input file reader for Protobuffer format
class BinaryReader: public IBinaryReader {
public:
    explicit BinaryReader();
    virtual ~BinaryReader() {
        finalize();
    }

    virtual OLAPStatus init(OLAPTablePtr table, BinaryFile* file);
    virtual OLAPStatus finalize();

    virtual OLAPStatus next(RowCursor* row, MemPool* mem_pool);

    virtual bool eof() {
        return _curr >= _content_len;
    }

private:
    char* _row_buf;
    size_t _row_buf_size;
};

class LzoBinaryReader: public IBinaryReader {
public:
    explicit LzoBinaryReader();
    virtual ~LzoBinaryReader() {
        finalize();
    }

    virtual OLAPStatus init(OLAPTablePtr table, BinaryFile* file);
    virtual OLAPStatus finalize();

    virtual OLAPStatus next(RowCursor* row, MemPool* mem_pool);

    virtual bool eof() {
        return _curr >= _content_len && _row_num == 0;
    }

private:
    OLAPStatus _next_block();

    typedef uint32_t RowNumType;
    typedef uint64_t CompressedSizeType;

    char* _row_buf;
    char* _row_compressed_buf;
    char* _row_info_buf;
    size_t _max_row_num;
    size_t _max_row_buf_size;
    size_t _max_compressed_buf_size;
    size_t _row_num;
    size_t _next_row_start;
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_PUSH_HANDLER_H
