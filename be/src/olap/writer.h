// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef BDG_PALO_BE_SRC_OLAP_WRITER_H
#define BDG_PALO_BE_SRC_OLAP_WRITER_H

#include "olap/olap_table.h"

namespace palo {
class OLAPData;
class OLAPIndex;
class OLAPTable;
class RowBlock;
class RowCursor;

// 抽象一个接口，接口只暴露row级别的写， 不暴露block级别接口
// 尽量和原接口兼容
// 先attach出内部指针，再填入数据的方式并不可取，但兼容的考虑先继续使用
class IWriter {
public:
    IWriter(bool is_push_write, SmartOLAPTable table) : 
            _is_push_write(is_push_write), 
            _table(table),
            _column_statistics(_table->num_key_fields(), std::pair<Field *, Field *>(NULL, NULL)),
            _row_index(0) {}
    virtual ~IWriter() {
        for (size_t i = 0; i < _column_statistics.size(); ++i) {
            SAFE_DELETE(_column_statistics[i].first);
            SAFE_DELETE(_column_statistics[i].second);
        } 
    }
    virtual OLAPStatus init() {
        OLAPStatus res = OLAP_SUCCESS;
        for (size_t i = 0; i < _column_statistics.size(); ++i) {
            _column_statistics[i].first = Field::create(_table->tablet_schema()[i]);
            if (_column_statistics[i].first == NULL) {
                OLAP_LOG_FATAL("fail to create column statistics field. [field_id=%lu]", i);
                return OLAP_ERR_MALLOC_ERROR;
            }
            if (!_column_statistics[i].first->allocate()) {
                OLAP_LOG_FATAL("fail to allocate column statistics field. [field_id=%lu]", i);
                return OLAP_ERR_MALLOC_ERROR;
            }
            _column_statistics[i].first->set_to_max();

            _column_statistics[i].second = Field::create(_table->tablet_schema()[i]);
            if (_column_statistics[i].second == NULL) {
                OLAP_LOG_FATAL("fail to create column statistics field. [field_id=%lu]", i);
                return OLAP_ERR_MALLOC_ERROR;
            }
            if (!_column_statistics[i].second->allocate()) {
                OLAP_LOG_FATAL("fail to allocate column statistics field. [field_id=%lu]", i);
                return OLAP_ERR_MALLOC_ERROR;
            }
            _column_statistics[i].second->set_null();
            _column_statistics[i].second->set_to_min();
        }
        return res;
    }
    virtual OLAPStatus attached_by(RowCursor* row_cursor) = 0;
    void next(const RowCursor& row_cursor) {
        for (size_t i = 0; i < _table->num_key_fields(); ++i) {
            /*
            if (NULL == row_cursor.get_field_by_index(i)) {
                _column_statistics[i].first->set_null();
                continue;
            }
            */
            if (_column_statistics[i].first->cmp(row_cursor.get_field_by_index(i)) > 0) {
                _column_statistics[i].first->copy(row_cursor.get_field_by_index(i));
            }

            if (_column_statistics[i].second->cmp(row_cursor.get_field_by_index(i)) < 0) {
                _column_statistics[i].second->copy(row_cursor.get_field_by_index(i));
            }
        }

        ++_row_index;
    }
    virtual OLAPStatus finalize() = 0;
    virtual OLAPStatus write_row_block(RowBlock* row_block) = 0;
    virtual uint64_t written_bytes() = 0;
    // Factory function
    // 调用者获得新建的对象, 并负责delete释放
    static IWriter* create(SmartOLAPTable table, OLAPIndex* index, bool is_push_write);

protected:
    bool _is_push_write;
    SmartOLAPTable _table;
    std::vector<std::pair<Field *, Field *> > _column_statistics; // first is min, second is max
    uint32_t _row_index;
};

// OLAPDataWriter writes rows into a new version, including data and indexes files.
// OLAPDataWriter does not take OLAPIndex ownership.
// Common usage is:
// 1.     index = new OLAPIndex(table, new_version...)
// 2.     OLAPDataWriter writer(table, index)
// 3.     writer.init()
// ===========================================
// 4.     loop:
//          make row block...
//          writer.write_row_block(row_block)
// OR----------------------------------------
// 4.     RowCursor* cursor
//        loop:
//          writer.attached_by(cursor)
//          reader.read(cursor)
//          writer.next()
// ===========================================
// 5.     writer.finalize()
// 6.     if errors happen in write_row_block() or finalize()
//          index->delete_all_files()
//          delete index
//
// 7.     index->load()
// 8.     we use index now ...
class OLAPDataWriter : public IWriter {
public:
    OLAPDataWriter(SmartOLAPTable table, OLAPIndex* index, bool is_push_write = false);

    virtual ~OLAPDataWriter();

    virtual OLAPStatus init();

    // Init with custom num rows of row block
    OLAPStatus init(uint32_t num_rows_per_row_block);

    // Write one row_block into OLAPData and OLAPIndex, if the segment size
    // exceeds max segment size, finalize the last segment, and add new segment.
    OLAPStatus write_row_block(RowBlock* row_block);

    // In order to avoid memory copy while reading and writing, attach the
    // row_cursor to the row block being written.
    // If the number of rows reached maximum, the row_block will be added into
    // OLAPData, and one index item will be added into OLAPIndex.
    virtual OLAPStatus attached_by(RowCursor* row_cursor);

    // Flush the row block written before to OLAPIndex
    OLAPStatus flush();

    // sync data to disk, ignore error
    void sync();

    // call finalize function after calling writes in spite of failue in writes.
    virtual OLAPStatus finalize();

    virtual uint64_t written_bytes();

private:
    OLAPIndex* _index;
    OLAPData* _data;
    // current OLAPData Segment size, it is used to prevent OLAPData Segment
    // size exceeding _max_segment_size(OLAP_MAX_SEGMENT_FILE_SIZE)
    uint32_t _current_segment_size;
    uint32_t _max_segment_size;  // default it is OLAP_MAX_SEGMENT_FILE_SIZE
    RowBlock* _row_block;
    int64_t _num_rows;

    // write limit
    bool _is_push_write;
    uint32_t _write_mbytes_per_sec;
    OlapStopWatch _speed_limit_watch;

    DISALLOW_COPY_AND_ASSIGN(OLAPDataWriter);
};

}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_WRITER_H
