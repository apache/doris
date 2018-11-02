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

#ifndef DORIS_BE_SRC_OLAP_WRITER_H
#define DORIS_BE_SRC_OLAP_WRITER_H

#include "olap/olap_table.h"
#include "olap/schema.h"
#include "olap/wrapper_field.h"

namespace doris {
class OLAPData;
class Rowset;
class OLAPTable;
class RowBlock;
class RowCursor;

// 抽象一个接口，接口只暴露row级别的写， 不暴露block级别接口
// 尽量和原接口兼容
// 先attach出内部指针，再填入数据的方式并不可取，但兼容的考虑先继续使用
class IWriter {
public:
    IWriter(bool is_push_write, OLAPTablePtr table) : 
            _is_push_write(is_push_write), 
            _table(table),
            _column_statistics(
                _table->num_key_fields(), std::pair<WrapperField*, WrapperField*>(NULL, NULL)),
            _row_index(0) {}
    virtual ~IWriter() {
        for (size_t i = 0; i < _column_statistics.size(); ++i) {
            SAFE_DELETE(_column_statistics[i].first);
            SAFE_DELETE(_column_statistics[i].second);
        } 
    }
    virtual OLAPStatus init() {
        for (size_t i = 0; i < _column_statistics.size(); ++i) {
            _column_statistics[i].first = WrapperField::create(_table->tablet_schema()[i]);
            DCHECK(_column_statistics[i].first != nullptr) << "fail to create column statistics field.";
            _column_statistics[i].first->set_to_max();

            _column_statistics[i].second = WrapperField::create(_table->tablet_schema()[i]);
            DCHECK(_column_statistics[i].second != nullptr) << "fail to create column statistics field.";
            _column_statistics[i].second->set_null();
            _column_statistics[i].second->set_to_min();
        }
        return OLAP_SUCCESS;
    }
    virtual OLAPStatus attached_by(RowCursor* row_cursor) = 0;
    void next(const RowCursor& row_cursor) {
        for (size_t i = 0; i < _table->num_key_fields(); ++i) {
            char* right = row_cursor.get_field_by_index(i)->get_field_ptr(row_cursor.get_buf());
            if (_column_statistics[i].first->cmp(right) > 0) {
                _column_statistics[i].first->copy(right);
            }

            if (_column_statistics[i].second->cmp(right) < 0) {
                _column_statistics[i].second->copy(right);
            }
        }

        ++_row_index;
    }
    void next(const char* row, const Schema* schema) {
        for (size_t i = 0; i < _table->num_key_fields(); ++i) {
            char* right = const_cast<char*>(row + schema->get_col_offset(i));
            if (_column_statistics[i].first->cmp(right) > 0) {
                _column_statistics[i].first->copy(right);
            }

            if (_column_statistics[i].second->cmp(right) < 0) {
                _column_statistics[i].second->copy(right);
            }
        }

        ++_row_index;
    }
    virtual OLAPStatus write(const char* row) = 0;
    virtual OLAPStatus finalize() = 0;
    virtual uint64_t written_bytes() = 0;
    virtual MemPool* mem_pool() = 0;
    // Factory function
    // 调用者获得新建的对象, 并负责delete释放
    static IWriter* create(OLAPTablePtr table, Rowset* index, bool is_push_write);
protected:
    bool _is_push_write;
    OLAPTablePtr _table;
    // first is min, second is max
    std::vector<std::pair<WrapperField*, WrapperField*>> _column_statistics;
    uint32_t _row_index;
};

// OLAPDataWriter writes rows into a new version, including data and indexes files.
// OLAPDataWriter does not take Rowset ownership.
// Common usage is:
// 1.     index = new Rowset(table, new_version...)
// 2.     OLAPDataWriter writer(table, index)
// 3.     writer.init()
// ===========================================
// 4.     loop:
//          make row block...
//          writer.flush_row_block(row_block)
// OR----------------------------------------
// 4.     RowCursor* cursor
//        loop:
//          writer.attached_by(cursor)
//          reader.read(cursor)
//          writer.next()
// ===========================================
// 5.     writer.finalize()
// 6.     if errors happen in flush_row_block() or finalize()
//          index->delete_all_files()
//          delete index
//
// 7.     index->load()
// 8.     we use index now ...
class OLAPDataWriter : public IWriter {
public:
    OLAPDataWriter(OLAPTablePtr table, Rowset* index, bool is_push_write = false);

    virtual ~OLAPDataWriter();

    virtual OLAPStatus init();

    // Init with custom num rows of row block
    OLAPStatus init(uint32_t num_rows_per_row_block);

    // In order to avoid memory copy while reading and writing, attach the
    // row_cursor to the row block being written.
    // If the number of rows reached maximum, the row_block will be added into
    // OLAPData, and one index item will be added into Rowset.
    virtual OLAPStatus attached_by(RowCursor* row_cursor);
    virtual OLAPStatus write(const char* row);

    // sync data to disk, ignore error
    void sync();

    // call finalize function after calling writes in spite of failue in writes.
    virtual OLAPStatus finalize();

    virtual uint64_t written_bytes();
    virtual MemPool* mem_pool();
private:
    // Flush the row block written before to Rowset
    OLAPStatus _flush_row_block();
    OLAPStatus _flush_segment_with_verfication();

    Rowset* _index;
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

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_WRITER_H
