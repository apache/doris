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

#include <cstdint> // for uint32_t
#include <cstddef> // for size_t
#include <memory> // for unique_ptr

#include "common/status.h" // for Status
#include "gen_cpp/segment_v2.pb.h" // for ColumnMetaPB
#include "olap/rowset/segment_v2/common.h" // for rowid_t
#include "olap/rowset/segment_v2/ordinal_page_index.h" // for OrdinalPageIndexIterator

namespace doris {

class ColumnBlock;
class Arena;
class RandomAccessFile;
class TypeInfo;

namespace segment_v2 {

class EncodingInfo;
class PageHandle;
class PagePointer;
class ParsedPage;
class ColumnIterator;

struct ColumnReaderOptions {
    bool verify_checksum = false;
};

// Used to read one column's data. And user should pass ColumnData meta
// when he want to read this column's data.
// There will be concurrent users to read the same column. So
// we should do our best to reduce resource usage through share
// same information, such as OrdinalPageIndex and Page data.
// This will cache data shared by all reader
class ColumnReader {
public:
    ColumnReader(const ColumnReaderOptions& opts, const ColumnMetaPB& meta, RandomAccessFile* file);
    ~ColumnReader();

    Status init();

    // create a new column iterator. Client should delete returned iterator
    Status new_iterator(ColumnIterator** iterator);

    // Seek to the first entry in the column.
    Status seek_to_first(OrdinalPageIndexIterator* iter);
    Status seek_at_or_before(rowid_t rowid, OrdinalPageIndexIterator* iter);

    // read a page from file into a page handle
    Status read_page(const PagePointer& pp, PageHandle* handle);

    bool is_nullable() const { return _meta.is_nullable(); }
    bool has_checksum() const { return _meta.has_checksum(); }
    const EncodingInfo* encoding_info() const { return _encoding_info; }
    const TypeInfo* type_info() const { return _type_info; }

private:
    Status _init_ordinal_index();

private:
    // input param
    ColumnReaderOptions _opts;
    // we need colun data to parse column data.
    // use shared_ptr here is to make things simple
    ColumnMetaPB _meta;
    RandomAccessFile* _file = nullptr;

    const TypeInfo* _type_info = nullptr;
    const EncodingInfo* _encoding_info = nullptr;

    // get page pointer from index
    std::unique_ptr<OrdinalPageIndex> _ordinal_index;
};

// Base iterator to read one column data
class ColumnIterator {
public:
    ColumnIterator() { }
    virtual ~ColumnIterator() { }

    // Seek to the first entry in the column.
    virtual Status seek_to_first() = 0;

    // Seek to the given ordinal entry in the column.
    // Entry 0 is the first entry written to the column.
    // If provided seek point is past the end of the file,
    // then returns false.
    virtual Status seek_to_ordinal(rowid_t ord_idx) = 0;

    // After one seek, we can call this function many times to read data 
    // into ColumnBlock. when read string type data, memory will allocated
    // from Arena
    virtual Status next_batch(size_t* n, ColumnBlock* dst) = 0;

    // Get current oridinal
    virtual rowid_t get_current_oridinal() const = 0;

#if 0
    // Call this function every time before next_batch.
    // This function will preload pages from disk into memory if necessary.
    Status prepare_batch(size_t n);

    // Fetch the next vector of values from the page into 'dst'.
    // The output vector must have space for up to n cells.
    //
    // return the size of entries.
    //
    // In the case that the values are themselves references
    // to other memory (eg Slices), the referred-to memory is
    // allocated in the dst column vector's arena.
    Status scan(size_t* n, ColumnBlock* dst, Arena* arena);

    // release next_batch related resource
    Status finish_batch();
#endif
};

#if 0
class DefaultValueIterator : public ColumnIterator {
};
#endif

// This iterator is used to read column data from file
class FileColumnIterator : public ColumnIterator {
public:
    FileColumnIterator(ColumnReader* reader);
    ~FileColumnIterator() override;

    Status seek_to_first() override;

    Status seek_to_ordinal(rowid_t ord_idx) override;

    Status next_batch(size_t* n, ColumnBlock* dst) override;

    // Get current oridinal
    rowid_t get_current_oridinal() const override { return _current_rowid; }

private:
    void _seek_to_pos_in_page(ParsedPage* page, uint32_t offset_in_page);
    Status _load_next_page(bool* eos);
    Status _read_page(const OrdinalPageIndexIterator& iter, ParsedPage* page);

private:
    ColumnReader* _reader;

    // We define an operation is one seek and follwing read.
    // If new seek is issued, there will be a new operation
    // current page
    // When _page is null, it means that this reader can not be read
    std::unique_ptr<ParsedPage> _page;

    // page iterator used to get next page when current page is finished.
    // This value will be reset when a new seek is issued
    OrdinalPageIndexIterator _page_iter;

    // current rowid
    rowid_t _current_rowid = 0;
};

}
}
