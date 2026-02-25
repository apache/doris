#pragma once

#include <gen_cpp/segment_v2.pb.h>
#include <stdint.h>

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/indexed_column_reader.h"
#include "util/once.h"

namespace roaring {
class Roaring;
}

namespace doris::segment_v2 {

class NestedOffsetsMappingIndex {
public:
    virtual ~NestedOffsetsMappingIndex() = default;

    virtual Status get_total_elements(const ColumnIteratorOptions& opts,
                                      uint64_t* total_elements) const = 0;

    virtual Status map_elements_to_parent_ords(const ColumnIteratorOptions& opts,
                                               const roaring::Roaring& element_bitmap,
                                               roaring::Roaring* parent_bitmap) const = 0;
};

class NestedOffsetsMappingIndexWriter {
public:
    explicit NestedOffsetsMappingIndexWriter(uint32_t block_size = 4096) : _block_size(block_size) {}

    Status build(const uint64_t* offsets, size_t num_rows);

    Status write(io::FileWriter* file_writer, ColumnMetaPB* offsets_meta,
                 CompressionTypePB compression_type) const;

private:
    uint32_t _block_size;
    std::vector<int128_t> _block_end_offset_keys;
    std::vector<std::string> _block_cumulative_offsets;
};

class NestedOffsetsMappingIndexReader : public NestedOffsetsMappingIndex {
public:
    NestedOffsetsMappingIndexReader(io::FileReaderSPtr file_reader, NestedOffsetsIndexPB pb,
                                    uint64_t offsets_num_rows, bool kept_in_memory)
            : _file_reader(std::move(file_reader)),
              _pb(std::move(pb)),
              _offsets_num_rows(offsets_num_rows),
              _kept_in_memory(kept_in_memory) {}

    Status get_total_elements(const ColumnIteratorOptions& opts,
                              uint64_t* total_elements) const override;

    Status map_elements_to_parent_ords(const ColumnIteratorOptions& opts,
                                       const roaring::Roaring& element_bitmap,
                                       roaring::Roaring* parent_bitmap) const override;

private:
    Status _load(bool use_page_cache, OlapReaderStatistics* stats) const;

    io::FileReaderSPtr _file_reader;
    NestedOffsetsIndexPB _pb;
    uint64_t _offsets_num_rows = 0;
    bool _kept_in_memory = false;

    mutable DorisCallOnce<Status> _load_once;
    mutable std::unique_ptr<IndexedColumnReader> _block_end_offsets_reader;
    mutable std::unique_ptr<IndexedColumnReader> _block_cumulative_offsets_reader;
};

} // namespace doris::segment_v2
