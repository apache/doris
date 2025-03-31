#pragma once


#include "olap/rowset/segment_v2/index_writer.h"

#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/analysis/LanguageBasedAnalyzer.h>
#include <CLucene/util/bkd/bkd_writer.h>
#include <glog/logging.h>

#include <limits>
#include <memory>
#include <ostream>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "io/fs/local_file_system.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif

#include "CLucene/analysis/standard95/StandardAnalyzer.h"

#ifdef __clang__
#pragma clang diagnostic pop
#endif

#include "common/config.h"
#include "gutil/strings/strip.h"
#include "olap/field.h"
#include "olap/inverted_index_parser.h"
#include "olap/key_coder.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/char_filter/char_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index_common.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/x_index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "runtime/collection_value.h"
#include "runtime/exec_env.h"
#include "util/debug_points.h"
#include "util/faststring.h"
#include "util/slice.h"
#include "util/string_util.h"

#include "olap/rowset/segment_v2/index_writer.h"
#include "olap/rowset/segment_v2/x_index_file_writer.h"
#include "vector/vector_index.h"
#include "vector/diskann_vector_index.h"
#include "olap/tablet_schema.h"
#include "util/string_util.h"

namespace doris::segment_v2 {

const int32_t MAX_FIELD_LEN = 0x7FFFFFFFL;
const int32_t MERGE_FACTOR = 100000000;
const int32_t MAX_LEAF_COUNT = 1024;
const float MAXMBSortInHeap = 512.0 * 8;
const int DIMS = 1;

class AnnIndexColumnWriter : public IndexColumnWriter {
public:
    static constexpr const char* INDEX_TYPE = "index_type";
    static constexpr const char* METRIC_TYPE = "metric_type";
    static constexpr const char* DIM = "dim";
    static constexpr const char* DISKANN_MAX_DEGREE = "max_degree";
    static constexpr const char* DISKANN_SEARCH_LIST = "search_list";


    explicit AnnIndexColumnWriter(const std::string& field_name,
                            XIndexFileWriter* index_file_writer,
                            const TabletIndex* index_meta,
                            const bool single_field = true);

    ~AnnIndexColumnWriter() override;

    Status init() override;
    void close_on_error() override;
    Status add_nulls(uint32_t count) override;
    Status add_array_nulls(uint32_t row_id) override;
    Status add_values(const std::string fn, const void* values, size_t count) override;
    Status add_array_values(size_t field_size, const void* value_ptr, const uint8_t* null_map,
                            const uint8_t* offsets_ptr, size_t count) override;
    Status add_array_values(size_t field_size, const CollectionValue* values,
                            size_t count) override;
    int64_t size() const override;
    Status finish() override;

private:
    Status open_index_directory();
    Status init_ann_index();

private:
    rowid_t _rid = 0;
    bool _single_field = true;
    std::shared_ptr<DorisFSDirectory> _dir = nullptr;
    std::shared_ptr<VectorIndex> _vector_index_writer;
    XIndexFileWriter* _index_file_writer;
    uint32_t _ignore_above;
    std::wstring _field_name;
    const TabletIndex* _index_meta;
};

} // namespace doris::segment_v2
