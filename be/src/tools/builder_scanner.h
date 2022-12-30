#pragma once
#include "common/object_pool.h"
#include "common/status.h"
#include "env/env.h"
#include "exec/parquet_scanner.h"
#include "exprs/cast_functions.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/segment_v2.pb.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "io/buffered_reader.h"
#include "io/file_reader.h"
#include "io/local_file_reader.h"
#include "json2pb/pb_to_json.h"
#include "olap/data_dir.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/row.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_id_generator.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "olap/storage_policy_mgr.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_cache.h"
#include "olap/utils.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "runtime/user_function_cache.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/file_utils.h"
#include "util/runtime_profile.h"
#include "util/time.h"
#include "vec/exec/format/parquet/vparquet_file_metadata.h"
#include "vec/exec/vbroker_scan_node.h"

namespace doris {

class BuilderScanner {
public:
    BuilderScanner(TabletSharedPtr tablet, const std::string& file_type, bool isHDFS);
    ~BuilderScanner() {}
    void init();
    void doSegmentBuild(const std::string& path);

private:
    int create_src_tuple(TDescriptorTable& t_desc_table, int next_slot_id);
    int create_dst_tuple(TDescriptorTable& t_desc_table, int next_slot_id);
    void create_expr_info();
    void init_desc_table();
    void build_scan_ranges(std::vector<TBrokerRangeDesc>& ranges, const std::string& path);
    RuntimeState _runtime_state;
    ObjectPool _obj_pool;
    TBrokerScanRangeParams _params;
    DescriptorTbl* _desc_tbl;
    TPlanNode _tnode;
    TabletSharedPtr _tablet;
    std::string _file_type;
    bool _isHDFS;
};

} // namespace doris
