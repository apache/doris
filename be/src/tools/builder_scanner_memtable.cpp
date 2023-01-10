#include "tools/builder_scanner_memtable.h"

#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "hdfs/hdfs.h"
#include "olap/delta_writer.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "vec/exec/vbroker_scan_node.h"

namespace doris {

static const int TUPLE_ID_DST = 0;
static const int TUPLE_ID_SRC = 1;
static const int BATCH_SIZE = 8192;

BuilderScannerMemtable::BuilderScannerMemtable(TabletSharedPtr tablet, const std::string& build_dir,
                                               const std::string& file_type, bool isHDFS)
        : _runtime_state(TQueryGlobals()),
          _tablet(tablet),
          _build_dir(build_dir),
          _file_type(file_type),
          _isHDFS(isHDFS) {
    init();
    _runtime_state.init_scanner_mem_trackers();
    TUniqueId uid;
    uid.hi = 1;
    uid.lo = 1;
    TQueryOptions _options;
    _options.batch_size = BATCH_SIZE;
    _options.enable_vectorized_engine = true;
    auto* _exec_env = ExecEnv::GetInstance();
    _runtime_state.init(uid, _options, TQueryGlobals(), _exec_env);
    _runtime_state.init_mem_trackers(uid);
}

void BuilderScannerMemtable::init() {
    create_expr_info();
    init_desc_table();

    // Node Id
    _tnode.node_id = 0;
    _tnode.node_type = TPlanNodeType::SCHEMA_SCAN_NODE;
    _tnode.num_children = 0;
    _tnode.limit = -1;
    _tnode.row_tuples.push_back(0);
    _tnode.nullable_tuples.push_back(false);
    _tnode.broker_scan_node.tuple_id = 0;
    _tnode.__isset.broker_scan_node = true;
}

TPrimitiveType::type BuilderScannerMemtable::getPrimitiveType(FieldType t) {
    switch (t) {
    case FieldType::OLAP_FIELD_TYPE_OBJECT: {
        return TPrimitiveType::OBJECT;
    }
    case FieldType::OLAP_FIELD_TYPE_HLL: {
        return TPrimitiveType::HLL;
    }
    case FieldType::OLAP_FIELD_TYPE_CHAR: {
        return TPrimitiveType::CHAR;
    }
    case FieldType::OLAP_FIELD_TYPE_VARCHAR: {
        return TPrimitiveType::VARCHAR;
    }
    case FieldType::OLAP_FIELD_TYPE_STRING: {
        return TPrimitiveType::STRING;
    }
    case FieldType::OLAP_FIELD_TYPE_DATE: {
        return TPrimitiveType::DATE;
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIME: {
        return TPrimitiveType::DATETIME;
    }
    case FieldType::OLAP_FIELD_TYPE_DATEV2: {
        return TPrimitiveType::DATEV2;
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2: {
        return TPrimitiveType::DATETIMEV2;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32: {
        return TPrimitiveType::DECIMAL32;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64: {
        return TPrimitiveType::DECIMAL64;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I: {
        return TPrimitiveType::DECIMAL128I;
    }
    case FieldType::OLAP_FIELD_TYPE_JSONB: {
        return TPrimitiveType::JSONB;
    }
    case FieldType::OLAP_FIELD_TYPE_BOOL: {
        return TPrimitiveType::BOOLEAN;
    }
    case FieldType::OLAP_FIELD_TYPE_TINYINT: {
        return TPrimitiveType::TINYINT;
    }
    case FieldType::OLAP_FIELD_TYPE_SMALLINT: {
        return TPrimitiveType::SMALLINT;
    }
    case FieldType::OLAP_FIELD_TYPE_INT: {
        return TPrimitiveType::INT;
    }
    case FieldType::OLAP_FIELD_TYPE_BIGINT: {
        return TPrimitiveType::BIGINT;
    }
    case FieldType::OLAP_FIELD_TYPE_LARGEINT: {
        return TPrimitiveType::LARGEINT;
    }
    case FieldType::OLAP_FIELD_TYPE_FLOAT: {
        return TPrimitiveType::FLOAT;
    }
    case FieldType::OLAP_FIELD_TYPE_DOUBLE: {
        return TPrimitiveType::DOUBLE;
    }
    case FieldType::OLAP_FIELD_TYPE_ARRAY: {
        return TPrimitiveType::ARRAY;
    }
    default: {
        LOG(FATAL) << "unknown type error:" << t;
    }
    }
}
TDescriptorTable BuilderScannerMemtable::create_descriptor_tablet() {
    TDescriptorTableBuilder dtb;

    // build DST table descriptor
    {
        TTupleDescriptorBuilder tuple_builder;
        for (int i = 0; i < _tablet->num_columns(); i++) {
            const auto& col = _tablet->tablet_schema()->column(i);

            tuple_builder.add_slot(TSlotDescriptorBuilder()
                                           .type(thrift_to_type(getPrimitiveType(col.type())))
                                           .column_name(col.name())
                                           .column_pos(i)
                                           .length(col.length())
                                           .build());
        }
        tuple_builder.build(&dtb);
    }

    // build SRC table descriptor
    {
        TTupleDescriptorBuilder tuple_builder;
        for (int i = 0; i < _tablet->num_columns(); i++) {
            const auto& col = _tablet->tablet_schema()->column(i);

            tuple_builder.add_slot(TSlotDescriptorBuilder()
                                           .type(thrift_to_type(getPrimitiveType(col.type())))
                                           .column_name(col.name())
                                           .column_pos(i)
                                           .length(col.length())
                                           .build());
        }
        tuple_builder.build(&dtb);
    }

    return dtb.desc_tbl();
}

void BuilderScannerMemtable::init_desc_table() {
    TDescriptorTable t_desc_table = create_descriptor_tablet();

    // table descriptors
    TTableDescriptor t_table_desc;

    t_table_desc.id = _tablet->table_id();
    t_table_desc.tableType = TTableType::OLAP_TABLE;
    t_table_desc.numCols = _tablet->num_columns();
    t_table_desc.numClusteringCols = 0;
    t_desc_table.tableDescriptors.push_back(t_table_desc);
    t_desc_table.__isset.tableDescriptors = true;

    DescriptorTbl::create(&_obj_pool, t_desc_table, &_desc_tbl);

    _runtime_state.set_desc_tbl(_desc_tbl);
}

void BuilderScannerMemtable::create_expr_info() {
    TTypeDesc varchar_type;
    {
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::VARCHAR);
        scalar_type.__set_len(65535);
        node.__set_scalar_type(scalar_type);
        varchar_type.types.push_back(node);
    }
    for (int i = 0; i < _tablet->num_columns(); i++) {
        auto col = _tablet->tablet_schema()->column(i);

        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = _tablet->num_columns() + i;
        slot_ref.slot_ref.tuple_id = 1;

        TExpr expr;
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(i, expr);
        _params.src_slot_ids.push_back(_tablet->num_columns() + i);
    }

    // _params.__isset.expr_of_dest_slot = true;
    _params.__set_dest_tuple_id(TUPLE_ID_DST);
    _params.__set_src_tuple_id(TUPLE_ID_SRC);
}

void BuilderScannerMemtable::build_scan_ranges(std::vector<TBrokerRangeDesc>& ranges,
                                               const std::vector<std::string>& files) {
    LOG(INFO) << "build scan ranges for files size:" << files.size() << " file_type:" << _file_type;
    for (const auto& file : files) {
        TBrokerRangeDesc range;
        range.start_offset = 0;
        range.size = -1;
        range.format_type = TFileFormatType::FORMAT_PARQUET;
        range.splittable = true;

        range.path = file;
        range.file_type = _isHDFS ? TFileType::FILE_HDFS : TFileType::FILE_LOCAL;
        ranges.push_back(range);
    }

    if (!ranges.size()) LOG(FATAL) << "cannot get valid scan file!";
}

void BuilderScannerMemtable::doSegmentBuild(const std::vector<std::string>& files) {
    vectorized::VBrokerScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    scan_node.init(_tnode);
    auto status = scan_node.prepare(&_runtime_state);
    if (!status.ok()) LOG(FATAL) << "prepare scan node fail:" << status.to_string();

    // set scan range
    std::vector<TScanRangeParams> scan_ranges;
    {
        TScanRangeParams scan_range_params;

        TBrokerScanRange broker_scan_range;
        broker_scan_range.params = _params;
        build_scan_ranges(broker_scan_range.ranges, files);
        scan_range_params.scan_range.__set_broker_scan_range(broker_scan_range);
        scan_ranges.push_back(scan_range_params);
    }

    scan_node.set_scan_ranges(scan_ranges);
    status = scan_node.open(&_runtime_state);
    if (!status.ok()) LOG(FATAL) << "open scan node fail:" << status.to_string();

    // std::unique_ptr<RowsetWriter> rowset_writer;
    PUniqueId load_id;
    load_id.set_hi(1);
    load_id.set_lo(1);
    int64_t transaction_id = 1;

    // delta writer
    TupleDescriptor* tuple_desc = _desc_tbl->get_tuple_descriptor(TUPLE_ID_DST);
    WriteRequest write_req = {_tablet->tablet_meta()->tablet_id(),
                              _tablet->schema_hash(),
                              WriteType::LOAD,
                              transaction_id,
                              _tablet->partition_id(),
                              load_id,
                              tuple_desc,
                              &(tuple_desc->slots())};

    DeltaWriter* delta_writer = nullptr;
    DeltaWriter::open(&write_req, &delta_writer, load_id);
    status = delta_writer->init();
    if (!status.ok()) LOG(FATAL) << "delta_writer init fail:" << status.to_string();

    std::filesystem::path segment_path(std::filesystem::path(_build_dir + "/segment"));
    std::filesystem::remove_all(segment_path);
    if (!std::filesystem::create_directory(segment_path)) LOG(FATAL) << "create segment path fail.";

    delta_writer->set_writer_path(segment_path.string());
    // Get block
    vectorized::Block block;
    bool eof = false;

    std::vector<int> rowidx;
    for (size_t i = 0; i < BATCH_SIZE; ++i) {
        rowidx.push_back(i);
    }

    while (!eof) {
        status = scan_node.get_next(&_runtime_state, &block, &eof);
        if (!status.ok()) {
            LOG(FATAL) << "scan error: " << status.to_string();
            break;
        }

        if (block.rows() != BATCH_SIZE) {
            std::vector<int> index;
            for (size_t i = 0; i < block.rows(); ++i) {
                index.push_back(i);
            }
            status = delta_writer->write(&block, index);
        } else
            status = delta_writer->write(&block, rowidx);

        if (!status.ok()) {
            LOG(FATAL) << "add block error: " << status.to_string();
            break;
        }

        block.clear();
    }
    status = delta_writer->close();
    if (!status.ok()) {
        LOG(FATAL) << "delta_writer close error: " << status.to_string();
    }
    PSlaveTabletNodes slave_tablet_nodes;
    status = delta_writer->close_wait(slave_tablet_nodes, false);
    if (!status.ok()) {
        LOG(FATAL) << "delta_writer close_wait error: " << status.to_string();
    }

    RowsetMetaSharedPtr rowset_meta = delta_writer->get_cur_rowset()->rowset_meta();
    std::vector<RowsetMetaSharedPtr> metas {rowset_meta};

    _tablet->tablet_meta()->revise_rs_metas(std::move(metas));
    if (!status.ok()) {
        LOG(FATAL) << "cannot add new rowset: " << status.to_string();
    }

    scan_node.close(&_runtime_state);
    {
        std::stringstream ss;
        scan_node.runtime_profile()->pretty_print(&ss);
        LOG(INFO) << ss.str();
    }
}

} // namespace doris
