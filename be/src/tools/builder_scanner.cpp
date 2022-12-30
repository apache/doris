#include "tools/builder_scanner.h"

namespace doris {

static const int TUPLE_ID_DST = 0;
static const int TUPLE_ID_SRC = 1;

BuilderScanner::BuilderScanner(TabletSharedPtr tablet, const std::string& file_type, bool isHDFS)
        : _runtime_state(TQueryGlobals()), _tablet(tablet), _file_type(file_type), _isHDFS(isHDFS) {
    init();
    _runtime_state.init_scanner_mem_trackers();
    TUniqueId uid;
    uid.hi = 1;
    uid.lo = 1;
    TQueryOptions _options;
    _options.batch_size = 8192;
    _options.enable_vectorized_engine = true;
    auto* _exec_env = ExecEnv::GetInstance();
    _runtime_state.init(uid, _options, TQueryGlobals(), _exec_env);
}

void BuilderScanner::init() {
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

int BuilderScanner::create_src_tuple(TDescriptorTable& t_desc_table, int next_slot_id) {
    for (int i = 0; i < _tablet->num_columns(); i++) {
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 1;
        const auto& col = _tablet->tablet_schema()->column(i);
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            switch (col.type()) {
            case FieldType::OLAP_FIELD_TYPE_DATE: {
                scalar_type.__set_type(TPrimitiveType::DATE);
                break;
            }
            case FieldType::OLAP_FIELD_TYPE_VARCHAR: {
                scalar_type.__set_type(TPrimitiveType::VARCHAR);
                break;
            }
            case FieldType::OLAP_FIELD_TYPE_BIGINT: {
                scalar_type.__set_type(TPrimitiveType::BIGINT);
                break;
            }
            case FieldType::OLAP_FIELD_TYPE_FLOAT: {
                scalar_type.__set_type(TPrimitiveType::FLOAT);
                break;
            }
            case FieldType::OLAP_FIELD_TYPE_DOUBLE: {
                scalar_type.__set_type(TPrimitiveType::DOUBLE);
                break;
            }
            case FieldType::OLAP_FIELD_TYPE_DATETIME: {
                scalar_type.__set_type(TPrimitiveType::DATETIME);
                break;
            }
            case FieldType::OLAP_FIELD_TYPE_DATEV2: {
                scalar_type.__set_type(TPrimitiveType::DATEV2);
                break;
            }
            case FieldType::OLAP_FIELD_TYPE_DATETIMEV2: {
                scalar_type.__set_type(TPrimitiveType::DATETIMEV2);
                break;
            }
            case FieldType::OLAP_FIELD_TYPE_TINYINT: {
                scalar_type.__set_type(TPrimitiveType::TINYINT);
                break;
            }
            case FieldType::OLAP_FIELD_TYPE_UNSIGNED_TINYINT: {
                scalar_type.__set_type(TPrimitiveType::SMALLINT);
                break;
            }
            case FieldType::OLAP_FIELD_TYPE_INT: {
                scalar_type.__set_type(TPrimitiveType::INT);
                break;
            }
            case FieldType::OLAP_FIELD_TYPE_DECIMAL:
            case FieldType::OLAP_FIELD_TYPE_DECIMAL32: {
                scalar_type.__set_type(TPrimitiveType::DECIMAL32);
                break;
            }
            case FieldType::OLAP_FIELD_TYPE_DECIMAL64: {
                scalar_type.__set_type(TPrimitiveType::DECIMAL64);
                break;
            }
            case FieldType::OLAP_FIELD_TYPE_DECIMAL128I: {
                scalar_type.__set_type(TPrimitiveType::DECIMAL128I);
                break;
            }
            case FieldType::OLAP_FIELD_TYPE_JSONB: {
                scalar_type.__set_type(TPrimitiveType::JSONB);
                break;
            }
            default:
                LOG(FATAL) << "unknown type error:" << col.type();
            }

            scalar_type.__set_len(col.length());
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = i;
        // Skip the first 8 bytes These 8 bytes are used to indicate whether the field is a null value
        slot_desc.byteOffset = i * 16 + 8;
        slot_desc.nullIndicatorByte = i / 8;
        slot_desc.nullIndicatorBit = i % 8;
        slot_desc.colName = _tablet->tablet_schema()->column(i).name();
        slot_desc.slotIdx = i + 1;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }

    {
        // TTupleDescriptor source
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = TUPLE_ID_SRC;
        //Here 8 bytes in order to handle null values
        t_tuple_desc.byteSize = _tablet->num_columns() * 16 + 8;
        t_tuple_desc.numNullBytes = 0;
        t_tuple_desc.tableId = _tablet->table_id();
        t_tuple_desc.__isset.tableId = true;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
    }
    return next_slot_id;
}

int BuilderScanner::create_dst_tuple(TDescriptorTable& t_desc_table, int next_slot_id) {
    int32_t byteOffset = 8;
    for (int i = 0; i < _tablet->num_columns(); i++, byteOffset += 16) {
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        const auto& col = _tablet->tablet_schema()->column(i);
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            switch (col.type()) {
            case FieldType::OLAP_FIELD_TYPE_DATE: {
                scalar_type.__set_type(TPrimitiveType::DATE);
                break;
            }
            case FieldType::OLAP_FIELD_TYPE_VARCHAR: {
                scalar_type.__set_type(TPrimitiveType::VARCHAR);
                break;
            }
            case FieldType::OLAP_FIELD_TYPE_BIGINT: {
                scalar_type.__set_type(TPrimitiveType::BIGINT);
                break;
            }
            default:
                LOG(FATAL) << " unknown type error:" << col.type();
            }

            scalar_type.__set_len(col.length());
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = i;
        slot_desc.byteOffset = byteOffset;
        slot_desc.nullIndicatorByte = i / 8;
        slot_desc.nullIndicatorBit = i % 8;
        slot_desc.colName = _tablet->tablet_schema()->column(i).name();
        slot_desc.slotIdx = i + 1;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }

    t_desc_table.__isset.slotDescriptors = true;
    {
        // TTupleDescriptor dest
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = TUPLE_ID_DST;
        t_tuple_desc.byteSize = byteOffset + 8; //Here 8 bytes in order to handle null values
        t_tuple_desc.numNullBytes = 0;
        t_tuple_desc.tableId = _tablet->table_id();
        t_tuple_desc.__isset.tableId = true;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
    }
    return next_slot_id;
}

void BuilderScanner::init_desc_table() {
    TDescriptorTable t_desc_table;

    // table descriptors
    TTableDescriptor t_table_desc;

    t_table_desc.id = _tablet->table_id();
    t_table_desc.tableType = TTableType::OLAP_TABLE;
    t_table_desc.numCols = _tablet->num_columns();
    t_table_desc.numClusteringCols = 0;
    t_desc_table.tableDescriptors.push_back(t_table_desc);
    t_desc_table.__isset.tableDescriptors = true;

    int next_slot_id = 0;

    next_slot_id = create_dst_tuple(t_desc_table, next_slot_id);
    next_slot_id = create_src_tuple(t_desc_table, next_slot_id);

    DescriptorTbl::create(&_obj_pool, t_desc_table, &_desc_tbl);

    _runtime_state.set_desc_tbl(_desc_tbl);
}

void BuilderScanner::create_expr_info() {
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
        slot_ref.slot_ref.slot_id = _tablet->num_columns() + i; // log_time id in src tuple
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

void BuilderScanner::build_scan_ranges(std::vector<TBrokerRangeDesc>& ranges,
                                       const std::string& path) {
    LOG(INFO) << "build scan ranges for:" << path << " file_type:" << _file_type;
    if (_isHDFS) {
    } else {
        for (const auto& file : std::filesystem::directory_iterator(path)) {
            auto file_path = file.path().string();
            if (file_path.substr(file_path.size() - _file_type.size()) == _file_type) {
                TBrokerRangeDesc range;
                range.start_offset = 0;
                range.size = -1;
                range.format_type = TFileFormatType::FORMAT_PARQUET;
                range.splittable = true;

                range.path = file_path;
                range.file_type = TFileType::FILE_LOCAL;
                ranges.push_back(range);
            }
        }
    }

    if (!ranges.size()) LOG(FATAL) << "cannot get valid scan file!";
}

void BuilderScanner::doSegmentBuild(const std::string& path) {
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
        build_scan_ranges(broker_scan_range.ranges, path);
        scan_range_params.scan_range.__set_broker_scan_range(broker_scan_range);
        scan_ranges.push_back(scan_range_params);
    }

    scan_node.set_scan_ranges(scan_ranges);
    status = scan_node.open(&_runtime_state);
    if (!status.ok()) LOG(FATAL) << "open scan node fail:" << status.to_string();

    std::unique_ptr<RowsetWriter> rowset_writer;
    PUniqueId load_id;
    load_id.set_hi(1);
    load_id.set_lo(1);
    int64_t transaction_id = 1;

    RowsetWriterContext context;
    context.txn_id = transaction_id;
    context.load_id = load_id;
    context.rowset_state = PREPARED;
    context.segments_overlap = OVERLAP_UNKNOWN;
    context.tablet_schema = _tablet->tablet_schema();

    status = _tablet->create_rowset_writer(context, &rowset_writer);

    if (!status.ok()) LOG(FATAL) << "create rowset error:" << status.to_string();

    std::filesystem::path segment_path(std::filesystem::path(path + "/segment"));
    if (!std::filesystem::exists(segment_path)) {
        LOG(INFO) << "create segment path.";
        if (!std::filesystem::create_directory(segment_path))
            LOG(FATAL) << "create segment path fail.";
    }

    rowset_writer->set_writer_path(segment_path.string());
    // Get block
    vectorized::Block block;
    bool eof = false;

    while (!eof) {
        status = scan_node.get_next(&_runtime_state, &block, &eof);
        if (!status.ok()) {
            LOG(FATAL) << "scan error: " << status.to_string();
            break;
        }
        status = rowset_writer->add_block(&block);
        if (!status.ok()) {
            LOG(FATAL) << "add block error: " << status.to_string();
            break;
        }

        block.clear();
    }
    status = rowset_writer->flush();
    if (!status.ok()) {
        LOG(FATAL) << "flush error: " << status.to_string();
    }

    rowset_writer->build();
    scan_node.close(&_runtime_state);
    {
        std::stringstream ss;
        scan_node.runtime_profile()->pretty_print(&ss);
        LOG(INFO) << ss.str();
    }
}

} // namespace doris
