# Doris Broker导入实现解析

## 背景

Doris支持多种导入方式，其中Broker导入是一种最常用的方式，用于实现将分布式存储系统（hdfs、bos等）中的文件导入到doris中。 Broker导入适用的场景是：

- 源数据在Broker可以访问的分布式存储系统中，如HDFS。

- 数据量在20G级别。

## 名词解释

* FE：Frontend，即 Palo 的前端节点。主要负责接收和返回客户端请求、元数据以及集群管理、查询计划生成等工作。关于Doris的架构图，参考[Doris架构介绍](http://doris.incubator.apache.org/)
* BE：Backend，即 Palo 的后端节点。主要负责数据存储与管理、查询计划执行等工作。
* Broker：请参考[Broker文档](http://doris.incubator.apache.org/documentation/cn/administrator-guide/broker.html)

## 实现原理

在Broker导入中，用户只要提供一份base表的数据，Doris会为用户进行一下处理：

- doris会自动基于base的的数据，为用户生成rollup表的数据，导入到对应的rollup中
- 实现负导入功能（仅针对聚合模型的SUM类型的value）
- 从path中提取字段
- 函数计算，包括strftime,now，hll_hash,md5等
- 保证导入整个过程的原子性

Broker load的语法以及使用方式，请参考[Broker导入文档](http://doris.incubator.apache.org/documentation/cn/administrator-guide/load-data/broker-load-manual.html)

### 导入流程

```
                 +
                 | 1. user create broker load
                 v
            +----+----+
            |         |
            |   FE    |
            |         |
            +----+----+
                 |
                 | 2. BE etl and load the data
    +--------------------------+
    |            |             |
+---v---+     +--v----+    +---v---+
|       |     |       |    |       |
|  BE   |     |  BE   |    |   BE  |
|       |     |       |    |       |
+---^---+     +---^---+    +---^---+
    |             |            |
    |             |            | 3. pull data from broker
+---+---+     +---+---+    +---+---+
|       |     |       |    |       |
|Broker |     |Broker |    |Broker |
|       |     |       |    |       |
+---^---+     +---^---+    +---^---+
    |             |            | 
+----------------------------------+
|       HDFS/BOS/AFS cluster       |
+----------------------------------+
```

整个导入过程大体如下：

- 用户将请求发送到FE，经过FE进行语法和语意分析，之后生成BrokerLoadJob
- BrokerLoadJob会经过LoadJob的Scheduler调度，生成一个BrokerLoadPendingTask
- BrokerLoadPendingTask会对导入源文件进行list，并且按照partition进行构建partition下文件列表
- 每个partition生成一个LoadLoadingTask，进行导入
- LoadLoadingTask生成一个分布式的导入执行计划，在后端BE中执行读取源文件，进行ETL转化，写入对应的tablet的过程。

其中关键步骤如下：

#### FE中的处理
1. 语法和语意处理

```
			 User Query
                 +
                 | mysql protocol
                 v
         +-------+-------+
         |               |
         |   QeService   |
         |               |
         +-------+-------+
				 |
                 v
         +-------+-------+
         |               |
         |  MysqlServer  |
         |               |
         +-------+-------+
				 |
                 v
       +---------+---------+
       |                   |
       |  ConnectScheduler |
       |                   |
       +---------+---------+
				 |
                 v
       +---------+---------+
       |                   |
       |  ConnectProcessor |
       |                   |
       +---------+---------+
				 |
                 v
         +-------+-------+
         |               |
         | StmtExecutor  |
         |               |
         +-------+-------+
```
上述流程，是一个查询发送到Doris之后，进行语法和语意分析所经过的处理流程。其中，在Doris中，MysqlServer是实现了Mysql Protocol的一个server，用户接收用户的mysql查询请求，经过ConnectScheduler的调度之后，有ConnectProcessor处理，并且最终由StmtExecutor进行语法和语意分析。

2. Load job执行

```
         +-------+-------+
         |    PENDING    |-----------------|
         +-------+-------+                 |
				 | BrokerLoadPendingTask   |
                 v                         |
         +-------+-------+                 |
         |    LOADING    |-----------------|
         +-------+-------+                 |
				 | LoadLodingTask          |
                 v                         |
         +-------+-------+                 |
         |  COMMITTED    |-----------------|
         +-------+-------+                 |
				 |                         |
                 v                         v  
         +-------+-------+         +-------+-------+     
         |   FINISHED    |         |   CANCELLED   |
         +-------+-------+         +-------+-------+
				 |                         Λ
                 |-------------------------|
```

用户发起的Broker导入的请求，最终在StmtExecutor经过语法和语意分析之后，会生成LoadStmt，然后在DdlExecutor中，会根据LoadStmt生成BrokerLoadJob。

```cpp
         if (ddlStmt instanceof LoadStmt) {
            LoadStmt loadStmt = (LoadStmt) ddlStmt;
            EtlJobType jobType;
            if (loadStmt.getBrokerDesc() != null) {
                jobType = EtlJobType.BROKER;
            } else {
                if (Config.disable_hadoop_load) {
                    throw new DdlException("Load job by hadoop cluster is disabled."
                            + " Try using broker load. See 'help broker load;'");
                }
                jobType = EtlJobType.HADOOP;
            }
            if (loadStmt.getVersion().equals(Load.VERSION) || jobType == EtlJobType.HADOOP) {
                catalog.getLoadManager().createLoadJobV1FromStmt(loadStmt, jobType, System.currentTimeMillis());
            } else {
                catalog.getLoadManager().createLoadJobFromStmt(loadStmt, origStmt);
            }
        }
```

BrokerLoadJob的执行，是采用状态机的方式执行的。作业的初始状态为PENDING，在PENDING状态的时候，会做两件事情：

1. 调用全局事务管理器，begin transaction，申请一个txn id。
2. 创建一个BrokerLoadPendingTask，该任务的主要作用就是查询用户传的文件路径是否正确，以及文件的个数和大小，将该信息按照partition的方式进行组织，存储在一个Map中（参考数据结构：BrokerPendingTaskAttachment）。
关键代码如下（BrokerLoadPendingTask.java）：

```
private void getAllFileStatus() throws UserException {
        long start = System.currentTimeMillis();
        long totalFileSize = 0;
        int totalFileNum = 0;
        for (Map.Entry<FileGroupAggKey, List<BrokerFileGroup>> entry : aggKeyToBrokerFileGroups.entrySet()) {
            FileGroupAggKey aggKey = entry.getKey();
            List<BrokerFileGroup> fileGroups = entry.getValue();

            List<List<TBrokerFileStatus>> fileStatusList = Lists.newArrayList();
            long tableTotalFileSize = 0;
            int tableTotalFileNum = 0;
            int groupNum = 0;
            for (BrokerFileGroup fileGroup : fileGroups) {
                long groupFileSize = 0;
                List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
                for (String path : fileGroup.getFilePaths()) {
                    BrokerUtil.parseBrokerFile(path, brokerDesc, fileStatuses);
                }
                fileStatusList.add(fileStatuses);
                for (TBrokerFileStatus fstatus : fileStatuses) {
                    groupFileSize += fstatus.getSize();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId())
                                .add("file_status", fstatus).build());
                    }
                }
                tableTotalFileSize += groupFileSize;
                tableTotalFileNum += fileStatuses.size();
                LOG.info("get {} files in file group {} for table {}. size: {}. job: {}",
                        fileStatuses.size(), groupNum, entry.getKey(), groupFileSize, callback.getCallbackId());
                groupNum++;
            }

            totalFileSize += tableTotalFileSize;
            totalFileNum += tableTotalFileNum;
            ((BrokerPendingTaskAttachment) attachment).addFileStatus(aggKey, fileStatusList);
            LOG.info("get {} files to be loaded. total size: {}. cost: {} ms, job: {}",
                    tableTotalFileNum, tableTotalFileSize, (System.currentTimeMillis() - start), callback.getCallbackId());
        }

        ((BrokerLoadJob) callback).setLoadFileInfo(totalFileNum, totalFileSize);
    }
```

其中，查询用户文件路径的代码就是：`BrokerUtil.parseBrokerFile(path, brokerDesc, fileStatuses);`

在BrokerLoadPedingTask执行完成之后，BrokerLoadJob的状态会变成LOADING状态，在LOADING阶段，主要做的事情就是依据导入指定的partition个数，每个partition创建一个LoadLoadingTask。该任务最主要的事情就是生成导入查询计划，并且分发到后端BE节点进行执行。

##### LoadLoadingTask

LoadLoadingTask是导入过程中**最重要的步骤**，如前所述，它会负责导入计划的生成和执行的分发和执行（依赖查询计划的执行框架Coordinator），生成的查询计划（由LoadingTaskPlanner来负责生成导入计划）如下所示：

```
Fragment:
    +------------------------------+
    |     +----------+---------+   |
    |     |   BrokerScanNode   |   |
    |     +----------+---------+   |
	|  			     |             |
    |                v             |
    |     +----------+---------+   |
    |     |   OlapTableSink    |   |
    |     +---------+----------+   |
    +------------------------------+
```

BrokerScanNode中会完成以下工作：

- 实现列

	将源文件的列映射到表中的列，并且支持列的表达式运算（Set功能）
- 实现负导入

	针对sum类型的value列，支持导入负数据
- 实现条件过滤
- column from path机制

	从文件路径中提取列的值

主要的逻辑如下：

```
DataDescription::analyzeColumn()

	private void analyzeColumns() throws AnalysisException {
        if ((fileFieldNames == null || fileFieldNames.isEmpty()) && (columnsFromPath != null && !columnsFromPath.isEmpty())) {
            throw new AnalysisException("Can not specify columns_from_path without column_list");
        }

        // used to check duplicated column name in COLUMNS and COLUMNS FROM PATH
        Set<String> columnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        // mesrge columns exprs from columns, columns from path and columnMappingList
        // 1. analyze columns
        if (fileFieldNames != null && !fileFieldNames.isEmpty()) {
            for (String columnName : fileFieldNames) {
                if (!columnNames.add(columnName)) {
                    throw new AnalysisException("Duplicate column: " + columnName);
                }
                ImportColumnDesc importColumnDesc = new ImportColumnDesc(columnName, null);
                parsedColumnExprList.add(importColumnDesc);
            }
        }

        // 2. analyze columns from path
        if (columnsFromPath != null && !columnsFromPath.isEmpty()) {
            if (isHadoopLoad) {
                throw new AnalysisException("Hadoop load does not support specifying columns from path");
            }
            for (String columnName : columnsFromPath) {
                if (!columnNames.add(columnName)) {
                    throw new AnalysisException("Duplicate column: " + columnName);
                }
                ImportColumnDesc importColumnDesc = new ImportColumnDesc(columnName, null);
                parsedColumnExprList.add(importColumnDesc);
            }
        }

        // 3: analyze column mapping
        if (columnMappingList == null || columnMappingList.isEmpty()) {
            return;
        }

        // used to check duplicated column name in SET clause
        Set<String> columnMappingNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        // Step2: analyze column mapping
        // the column expr only support the SlotRef or eq binary predicate which's child(0) must be a SloRef.
        // the duplicate column name of SloRef is forbidden.
        for (Expr columnExpr : columnMappingList) {
            if (!(columnExpr instanceof BinaryPredicate)) {
                throw new AnalysisException("Mapping function expr only support the column or eq binary predicate. "
                        + "Expr: " + columnExpr.toSql());
            }
            BinaryPredicate predicate = (BinaryPredicate) columnExpr;
            if (predicate.getOp() != Operator.EQ) {
                throw new AnalysisException("Mapping function expr only support the column or eq binary predicate. "
                        + "The mapping operator error, op: " + predicate.getOp());
            }
            Expr child0 = predicate.getChild(0);
            if (!(child0 instanceof SlotRef)) {
                throw new AnalysisException("Mapping function expr only support the column or eq binary predicate. "
                        + "The mapping column error. column: " + child0.toSql());
            }
            String column = ((SlotRef) child0).getColumnName();
            if (!columnMappingNames.add(column)) {
                throw new AnalysisException("Duplicate column mapping: " + column);
            }
            // hadoop load only supports the FunctionCallExpr
            Expr child1 = predicate.getChild(1);
            if (isHadoopLoad && !(child1 instanceof FunctionCallExpr)) {
                throw new AnalysisException("Hadoop load only supports the designated function. "
                        + "The error mapping function is:" + child1.toSql());
            }
            ImportColumnDesc importColumnDesc = new ImportColumnDesc(column, child1);
            parsedColumnExprList.add(importColumnDesc);
            if (child1 instanceof FunctionCallExpr) {
                analyzeColumnToHadoopFunction(column, child1);
            }
        }
    }

```
该函数会进行

1. 数据源的列和目标表的列之间映射关系的计算
2. 将ColumnsFromPath的列也会提取出来，
3. 分析函数计算相关的列

分析的各个列的映射结果会保存在parsedColumnExprList。

```
DataDescription::analyzeColumnToHadoopFunction()

    private void analyzeColumnToHadoopFunction(String columnName, Expr child1) throws AnalysisException {
        Preconditions.checkState(child1 instanceof FunctionCallExpr); 
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) child1;
        String functionName = functionCallExpr.getFnName().getFunction();
        if (!HADOOP_SUPPORT_FUNCTION_NAMES.contains(functionName.toLowerCase())) {
            return;
        }
        List<Expr> paramExprs = functionCallExpr.getParams().exprs();
        List<String> args = Lists.newArrayList();

        for (Expr paramExpr : paramExprs) {
            if (paramExpr instanceof SlotRef) {
                SlotRef slot = (SlotRef) paramExpr;
                args.add(slot.getColumnName());
            } else if (paramExpr instanceof StringLiteral) {
                StringLiteral literal = (StringLiteral) paramExpr;
                args.add(literal.getValue());
            } else if (paramExpr instanceof NullLiteral) {
                args.add(null);
            } else {
                if (isHadoopLoad) {
                    // hadoop function only support slot, string and null parameters
                    throw new AnalysisException("Mapping function args error, arg: " + paramExpr.toSql());
                }
            }
        }

        Pair<String, List<String>> functionPair = new Pair<String, List<String>>(functionName, args);
        columnToHadoopFunction.put(columnName, functionPair);
    }
```
上述是针对函数计算的列（Set中的列映射关系），最终得到该列使用的函数名和参数列表。

```
BrokerScanNode::finalizeParams()

	private void finalizeParams(ParamCreateContext context) throws UserException, AnalysisException {
        Map<String, SlotDescriptor> slotDescByName = context.slotDescByName;
        Map<String, Expr> exprMap = context.exprMap;
        Map<Integer, Integer> destSidToSrcSidWithoutTrans = Maps.newHashMap();

        boolean isNegative = context.fileGroup.isNegative();
        for (SlotDescriptor destSlotDesc : desc.getSlots()) {
            if (!destSlotDesc.isMaterialized()) {
                continue;
            }
            Expr expr = null;
            if (exprMap != null) {
                expr = exprMap.get(destSlotDesc.getColumn().getName());
            }
            if (expr == null) {
                SlotDescriptor srcSlotDesc = slotDescByName.get(destSlotDesc.getColumn().getName());
                if (srcSlotDesc != null) {
                    destSidToSrcSidWithoutTrans.put(destSlotDesc.getId().asInt(), srcSlotDesc.getId().asInt());
                    // If dest is allow null, we set source to nullable
                    if (destSlotDesc.getColumn().isAllowNull()) {
                        srcSlotDesc.setIsNullable(true);
                    }
                    expr = new SlotRef(srcSlotDesc);
                } else {
                    Column column = destSlotDesc.getColumn();
                    if (column.getDefaultValue() != null) {
                        expr = new StringLiteral(destSlotDesc.getColumn().getDefaultValue());
                    } else {
                        if (column.isAllowNull()) {
                            expr = NullLiteral.create(column.getType());
                        } else {
                            throw new UserException("Unknown slot ref("
                                    + destSlotDesc.getColumn().getName() + ") in source file");
                        }
                    }
                }
            }

            // check hll_hash
            if (destSlotDesc.getType().getPrimitiveType() == PrimitiveType.HLL) {
                if (!(expr instanceof FunctionCallExpr)) {
                    throw new AnalysisException("HLL column must use hll_hash function, like "
                            + destSlotDesc.getColumn().getName() + "=hll_hash(xxx)");
                }
                FunctionCallExpr fn = (FunctionCallExpr) expr;
                if (!fn.getFnName().getFunction().equalsIgnoreCase("hll_hash") && !fn.getFnName().getFunction().equalsIgnoreCase("hll_empty")) {
                    throw new AnalysisException("HLL column must use hll_hash function, like "
                            + destSlotDesc.getColumn().getName() + "=hll_hash(xxx) or " + destSlotDesc.getColumn().getName() + "=hll_empty()");
                }
                expr.setType(Type.HLL);
            }

            checkBitmapCompatibility(destSlotDesc, expr);

            // analyze negative
            if (isNegative && destSlotDesc.getColumn().getAggregationType() == AggregateType.SUM) {
                expr = new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY, expr, new IntLiteral(-1));
                expr.analyze(analyzer);
            }
            expr = castToSlot(destSlotDesc, expr);
            context.params.putToExpr_of_dest_slot(destSlotDesc.getId().asInt(), expr.treeToThrift());
        }
        context.params.setDest_sid_to_src_sid_without_trans(destSidToSrcSidWithoutTrans);
        context.params.setSrc_tuple_id(context.tupleDescriptor.getId().asInt());
        context.params.setDest_tuple_id(desc.getId().asInt());
        context.params.setStrict_mode(strictMode);
        // Need re compute memory layout after set some slot descriptor to nullable
        context.tupleDescriptor.computeMemLayout();
    }
```
该函数实现分析哪些列是在数据源中直接指定的列（包括columnsFromPath的列），以及怎么从源列转化生成目标列，哪些列是函数计算的列，数据源的schema，目标表的schema等信息。最终，这些参数都会设置到BrokerScanNode的参数TBrokerScanRangeParams中，最终经过序列化之后通过rpc接口传给BE，BE会依据这些信息执行具体的ETL。该结构体定义如下：

```
struct TBrokerScanRangeParams {
	// 列分隔符
    1: required byte column_separator;
	// 行分隔符
    2: required byte line_delimiter;

    // 数据源schema id
	// 需要依赖TDescriptorTable获取对应的tuple descriptor
    3: required Types.TTupleId src_tuple_id
    // 数据源列的类型id
	// 需要依赖TDescriptorTable获取对应的slot descriptor
    4: required list<Types.TSlotId> src_slot_ids

    // 目标表的tuple id
    5: required Types.TTupleId dest_tuple_id
    // set中执行的列
    6: optional map<Types.TSlotId, Exprs.TExpr> expr_of_dest_slot

    // properties need to access broker.
    7: optional map<string, string> properties;

    // If partition_ids is set, data that doesn't in this partition will be filtered.
    8: optional list<i64> partition_ids
        
    // 数据源列直接映射成目标列
    9: optional map<Types.TSlotId, Types.TSlotId> dest_sid_to_src_sid_without_trans
    // strictMode is a boolean
    // if strict mode is true, the incorrect data (the result of cast is null) will not be loaded
    10: optional bool strict_mode
}
```

#### BE中的处理

BE中主要就是执行在LoadLoadingTask任务中下发的导入执行计划，需要依赖于Doris的查询框架(请参考后续文章)。这里大概介绍一下BE中查询计划的执行框架。在BE中，查询计划分为逻辑查询计划(Single PlanFragment)和可执行分布式查询计划（Distributed PlanFragment），在BE中主要涉及可执行分布式查询计划，其中PlanFragment的概念与Spark中的stage概念类似，表示一个计划能够在一个BE中单独执行的计划片段。BE实现了PlanFragment执行的框架，逻辑在plan_fragment_executor.cpp中，大概逻辑如下：
```
Status PlanFragmentExecutor::open() {
    ...
    Status status = open_internal();
    ...
    return status;
}

Status PlanFragmentExecutor::open_internal() {
    ...
    if (_sink.get() == NULL) {
        return Status::OK;
    }
    // 打开 data sink
    RETURN_IF_ERROR(_sink->open(runtime_state()));

   	...
    while (true) {
    	// 调用 get_next() 从 ScanNode 获取数据
        RETURN_IF_ERROR(get_next_internal(&batch));
        ...
        // 通过 DataSink 的 send() 方法发送数据
        RETURN_IF_ERROR(_sink->send(runtime_state(), batch));
    }

    ...

    return Status::OK;
}
```
一个PlanFragment中包含多个ExecNode组成的树状执行计划（类型spark stage中包含的rdd），其中SinkNode(上面中的_sink变量)类型spark中带action操作的rdd，执行数据序列化操作，可能后续会进行持久化，也可能通过网络发送到下一个PlanFragment中。get_next_internal会调用PlanNode执行计划树获取数据，然后通过SinkNode发送到下一个PlanFragment或者输出结果。

再介绍一下ExecNode的定义，主要接口如下：
```
class ExecNode {
public:
    // Init conjuncts.
    ExecNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    virtual ~ExecNode();

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);

    virtual Status prepare(RuntimeState* state);

    virtual Status open(RuntimeState* state);

    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) = 0;
}
```

##### BrokerScanNode

BrokerScanNode就是派生自ExecNode，实现get_next()接口读取hdfs等分布式存储系统中的数据。PlanFragmentExecutor中调用get_next_internal会调用BrokerScanNode::get_next()来实际地读取数据，并且是按照RowBatch的方式向上返回数据。

下图是BrokerScanNode的调用栈：

```
+-----------------------+
|BrokerScanNode         |
|                       |
|  open()               |
|  |                    |
|  +->scan_worker()     |
|     |                 |
|     +->scanner_scan() | +-----------------------------------------+
|                 |     | |BaseScanner                              |
|  get_next()     |     | |                                         |
|    |            +--------->get_next()                             |
|    |                  | |  |                                      |
|    +->row batch queue | |  +-->fill_slots_of_columns_from_path()  |
|           ^           | |  |-->fill_dest_tuple()                  |
|           |           | +-----------------------------------------+
+-----------------------+                  |
            |							   |
            |                              |
            +------------------------------+
```
BrokerScanNode采用多线程+生产者和消费者模型，来实现数据的读取。底层通过支持不同类型的Scanner，来实现对不同格式的文件的读取，现在支持CSV、Parquet、ORC三种格式。下面以csv格式（实现在BrokerScanner）具体讲一下以下几个问题的实现：

1. 如何进行ETL
	
- 按照TBrokerScanRangeParams参数，初始化src tuple(slot) descriptor,dst tuple descriptor以及列的映射关系

```
Status BaseScanner::init_expr_ctxes() {
    // Constcut _src_slot_descs
    const TupleDescriptor* src_tuple_desc =
        _state->desc_tbl().get_tuple_descriptor(_params.src_tuple_id);
    if (src_tuple_desc == nullptr) {
        std::stringstream ss;
        ss << "Unknown source tuple descriptor, tuple_id=" << _params.src_tuple_id;
        return Status::InternalError(ss.str());
    }

    std::map<SlotId, SlotDescriptor*> src_slot_desc_map;
    for (auto slot_desc : src_tuple_desc->slots()) {
        src_slot_desc_map.emplace(slot_desc->id(), slot_desc);
    }
    for (auto slot_id : _params.src_slot_ids) {
        auto it = src_slot_desc_map.find(slot_id);
        if (it == std::end(src_slot_desc_map)) {
            std::stringstream ss;
            ss << "Unknown source slot descriptor, slot_id=" << slot_id;
            return Status::InternalError(ss.str());
        }
        _src_slot_descs.emplace_back(it->second);
    }
    // Construct source tuple and tuple row
    _src_tuple = (Tuple*) _mem_pool.allocate(src_tuple_desc->byte_size());
    _src_tuple_row = (TupleRow*) _mem_pool.allocate(sizeof(Tuple*));
    _src_tuple_row->set_tuple(0, _src_tuple);
    _row_desc.reset(new RowDescriptor(_state->desc_tbl(),
                                      std::vector<TupleId>({_params.src_tuple_id}),
                                      std::vector<bool>({false})));

    // Construct dest slots information
    _dest_tuple_desc = _state->desc_tbl().get_tuple_descriptor(_params.dest_tuple_id);
    if (_dest_tuple_desc == nullptr) {
        std::stringstream ss;
        ss << "Unknown dest tuple descriptor, tuple_id=" << _params.dest_tuple_id;
        return Status::InternalError(ss.str());
    }

    bool has_slot_id_map = _params.__isset.dest_sid_to_src_sid_without_trans;
    for (auto slot_desc : _dest_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }
        auto it = _params.expr_of_dest_slot.find(slot_desc->id());
        if (it == std::end(_params.expr_of_dest_slot)) {
            std::stringstream ss;
            ss << "No expr for dest slot, id=" << slot_desc->id()
                << ", name=" << slot_desc->col_name();
            return Status::InternalError(ss.str());
        }
        ExprContext* ctx = nullptr;
        RETURN_IF_ERROR(Expr::create_expr_tree(_state->obj_pool(), it->second, &ctx));
        RETURN_IF_ERROR(ctx->prepare(_state, *_row_desc.get(), _mem_tracker.get()));
        RETURN_IF_ERROR(ctx->open(_state));
        _dest_expr_ctx.emplace_back(ctx);
        if (has_slot_id_map) {
            auto it = _params.dest_sid_to_src_sid_without_trans.find(slot_desc->id());
            if (it == std::end(_params.dest_sid_to_src_sid_without_trans)) {
                _src_slot_descs_order_by_dest.emplace_back(nullptr);
            } else {
                auto _src_slot_it = src_slot_desc_map.find(it->second);
                if (_src_slot_it == std::end(src_slot_desc_map)) {
                     std::stringstream ss;
                     ss << "No src slot " << it->second << " in src slot descs";
                     return Status::InternalError(ss.str());
                }
                _src_slot_descs_order_by_dest.emplace_back(_src_slot_it->second);
            }
        }
    }
    return Status::OK();
}
```

- 执行具体的列读取和转换

```
bool BrokerScanner::line_to_src_tuple(const Slice& line) {

    if (!validate_utf8(line.data, line.size)) {
        std::stringstream error_msg;
        error_msg << "data is not encoded by UTF-8";
        _state->append_error_msg_to_file(std::string(line.data, line.size),
                                         error_msg.str());
        _counter->num_rows_filtered++;
        return false;
    }

    std::vector<Slice> values;
    {
        split_line(line, &values);
    }

    // range of current file
    const TBrokerRangeDesc& range = _ranges.at(_next_range - 1);
    const std::vector<std::string>& columns_from_path = range.columns_from_path;
    if (values.size() + columns_from_path.size() < _src_slot_descs.size()) {
        std::stringstream error_msg;
        error_msg << "actual column number is less than schema column number. "
            << "actual number: " << values.size() << " sep: " << _value_separator << ", "
            << "schema number: " << _src_slot_descs.size() << "; ";
        _state->append_error_msg_to_file(std::string(line.data, line.size),
                                         error_msg.str());
        _counter->num_rows_filtered++;
        return false;
    } else if (values.size() + columns_from_path.size() > _src_slot_descs.size()) {
        std::stringstream error_msg;
        error_msg << "actual column number is more than schema column number. "
            << "actual number: " << values.size() << " sep: " << _value_separator << ", "
            << "schema number: " << _src_slot_descs.size() << "; ";
        _state->append_error_msg_to_file(std::string(line.data, line.size),
                                         error_msg.str());
        _counter->num_rows_filtered++;
        return false;
    }

    for (int i = 0; i < values.size(); ++i) {
        auto slot_desc = _src_slot_descs[i];
        const Slice& value = values[i];
        if (slot_desc->is_nullable() && is_null(value)) {
            _src_tuple->set_null(slot_desc->null_indicator_offset());
            continue;
        }
        _src_tuple->set_not_null(slot_desc->null_indicator_offset());
        void* slot = _src_tuple->get_slot(slot_desc->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        str_slot->ptr = value.data;
        str_slot->len = value.size;
    }

    if (range.__isset.num_of_columns_from_file) {
        fill_slots_of_columns_from_path(range.num_of_columns_from_file, columns_from_path);
    }

    return true;
}

void BaseScanner::fill_slots_of_columns_from_path(int start, const std::vector<std::string>& columns_from_path) {
    // values of columns from path can not be null
    for (int i = 0; i < columns_from_path.size(); ++i) {
        auto slot_desc = _src_slot_descs.at(i + start);
        _src_tuple->set_not_null(slot_desc->null_indicator_offset());
        void* slot = _src_tuple->get_slot(slot_desc->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        const std::string& column_from_path = columns_from_path[i];
        str_slot->ptr = const_cast<char*>(column_from_path.c_str());
        str_slot->len = column_from_path.size();
    }
}
```
上述的代码会从源数据文件中读取源数据列和ColumnsFromPath的列。

```
bool BaseScanner::fill_dest_tuple(const Slice& line, Tuple* dest_tuple, MemPool* mem_pool) {
    int ctx_idx = 0;
    for (auto slot_desc : _dest_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }

        int dest_index = ctx_idx++;
        ExprContext* ctx = _dest_expr_ctx[dest_index];
        void* value = ctx->get_value(_src_tuple_row);
        if (value == nullptr) {
            // Only when the expr return value is null, we will check the error message.
            std::string expr_error = ctx->get_error_msg();
            if (!expr_error.empty()) {
                _state->append_error_msg_to_file(_src_tuple_row->to_string(*(_row_desc.get())), expr_error);
                _counter->num_rows_filtered++;
                // The ctx is reused, so must clear the error state and message.
                ctx->clear_error_msg();
                return false;
            }
            // If _strict_mode is false, _src_slot_descs_order_by_dest size could be zero
            if (_strict_mode && (_src_slot_descs_order_by_dest[dest_index] != nullptr) &&
                !_src_tuple->is_null(_src_slot_descs_order_by_dest[dest_index]->null_indicator_offset())) {
                //Type of the slot is must be Varchar in _src_tuple.
                StringValue* raw_value = _src_tuple->get_string_slot(_src_slot_descs_order_by_dest[dest_index]->tuple_offset());
                std::string raw_string;
                if (raw_value != nullptr) {//is not null then get raw value
                    raw_string = raw_value->to_string();
                }
                std::stringstream error_msg;
                error_msg << "column(" << slot_desc->col_name() << ") value is incorrect "
                    << "while strict mode is " << std::boolalpha << _strict_mode 
                    << ", src value is " << raw_string;
                _state->append_error_msg_to_file(_src_tuple_row->to_string(*(_row_desc.get())), error_msg.str());
                _counter->num_rows_filtered++;
                return false;
            }
            if (!slot_desc->is_nullable()) {
                std::stringstream error_msg;
                error_msg << "column(" << slot_desc->col_name() << ") value is null "
                          << "while columns is not nullable";
                _state->append_error_msg_to_file(_src_tuple_row->to_string(*(_row_desc.get())), error_msg.str());
                _counter->num_rows_filtered++;
                return false;
            }
            dest_tuple->set_null(slot_desc->null_indicator_offset());
            continue;
        }
        if (slot_desc->is_nullable()) {
            dest_tuple->set_not_null(slot_desc->null_indicator_offset());
        }
        void* slot = dest_tuple->get_slot(slot_desc->tuple_offset());
        RawValue::write(value, slot, slot_desc->type(), mem_pool);
        continue;
    }
    return true;
}
```
上面的代码会将src_tuple转成dst_tuple，关键代码是
`void* value = ctx->get_value(_src_tuple_row);`
这里换完成类型的转化和函数的计算。


2. 如何进行条件过滤

在FE生成BrokerScanNode的时候，会将broker load语句中的where过滤条件转化成conjuncts，最终会在BE中执行BrokerScanNode的时候对读取的行执行conjuncts过滤：

```
Status BrokerScanNode::scanner_scan(
        const TBrokerScanRange& scan_range, 
        const std::vector<ExprContext*>& conjunct_ctxs, 
        const std::vector<ExprContext*>& partition_expr_ctxs,
        ScannerCounter* counter) {
	...
	// Get from scanner
    RETURN_IF_ERROR(scanner->get_next(tuple, tuple_pool, &scanner_eof));
    if (scanner_eof) {
    	continue;
    }

    // eval conjuncts of this row.
	// 用where条件进行行过滤
    if (eval_conjuncts(&conjunct_ctxs[0], conjunct_ctxs.size(), row)) {
    	row_batch->commit_last_row();
        char* new_tuple = reinterpret_cast<char*>(tuple);
        new_tuple += _tuple_desc->byte_size();
        tuple = reinterpret_cast<Tuple*>(new_tuple);
        // counter->num_rows_returned++;
     } else {
     	counter->num_rows_unselected++;
     }
	...
}
```

##### OlapTableSink

OlapTableSink负责将BrokerScanNode读取的数据写入到对应的tablet中，其调用栈如下：

```
+----------------------------------------------------------------------------+
|OlapTableSink                                                               |
|                +-------------+                                             |
|  prepare()     |IndexChannel |                                             |
|   |            |             | +----------------------------------------+  |
|   +-------------->init()     | |NodeChannel                             |  |
|                |   |         | |                                        |  |
|                |   +------------->init()                                |  |
|                |             | |                                        |  |
|  open()        |             | |                                        |  |
|   |            |             | |                                        |  |
|   +------------------------------>open()                                |  |
|                |             | |   |                                    |  |
|                |             | |   +->RPC tablet_writer_open()          |  |
|                |             | |       |                                |  |  +---------------------------------------------------------+
|                |             | |       +--------------------------------------->LoadChannel                                             |
|                |             | |                                        |  |  |    |                                                    |
|                |             | |                                        |  |  |    |          +------------------------+                |
|  send()        |             | |                                        |  |  |  open()-------->TabletsChannel         |                |
|   |            |             | |                                        |  |  |               |  |                     |                |
|   +-------------->add_row()  | |                                        |  |  |               |  +->open_all_writers() |                |
|                |   |         | |                                        |  |  |               |            |           | +------------+ |
|                |   +------------->add_row()                             |  |  |               |            +------------->DeltaWriter | |
|                |             | |   |                                    |  |  |               |                        | |            | |
|                |             | |   +->_send_cur_batch()                 |  |  |               |                        | |    open()  | |
|                |             | |   ^   |                                |  |  |               |                        | |            | |
|                |             | |   |   +->RPC tablet_writer_add_batch() |  |  |               |                        | |            | |
|                |             | |   |       |                            |  |  |               |                        | |            | |
|                |             | |   |       +------------------------------------->add_batch() |                        | |            | |
|                |             | |   |                                    |  |  |        |      |                        | |            | |
|  close()       |             | |   |                                    |  |  |        +-------->add_batch()           | |            | |
|   |            |             | |   |                                    |  |  |       		|  |                     | |            | |
|   +-------------->close()    | |   |                                    |  |  |               |  +--------------------------->write() | |
|                |   |         | |   |                                    |  |  |               |                        | |            | |
|                |   +------------->close()                               |  |  |               |                        | +------------+ | 
|                |             | |                                        |  |  |               |                        |                |
|                +-------------+ +----------------------------------------+  |  |               +------------------------+                |
+----------------------------------------------------------------------------+  +---------------------------------------------------------+
```
OlapTableSink导入的逻辑是将一个table所有的base和rolllup index按照IndexChannel进行组织，每个IndexChannel负责一个index的数据的导入。IndexChannel中将Index包括的tablet按照tablet所在的Node（也就是BE）进行组织，将同一个Node上的tablet有NodeChannel来负责写入。NodeChannel中会在Open的时候，向对应的BE机器发送rpc请求tablet_writer_open，创建LoadChannel对象，负责这次导入在这个节点上相关的tablet数据的写入，LoadChannel对象会管理不同index所对应的TabletsChannel，TabletsChannel负责统一个index的tablet的数据写入，TabletChannel最终会创建tablet对应的DeltaWriter进行tablet数据的写入。为了优化写入，DeltaWriter采用类似LSM的机制（缺少write ahead log，所以无法实现BE宕机之后恢复导入），先写入内存的MemTable，等MemTable满了之后，再将内存的数据刷入磁盘，生成Segment文件。

其中，OlapTableSink依赖于**关键参数**：
```

struct TOlapTableIndexSchema {
    1: required i64 id
    2: required list<string> columns
    3: required i32 schema_hash
}                                                                                                                                                                   

struct TOlapTableSchemaParam {
    1: required i64 db_id
    2: required i64 table_id
    3: required i64 version

    // Logical columns, contain all column that in logical table
    4: required list<TSlotDescriptor> slot_descs
    5: required TTupleDescriptor tuple_desc
    6: required list<TOlapTableIndexSchema> indexes
}

struct TOlapTableSink {
    1: required Types.TUniqueId load_id
    2: required i64 txn_id
    3: required i64 db_id
    4: required i64 table_id
    5: required i32 tuple_id
    6: required i32 num_replicas
    7: required bool need_gen_rollup
    8: optional string db_name
    9: optional string table_name
    10: required Descriptors.TOlapTableSchemaParam schema
    11: required Descriptors.TOlapTablePartitionParam partition
    12: required Descriptors.TOlapTableLocationParam location
    13: required Descriptors.TPaloNodesInfo nodes_info
    14: optional i64 load_channel_timeout_s // the timeout of load channels in second
}

```
其中主要包括这个table包含的schema信息和所有的index的schema信息、partition和bucket信息、txn_id、BE机器信息等等导入的关键信息。
下面主要讲述一下导入中的几个关键问题

1. 如何将数据同时导入base和rollup index

OlapTableSink会将每一条base表的数据发送到所有的rollup index的IndexChannel中

2. 如何实现数据的分区和分桶

```
Status OlapTableSink::send(RuntimeState* state, RowBatch* input_batch) {
	...
	for (int i = 0; i < batch->num_rows(); ++i) {
        Tuple* tuple = batch->get_row(i)->get_tuple(0);
        if (num_invalid_rows > 0 && _filter_bitmap.Get(i)) {
            continue;
        }
        const OlapTablePartition* partition = nullptr;
        uint32_t dist_hash = 0;
        if (!_partition->find_tablet(tuple, &partition, &dist_hash)) {
            std::stringstream ss;
            ss << "no partition for this tuple. tuple="
                << Tuple::to_string(tuple, *_output_tuple_desc);
#if BE_TEST
            LOG(INFO) << ss.str();
#else
            state->append_error_msg_to_file("", ss.str());
#endif
            _number_filtered_rows++;
            continue;
        }
        _partition_ids.emplace(partition->id);
        uint32_t tablet_index = dist_hash % partition->num_buckets;
        for (int j = 0; j < partition->indexes.size(); ++j) {
            int64_t tablet_id = partition->indexes[j].tablets[tablet_index];
            RETURN_IF_ERROR(_channels[j]->add_row(tuple, tablet_id));
            _number_output_rows++;
        }
    }
	...
}
```

其中：

`_partition->find_tablet(tuple, &partition, &dist_hash)`
会计算出tuple所属的partition和bucket的hash值，然后通过
`uint32_t tablet_index = dist_hash % partition->num_buckets;`
计算得到对应的tablet

3. 如何将base的数据是转化为rollup index的数据（包括列变化、聚合计算）

```
void MemTable::insert(const Tuple* tuple) {
    bool overwritten = false;
    uint8_t* _tuple_buf = nullptr;
    if (_keys_type == KeysType::DUP_KEYS) {
        // Will insert directly, so use memory from _table_mem_pool
        _tuple_buf = _table_mem_pool->allocate(_schema_size);
        ContiguousRow row(_schema, _tuple_buf);
        _tuple_to_row(tuple, &row, _table_mem_pool.get());
        _skip_list->Insert((TableKey)_tuple_buf, &overwritten);
        DCHECK(!overwritten) << "Duplicate key model meet overwrite in SkipList";
        return;
    }

    // For non-DUP models, for the data rows passed from the upper layer, when copying the data,
    // we first allocate from _buffer_mem_pool, and then check whether it already exists in
    // _skiplist.  If it exists, we aggregate the new row into the row in skiplist.
    // otherwise, we need to copy it into _table_mem_pool before we can insert it.
    _tuple_buf = _buffer_mem_pool->allocate(_schema_size);
    ContiguousRow src_row(_schema, _tuple_buf);
    _tuple_to_row(tuple, &src_row, _buffer_mem_pool.get());

    bool is_exist = _skip_list->Find((TableKey)_tuple_buf, &_hint);
    if (is_exist) {
        _aggregate_two_row(src_row, _hint.curr->key);
    } else {
        _tuple_buf = _table_mem_pool->allocate(_schema_size);
        ContiguousRow dst_row(_schema, _tuple_buf);
        copy_row_in_memtable(&dst_row, src_row, _table_mem_pool.get());
        _skip_list->InsertWithHint((TableKey)_tuple_buf, is_exist, &_hint);
    }

    // Make MemPool to be reusable, but does not free its memory
    _buffer_mem_pool->clear();
}

void MemTable::_tuple_to_row(const Tuple* tuple, ContiguousRow* row, MemPool* mem_pool) {
    for (size_t i = 0; i < _slot_descs->size(); ++i) {
        auto cell = row->cell(i);
        const SlotDescriptor* slot = (*_slot_descs)[i];

        bool is_null = tuple->is_null(slot->null_indicator_offset());
        const void* value = tuple->get_slot(slot->tuple_offset());
        _schema->column(i)->consume(
                &cell, (const char*)value, is_null, mem_pool, &_agg_object_pool);
    }
}
```
DeltaWriter::write()会通过调用MemTable::insert（）将数据写入到MemTable中，上述函数完成将base表的数据转为rollup表的数据的逻辑。其中，insert参数tuple是base表的数据，_tuple_to_row具体完成了将tuple转化成rollup index的数据。并且底层使用跳表保存数据集合，会自动对数据行进行排序。如果是聚合模型，还会对数据进行聚合计算。

4. 如何实现导入的原子性

	在LoadLoadingTask任务下发之后，就会开始事务在BE的中执行，并且在BE中DeltaWriter会调用prepare_txn将事务进行持久化。如果所有的LoadLoadingTask quorum完成，则会认为该事务成功，会通过publish_version rpc向BE发送PublishVersionTask，进行事务的提交。

## 总结

以上大体上介绍了broker的实现机制，包括数据读取、ETL、index数据生成等。但是还有一块没有介绍的就是MemTable的数据是如何写到磁盘中，生成Segment文件。这部分后续会在另外一篇文章中进行阐述。