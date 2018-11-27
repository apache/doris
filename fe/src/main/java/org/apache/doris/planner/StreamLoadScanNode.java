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

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.ColumnSeparator;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.ImportColumnsStmt;
import org.apache.doris.analysis.ImportWhereStmt;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TBrokerRangeDesc;
import org.apache.doris.thrift.TBrokerScanNode;
import org.apache.doris.thrift.TBrokerScanRange;
import org.apache.doris.thrift.TBrokerScanRangeParams;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TStreamLoadPutRequest;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * used to scan from stream
 */
public class StreamLoadScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(StreamLoadScanNode.class);

    // TODO(zc): now we use scanRange
    // input parameter
    private Table dstTable;
    private TStreamLoadPutRequest request;

    // helper
    private Analyzer analyzer;
    private TupleDescriptor srcTupleDesc;
    private TBrokerScanRange brokerScanRange;

    private Map<String, SlotDescriptor> slotDescByName = Maps.newHashMap();
    private Map<String, Expr> exprsByName = Maps.newHashMap();

    // used to construct for streaming loading
    public StreamLoadScanNode(
            PlanNodeId id, TupleDescriptor tupleDesc, Table dstTable, TStreamLoadPutRequest request) {
        super(id, tupleDesc, "StreamLoadScanNode");
        this.dstTable = dstTable;
        this.request = request;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        // can't call super.init(), because after super.init, conjuncts would be null
        assignConjuncts(analyzer);

        this.analyzer = analyzer;
        brokerScanRange = new TBrokerScanRange();

        TBrokerRangeDesc rangeDesc = new TBrokerRangeDesc();
        rangeDesc.file_type = request.getFileType();
        rangeDesc.format_type = request.getFormatType();
        rangeDesc.splittable = false;
        switch (request.getFileType()) {
            case FILE_LOCAL:
                rangeDesc.path = request.getPath();
                break;
            case FILE_STREAM:
                rangeDesc.path = "Invalid Path";
                rangeDesc.load_id = request.getLoadId();
                break;
            default:
                throw new UserException("unsupported file type, type=" + request.getFileType());
        }
        rangeDesc.start_offset = 0;
        rangeDesc.size = -1;
        brokerScanRange.addToRanges(rangeDesc);

        srcTupleDesc = analyzer.getDescTbl().createTupleDescriptor("StreamLoadScanNode");

        TBrokerScanRangeParams params = new TBrokerScanRangeParams();

        // parse columns header. this contain map from input column to column of destination table
        // columns: k1, k2, v1, v2=k1 + k2
        // this means that there are three columns(k1, k2, v1) in source file,
        // and v2 is derived from (k1 + k2)
        if (request.isSetColumns()) {
            String columnsSQL = new String("COLUMNS " + request.getColumns());
            SqlParser parser = new SqlParser(new SqlScanner(new StringReader(columnsSQL)));
            ImportColumnsStmt columnsStmt;
            try {
                columnsStmt = (ImportColumnsStmt) parser.parse().value;
            } catch (Error e) {
                LOG.warn("error happens when parsing columns, sql={}", columnsSQL, e);
                throw new AnalysisException("failed to parsing columns' header, maybe contain unsupported character");
            } catch (AnalysisException e) {
                LOG.warn("analyze columns' statement failed, sql={}, error={}",
                        columnsSQL, parser.getErrorMsg(columnsSQL), e);
                String errorMessage = parser.getErrorMsg(columnsSQL);
                if (errorMessage == null) {
                    throw  e;
                } else {
                    throw new AnalysisException(errorMessage, e);
                }
            } catch (Exception e) {
                LOG.warn("failed to parse columns header, sql={}", columnsSQL, e);
                throw new UserException("parse columns header failed", e);
            }

            for (ImportColumnDesc columnDesc : columnsStmt.getColumns()) {
                if (columnDesc.getExpr() != null) {
                    exprsByName.put(columnDesc.getColumn(), columnDesc.getExpr());
                } else {
                    SlotDescriptor slotDesc = analyzer.getDescTbl().addSlotDescriptor(srcTupleDesc);
                    slotDesc.setType(ScalarType.createType(PrimitiveType.VARCHAR));
                    slotDesc.setIsMaterialized(true);
                    slotDesc.setIsNullable(false);
                    params.addToSrc_slot_ids(slotDesc.getId().asInt());
                    slotDescByName.put(columnDesc.getColumn(), slotDesc);
                }
            }

            // analyze all exprs
            for (Map.Entry<String, Expr> entry : exprsByName.entrySet()) {
                ExprSubstitutionMap smap = new ExprSubstitutionMap();
                List<SlotRef> slots = Lists.newArrayList();
                entry.getValue().collect(SlotRef.class, slots);
                for (SlotRef slot : slots) {
                    SlotDescriptor slotDesc = slotDescByName.get(slot.getColumnName());
                    if (slotDesc == null) {
                        throw new UserException("unknown reference column, column=" + entry.getKey()
                                + ", reference=" + slot.getColumnName());
                    }
                    smap.getLhs().add(slot);
                    smap.getRhs().add(new SlotRef(slotDesc));
                }
                Expr expr = entry.getValue().clone(smap);
                expr.analyze(analyzer);

                // check if contain aggregation
                List<FunctionCallExpr> funcs = Lists.newArrayList();
                expr.collect(FunctionCallExpr.class, funcs);
                for (FunctionCallExpr fn : funcs) {
                    if (fn.isAggregateFunction()) {
                        throw new AnalysisException("Don't support aggregation function in load expression");
                    }
                }

                exprsByName.put(entry.getKey(), expr);
            }
        } else {
            for (Column column : dstTable.getBaseSchema()) {
                SlotDescriptor slotDesc = analyzer.getDescTbl().addSlotDescriptor(srcTupleDesc);
                slotDesc.setType(ScalarType.createType(PrimitiveType.VARCHAR));
                slotDesc.setIsMaterialized(true);
                slotDesc.setIsNullable(false);
                params.addToSrc_slot_ids(slotDesc.getId().asInt());

                slotDescByName.put(column.getName(), slotDesc);
            }
        }

        // analyze where statement
        if (request.isSetWhere()) {
            Map<String, SlotDescriptor> dstDescMap = Maps.newHashMap();
            for (SlotDescriptor slotDescriptor : desc.getSlots()) {
                dstDescMap.put(slotDescriptor.getColumn().getName(), slotDescriptor);
            }

            String whereSQL = new String("WHERE " + request.getWhere());
            SqlParser parser = new SqlParser(new SqlScanner(new StringReader(whereSQL)));
            ImportWhereStmt whereStmt;
            try {
                whereStmt = (ImportWhereStmt) parser.parse().value;
            } catch (Error e) {
                LOG.warn("error happens when parsing where header, sql={}", whereSQL, e);
                throw new AnalysisException("failed to parsing where header, maybe contain unsupported character");
            } catch (AnalysisException e) {
                LOG.warn("analyze where statement failed, sql={}, error={}",
                        whereSQL, parser.getErrorMsg(whereSQL), e);
                String errorMessage = parser.getErrorMsg(whereSQL);
                if (errorMessage == null) {
                    throw  e;
                } else {
                    throw new AnalysisException(errorMessage, e);
                }
            } catch (Exception e) {
                LOG.warn("failed to parse where header, sql={}", whereSQL, e);
                throw new UserException("parse columns header failed", e);
            }

            // substitute SlotRef in filter expression
            Expr whereExpr = whereStmt.getExpr();

            List<SlotRef> slots = Lists.newArrayList();
            whereExpr.collect(SlotRef.class, slots);

            ExprSubstitutionMap smap = new ExprSubstitutionMap();
            for (SlotRef slot : slots) {
                SlotDescriptor slotDesc = dstDescMap.get(slot.getColumnName());
                if (slotDesc == null) {
                    throw new UserException("unknown column reference in where statement, reference="
                            + slot.getColumnName());
                }
                smap.getLhs().add(slot);
                smap.getRhs().add(new SlotRef(slotDesc));
            }
            whereExpr= whereExpr.clone(smap);
            whereExpr.analyze(analyzer);
            if (whereExpr.getType() != Type.BOOLEAN) {
                throw new UserException("where statement is not a valid statement return bool");
            }
            addConjuncts(whereExpr.getConjuncts());
        }

        computeStats(analyzer);
        createDefaultSmap(analyzer);

        if (request.isSetColumnSeparator()) {
            String sep = ColumnSeparator.convertSeparator(request.getColumnSeparator());
            params.setColumn_separator(sep.getBytes(Charset.forName("UTF-8"))[0]);
        } else {
            params.setColumn_separator((byte) '\t');
        }
        params.setLine_delimiter((byte) '\n');
        params.setSrc_tuple_id(srcTupleDesc.getId().asInt());
        params.setDest_tuple_id(desc.getId().asInt());
        brokerScanRange.setParams(params);

        brokerScanRange.setBroker_addresses(Lists.newArrayList());
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException, UserException {
        finalizeParams();
    }

    private void finalizeParams() throws UserException {
        for (SlotDescriptor dstSlotDesc : desc.getSlots()) {
            if (!dstSlotDesc.isMaterialized()) {
                continue;
            }
            Expr expr = null;
            if (exprsByName != null) {
                expr = exprsByName.get(dstSlotDesc.getColumn().getName());
            }
            if (expr == null) {
                SlotDescriptor srcSlotDesc = slotDescByName.get(dstSlotDesc.getColumn().getName());
                if (srcSlotDesc != null) {
                    // If dest is allow null, we set source to nullable
                    if (dstSlotDesc.getColumn().isAllowNull()) {
                        srcSlotDesc.setIsNullable(true);
                    }
                    expr = new SlotRef(srcSlotDesc);
                } else {
                    Column column = dstSlotDesc.getColumn();
                    if (column.getDefaultValue() != null) {
                        expr = new StringLiteral(dstSlotDesc.getColumn().getDefaultValue());
                    } else {
                        if (column.isAllowNull()) {
                            expr = NullLiteral.create(column.getType());
                        } else {
                            throw new AnalysisException("column has no source field, column=" + column.getName());
                        }
                    }
                }
            }
            // check hll_hash
            if (dstSlotDesc.getType().getPrimitiveType() == PrimitiveType.HLL) {
                if (!(expr instanceof FunctionCallExpr)) {
                    throw new AnalysisException("HLL column must use hll_hash function, like "
                            + dstSlotDesc.getColumn().getName() + "=hll_hash(xxx)");
                }
                FunctionCallExpr fn = (FunctionCallExpr) expr;
                if (!fn.getFnName().getFunction().equalsIgnoreCase("hll_hash")) {
                    throw new AnalysisException("HLL column must use hll_hash function, like "
                            + dstSlotDesc.getColumn().getName() + "=hll_hash(xxx)");
                }
                expr.setType(Type.HLL);
            }
            expr = castToSlot(dstSlotDesc, expr);
            brokerScanRange.params.putToExpr_of_dest_slot(dstSlotDesc.getId().asInt(), expr.treeToThrift());
        }
        brokerScanRange.params.setDest_tuple_id(desc.getId().asInt());
        LOG.info("brokerScanRange is {}", brokerScanRange);

        // Need re compute memory layout after set some slot descriptor to nullable
        srcTupleDesc.computeMemLayout();
    }

    private Expr castToSlot(SlotDescriptor slotDesc, Expr expr) throws UserException {
        if (slotDesc.getType().isNull()) {
            return expr;
        }
        PrimitiveType dstType = slotDesc.getType().getPrimitiveType();
        PrimitiveType srcType = expr.getType().getPrimitiveType();
        if (dstType.isStringType()) {
            if (srcType.isStringType()) {
                return expr;
            } else {
                CastExpr castExpr = (CastExpr)expr.castTo(Type.VARCHAR);
                return castExpr;
            }
        } else if (dstType != srcType) {
            CastExpr castExpr = (CastExpr)expr.castTo(slotDesc.getType());
            return castExpr;
        }

        return expr;
    }

    @Override
    protected void toThrift(TPlanNode planNode) {
        planNode.setNode_type(TPlanNodeType.BROKER_SCAN_NODE);
        TBrokerScanNode brokerScanNode = new TBrokerScanNode(desc.getId().asInt());
        planNode.setBroker_scan_node(brokerScanNode);
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        TScanRangeLocations locations = new TScanRangeLocations();
        TScanRange scanRange = new TScanRange();
        scanRange.setBroker_scan_range(brokerScanRange);
        locations.setScan_range(scanRange);
        locations.setLocations(Lists.newArrayList());
        return Lists.newArrayList(locations);
    }

    @Override
    public int getNumInstances() { return 1; }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        return "StreamLoadScanNode";
    }
}
