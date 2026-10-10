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

package org.apache.doris.datasource.doris.source;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.ExprToExternalSqlVisitor;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.scan.FileQueryScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TRemoteDorisFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Scans a table of another Doris cluster over Arrow Flight SQL: runs a query on a remote frontend, and the backends
 * read its result straight from the remote backends with DoGet, one endpoint of the result per split.
 *
 * <p>The remote query runs when the coordinator dispatches the plan, never while it is planned. The scan is in batch
 * mode: planning builds the query (EXPLAIN shows it) and a scan range per backend pointing at a split source, and the
 * splits come from {@link #startSplit}, which the split assignment calls once the coordinator starts it
 * (ScanNode#start). So a plan nobody runs - an EXPLAIN, a plan built only to be inspected, a statement that fails
 * before dispatch - never reaches the remote frontend, and the Flight SQL session the query runs in belongs to the
 * split assignment, which the coordinator stops when it closes or cancels.
 */
public class RemoteDorisScanNode extends FileQueryScanNode {
    private static final Logger LOG = LogManager.getLogger(RemoteDorisScanNode.class);

    public static final String BOOLEAN_TRUE_REPRESENTATION = "1";

    private final List<String> columns = new ArrayList<String>();
    private final List<String> filters = new ArrayList<String>();

    private RemoteDorisSource source;

    public RemoteDorisScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv,
                               SessionVariable sv, ScanContext scanContext) {
        super(id, desc, "REMOTE_DORIS_SCAN_NODE", scanContext, needCheckColumnPriv, sv);
    }

    @Override
    protected void doInitialize() throws UserException {
        super.doInitialize();
        source = new RemoteDorisSource(desc);
    }

    // Builds the query the scan runs on the remote frontend from its slots and conjuncts, final by now: EXPLAIN
    // shows it, startSplit runs it.
    @Override
    protected void convertPredicate() {
        createColumns();
        createFilters();
    }

    @Override
    public boolean isBatchMode() {
        return true;
    }

    // The params of the scan depend on none of its splits, and producing one means running the remote query.
    @Override
    protected boolean needsSampleSplit() {
        return false;
    }

    // Unknown until the remote frontend has run the query: each endpoint of its result is a split.
    @Override
    public int numApproximateSplits() {
        return 0;
    }

    /**
     * Runs the query on the remote frontend and hands the endpoints of its result to the backends as the splits.
     * The split assignment calls this when the coordinator dispatches the plan. The Flight SQL session the query
     * runs in goes to the split assignment, which ends it when the coordinator stops this scan: see
     * {@link RemoteDorisFlightSession} for why it lives that long.
     */
    @Override
    public void startSplit(int numBackends) throws UserException {
        List<Split> splits = new ArrayList<>();
        for (Pair<String, ByteBuffer> locationAndTicket : executeQuery()) {
            splits.add(new RemoteDorisSplit(locationAndTicket.first, locationAndTicket.second));
        }
        splitAssignment.addToQueue(splits);
        splitAssignment.finishSchedule();
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (split instanceof RemoteDorisSplit) {
            RemoteDorisSplit dorisArrowSplit = (RemoteDorisSplit) split;
            TRemoteDorisFileDesc fileDesc = new TRemoteDorisFileDesc();
            fileDesc.setIp(source.getHostAndArrowPort().key());
            fileDesc.setArrowPort(source.getHostAndArrowPort().value().toString());
            fileDesc.setTicket(dorisArrowSplit.getTicket());
            fileDesc.setLocationUri(dorisArrowSplit.getLocation());
            fileDesc.setUser(source.getCatalog().getUsername());
            fileDesc.setPassword(source.getCatalog().getPassword());

            // set TTableFormatFileDesc
            TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
            tableFormatFileDesc.setRemoteDorisParams(fileDesc);
            tableFormatFileDesc.setTableFormatType(((RemoteDorisSplit) split).getTableFormatType().value());

            // set TFileRangeDesc
            rangeDesc.setTableFormatParams(tableFormatFileDesc);
        }
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(source.getTargetTable().getExternalTableName()).append("\n");
        if (detailLevel == TExplainLevel.BRIEF) {
            return output.toString();
        }
        output.append(prefix).append("QUERY: ").append(getQueryStr()).append("\n");
        if (!conjuncts.isEmpty()) {
            Expr expr = convertConjunctsToAndCompoundPredicate(conjuncts);
            output.append(prefix).append("PREDICATES: ")
                    .append(expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE)).append("\n");
        }

        return output.toString();
    }

    @Override
    protected TFileFormatType getFileFormatType() throws UserException {
        return TFileFormatType.FORMAT_ARROW;
    }

    @Override
    protected List<String> getPathPartitionKeys() throws UserException {
        return new ArrayList<>();
    }

    @Override
    protected TableIf getTargetTable() throws UserException {
        return desc.getTable();
    }

    @Override
    protected Map<String, String> getLocationProperties() throws UserException {
        return source.getCatalog().getProperties();
    }

    // Runs the query on the remote frontends in turn, until one answers or the attempts run out. Once the scan is
    // stopped - the coordinator was cancelled meanwhile, and reports why - it stops trying, with no split.
    private List<Pair<String, ByteBuffer>> executeQuery() throws UserException {
        String queryStr = getQueryStr();
        Exception lastException = null;

        for (int i = 0; i < source.getCatalog().getQueryRetryCount(); i++) {
            if (splitAssignment.isStop()) {
                return new ArrayList<>();
            }
            try {
                return executeFlightSqlQuery(
                    source.nextHostAndArrowPort(),
                    source.getCatalog().getUsername(),
                    source.getCatalog().getPassword(),
                    queryStr,
                    source.getCatalog().getQueryTimeoutSec()
                );
            } catch (Exception e) {
                LOG.warn("arrow request node [{}] failures {}, try next nodes",
                        source.getHostAndArrowPort().toString(), e);
                lastException = new RuntimeException(e.getMessage());
            }
        }

        throw new UserException("Failed to execute query: " + queryStr, lastException);
    }

    // Opens a Flight SQL session on the remote frontend and runs the query in it. The session goes to the split
    // assignment, which ends it when the coordinator stops this scan - at once if it did meanwhile: its query serves
    // the BE's DoGet of the endpoints returned here. A session whose query failed is closed right away, so a retry
    // on the next node leaves nothing behind.
    @VisibleForTesting
    List<Pair<String, ByteBuffer>> executeFlightSqlQuery(Pair<String, Integer> hostAndPort,
                     String user, String psw, String sql, int timeoutSec) throws Exception {
        RemoteDorisFlightSession session = RemoteDorisFlightSession.open(hostAndPort, user, psw, timeoutSec);
        FlightInfo info;
        try {
            info = session.execute(sql, timeoutSec);
        } catch (Throwable t) {
            session.close();
            throw t;
        }
        splitAssignment.addCloseable(session);
        return processFlightEndpoints(info.getEndpoints());
    }

    private void createColumns() {
        columns.clear();
        for (SlotDescriptor slot : desc.getSlots()) {
            Column col = slot.getColumn();
            columns.add("`" + col.getName() + "`");
        }
        if (columns.isEmpty()) {
            columns.add("*");
        }
    }

    @VisibleForTesting
    String getQueryStr() {
        StringBuilder sql = new StringBuilder("SELECT ");

        if (source.getCatalog().enableParallelResultSink()) {
            sql.append("/*+ SET_VAR(enable_parallel_result_sink=true) */ ");
        } else {
            sql.append("/*+ SET_VAR(enable_parallel_result_sink=false) */ ");
        }

        sql.append(Joiner.on(", ").join(columns));

        sql.append(" FROM ").append(source.getTargetTable().getExternalTableName());

        if (!filters.isEmpty()) {
            sql.append(" WHERE (");
            sql.append(Joiner.on(") AND (").join(filters));
            sql.append(")");
        }

        if (limit != -1) {
            sql.append(" LIMIT ").append(limit);
        }

        return sql.toString();
    }

    private void createFilters() {
        if (conjuncts.isEmpty()) {
            return;
        }

        List<SlotRef> slotRefs = Lists.newArrayList();
        Expr.collectList(conjuncts, SlotRef.class, slotRefs);
        ExprSubstitutionMap sMap = new ExprSubstitutionMap();
        for (SlotRef slotRef : slotRefs) {
            SlotRef slotRef1 = (SlotRef) slotRef.clone();
            slotRef1.setTableNameInfoToNull();
            slotRef1.setLabel("`" + slotRef1.getColumnName() + "`");
            sMap.put(slotRef, slotRef1);
        }

        ArrayList<Expr> conjunctsList = Expr.cloneList(conjuncts, sMap);
        for (Expr expr : conjunctsList) {
            String filter = conjunctExprToString(expr, desc.getTable());
            filters.add(filter);
        }
    }

    private String conjunctExprToString(Expr expr, TableIf tbl) {
        if (expr.contains(DateLiteral.class) && expr instanceof BinaryPredicate) {
            ArrayList<Expr> children = expr.getChildren();
            ToSqlParams params = new ToSqlParams(false, true, TableIf.TableType.DORIS_EXTERNAL_TABLE, tbl);
            String filter = children.get(0).accept(ExprToExternalSqlVisitor.INSTANCE, params);
            filter += " " + ((BinaryPredicate) expr).getOp().toString() + " ";

            filter += children.get(1).accept(ExprToExternalSqlVisitor.INSTANCE, params);

            return filter;
        }

        // Only for old planner
        if (expr.contains(BoolLiteral.class) && BOOLEAN_TRUE_REPRESENTATION.equals(expr.getStringValue())
                && expr.getChildren().isEmpty()) {
            return "1 = 1";
        }

        return expr.accept(ExprToExternalSqlVisitor.INSTANCE,
                new ToSqlParams(false, true, TableIf.TableType.DORIS_EXTERNAL_TABLE, tbl));
    }

    private List<Pair<String, ByteBuffer>> processFlightEndpoints(List<FlightEndpoint> endpoints) {
        List<Pair<String, ByteBuffer>> uniquePairs = new ArrayList<>();
        Set<String> seenPairs = new HashSet<>();
        for (FlightEndpoint endpoint : endpoints) {
            ByteBuffer ticket = endpoint.getTicket().serialize();
            for (Location location : endpoint.getLocations()) {
                String uri = location.getUri().toString();
                String compositeKey = ticket.hashCode() + uri;

                if (seenPairs.add(compositeKey)) {
                    uniquePairs.add(Pair.of(uri, ticket));
                }
            }
        }
        return uniquePairs;
    }
}
