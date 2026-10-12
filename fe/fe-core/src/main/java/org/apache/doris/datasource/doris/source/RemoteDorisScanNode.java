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
import org.apache.doris.qe.ConnectContext;
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
import java.util.stream.Collectors;

public class RemoteDorisScanNode extends FileQueryScanNode {
    private static final Logger LOG = LogManager.getLogger(RemoteDorisScanNode.class);

    public static final String BOOLEAN_TRUE_REPRESENTATION = "1";

    private final List<String> columns = new ArrayList<String>();
    private final List<String> filters = new ArrayList<String>();

    private RemoteDorisSource source;

    // The Flight SQL session this scan opened on the remote frontend, from getSplits until stop()
    // closes it (see RemoteDorisFlightSession for why it must live that long and no longer). All
    // three guarded by this: stop() may run on another thread than the one that planned the query -
    // a KILL, the timeout checker - and more than once (cancel, then close).
    private RemoteDorisFlightSession flightSession;
    private boolean stopped;
    // Whether stop() ended a session this scan had opened: the endpoints handed to the backend
    // belong to that session's query, and a plan dispatched again with them (the same-plan retry
    // of StmtExecutor.handleQueryWithRetry) would read what the remote frontend may have torn
    // down with the session.
    private boolean sessionClosedByStop;

    public RemoteDorisScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv,
                               SessionVariable sv, ScanContext scanContext) {
        super(id, desc, "REMOTE_DORIS_SCAN_NODE", scanContext, needCheckColumnPriv, sv);
    }

    @Override
    protected void doInitialize() throws UserException {
        super.doInitialize();
        source = new RemoteDorisSource(desc);
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        List<Pair<String, ByteBuffer>> locationAndTicketList = executeQuery();

        return locationAndTicketList.stream()
            .map(locationAndTicket -> new RemoteDorisSplit(locationAndTicket.first, locationAndTicket.second))
            .collect(Collectors.toList());
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

    // Executes a SQL query using the Apache Arrow Flight SQL protocol with the provided credentials.
    private List<Pair<String, ByteBuffer>> executeQuery() {
        createColumns();
        createFilters();

        if (isExplainStatement()) {
            return new ArrayList<>();
        }

        String queryStr = getQueryStr();
        Exception lastException = null;

        for (int i = 0; i < source.getCatalog().getQueryRetryCount(); i++) {
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

        throw new RuntimeException("Failed to execute query: " + queryStr, lastException);
    }

    // Opens a Flight SQL session on the remote frontend and runs the query in it. The session is
    // kept until stop(): its query serves the BE's DoGet of the endpoints returned here. A session
    // whose query failed is closed right away, so a retry on the next node leaves nothing behind.
    @VisibleForTesting
    List<Pair<String, ByteBuffer>> executeFlightSqlQuery(Pair<String, Integer> hostAndPort,
                     String user, String psw, String sql, int timeoutSec) throws Exception {
        RemoteDorisFlightSession session = RemoteDorisFlightSession.open(hostAndPort, user, psw);
        FlightInfo info;
        try {
            info = session.execute(sql, timeoutSec);
        } catch (Throwable t) {
            session.close();
            throw t;
        }
        keepFlightSession(session);
        return processFlightEndpoints(info.getEndpoints());
    }

    /**
     * Holds {@code session} until {@link #stop()}. A session handed over after stop() already ran,
     * or on top of one still held, is closed at once instead: this scan owns one session at most,
     * and none once stopped. The statement registers the node as well: stop() is the coordinator's
     * to call, but a plan that never gets one, or whose coordinator nobody closes, is stopped when
     * the statement ends instead ({@link org.apache.doris.nereids.StatementContext#stopScanNodeAtClose}).
     */
    @VisibleForTesting
    void keepFlightSession(RemoteDorisFlightSession session) {
        RemoteDorisFlightSession toClose;
        synchronized (this) {
            if (stopped) {
                toClose = session;
            } else {
                toClose = flightSession;
                flightSession = session;
            }
        }
        if (toClose != null) {
            toClose.close();
        }
        if (toClose != session) {
            ConnectContext.get().getStatementContext().stopScanNodeAtClose(this);
        }
    }

    /**
     * True once {@link #stop()} ended the session this scan opened: the endpoints in its scan
     * ranges belong to that session's query on the remote frontend, so the same plan must not be
     * dispatched again (see {@link ScanNode#cannotBeRedispatched()}).
     */
    @Override
    public boolean cannotBeRedispatched() {
        synchronized (this) {
            return sessionClosedByStop;
        }
    }

    /**
     * True while this scan holds a Flight SQL session on the remote frontend: the local coordinator
     * has to stay alive until the BE has finished reading the remote query, since closing it is what
     * ends the session ({@link #stop()}) - and the remote frontend cancels what a closed session was
     * still running. Without this, an Arrow Flight SQL query on this frontend would close its
     * coordinator right after dispatch (#67503), while its BE may still be reading.
     */
    @Override
    public boolean coordinatorMustOutliveDispatch() {
        if (super.coordinatorMustOutliveDispatch()) {
            return true;
        }
        synchronized (this) {
            return flightSession != null;
        }
    }

    /**
     * Ends the Flight SQL session on the remote frontend, in addition to what {@code FileQueryScanNode}
     * releases. Called by the coordinator when the local query closes or is cancelled, i.e. when the
     * BE is done with (or gave up on) the remote query's endpoints.
     */
    @Override
    public void stop() {
        super.stop();
        RemoteDorisFlightSession session;
        synchronized (this) {
            stopped = true;
            session = flightSession;
            flightSession = null;
            if (session != null) {
                sessionClosedByStop = true;
            }
        }
        if (session != null) {
            session.close();
        }
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

    private String getQueryStr() {
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

    // TODO: Use AST parsing instead of string matching for EXPLAIN detection
    private boolean isExplainStatement() {
        return ConnectContext.get().getStatementContext().getOriginStatement().originStmt
            .trim().toLowerCase().startsWith("explain");
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
