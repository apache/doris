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

package org.apache.doris.datasource.source;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.arrowflight.FlightSqlClientLoadBalancer;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TArrowFlightFileDesc;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ArrowFlightScanNode extends FileQueryScanNode {
    private static final Logger LOG = LogManager.getLogger(ArrowFlightScanNode.class);

    public static final String BOOLEAN_TRUE_REPRESENTATION = "1";

    private final List<String> columns = new ArrayList<>();
    private final List<String> filters = new ArrayList<>();

    private final ArrowFlightSource source;

    public ArrowFlightScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv,
                               SessionVariable sv, ArrowFlightSource source) {
        super(id, desc, "ARROW_FLIGHT_SCAN_NODE", needCheckColumnPriv, sv);
        this.source = source;
    }

    @Override
    protected void doInitialize() throws UserException {
        super.doInitialize();
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        List<Pair<String, ByteBuffer>> locationAndTicketList = executeQuery();

        return locationAndTicketList.stream()
            .map(locationAndTicket -> new ArrowFlightSplit(locationAndTicket.first, locationAndTicket.second))
            .collect(Collectors.toList());
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (split instanceof ArrowFlightSplit) {
            ArrowFlightSplit dorisArrowSplit = (ArrowFlightSplit) split;
            TArrowFlightFileDesc fileDesc = new TArrowFlightFileDesc();
            fileDesc.setTicket(dorisArrowSplit.getTicket());
            fileDesc.setLocationUri(dorisArrowSplit.getLocation());
            fileDesc.setUser(source.getUsername());
            fileDesc.setPassword(source.getPassword());

            // set TTableFormatFileDesc
            TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
            tableFormatFileDesc.setArrowFlightParams(fileDesc);
            tableFormatFileDesc.setTableFormatType(((ArrowFlightSplit) split).getTableFormatType().value());

            // set TFileRangeDesc
            rangeDesc.setTableFormatParams(tableFormatFileDesc);
        }
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(source.getTargetTableName()).append("\n");
        if (detailLevel == TExplainLevel.BRIEF) {
            return output.toString();
        }
        output.append(prefix).append("QUERY: ").append(getQueryStr()).append("\n");
        if (!conjuncts.isEmpty()) {
            Expr expr = convertConjunctsToAndCompoundPredicate(conjuncts);
            output.append(prefix).append("PREDICATES: ").append(expr.toSql()).append("\n");
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
        return source.getProperties();
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

        for (int i = 0; i < source.getQueryRetryCount(); i++) {
            try {
                FlightSqlClientLoadBalancer.FlightSqlClientWithOptions clientWithOptions =
                        source.getSqlClientLoadBalancer().randomClient();
                FlightSqlClient flightSqlClient = clientWithOptions.getFlightSqlClient();
                CallOption[] callOptions = clientWithOptions.getCallOptions();

                FlightInfo info = flightSqlClient.execute(queryStr, callOptions);

                return processFlightEndpoints(info.getEndpoints());
            } catch (Exception e) {
                LOG.warn("arrow request failures, try next node", e);
                lastException = new RuntimeException(e.getMessage());
            }
        }

        throw new RuntimeException("Failed to execute query: " + queryStr, lastException);
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

        sql.append(Joiner.on(", ").join(columns));

        sql.append(" FROM ").append(source.getTargetTableName());

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
            String filter = children.get(0).toExternalSql(TableIf.TableType.DORIS_EXTERNAL_TABLE, tbl);
            filter += " " + ((BinaryPredicate) expr).getOp().toString() + " ";

            filter += children.get(1).toExternalSql(TableIf.TableType.DORIS_EXTERNAL_TABLE, tbl);

            return filter;
        }

        // Only for old planner
        if (expr.contains(BoolLiteral.class) && BOOLEAN_TRUE_REPRESENTATION.equals(expr.getStringValue())
                && expr.getChildren().isEmpty()) {
            return "1 = 1";
        }

        return expr.toExternalSql(TableIf.TableType.DORIS_EXTERNAL_TABLE, tbl);
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
