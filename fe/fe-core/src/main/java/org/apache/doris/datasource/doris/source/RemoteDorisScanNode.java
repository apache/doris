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
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TArrowResultFileDesc;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RemoteDorisScanNode extends FileQueryScanNode {
    private static final Logger LOG = LogManager.getLogger(RemoteDorisScanNode.class);

    private static final int ADBC_EXEC_RETRY = 3;

    private final List<String> columns = new ArrayList<String>();
    private final List<String> filters = new ArrayList<String>();

    private RemoteDorisSource source;

    public RemoteDorisScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv,
                               SessionVariable sv) {
        super(id, desc, "REMOTE_DORIS_SCAN_NODE", StatisticalType.REMOTE_DORIS_SCAN_NODE, needCheckColumnPriv, sv);
    }

    @Override
    protected void doInitialize() throws UserException {
        super.doInitialize();
        source = new RemoteDorisSource(desc);
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        List<Pair<String, byte[]>> locationAndTicketList = executeAdbcQuery();

        List<Split> splits = Lists.newArrayList();

        for (Pair<String, byte[]> locationAndTicket : locationAndTicketList) {
            splits.add(new RemoteDorisSplit(locationAndTicket.first, locationAndTicket.second));
        }

        return splits;
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (split instanceof RemoteDorisSplit) {
            RemoteDorisSplit dorisArrowSplit = (RemoteDorisSplit) split;
            TArrowResultFileDesc fileDesc = new TArrowResultFileDesc();
            fileDesc.setIp(source.getHostAndArrowPort().key());
            fileDesc.setArrowPort(source.getHostAndArrowPort().value().toString());
            fileDesc.setTicket(Base64.getEncoder().encodeToString(dorisArrowSplit.getTicket()));
            fileDesc.setLocationUri(dorisArrowSplit.getLocation());
            fileDesc.setUser(source.getCatalog().getUsername());
            fileDesc.setPassword(source.getCatalog().getPassword());

            // set TTableFormatFileDesc
            TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
            tableFormatFileDesc.setArrowResultParams(fileDesc);
            tableFormatFileDesc.setTableFormatType(TableFormatType.ARROW_RESULT.value());

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
        output.append(prefix).append("QUERY: ").append(getAdbcQueryStr()).append("\n");
        if (!conjuncts.isEmpty()) {
            Expr expr = convertConjunctsToAndCompoundPredicate(conjuncts);
            output.append(prefix).append("PREDICATES: ").append(expr.toSql()).append("\n");
        }

        return output.toString();
    }

    @Override
    protected TFileFormatType getFileFormatType() throws UserException {
        return TFileFormatType.FORMAT_JNI;
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

    // Send an adbc request and receive the response result from adbc
    private List<Pair<String, byte[]>> executeAdbcQuery() {
        createAdbcColumns();
        createAdbcFilters();

        String queryStr = getAdbcQueryStr();
        RuntimeException scratchExceptionForThrow = null;

        for (int i = 0; i < ADBC_EXEC_RETRY; i++) {
            try {
                return getUriAndTickets(
                    source.nextHostAndArrowPort(),
                    source.getCatalog().getUsername(),
                    source.getCatalog().getPassword(),
                    queryStr
                );
            } catch (Exception e) {
                LOG.warn("arrow request node [{}] failures {}, try next nodes",
                        source.getHostAndArrowPort().toString(), e);
                scratchExceptionForThrow = new RuntimeException(e.getMessage());
            }
        }

        LOG.error("try all arrow nodes [{}], no other nodes left, sql:{}", source.getHostAndArrowPort(), queryStr);
        throw scratchExceptionForThrow;
    }

    public static List<Pair<String, byte[]>> getUriAndTickets(Pair<String, Integer> hostAndPort,
                                                              String user, String psw, String sql) throws Exception {
        List<Pair<String, byte[]>> uniquePairs = new ArrayList<>();
        FlightClient clientFE = null;
        FlightSqlClient sqlClientFE = null;
        try {
            BufferAllocator allocatorFE = new RootAllocator(Integer.MAX_VALUE);
            final Location clientLocationFE = new Location(
                    new URI("grpc", null, hostAndPort.first,
                        hostAndPort.second, null, null, null)
            );

            clientFE = FlightClient.builder(allocatorFE, clientLocationFE).build();
            sqlClientFE = new FlightSqlClient(clientFE);

            CredentialCallOption credentialCallOption = clientFE.authenticateBasicToken(user, psw).get();
            FlightSqlClient.PreparedStatement preparedStatement = sqlClientFE.prepare(sql, credentialCallOption);
            FlightInfo info = preparedStatement.execute(credentialCallOption);
            List<FlightEndpoint> endpoints = info.getEndpoints();

            Set<String> seenPairs = new HashSet<>();
            for (FlightEndpoint endpoint : endpoints) {
                byte[] ticket = endpoint.getTicket().getBytes();
                String ticketBase64 = Base64.getEncoder().encodeToString(ticket);

                for (Location location : endpoint.getLocations()) {
                    String uri = location.getUri().toString();
                    String compositeKey = ticketBase64 + "|" + uri;

                    if (seenPairs.add(compositeKey)) {
                        uniquePairs.add(Pair.of(uri, ticket));
                    }
                }
            }
        } finally {
            if (sqlClientFE != null) {
                sqlClientFE.close();
            }
            if (clientFE != null) {
                clientFE.close();
            }
        }

        return uniquePairs;
    }

    private void createAdbcColumns() {
        columns.clear();
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }
            Column col = slot.getColumn();
            columns.add("`" + col.getName() + "`");
        }
        if (columns.isEmpty()) {
            columns.add("*");
        }
    }

    private String getAdbcQueryStr() {
        StringBuilder sql = new StringBuilder("SELECT ");

        if (source.getCatalog().enableParallelResultSink()) {
            sql.append("/*+ SET_VAR(enable_parallel_result_sink=true) */ ");
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

    private void createAdbcFilters() {
        if (conjuncts.isEmpty()) {
            return;
        }

        List<SlotRef> slotRefs = Lists.newArrayList();
        Expr.collectList(conjuncts, SlotRef.class, slotRefs);
        ExprSubstitutionMap sMap = new ExprSubstitutionMap();
        for (SlotRef slotRef : slotRefs) {
            SlotRef slotRef1 = (SlotRef) slotRef.clone();
            slotRef1.setTblName(null);
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
        if (expr.contains(BoolLiteral.class) && "1".equals(expr.getStringValue()) && expr.getChildren().isEmpty()) {
            return "1 = 1";
        }

        return expr.toExternalSql(TableIf.TableType.DORIS_EXTERNAL_TABLE, tbl);
    }
}
