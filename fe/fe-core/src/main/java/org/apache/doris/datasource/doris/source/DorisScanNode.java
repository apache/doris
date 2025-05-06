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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DorisTable;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalScanNode;
import org.apache.doris.datasource.FederationBackendPolicy;
import org.apache.doris.datasource.doris.DorisExternalTable;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.statistics.query.StatsDelta;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TDorisAdbcScanNode;
import org.apache.doris.thrift.TDorisArrowScanRange;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class DorisScanNode extends ExternalScanNode {
    private static final Logger LOG = LogManager.getLogger(DorisScanNode.class);

    private final List<String> columns = new ArrayList<String>();
    private final List<String> filters = new ArrayList<String>();
    private String tableName;
    private boolean adbcExecComplete = false;

    private final DorisTable table;

    private final List<Pair<String, Integer>> hostsAndArrowPort;
    private int currentHostIndex = 0;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    private List<Backend> backends;
    private int currentBeIndex = 0;

    private List<Pair<String, byte[]>> flightInfoResult;



    public DorisScanNode(PlanNodeId id, TupleDescriptor desc) {
        this(id, desc, false);
    }

    public DorisScanNode(PlanNodeId id, TupleDescriptor desc, boolean isDorisExternalTable) {
        super(id, desc, "DorisScanNode", StatisticalType.DORIS_SCAN_NODE, false);
        if (isDorisExternalTable) {
            DorisExternalTable externalTable = (DorisExternalTable) (desc.getTable());
            table = externalTable.getDorisTable();
        } else {
            table = (DorisTable) (desc.getTable());
        }
        tableName = table.getExternalTableName();
        hostsAndArrowPort = parseArrowNodes(table.getFeArrowNodes());
        currentHostIndex = random.nextInt(hostsAndArrowPort.size());
    }

    private List<Pair<String, Integer>> parseArrowNodes(List<String> feArrowNodes) {
        if (feArrowNodes == null || feArrowNodes.isEmpty()) {
            throw new RuntimeException("fe arrow nodes not set");
        }

        List<Pair<String, Integer>> hostsAndArrowPort = new ArrayList<>();
        for (String feArrowNode : feArrowNodes) {
            String[] split = feArrowNode.split(":");
            if (split.length != 2) {
                throw new RuntimeException("fe arrow nodes format error, must ip:arrow_port,ip:arrow_port..");
            }
            hostsAndArrowPort.add(Pair.of(split[0].trim(), Integer.parseInt(split[1].trim())));
        }

        return hostsAndArrowPort;
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
            String filter = conjunctExprToString(expr, table);
            filters.add(filter);
        }
    }

    public static String conjunctExprToString(Expr expr, TableIf tbl) {
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

        sql.append(Joiner.on(", ").join(columns));

        sql.append(" FROM ").append(tableName);

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

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(tableName).append("\n");
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
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
    }

    /**
     * Used for Nereids. Should NOT use this function in anywhere else.
     */
    @Override
    public void init() throws UserException {
        super.init();
    }

    // Send an adbc request and receive the response result from adbc
    private void executeAdbcQuery() {
        createAdbcColumns();
        createAdbcFilters();

        String queryStr = getAdbcQueryStr();
        int retrySize = hostsAndArrowPort.size() * 3;
        RuntimeException scratchExceptionForThrow = null;

        for (int i = 0; i < retrySize; i++) {
            Pair<String, Integer> nextHostAndPort = nextHostAndPort();
            if (nextHostAndPort == null) {
                continue;
            }
            try {
                this.flightInfoResult =
                    getFlightInfoResult(nextHostAndPort, table.getUserName(), table.getPassword(), queryStr);
                this.adbcExecComplete = true;
                return;
            } catch (Exception e) {
                LOG.warn("arrow request node [{}] failures {}, try next nodes", nextHostAndPort.first, e);
                scratchExceptionForThrow = new RuntimeException(e.getMessage());
            }
        }

        LOG.warn("try all arrow nodes [{}], no other nodes left", this.hostsAndArrowPort);
        if (scratchExceptionForThrow != null) {
            throw scratchExceptionForThrow;
        }
    }

    private Pair<String, Integer> nextHostAndPort() {
        Pair<String, Integer> hostAndArrowPortPair = this.hostsAndArrowPort.get(currentHostIndex);
        currentHostIndex++;
        currentHostIndex = currentHostIndex % hostsAndArrowPort.size();
        return hostAndArrowPortPair;
    }

    private List<Pair<String, byte[]>> getFlightInfoResult(Pair<String, Integer> hostAndPort,
                                                           String user, String psw, String sql) throws Exception {
        List<Pair<String, byte[]>> list = new ArrayList<>();
        FlightClient clientFE = null;
        FlightSqlClient sqlClientFE = null;
        try {
            BufferAllocator allocatorFE = new RootAllocator(Integer.MAX_VALUE);
            final Location clientLocationFE = new Location(
                    new URI("grpc", null, hostAndPort.first, hostAndPort.second, null, null, null)
            );

            clientFE = FlightClient.builder(allocatorFE, clientLocationFE).build();
            sqlClientFE = new FlightSqlClient(clientFE);

            CredentialCallOption credentialCallOption = clientFE.authenticateBasicToken(user, psw).get();
            FlightSqlClient.PreparedStatement preparedStatement = sqlClientFE.prepare(sql, credentialCallOption);
            FlightInfo info = preparedStatement.execute(credentialCallOption);
            List<FlightEndpoint> endpoints = info.getEndpoints();


            for (FlightEndpoint endpoint : endpoints) {
                Ticket ticket = endpoint.getTicket();
                List<Location> locations = endpoint.getLocations();
                for (Location location : locations) {
                    Pair<String, byte[]> pair = Pair.of(location.getUri().toString(), ticket.getBytes());
                    list.add(pair);
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

        return list;
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        if (!adbcExecComplete) {
            executeAdbcQuery();
        }
        createScanRangeLocations();
    }

    @Override
    public void finalizeForNereids() throws UserException {
        if (!adbcExecComplete) {
            executeAdbcQuery();
        }
        createScanRangeLocations();
    }


    @Override
    protected void createScanRangeLocations() throws UserException {
        if (!adbcExecComplete) {
            executeAdbcQuery();
        }
        scanRangeLocations = getShardLocations();
    }

    private List<TScanRangeLocations> getShardLocations() throws UserException {
        if (flightInfoResult == null || flightInfoResult.isEmpty()) {
            throw new UserException("arrow flight info result is null, Try it later");
        }

        List<TScanRangeLocations> result = Lists.newArrayList();

        FederationBackendPolicy backendPolicy = new FederationBackendPolicy();
        backendPolicy.init();
        Collection<Backend> backends = backendPolicy.getBackends();

        int numBackends = table.getMaxExecBeNum();
        numBackends = numBackends < 1 ? backends.size() : Math.min(numBackends, backends.size());

        initExecBackends(backends, numBackends);

        for (Pair<String, byte[]> pair : flightInfoResult) {
            TScanRangeLocations locations = new TScanRangeLocations();

            TScanRangeLocation location = new TScanRangeLocation();
            Backend be = getNextBe();
            location.setBackendId(be.getId());
            location.setServer(new TNetworkAddress(be.getHost(), be.getBePort()));
            locations.addToLocations(location);

            TDorisArrowScanRange dorisArrowScanRange = new TDorisArrowScanRange();
            dorisArrowScanRange.setUriStr(pair.first);
            dorisArrowScanRange.setTicket(pair.second);

            TScanRange scanRange = new TScanRange();
            scanRange.setDorisArrowScanRange(dorisArrowScanRange);
            locations.setScanRange(scanRange);

            // result
            result.add(locations);
        }

        return result;
    }

    private Backend getNextBe() {
        Backend backend = backends.get(currentBeIndex);
        currentBeIndex++;
        currentBeIndex = currentBeIndex % backends.size();
        return backend;
    }

    private void initExecBackends(Collection<Backend> backends, int numBackends) {
        List<Backend> list = new ArrayList<>(backends);
        if (backends.size() == numBackends) {
            this.backends = list;
        } else {
            Collections.shuffle(list);
            this.backends = new ArrayList<>(list.subList(0, numBackends));
        }
        currentBeIndex = random.nextInt(backends.size());
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.DORIS_ADBC_SCAN_NODE;
        msg.doris_adbc_scan_node = new TDorisAdbcScanNode();
        msg.doris_adbc_scan_node.setTupleId(desc.getId().asInt());
        msg.doris_adbc_scan_node.setTableName(tableName);
        msg.doris_adbc_scan_node.setUser(table.getUserName());
        msg.doris_adbc_scan_node.setPasswd(table.getPassword());
        msg.doris_adbc_scan_node.setFeArrowNodes(hostsAndArrowPort.stream()
                .collect(Collectors.toMap(
                    pair -> pair.first,
                    pair -> pair.second.toString()
                )));
    }

    @Override
    public StatsDelta genStatsDelta() throws AnalysisException {
        return new StatsDelta(Env.getCurrentEnv().getCurrentCatalog().getId(),
            Env.getCurrentEnv().getCurrentCatalog().getDbOrAnalysisException(table.getQualifiedDbName()).getId(),
            table.getId(), -1L);
    }
}
