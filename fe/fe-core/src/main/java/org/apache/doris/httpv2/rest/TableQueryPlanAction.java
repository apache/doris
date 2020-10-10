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

package org.apache.doris.httpv2.rest;

import io.netty.handler.codec.http.HttpResponseStatus;

import org.apache.doris.analysis.InlineViewRef;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DorisHttpException;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.util.HttpUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TMemoryScratchSink;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TPlanFragment;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TQueryPlanInfo;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TTabletVersionInfo;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This class responsible for parse the sql and generate the query plan fragment for a (only one) table{@see OlapTable}
 * the related tablet maybe pruned by query planer according the `where` predicate.
 */
@RestController
public class TableQueryPlanAction extends RestBaseController {
    public static final Logger LOG = LogManager.getLogger(TableQueryPlanAction.class);

    @RequestMapping(path = "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_query_plan", method = {RequestMethod.GET, RequestMethod.POST})
    public Object query_plan(
            @PathVariable(value = DB_KEY) final String dbName,
            @PathVariable(value = TABLE_KEY) final String tblName,
            HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        // just allocate 2 slot for top holder map
        Map<String, Object> resultMap = new HashMap<>(4);

        String postContent = HttpUtil.getBody(request);
        try {
            // may be these common validate logic should be moved to one base class
            String sql;
            if (Strings.isNullOrEmpty(postContent)) {
                return ResponseEntityBuilder.badRequest("POST body must contains [sql] root object");
            }
            JSONObject jsonObject;
            try {
                jsonObject = new JSONObject(postContent);
            } catch (JSONException e) {
                return ResponseEntityBuilder.badRequest("malformed json: " + e.getMessage());
            }

            sql = String.valueOf(jsonObject.opt("sql"));
            if (Strings.isNullOrEmpty(sql)) {
                return ResponseEntityBuilder.badRequest("POST body must contains [sql] root object");
            }
            LOG.info("receive SQL statement [{}] from external service [ user [{}]] for database [{}] table [{}]",
                    sql, ConnectContext.get().getCurrentUserIdentity(), dbName, tblName);

            String fullDbName = getFullDbName(dbName);
            // check privilege for select, otherwise return HTTP 401
            checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tblName, PrivPredicate.SELECT);
            Database db = Catalog.getCurrentCatalog().getDb(fullDbName);
            if (db == null) {
                return ResponseEntityBuilder.okWithCommonError("Database [" + dbName + "] " + "does not exists");
            }
            // may be should acquire writeLock
            db.readLock();
            try {
                Table table = db.getTable(tblName);
                if (table == null) {
                    return ResponseEntityBuilder.okWithCommonError("Table [" + tblName + "] " + "does not exists");
                }
                // just only support OlapTable, ignore others such as ESTable
                if (table.getType() != Table.TableType.OLAP) {
                    return ResponseEntityBuilder.okWithCommonError("only support OlapTable currently, "
                            + "but Table [" + tblName + "] " + "is not a OlapTable");
                }
                // parse/analysis/plan the sql and acquire tablet distributions
                handleQuery(ConnectContext.get(), fullDbName, tblName, sql, resultMap);
            } finally {
                db.readUnlock();
            }
        } catch (DorisHttpException e) {
            // status code  should conforms to HTTP semantic
            resultMap.put("status", e.getCode().code());
            resultMap.put("exception", e.getMessage());
        }
        return ResponseEntityBuilder.ok(resultMap);
    }


    /**
     * process the sql syntax and return the resolved pruned tablet
     *
     * @param context context for analyzer
     * @param sql     the single table select statement
     * @param result  the acquired results
     * @return
     * @throws DorisHttpException
     */
    private void handleQuery(ConnectContext context, String requestDb, String requestTable, String sql,
                             Map<String, Object> result) throws DorisHttpException {
        // use SE to resolve sql
        StmtExecutor stmtExecutor = new StmtExecutor(context, new OriginStatement(sql, 0), false);
        try {
            TQueryOptions tQueryOptions = context.getSessionVariable().toThrift();
            // Conduct Planner create SingleNodePlan#createPlanFragments
            tQueryOptions.num_nodes = 1;
            // analyze sql
            stmtExecutor.analyze(tQueryOptions);
        } catch (Exception e) {
            throw new DorisHttpException(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        }
        // the parsed logical statement
        StatementBase query = stmtExecutor.getParsedStmt();
        // only process select semantic
        if (!(query instanceof SelectStmt)) {
            throw new DorisHttpException(HttpResponseStatus.BAD_REQUEST, "Select statement needed, but found [" + sql + " ]");
        }
        SelectStmt stmt = (SelectStmt) query;
        // just only process sql like `select * from table where <predicate>`, only support executing scan semantic
        if (stmt.hasAggInfo() || stmt.hasAnalyticInfo()
                || stmt.hasOrderByClause() || stmt.hasOffset() || stmt.hasLimit() || stmt.isExplain()) {
            throw new DorisHttpException(HttpResponseStatus.BAD_REQUEST, "only support single table filter-prune-scan, but found [ " + sql + "]");
        }
        // process only one table by one http query
        List<TableRef> fromTables = stmt.getTableRefs();
        if (fromTables.size() != 1) {
            throw new DorisHttpException(HttpResponseStatus.BAD_REQUEST, "Select statement must have only one table");
        }

        TableRef fromTable = fromTables.get(0);
        if (fromTable instanceof InlineViewRef) {
            throw new DorisHttpException(HttpResponseStatus.BAD_REQUEST, "Select statement must not embed another statement");
        }
        // check consistent http requested resource with sql referenced
        // if consistent in this way, can avoid check privilege
        TableName tableAndDb = fromTables.get(0).getName();
        if (!(tableAndDb.getDb().equals(requestDb) && tableAndDb.getTbl().equals(requestTable))) {
            throw new DorisHttpException(HttpResponseStatus.BAD_REQUEST, "requested database and table must consistent with sql: request [ "
                    + requestDb + "." + requestTable + "]" + "and sql [" + tableAndDb.toString() + "]");
        }

        // acquired Planner to get PlanNode and fragment templates
        Planner planner = stmtExecutor.planner();
        // acquire ScanNode to obtain pruned tablet
        // in this way, just retrieve only one scannode
        List<ScanNode> scanNodes = planner.getScanNodes();
        if (scanNodes.size() != 1) {
            throw new DorisHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Planner should plan just only one ScanNode but found [ " + scanNodes.size() + "]");
        }
        List<TScanRangeLocations> scanRangeLocations = scanNodes.get(0).getScanRangeLocations(0);
        // acquire the PlanFragment which the executable template
        List<PlanFragment> fragments = planner.getFragments();
        if (fragments.size() != 1) {
            throw new DorisHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Planner should plan just only one PlanFragment but found [ " + fragments.size() + "]");
        }

        TQueryPlanInfo tQueryPlanInfo = new TQueryPlanInfo();


        // acquire TPlanFragment
        TPlanFragment tPlanFragment = fragments.get(0).toThrift();
        // set up TMemoryScratchSink
        TDataSink tDataSink = new TDataSink();
        tDataSink.type = TDataSinkType.MEMORY_SCRATCH_SINK;
        tDataSink.memory_scratch_sink = new TMemoryScratchSink();
        tPlanFragment.output_sink = tDataSink;

        tQueryPlanInfo.plan_fragment = tPlanFragment;
        tQueryPlanInfo.desc_tbl = query.getAnalyzer().getDescTbl().toThrift();
        // set query_id
        UUID uuid = UUID.randomUUID();
        tQueryPlanInfo.query_id = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());

        Map<Long, TTabletVersionInfo> tablet_info = new HashMap<>();
        // acquire resolved tablet distribution
        Map<String, Node> tabletRoutings = assemblePrunedPartitions(scanRangeLocations);
        tabletRoutings.forEach((tabletId, node) -> {
            long tablet = Long.parseLong(tabletId);
            tablet_info.put(tablet, new TTabletVersionInfo(tablet, node.version, node.versionHash, node.schemaHash));
        });
        tQueryPlanInfo.tablet_info = tablet_info;

        // serialize TQueryPlanInfo and encode plan with Base64 to string in order to translate by json format
        TSerializer serializer = new TSerializer();
        String opaqued_query_plan;
        try {
            byte[] query_plan_stream = serializer.serialize(tQueryPlanInfo);
            opaqued_query_plan = Base64.getEncoder().encodeToString(query_plan_stream);
        } catch (TException e) {
            throw new DorisHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR, "TSerializer failed to serialize PlanFragment, reason [ " + e.getMessage() + " ]");
        }
        result.put("partitions", tabletRoutings);
        result.put("opaqued_query_plan", opaqued_query_plan);
        result.put("status", 200);
    }

    /**
     * acquire all involved (already pruned) tablet routing
     *
     * @param scanRangeLocationsList
     * @return
     */
    private Map<String, Node> assemblePrunedPartitions(List<TScanRangeLocations> scanRangeLocationsList) {
        Map<String, Node> result = new HashMap<>();
        for (TScanRangeLocations scanRangeLocations : scanRangeLocationsList) {
            // only process palo(doris) scan range
            TPaloScanRange scanRange = scanRangeLocations.scan_range.palo_scan_range;
            Node tabletRouting = new Node(Long.parseLong(scanRange.version),
                    Long.parseLong(scanRange.version_hash), Integer.parseInt(scanRange.schema_hash));
            for (TNetworkAddress address : scanRange.hosts) {
                tabletRouting.addRouting(address.hostname + ":" + address.port);
            }
            result.put(String.valueOf(scanRange.tablet_id), tabletRouting);
        }
        return result;
    }

    // helper class for json transformation
    final class Node {
        // ["host1:port1", "host2:port2", "host3:port3"]
        public List<String> routings = new ArrayList<>();
        public long version;
        public long versionHash;
        public int schemaHash;

        public Node(long version, long versionHash, int schemaHash) {
            this.version = version;
            this.versionHash = versionHash;
            this.schemaHash = schemaHash;
        }

        private void addRouting(String routing) {
            routings.add(routing);
        }
    }
}
