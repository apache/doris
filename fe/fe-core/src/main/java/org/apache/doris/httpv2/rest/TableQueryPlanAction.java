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

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.DorisHttpException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.rest.manager.HttpUtils;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TMemoryScratchSink;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TPlanFragment;
import org.apache.doris.thrift.TQueryPlanInfo;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TTabletVersionInfo;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Strings;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
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

    @RequestMapping(path = "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_query_plan",
            method = {RequestMethod.GET, RequestMethod.POST})
    public Object query_plan(
            @PathVariable(value = DB_KEY) final String dbName,
            @PathVariable(value = TABLE_KEY) final String tblName,
            HttpServletRequest request, HttpServletResponse response) {
        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }

        executeCheckPassword(request, response);
        // just allocate 2 slot for top holder map
        Map<String, Object> resultMap = new HashMap<>(4);

        try {
            String postContent = HttpUtils.getBody(request);
            // may be these common validate logic should be moved to one base class
            if (Strings.isNullOrEmpty(postContent)) {
                return ResponseEntityBuilder.badRequest("POST body must contains [sql] root object");
            }
            JSONObject jsonObject = (JSONObject) JSONValue.parse(postContent);
            if (jsonObject == null) {
                return ResponseEntityBuilder.badRequest("malformed json: " + postContent);
            }

            String sql = (String) jsonObject.get("sql");
            if (Strings.isNullOrEmpty(sql)) {
                return ResponseEntityBuilder.badRequest("POST body must contains [sql] root object");
            }
            LOG.info("receive SQL statement [{}] from external service [ user [{}]] for database [{}] table [{}]",
                    sql, ConnectContext.get().getCurrentUserIdentity(), dbName, tblName);

            String fullDbName = getFullDbName(dbName);
            // check privilege for select, otherwise return HTTP 401
            checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tblName, PrivPredicate.SELECT);
            Table table;
            try {
                Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
                table = db.getTableOrMetaException(tblName, TableIf.TableType.OLAP);
            } catch (MetaNotFoundException e) {
                return ResponseEntityBuilder.okWithCommonError(e.getMessage());
            }
            table.readLock();
            try {
                if (ConnectContext.get() != null
                        && ConnectContext.get().getSessionVariable() != null) {
                    // Disable some optimizations, since it's not fully supported
                    // TODO support it
                    ConnectContext.get().getSessionVariable().setEnableTwoPhaseReadOpt(false);
                }
                if (Config.isCloudMode()) { // Choose a cluster to for this query
                    ConnectContext.get().getCurrentCloudCluster();
                }
                // parse/analysis/plan the sql and acquire tablet distributions
                handleQuery(ConnectContext.get(), fullDbName, tblName, sql, resultMap);
            } finally {
                table.readUnlock();
            }
        } catch (DorisHttpException e) {
            // status code  should conforms to HTTP semantic
            resultMap.put("status", e.getCode().code());
            resultMap.put("exception", e.getMessage());
        } catch (Exception e) {
            resultMap.put("status", "1");
            resultMap.put("exception", e.getMessage());
        }
        return ResponseEntityBuilder.ok(resultMap);
    }


    /**
     * process the sql syntax and return the resolved pruned tablet
     *
     * @param context context for analyzer
     * @param sql the single table select statement
     * @param result the acquired results
     * @return
     * @throws DorisHttpException
     */
    private void handleQuery(ConnectContext context, String requestDb, String requestTable, String sql,
            Map<String, Object> result) throws DorisHttpException {
        List<StatementBase> stmts = null;
        SessionVariable sessionVariable = context.getSessionVariable();
        boolean needSetParallelResultSinkToFalse = false;
        try {
            try {
                if (!sessionVariable.enableParallelResultSink()) {
                    sessionVariable.setParallelResultSink(true);
                    needSetParallelResultSinkToFalse = true;
                }
                stmts = new NereidsParser().parseSQL(sql, context.getSessionVariable());
            } catch (Exception e) {
                throw new DorisHttpException(HttpResponseStatus.BAD_REQUEST, e.getMessage());
            }
            // the parsed logical statement
            StatementBase query = stmts.get(0);
            if (!(query instanceof LogicalPlanAdapter)) {
                throw new DorisHttpException(HttpResponseStatus.BAD_REQUEST,
                        "Select statement needed, but found [" + sql + " ]");
            }
            LogicalPlan parsedPlan = ((LogicalPlanAdapter) query).getLogicalPlan();
            // only process select semantic
            if (parsedPlan instanceof Command) {
                throw new DorisHttpException(HttpResponseStatus.BAD_REQUEST,
                        "Select statement needed, but found [" + sql + " ]");
            }

            if (!parsedPlan.collectToList(LogicalSubQueryAlias.class::isInstance).isEmpty()) {
                throw new DorisHttpException(HttpResponseStatus.BAD_REQUEST,
                        "Select statement must not embed another statement");
            }

            List<UnboundRelation> unboundRelations = parsedPlan.collectToList(UnboundRelation.class::isInstance);
            if (unboundRelations.size() != 1) {
                throw new DorisHttpException(HttpResponseStatus.BAD_REQUEST,
                        "Select statement must have only one table");
            }

            // check consistent http requested resource with sql referenced
            // if consistent in this way, can avoid check privilege
            List<String> tableQualifier = RelationUtil.getQualifierName(context,
                    unboundRelations.get(0).getNameParts());
            if (tableQualifier.size() != 3) {
                throw new DorisHttpException(HttpResponseStatus.BAD_REQUEST,
                        "can't find table " + String.join(",", tableQualifier));
            }
            String dbName = tableQualifier.get(1);
            String tableName = tableQualifier.get(2);

            if (GlobalVariable.lowerCaseTableNames == 0) {
                if (!(dbName.equals(requestDb) && tableName.equals(requestTable))) {
                    throw new DorisHttpException(HttpResponseStatus.BAD_REQUEST,
                            "requested database and table must consistent with sql: request [ "
                                    + requestDb + "." + requestTable + "]" + "and sql [" + dbName
                                    + "." + tableName + "]");
                }
            } else {
                if (!(dbName.equalsIgnoreCase(requestDb)
                        && tableName.equalsIgnoreCase(requestTable))) {
                    throw new DorisHttpException(HttpResponseStatus.BAD_REQUEST,
                            "requested database and table must consistent with sql: request [ "
                                    + requestDb + "." + requestTable + "]" + "and sql [" + dbName
                                    + "." + tableName + "]");
                }
            }
            NereidsPlanner nereidsPlanner = new NereidsPlanner(context.getStatementContext());
            LogicalPlan rewrittenPlan = (LogicalPlan) nereidsPlanner.planWithLock(parsedPlan,
                    PhysicalProperties.GATHER, ExplainCommand.ExplainLevel.REWRITTEN_PLAN);
            if (!rewrittenPlan.allMatch(planTreeNode -> planTreeNode instanceof LogicalOlapScan
                    || planTreeNode instanceof LogicalFilter || planTreeNode instanceof LogicalProject
                    || planTreeNode instanceof LogicalResultSink)) {
                throw new DorisHttpException(HttpResponseStatus.BAD_REQUEST,
                        "only support single table filter-prune-scan, but found [ " + sql + "]");
            }
            NereidsPlanner planner = new NereidsPlanner(context.getStatementContext());
            try {
                planner.plan(query, context.getSessionVariable().toThrift());
            } catch (Exception ex) {
                throw new DorisHttpException(HttpResponseStatus.BAD_REQUEST,
                        "only support single table filter-prune-scan, but found [ " + sql + "]");
            }

            // acquire ScanNode to obtain pruned tablet
            // in this way, just retrieve only one scannode
            List<ScanNode> scanNodes = planner.getScanNodes();
            if (scanNodes.size() != 1) {
                throw new DorisHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                        "Planner should plan just only one ScanNode but found [ " + scanNodes.size() + "]");
            }
            List<TScanRangeLocations> scanRangeLocations = scanNodes.get(0).getScanRangeLocations(0);
            // acquire the PlanFragment which the executable template
            List<PlanFragment> fragments = planner.getFragments();
            if (fragments.size() != 1) {
                throw new DorisHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                        "Planner should plan just only one PlanFragment but found [ " + fragments.size() + "]");
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
            tQueryPlanInfo.desc_tbl = planner.getDescTable().toThrift();
            // set query_id
            UUID uuid = UUID.randomUUID();
            tQueryPlanInfo.query_id = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());

            Map<Long, TTabletVersionInfo> tabletInfo = new HashMap<>();
            // acquire resolved tablet distribution
            Map<String, Node> tabletRoutings = assemblePrunedPartitions(scanRangeLocations);
            tabletRoutings.forEach((tabletId, node) -> {
                long tablet = Long.parseLong(tabletId);
                tabletInfo.put(tablet, new TTabletVersionInfo(tablet, node.version,
                        0L /*version hash*/, node.schemaHash));
            });
            tQueryPlanInfo.tablet_info = tabletInfo;

            // serialize TQueryPlanInfo and encode plan with Base64 to string in order to translate by json format
            TSerializer serializer;
            String opaquedQueryPlan;
            try {
                serializer = new TSerializer();
                byte[] queryPlanStream = serializer.serialize(tQueryPlanInfo);
                opaquedQueryPlan = Base64.getEncoder().encodeToString(queryPlanStream);
            } catch (TException e) {
                throw new DorisHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                        "TSerializer failed to serialize PlanFragment, reason [ " + e.getMessage() + " ]");
            }
            result.put("partitions", tabletRoutings);
            result.put("opaqued_query_plan", opaquedQueryPlan);
            result.put("status", 200);
        } finally {
            if (needSetParallelResultSinkToFalse) {
                sessionVariable.setParallelResultSink(false);
            }
        }

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
            Node tabletRouting = new Node(Long.parseLong(scanRange.version), 0 /* schema hash is not used */);
            for (TNetworkAddress address : scanRange.hosts) {
                tabletRouting.addRouting(NetUtils
                        .getHostPortInAccessibleFormat(address.hostname, address.port));
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
        public int schemaHash;

        public Node(long version, int schemaHash) {
            this.version = version;
            this.schemaHash = schemaHash;
        }

        private void addRouting(String routing) {
            routings.add(routing);
        }
    }
}
