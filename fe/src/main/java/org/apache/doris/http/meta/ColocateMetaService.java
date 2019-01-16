package org.apache.doris.http.meta;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.DdlException;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;
import org.apache.doris.http.rest.RestBaseAction;
import org.apache.doris.http.rest.RestBaseResult;
import org.apache.doris.http.rest.RestResult;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.ColocatePersistInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.lang.reflect.Type;

/*
 * the colocate meta define in {@link ColocateTableIndex}
 */
public class ColocateMetaService {
    private static final Logger LOG = LogManager.getLogger(ColocateMetaService.class);
    private static final String GROUP_ID = "group_id";
    private static final String TABLE_ID = "table_id";
    private static final String DB_ID = "db_id";

    private static ColocateTableIndex colocateIndex = Catalog.getCurrentColocateIndex();

    private static long checkAndGetGroupId(BaseRequest request) throws DdlException {
        long groupId = Long.valueOf(request.getSingleParameter(GROUP_ID).trim());
        LOG.info("groupId is {}", groupId);
        if (!colocateIndex.isGroupExist(groupId)) {
            throw new DdlException("the group " + groupId + "isn't  exist");
        }
        return groupId;
    }

    private static long getTableId(BaseRequest request) throws DdlException {
        long tableId = Long.valueOf(request.getSingleParameter(TABLE_ID).trim());
        LOG.info("tableId is {}", tableId);
        return tableId;
    }

    private static long getDbId(BaseRequest request) throws DdlException {
        long dbId = Long.valueOf(request.getSingleParameter(DB_ID).trim());
        LOG.info("dbId is {}", dbId);
        return dbId;
    }

    //get all colocate meta
    public static class ColocateMetaAction extends RestBaseAction {
        ColocateMetaAction(ActionController controller) {
            super(controller);
        }

        public static void registerAction(ActionController controller) throws IllegalArgException {
            ColocateMetaAction action = new ColocateMetaAction(controller);
            controller.registerHandler(HttpMethod.GET, "/api/colocate", action);
        }

        @Override
        public void executeWithoutPassword(AuthorizationInfo authInfo, BaseRequest request, BaseResponse response)
                throws DdlException {
            checkGlobalAuth(authInfo, PrivPredicate.ADMIN);

            RestResult result = new RestResult();
            result.addResultEntry("colocate_meta", Catalog.getCurrentColocateIndex());
            sendResult(request, response, result);
        }
    }

    //add a table to a colocate group
    public static class TableGroupAction extends RestBaseAction {
        TableGroupAction(ActionController controller) {
            super(controller);
        }

        public static void registerAction(ActionController controller) throws IllegalArgException {
            TableGroupAction action = new TableGroupAction(controller);
            controller.registerHandler(HttpMethod.POST, "/api/colocate/table_group", action);
        }

        @Override
        public void executeWithoutPassword(AuthorizationInfo authInfo, BaseRequest request, BaseResponse response)
                throws DdlException {
            checkGlobalAuth(authInfo, PrivPredicate.ADMIN);

            long groupId = checkAndGetGroupId(request);
            long tableId = getTableId(request);
            long dbId = getDbId(request);

            Database database = Catalog.getInstance().getDb(dbId);
            if (database == null) {
                throw new DdlException("the db " + dbId + " isn't  exist");
            }
            if (database.getTable(tableId) == null) {
                throw new DdlException("the table " + tableId + " isn't  exist");
            }
            if (database.getTable(groupId) == null) {
                throw new DdlException("the parent table " + groupId + " isn't  exist");
            }

            LOG.info("will add table {} to group {}", tableId, groupId);
            colocateIndex.addTableToGroup(dbId, tableId, groupId);
            ColocatePersistInfo info = ColocatePersistInfo.CreateForAddTable(tableId, groupId, dbId, new ArrayList<>());
            Catalog.getInstance().getEditLog().logColocateAddTable(info);
            LOG.info("table {} has added to group {}", tableId, groupId);

            sendResult(request, response);
        }
    }

    //remove a table from a colocate group
    public static class TableAction extends RestBaseAction {
        TableAction(ActionController controller) {
            super(controller);
        }

        public static void registerAction(ActionController controller) throws IllegalArgException {
            TableAction action = new TableAction(controller);
            controller.registerHandler(HttpMethod.DELETE, "/api/colocate/table", action);
        }

        @Override
        public void executeWithoutPassword(AuthorizationInfo authInfo, BaseRequest request, BaseResponse response)
                throws DdlException {
            checkGlobalAuth(authInfo, PrivPredicate.ADMIN);
            long tableId = getTableId(request);

            LOG.info("will delete table {} from colocate meta", tableId);
            Catalog.getCurrentColocateIndex().removeTable(tableId);
            ColocatePersistInfo colocateInfo = ColocatePersistInfo.CreateForRemoveTable(tableId);
            Catalog.getInstance().getEditLog().logColocateRemoveTable(colocateInfo);
            LOG.info("table {}  has deleted from colocate meta", tableId);

            sendResult(request, response);
        }
    }

    //mark a colocate group to balancing or stable
    public static class BalancingGroupAction extends RestBaseAction {
        BalancingGroupAction(ActionController controller) {
            super(controller);
        }

        public static void registerAction(ActionController controller) throws IllegalArgException {
            BalancingGroupAction action = new BalancingGroupAction(controller);
            controller.registerHandler(HttpMethod.POST, "/api/colocate/balancing_group", action);
            controller.registerHandler(HttpMethod.DELETE, "/api/colocate/balancing_group", action);
        }

        @Override
        public void executeWithoutPassword(AuthorizationInfo authInfo, BaseRequest request, BaseResponse response)
                throws DdlException {
            checkGlobalAuth(authInfo, PrivPredicate.ADMIN);

            long groupId = checkAndGetGroupId(request);

            HttpMethod method = request.getRequest().method();
            if (method.equals(HttpMethod.POST)) {
                colocateIndex.markGroupBalancing(groupId);
                ColocatePersistInfo info = ColocatePersistInfo.CreateForMarkBalancing(groupId);
                Catalog.getInstance().getEditLog().logColocateMarkBalancing(info);
                LOG.info("mark colocate group {}  balancing", groupId);
            } else if (method.equals(HttpMethod.DELETE)) {
                colocateIndex.markGroupStable(groupId);
                ColocatePersistInfo info = ColocatePersistInfo.CreateForMarkStable(groupId);
                Catalog.getInstance().getEditLog().logColocateMarkStable(info);
                LOG.info("mark colocate group {}  stable", groupId);
            } else {
                response.appendContent(new RestBaseResult("HTTP method is not allowed.").toJson());
                writeResponse(request, response, HttpResponseStatus.METHOD_NOT_ALLOWED);
            }

            sendResult(request, response);
        }
    }

    //update a backendsPerBucketSeq meta for a colocate group
    public static class BucketSeqAction extends RestBaseAction {
        private static final Logger LOG = LogManager.getLogger(BucketSeqAction.class);

        BucketSeqAction(ActionController controller) {
            super(controller);
        }

        public static void registerAction(ActionController controller) throws IllegalArgException {
            BucketSeqAction action = new BucketSeqAction(controller);
            controller.registerHandler(HttpMethod.POST, "/api/colocate/bucketseq", action);
        }

        @Override
        public void executeWithoutPassword(AuthorizationInfo authInfo, BaseRequest request, BaseResponse response)
                throws DdlException {
            checkGlobalAuth(authInfo, PrivPredicate.ADMIN);

            final String clusterName = authInfo.cluster;
            if (Strings.isNullOrEmpty(clusterName)) {
                throw new DdlException("No cluster selected.");
            }
            long groupId = checkAndGetGroupId(request);

            String meta = request.getContent();
            Type type = new TypeToken<List<List<Long>>>() {}.getType();
            List<List<Long>> backendsPerBucketSeq = new Gson().fromJson(meta, type);
            LOG.info("HttpServer {}", backendsPerBucketSeq);

            List<Long> clusterBackendIds = Catalog.getCurrentSystemInfo().getClusterBackendIds(clusterName, true);
            //check the Backend id
            for (List<Long> backendIds : backendsPerBucketSeq) {
                for (Long beId : backendIds) {
                    if (!clusterBackendIds.contains(beId)) {
                        throw new DdlException("The backend " + beId + " is not exist or alive");
                    }
                }
            }

            int metaSize = colocateIndex.getBackendsPerBucketSeq(groupId).size();
            Preconditions.checkState(backendsPerBucketSeq.size() == metaSize,
                    backendsPerBucketSeq.size() + " vs. " + metaSize);
            updateBackendPerBucketSeq(groupId, backendsPerBucketSeq);
            LOG.info("the group {} backendsPerBucketSeq meta has updated", groupId);

            sendResult(request, response);
        }

        private void updateBackendPerBucketSeq(Long groupId, List<List<Long>> backendsPerBucketSeq) {
            colocateIndex.markGroupBalancing(groupId);
            ColocatePersistInfo info1 = ColocatePersistInfo.CreateForMarkBalancing(groupId);
            Catalog.getInstance().getEditLog().logColocateMarkBalancing(info1);

            colocateIndex.addBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
            ColocatePersistInfo info2 = ColocatePersistInfo.CreateForBackendsPerBucketSeq(groupId,
                    backendsPerBucketSeq);
            Catalog.getInstance().getEditLog().logColocateBackendsPerBucketSeq(info2);
        }
    }

}
