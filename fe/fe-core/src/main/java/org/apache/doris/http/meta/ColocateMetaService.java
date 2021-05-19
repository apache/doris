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

package org.apache.doris.http.meta;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.ColocateGroupSchema;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
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
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Type;
import java.util.List;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

/*
 * the colocate meta define in {@link ColocateTableIndex}
 * The actions in ColocateMetaService is for modifying or showing colocate group info manually.
 * 
 * ColocateMetaAction:
 *  get all information in ColocateTableIndex, as a json string
 *      eg:
 *          GET /api/colocate
 *      return:
 *          {"colocate_meta":{"groupName2Id":{...},"group2Tables":{}, ...},"status":"OK"}
 * 
 *      eg:
 *          POST    /api/colocate/group_stable?db_id=123&group_id=456  (mark group[123.456] as unstable)
 *          DELETE  /api/colocate/group_stable?db_id=123&group_id=456  (mark group[123.456] as stable)
 *          
 * BucketSeqAction:
 *  change the backends per bucket sequence of a group
 *      eg:
 *          POST    /api/colocate/bucketseq?db_id=123&group_id=456
 */
public class ColocateMetaService {
    private static final Logger LOG = LogManager.getLogger(ColocateMetaService.class);
    private static final String GROUP_ID = "group_id";
    private static final String TABLE_ID = "table_id";
    private static final String DB_ID = "db_id";

    private static ColocateTableIndex colocateIndex = Catalog.getCurrentColocateIndex();

    private static GroupId checkAndGetGroupId(BaseRequest request) throws DdlException {
        long grpId = Long.valueOf(request.getSingleParameter(GROUP_ID).trim());
        long dbId = Long.valueOf(request.getSingleParameter(DB_ID).trim());
        GroupId groupId = new GroupId(dbId, grpId);

        if (!colocateIndex.isGroupExist(groupId)) {
            throw new DdlException("the group " + groupId + "isn't  exist");
        }
        return groupId;
    }

    public static class ColocateMetaBaseAction extends RestBaseAction {
        ColocateMetaBaseAction(ActionController controller) {
            super(controller);
        }

        @Override
        public void executeWithoutPassword(BaseRequest request, BaseResponse response)
                throws DdlException {
            if (redirectToMaster(request, response)) {
                return;
            }
            checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
            executeInMasterWithAdmin(request, response);
        }

        // implement in derived classes
        protected void executeInMasterWithAdmin(BaseRequest request, BaseResponse response)
                throws DdlException {
            throw new DdlException("Not implemented");
        }
    }

    // get all colocate meta
    public static class ColocateMetaAction extends ColocateMetaBaseAction {
        ColocateMetaAction(ActionController controller) {
            super(controller);
        }

        public static void registerAction(ActionController controller) throws IllegalArgException {
            ColocateMetaAction action = new ColocateMetaAction(controller);
            controller.registerHandler(HttpMethod.GET, "/api/colocate", action);
        }

        @Override
        public void executeInMasterWithAdmin(BaseRequest request, BaseResponse response)
                throws DdlException {
            response.setContentType("application/json");
            RestResult result = new RestResult();
            result.addResultEntry("colocate_meta", Catalog.getCurrentColocateIndex());
            sendResult(request, response, result);
        }
    }

    // mark a colocate group as stable or unstable
    public static class MarkGroupStableAction extends ColocateMetaBaseAction {
        MarkGroupStableAction(ActionController controller) {
            super(controller);
        }

        public static void registerAction(ActionController controller) throws IllegalArgException {
            MarkGroupStableAction action = new MarkGroupStableAction(controller);
            controller.registerHandler(HttpMethod.POST, "/api/colocate/group_stable", action);
            controller.registerHandler(HttpMethod.DELETE, "/api/colocate/group_stable", action);
        }

        @Override
        public void executeInMasterWithAdmin(BaseRequest request, BaseResponse response)
                throws DdlException {
            GroupId groupId = checkAndGetGroupId(request);

            HttpMethod method = request.getRequest().method();
            if (method.equals(HttpMethod.POST)) {
                colocateIndex.markGroupUnstable(groupId, true);
            } else if (method.equals(HttpMethod.DELETE)) {
                colocateIndex.markGroupStable(groupId, true);
            } else {
                response.appendContent(new RestBaseResult("HTTP method is not allowed.").toJson());
                writeResponse(request, response, HttpResponseStatus.METHOD_NOT_ALLOWED);
            }

            sendResult(request, response);
        }
    }

    // update a backendsPerBucketSeq meta for a colocate group
    public static class BucketSeqAction extends ColocateMetaBaseAction {
        private static final Logger LOG = LogManager.getLogger(BucketSeqAction.class);

        BucketSeqAction(ActionController controller) {
            super(controller);
        }

        public static void registerAction(ActionController controller) throws IllegalArgException {
            BucketSeqAction action = new BucketSeqAction(controller);
            controller.registerHandler(HttpMethod.POST, "/api/colocate/bucketseq", action);
        }

        @Override
        public void executeInMasterWithAdmin(BaseRequest request, BaseResponse response)
                throws DdlException {
            final String clusterName = ConnectContext.get().getClusterName();
            if (Strings.isNullOrEmpty(clusterName)) {
                throw new DdlException("No cluster selected.");
            }
            GroupId groupId = checkAndGetGroupId(request);

            String meta = request.getContent();
            Type type = new TypeToken<List<List<Long>>>() {}.getType();
            List<List<Long>> backendsPerBucketSeq = new Gson().fromJson(meta, type);
            LOG.info("get buckets sequence: {}", backendsPerBucketSeq);

            ColocateGroupSchema groupSchema = Catalog.getCurrentColocateIndex().getGroupSchema(groupId);
            if (backendsPerBucketSeq.size() != groupSchema.getBucketsNum()) {
                throw new DdlException("Invalid bucket num. expected: " + groupSchema.getBucketsNum() + ", actual: "
                        + backendsPerBucketSeq.size());
            }

            List<Long> clusterBackendIds = Catalog.getCurrentSystemInfo().getClusterBackendIds(clusterName, true);
            //check the Backend id
            for (List<Long> backendIds : backendsPerBucketSeq) {
                if (backendIds.size() != groupSchema.getReplicaAlloc().getTotalReplicaNum()) {
                    throw new DdlException("Invalid backend num per bucket. expected: "
                            + groupSchema.getReplicaAlloc().getTotalReplicaNum() + ", actual: " + backendIds.size());
                }
                for (Long beId : backendIds) {
                    if (!clusterBackendIds.contains(beId)) {
                        throw new DdlException("The backend " + beId + " does not exist or not available");
                    }
                }
            }

            int bucketsNum = colocateIndex.getBackendsPerBucketSeq(groupId).size();
            Preconditions.checkState(backendsPerBucketSeq.size() == bucketsNum,
                    backendsPerBucketSeq.size() + " vs. " + bucketsNum);
            updateBackendPerBucketSeq(groupId, backendsPerBucketSeq);
            LOG.info("the group {} backendsPerBucketSeq meta has been changed to {}", groupId, backendsPerBucketSeq);

            sendResult(request, response);
        }

        private void updateBackendPerBucketSeq(GroupId groupId, List<List<Long>> backendsPerBucketSeq) {
            colocateIndex.addBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
            ColocatePersistInfo info2 = ColocatePersistInfo.createForBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
            Catalog.getCurrentCatalog().getEditLog().logColocateBackendsPerBucketSeq(info2);
        }
    }

}
