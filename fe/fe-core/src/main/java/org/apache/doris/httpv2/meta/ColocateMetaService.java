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

package org.apache.doris.httpv2.meta;

import org.apache.doris.catalog.ColocateGroupSchema;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.rest.RestBaseController;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import java.lang.reflect.Type;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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

@RestController
public class ColocateMetaService extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(ColocateMetaService.class);
    private static final String GROUP_ID = "group_id";
    private static final String DB_ID = "db_id";

    private static ColocateTableIndex colocateIndex = Env.getCurrentColocateIndex();

    private static GroupId checkAndGetGroupId(HttpServletRequest request) throws DdlException {
        long grpId = Long.valueOf(request.getParameter(GROUP_ID).trim());
        long dbId = Long.valueOf(request.getParameter(DB_ID).trim());
        GroupId groupId = new GroupId(dbId, grpId);

        if (!colocateIndex.isGroupExist(groupId)) {
            throw new DdlException("the group " + groupId + "isn't  exist");
        }
        return groupId;
    }

    public Object executeWithoutPassword(HttpServletRequest request, HttpServletResponse response)
            throws Exception {
        executeCheckPassword(request, response);
        RedirectView redirectView = redirectToMasterOrException(request, response);
        if (redirectView != null) {
            return redirectView;
        }
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
        return null;
    }

    @RequestMapping(path = "/api/colocate", method = RequestMethod.GET)
    public Object colocate(HttpServletRequest request, HttpServletResponse response) throws Exception {
        executeWithoutPassword(request, response);
        return ResponseEntityBuilder.ok(Env.getCurrentColocateIndex());
    }

    @RequestMapping(path = "/api/colocate/group_stable", method = {RequestMethod.POST, RequestMethod.DELETE})
    public Object group_stable(HttpServletRequest request, HttpServletResponse response)
            throws Exception {
        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }

        executeWithoutPassword(request, response);
        GroupId groupId = checkAndGetGroupId(request);

        String method = request.getMethod();
        if ("POST".equalsIgnoreCase(method)) {
            colocateIndex.markGroupUnstable(groupId, "mark unstable via http api", true);
        } else if ("DELETE".equalsIgnoreCase(method)) {
            colocateIndex.markGroupStable(groupId, true);
        }
        return ResponseEntityBuilder.ok();
    }

    @RequestMapping(path = "/api/colocate/bucketseq", method = RequestMethod.POST)
    public Object bucketseq(HttpServletRequest request, HttpServletResponse response, @RequestBody String meta)
            throws Exception {
        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }

        executeWithoutPassword(request, response);
        GroupId groupId = checkAndGetGroupId(request);

        Type type = new TypeToken<List<List<Long>>>() {
        }.getType();
        List<List<Long>> backendsPerBucketSeq = new Gson().fromJson(meta, type);
        LOG.info("get buckets sequence: {}", backendsPerBucketSeq);

        ColocateGroupSchema groupSchema = Env.getCurrentColocateIndex().getGroupSchema(groupId);
        if (backendsPerBucketSeq.size() != groupSchema.getBucketsNum()) {
            return ResponseEntityBuilder.okWithCommonError("Invalid bucket num. expected: "
                    + groupSchema.getBucketsNum() + ", actual: " + backendsPerBucketSeq.size());
        }

        List<Long> clusterBackendIds = Env.getCurrentSystemInfo().getAllBackendIds(true);
        //check the Backend id
        for (List<Long> backendIds : backendsPerBucketSeq) {
            if (backendIds.size() != groupSchema.getReplicaAlloc().getTotalReplicaNum()) {
                return ResponseEntityBuilder.okWithCommonError("Invalid backend num per bucket. expected: "
                        + groupSchema.getReplicaAlloc().getTotalReplicaNum() + ", actual: " + backendIds.size());
            }
            for (Long beId : backendIds) {
                if (!clusterBackendIds.contains(beId)) {
                    return ResponseEntityBuilder.okWithCommonError("The backend " + beId
                            + " does not exist or not available");
                }
            }
        }

        int bucketsNum = colocateIndex.getBackendsPerBucketSeq(groupId).size();
        Preconditions.checkState(backendsPerBucketSeq.size() == bucketsNum,
                backendsPerBucketSeq.size() + " vs. " + bucketsNum);
        updateBackendPerBucketSeq(groupId, backendsPerBucketSeq);
        LOG.info("the group {} backendsPerBucketSeq meta has been changed to {}", groupId, backendsPerBucketSeq);
        return ResponseEntityBuilder.ok();
    }

    private void updateBackendPerBucketSeq(GroupId groupId, List<List<Long>> backendsPerBucketSeq)
            throws Exception {
        throw new Exception("Currently not support");
    }
}
