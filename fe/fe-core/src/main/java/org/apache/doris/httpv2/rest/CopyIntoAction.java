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

import org.apache.doris.analysis.CopyStmt;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB;
import org.apache.doris.cloud.proto.Cloud.StagePB;
import org.apache.doris.cloud.proto.Cloud.StagePB.StageType;
import org.apache.doris.cloud.storage.RemoteBase;
import org.apache.doris.cloud.storage.RemoteBase.ObjectInfo;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.DorisHttpException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.httpv2.rest.manager.HttpUtils;
import org.apache.doris.httpv2.util.ExecutionResultSet;
import org.apache.doris.httpv2.util.StatementSubmitter;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
@RequestMapping(path = "/copy")
public class CopyIntoAction extends RestBaseController {
    static final String pattern =
            "^((2(5[0-5]|[0-4]\\d))|[0-1]?\\d{1,2})(\\.((2(5[0-5]|[0-4]\\d))|[0-1]?\\d{1,2})){3}$";
    static Pattern pat = Pattern.compile(pattern);

    private static final Logger LOG = LogManager.getLogger(CopyIntoAction.class);
    private static StatementSubmitter stmtSubmitter = new StatementSubmitter();

    private final String endpointHeader = "__USE_ENDPOINT__";

    private final String internal = "internal";

    private final String external = "external";

    // for ut
    public static StatementSubmitter getStmtSubmitter() {
        return stmtSubmitter;
    }

    private boolean isIP(String addr) {
        if (Strings.isNullOrEmpty(addr)) {
            return false;
        }
        String addrTrim = addr.trim();
        String[] ep = addrTrim.split(":");
        String ip = ep.length == 2 ? ep[0] : addr;
        int port = 0;
        try {
            port = Integer.parseInt(ep.length == 2 ? ep[1] : "0");

        } catch (NumberFormatException e) {
            return false;
        }
        if (port < 0 || port > 65536) {
            return false;
        }

        if (ip.length() < 7 || ip.length() > 15) {
            return false;
        }
        Matcher mat = pat.matcher(ip);
        return mat.find();
    }

    private static Map<String, String> getHeadersInfo(HttpServletRequest request) {
        Map<String, String> map = new HashMap<>();
        try {
            Enumeration<String> headerNames = request.getHeaderNames();
            if (headerNames == null) {
                return map;
            }
            while (headerNames.hasMoreElements()) {
                String key = headerNames.nextElement();
                String value = request.getHeader(key);
                map.put(key, value);
            }
            return map;
        } catch (Exception ignore) {
            LOG.warn("get request header info failed.");
        }
        return map;
    }

    private boolean internalEndpoint(String host) throws DdlException {
        return !(!Strings.isNullOrEmpty(host) && (isIP(host)));
    }

    // curl  -u user:password -H "fileName: file" -T file -L http://127.0.0.1:12104/copy/upload
    @RequestMapping(path = "/upload", method = RequestMethod.PUT)
    public Object copy(HttpServletRequest request, HttpServletResponse response) {
        MetricRepo.HTTP_COUNTER_COPY_INFO_UPLOAD_REQUEST.increase(1L);
        LOG.info("upload request parameter {} header {}", request.getParameterMap(), getHeadersInfo(request));
        Map<String, Object> resultMap = new HashMap<>(3);
        try {
            long startTime = System.currentTimeMillis();
            executeCheckPassword(request, response);
            String fileName = request.getHeader("fileName");
            if (Strings.isNullOrEmpty(fileName)) {
                return ResponseEntityBuilder.badRequest("http header must have filename entry");
            }
            String eh = request.getHeader(endpointHeader);
            // default use endpoint
            String host = request.getHeader("Host");
            boolean isInternal = Strings.isNullOrEmpty(eh) ? internalEndpoint(host)
                    : eh.equals(internal) || (!eh.equals(external));
            String mysqlUserName = ClusterNamespace
                    .getNameFromFullName(ConnectContext.get().getCurrentUserIdentity().getQualifiedUser());

            String userId = Env.getCurrentEnv().getAuth().getUserId(mysqlUserName);
            LOG.info("receive Presigned url request [ user [{}]] for filename [{}], isInternal [{}], userId [{}]",
                    mysqlUserName, fileName, isInternal, userId);

            // use userName, fileName to get presigned url from ms EXTERNAL
            // 1. rpc to ms, by unique_id、username
            List<StagePB> stages = ((CloudInternalCatalog) Env.getCurrentInternalCatalog())
                    .getStage(StageType.INTERNAL, mysqlUserName, null, userId);
            if (stages == null || stages.isEmpty()) {
                throw new DdlException("Failed to get internal stage for user: " + mysqlUserName);
            }
            StagePB internalStage = stages.get(0);
            ObjectStoreInfoPB objPb = internalStage.getObjInfo();
            if (!isInternal) {
                // external, use external endpoint to set endpoint
                Cloud.ObjectStoreInfoPB.Builder obj =
                        Cloud.ObjectStoreInfoPB.newBuilder(internalStage.getObjInfo());
                boolean hasExternal = internalStage.getObjInfo().hasExternalEndpoint();
                LOG.debug("meta service msHasExternal: {}", hasExternal);
                String endpoint = hasExternal
                        ? internalStage.getObjInfo().getExternalEndpoint() : internalStage.getObjInfo().getEndpoint();
                if (Strings.isNullOrEmpty(endpoint)) {
                    // fix 'java.net.URISyntaxException: Expected authority at index 7: http://'
                    LOG.warn("use external endpoind but not set, plz check instance's obj info");
                    throw new DorisHttpException(HttpResponseStatus.BAD_REQUEST, "use external endpoint but not set");
                }
                obj.setEndpoint(endpoint);
                objPb = obj.build();
            }
            LOG.debug("obj info : {}, isInternal {}", objPb.toString(), isInternal);

            // 2. call RemoteBase to get pre-signedUrl
            RemoteBase rb = RemoteBase.newInstance(new ObjectInfo(objPb));
            String signedUrl = rb.getPresignedUrl(fileName);
            LOG.info("get internal stage remote info: {}, and signedUrl: {}", rb.toString(), signedUrl);
            long elapseMs = System.currentTimeMillis() - startTime;
            MetricRepo.HISTO_HTTP_COPY_INTO_UPLOAD_LATENCY.update(elapseMs);
            return redirectToObj(signedUrl);
        } catch (DorisHttpException e) {
            // status code  should conforms to HTTP semantic
            MetricRepo.HTTP_COUNTER_COPY_INFO_UPLOAD_ERR.increase(1L);
            resultMap.put("code", e.getCode().code());
            resultMap.put("msg", e.getMessage());
        } catch (UnauthorizedException e) {
            MetricRepo.HTTP_COUNTER_COPY_INFO_UPLOAD_ERR.increase(1L);
            return ResponseEntityBuilder.unauthorized(e.getMessage());
        } catch (Exception e) {
            MetricRepo.HTTP_COUNTER_COPY_INFO_UPLOAD_ERR.increase(1L);
            resultMap.put("code", "1");
            resultMap.put("exception", e.getMessage());
        }

        // should not come here
        MetricRepo.HTTP_COUNTER_COPY_INFO_UPLOAD_ERR.increase(1L);
        return ResponseEntityBuilder.ok(resultMap);
    }

    @RequestMapping(path = "/query", method = RequestMethod.POST)
    public Object loadQuery(HttpServletRequest request, HttpServletResponse response)
            throws InterruptedException, IOException {
        MetricRepo.HTTP_COUNTER_COPY_INFO_QUERY_REQUEST.increase(1L);
        String postContent = HttpUtils.getBody(request);
        LOG.info("query request parameter {} header {} body {}", request.getParameterMap(), getHeadersInfo(request),
                postContent);
        Map<String, Object> resultMap = new HashMap<>(3);
        try {
            long startTime = System.currentTimeMillis();
            ActionAuthorizationInfo authInfo = executeCheckPassword(request, response);
            if (Strings.isNullOrEmpty(postContent)) {
                return ResponseEntityBuilder.badRequest("POST body must contain json object");
            }
            JSONObject jsonObject = (JSONObject) JSONValue.parse(postContent);
            if (jsonObject == null) {
                return ResponseEntityBuilder.badRequest("malformed json: " + postContent);
            }

            String copyIntoSql = (String) jsonObject.get("sql");

            if (Strings.isNullOrEmpty(copyIntoSql)) {
                return ResponseEntityBuilder.badRequest("POST body must contain [sql] root object");
            }

            String clusterName = (String) jsonObject.getOrDefault("cluster", "");
            StatementBase copyIntoStmt = StatementSubmitter.analyzeStmt(copyIntoSql);
            if (!(copyIntoStmt instanceof CopyStmt)) {
                return ResponseEntityBuilder.badRequest("just support copy into sql: " + copyIntoSql);
            }

            LOG.info("copy into stmt: {}", copyIntoSql);

            ConnectContext.get().changeDefaultCatalog(InternalCatalog.INTERNAL_CATALOG_NAME);
            return executeQuery(authInfo, copyIntoSql, response, clusterName, startTime);
        } catch (DorisHttpException e) {
            // status code  should conforms to HTTP semantic
            resultMap.put("code", e.getCode().code());
            resultMap.put("msg", e.getMessage());
        } catch (UnauthorizedException e) {
            MetricRepo.HTTP_COUNTER_COPY_INFO_QUERY_ERR.increase(1L);
            return ResponseEntityBuilder.unauthorized(e.getMessage());
        } catch (Exception e) {
            resultMap.put("code", "1");
            resultMap.put("exception", e.getMessage());
        }
        MetricRepo.HTTP_COUNTER_COPY_INFO_QUERY_ERR.increase(1L);
        return ResponseEntityBuilder.ok(resultMap);
    }

    /**
     * Execute a copy into
     * @param authInfo check user and password
     * @return response
     */
    private ResponseEntity executeQuery(ActionAuthorizationInfo authInfo, String copyIntoStmt,
                                        HttpServletResponse response, String clusterName, long startTime) {
        StatementSubmitter.StmtContext stmtCtx = new StatementSubmitter.StmtContext(copyIntoStmt,
                authInfo.fullUserName, authInfo.password, 1000, false, response, clusterName);
        Future<ExecutionResultSet> future = stmtSubmitter.submitBlock(stmtCtx);

        try {
            ExecutionResultSet resultSet = future.get();
            long elapseMs = System.currentTimeMillis() - startTime;
            MetricRepo.HISTO_HTTP_COPY_INTO_QUERY_LATENCY.update(elapseMs);
            return ResponseEntityBuilder.ok(resultSet.getResult());
        } catch (InterruptedException e) {
            LOG.warn("failed to execute stmt {}, ", copyIntoStmt, e);
            return ResponseEntityBuilder.okWithCommonError("Failed to execute sql: " + e.getMessage());
        } catch (ExecutionException e) {
            LOG.warn("failed to execute stmt {}", copyIntoStmt, e);
            return ResponseEntityBuilder.okWithCommonError("Failed to execute sql: " + e.getMessage());
        }
    }
}
