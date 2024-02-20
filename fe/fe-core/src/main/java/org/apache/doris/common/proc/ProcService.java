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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// proc service entry
public final class ProcService {
    private static final Logger LOG = LogManager.getLogger(ProcService.class);
    private static volatile ProcService INSTANCE;

    private BaseProcDir root;

    private ProcService() {
        root = new BaseProcDir();
        root.register("auth", new AuthProcDir(Env.getCurrentEnv().getAuth()));
        root.register("backends", new BackendsProcDir(Env.getCurrentSystemInfo()));
        root.register("catalogs", new CatalogsProcDir(Env.getCurrentEnv()));
        root.register("dbs", new DbsProcDir(Env.getCurrentEnv(), Env.getCurrentInternalCatalog()));
        root.register("jobs", new JobsDbProcDir(Env.getCurrentEnv()));
        root.register("statistic", new StatisticProcNode(Env.getCurrentEnv()));
        root.register("tasks", new TasksProcDir());
        root.register("frontends", new FrontendsProcNode(Env.getCurrentEnv()));
        root.register("brokers", Env.getCurrentEnv().getBrokerMgr().getProcNode());
        root.register("resources", Env.getCurrentEnv().getResourceMgr().getProcNode());
        root.register("transactions", new TransDbProcDir());
        root.register("trash", new TrashProcDir());
        root.register("monitor", new MonitorProcDir());
        root.register("current_queries", new CurrentQueryStatisticsProcDir());
        root.register("current_query_stmts", new CurrentQueryStatementsProcNode());
        root.register("current_backend_instances", new CurrentQueryBackendInstanceProcDir());
        root.register("cluster_balance", new ClusterBalanceProcDir());
        root.register("cluster_health", new ClusterHealthProcDir());
        root.register("routine_loads", new RoutineLoadsProcDir());
        root.register("stream_loads", new StreamLoadProcNode());
        root.register("colocation_group", new ColocationGroupProcDir());
        root.register("bdbje", new BDBJEProcDir());
        root.register("diagnose", new DiagnoseProcDir());
        root.register("binlog", new BinlogProcDir());
    }

    // 通过指定的路径获得对应的PROC Node
    // 当前不支持".."，“.”这些通/配符，但是能够处理'//'这样的情况
    // 对于space：之前的space字段可以过滤掉，后面在遇到space字段就直接终止
    public ProcNodeInterface open(String path) throws AnalysisException {
        // input is invalid
        if (Strings.isNullOrEmpty(path)) {
            throw new AnalysisException("Path is null");
        }

        int last = 0;
        int pos = 0;
        int len = path.length();
        boolean meetRoot = false;
        boolean meetEnd = false;
        ProcNodeInterface curNode = null;

        while (pos < len && !meetEnd) {
            char ch = path.charAt(pos);
            switch (ch) {
                case '/':
                    if (!meetRoot) {
                        curNode = root;
                        meetRoot = true;
                    } else {
                        String name = path.substring(last, pos);
                        if (!(curNode instanceof ProcDirInterface)) {
                            String errMsg = path.substring(0, pos) + " is not a directory";
                            LOG.warn(errMsg);
                            throw new AnalysisException(errMsg);
                        }
                        curNode = ((ProcDirInterface) curNode).lookup(name);
                    }
                    // swallow next "/"
                    while (pos < len && path.charAt(pos) == '/') {
                        pos++;
                    }
                    // now assign last from pos(new start of file name)
                    last = pos;
                    break;
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    if (meetRoot) {
                        meetEnd = true;
                    } else {
                        last++;
                        pos++;
                    }
                    break;
                default:
                    if (!meetRoot) {
                        // starts without '/'
                        String errMsg = "Path(" + path + ") does not start with '/'";
                        LOG.warn(errMsg);
                        throw new AnalysisException(errMsg);
                    }
                    pos++;
                    break;
            }
        }
        // the last character of path is '/', the current is must a directory
        if (pos == last) {
            // now pos == path.length()
            if (!(curNode instanceof ProcDirInterface)) {
                String errMsg = path + " is not a directory";
                LOG.warn(errMsg);
                throw new AnalysisException(errMsg);
            }
            return curNode;
        }

        if (!(curNode instanceof ProcDirInterface)) {
            String errMsg = path.substring(0, pos) + " is not a directory";
            LOG.warn(errMsg);
            throw new AnalysisException(errMsg);
        }

        // 这里使用pos，因为有可能path后面会有space字段被提前截断
        curNode = ((ProcDirInterface) curNode).lookup(path.substring(last, pos));
        if (curNode == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_PROC_PATH, path);
        }
        return curNode;
    }

    // 将node注册到根节点下的name下
    public synchronized boolean register(String name, ProcNodeInterface node) {
        if (Strings.isNullOrEmpty(name) || node == null) {
            LOG.warn("register proc service invalid input.");
            return false;
        }
        if (root.lookup(name) != null) {
            LOG.warn("node(" + name + ") already exists.");
            return false;
        }
        return root.register(name, node);
    }

    public static ProcService getInstance() {
        if (INSTANCE == null) {
            synchronized (ProcService.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ProcService();
                }
            }
        }
        return INSTANCE;
    }

    // 用于测试的时候将已经注册的内容进行清空
    public static void destroy() {
        INSTANCE = null;
    }

}
