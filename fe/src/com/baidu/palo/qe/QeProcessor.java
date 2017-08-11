// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.qe;

import com.baidu.palo.common.InternalException;
import com.baidu.palo.thrift.TReportExecStatusParams;
import com.baidu.palo.thrift.TReportExecStatusResult;
import com.baidu.palo.thrift.TStatus;
import com.baidu.palo.thrift.TStatusCode;
import com.baidu.palo.thrift.TUniqueId;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class QeProcessor {
    private static final Logger LOG = LogManager.getLogger(QeProcessor.class);

    public QeProcessor() {
    }

    public TReportExecStatusResult reportExecStatus(TReportExecStatusParams params) {
        LOG.info("ReportExecStatus(): instance_id=" + params.fragment_instance_id.toString()
                + "queryID=" +  params.query_id.toString() + " params=" + params);
        Coordinator coord = null;
        TReportExecStatusResult result = new TReportExecStatusResult();
        synchronized (coordinatorMap) {
            if (coordinatorMap.containsKey(params.query_id)) {
                coord = coordinatorMap.get(params.query_id);
            } else {
                result.setStatus(new TStatus(TStatusCode.RUNTIME_ERROR));
                LOG.info("ReportExecStatus() runtime error");
                return result;
            }
        }
        try {
            coord.updateFragmentExecStatus(params);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
            return result;
        }
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static Map<TUniqueId, Coordinator> coordinatorMap = new HashMap<TUniqueId, Coordinator>();
    
    public static synchronized void registerQuery(TUniqueId queryId, Coordinator coord) throws InternalException {
        LOG.info("register query id = " + queryId.toString());
        if (coordinatorMap.containsKey(queryId)) {
            throw new InternalException("queryId " + queryId + " already exists");       
        }
        coordinatorMap.put(queryId, coord);
    }
    
    public static synchronized void unregisterQuery(TUniqueId queryId) {
        LOG.info("deregister query id = " + queryId.toString());
        coordinatorMap.remove(queryId);
    }
}
