// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.http.rest;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.http.ActionController;
import com.baidu.palo.http.BaseRequest;
import com.baidu.palo.http.BaseResponse;
import com.baidu.palo.http.IllegalArgException;
import com.baidu.palo.load.LoadJob;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class LoadErrorMessageAction extends RestBaseAction {
    
    public static final String DB_NAME_PARAM = "db_name";
    public static final String LABEL_NAME_PARAM = "label_name";
    
    public LoadErrorMessageAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction (ActionController controller) 
            throws IllegalArgException {
        LoadErrorMessageAction action = new LoadErrorMessageAction(controller);
        controller.registerHandler(
                HttpMethod.GET, 
                "/rest/{" + DB_NAME_PARAM + "}/{" + LABEL_NAME_PARAM  + "}/_get_error_message", action);
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        response.appendContent("in LoadErrorMessage action.");
        String dbName = request.getSingleParameter(DB_NAME_PARAM);
        String label = request.getSingleParameter(LABEL_NAME_PARAM);
        
        Database db = Catalog.getInstance().getDb(dbName);
        if (db == null) {
            response.appendContent("Db name is not exist.");
            writeResponse(request, response, HttpResponseStatus.OK);
            return;
        }
        long dbId = db.getId();
        List<LoadJob> jobList = Catalog.getInstance().getLoadInstance().getDbLoadJobs(dbId);
        if (jobList == null) {
            response.appendContent("No job match you condition.");
            writeResponse(request, response, HttpResponseStatus.OK);
            return;
        }
        LoadJob job = getLatestSubmitLoadJob(jobList);
        
        // Backend backend = ClusterInfoService.getInstance().getBackend(job.getEtlBackendId());
        // String beHost = backend.getHost();
        String beHost = "10.26.166.12";
        
        // TODO(lingbin):Get the http port from heartbeat of BE machine which run this ETL job
        int bePort = 8540;
        
        String bePath = "/api/" + dbName + "/" + label + "/_download";
        String query = "?file=error.log";
        
        URI resultUriObj = null;
        try {
            resultUriObj = new URI("http", null, beHost,
                    bePort, bePath, query, null);
        } catch (URISyntaxException e) {
            response.appendContent("failed to construct result url.");
            writeResponse(request, response, HttpResponseStatus.OK);
            return;
        }
        
        response.appendContent("you will be redirect to " + resultUriObj.toString());
        response.addHeader(HttpHeaders.Names.LOCATION, resultUriObj.toString());
        writeResponse(request, response, HttpResponseStatus.TEMPORARY_REDIRECT);
    }
    
    private LoadJob getLatestSubmitLoadJob (List<LoadJob> jobList) {
        if (jobList.isEmpty()) {
            return null;
        }
        
        LoadJob lasterSubmitJob = jobList.get(0);;
        long latestSubmitTimeMs = lasterSubmitJob.getCreateTimeMs();
        for (LoadJob job : jobList) {
            long jobSubmitTimeMs = job.getCreateTimeMs();
            if (jobSubmitTimeMs > latestSubmitTimeMs) {
                latestSubmitTimeMs = jobSubmitTimeMs;
                lasterSubmitJob = job;
            }
        }
        return lasterSubmitJob;
    }
}
