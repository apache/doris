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

package org.apache.doris.cdcloader.mysql.rest;


import org.apache.doris.cdcloader.common.rest.ResponseEntityBuilder;
import org.apache.doris.cdcloader.mysql.loader.SplitReader;
import org.apache.doris.cdcloader.mysql.loader.DorisRecord;
import org.apache.doris.cdcloader.mysql.loader.LoadContext;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class LoaderController extends BaseController {
    @RequestMapping(path = "/api/fetchRecords", method = RequestMethod.GET)
    public Object rowcount(@RequestParam("fetchSize") int fetchSize,
                           @RequestParam(value = "schedule", defaultValue = "false") boolean schedule,
                           @RequestParam("jobId") long jobId) throws Exception {
        if(!checkJobId(jobId)){
            return ResponseEntityBuilder.jobIdInconsistent();
        }
        LoadContext context  = LoadContext.getInstance();
        SplitReader splitReader = context.getSplitReader();
        List<DorisRecord> dorisRecords = splitReader.fetchRecords(fetchSize, schedule);
        return ResponseEntityBuilder.ok(dorisRecords);
    }

    @RequestMapping(path = "/api/status", method = RequestMethod.GET)
    public Object isStarted(@RequestParam("jobId") long jobId) {
        if(!checkJobId(jobId)){
            return ResponseEntityBuilder.jobIdInconsistent();
        }
        LoadContext context  = LoadContext.getInstance();
        SplitReader splitReader = context.getSplitReader();
        if(splitReader != null){
            return ResponseEntityBuilder.ok(splitReader.isStarted());
        }else{
            return ResponseEntityBuilder.ok(false);
        }
    }

    @RequestMapping(path = "/api/getSnapshotSplits", method = RequestMethod.GET)
    public Object getSnapshotSplits(@RequestParam("jobId") long jobId) {
        if(!checkJobId(jobId)){
            return ResponseEntityBuilder.jobIdInconsistent();
        }
        //获取当前已经切分好的chunk splits
        return ResponseEntityBuilder.ok();
    }

    @RequestMapping(path = "/api/job/cdc/heartbeat", method = RequestMethod.POST)
    public Object heartbeat(@RequestParam("jobId") long jobId) {
        if(!checkJobId(jobId)){
            return ResponseEntityBuilder.jobIdInconsistent();
        }
        return ResponseEntityBuilder.ok();
    }

}
