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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.MaterializedView;
import org.apache.doris.mtmv.MTMVUtils.TriggerMode;
import org.apache.doris.mtmv.metadata.MTMVJob;
import org.apache.doris.mtmv.metadata.MTMVJob.JobSchedule;

import java.util.concurrent.TimeUnit;

public class MTMVJobFactory {
    public static MTMVJob buildJob(MaterializedView materializedView) {
        MTMVJob job = new MTMVJob(materializedView.getName());
        JobSchedule jobSchedule = new JobSchedule(System.currentTimeMillis() / 1000, 1, TimeUnit.MINUTES);
        job.setSchedule(jobSchedule);
        job.setTriggerMode(TriggerMode.PERIODICAL);
        job.setDbName("fake");
        job.setQuery("select * from fake");
        return job;
    }
}
