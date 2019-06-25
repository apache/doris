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

package org.apache.doris.task;

import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.load.DppConfig;
import org.apache.doris.load.DppScheduler;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.LoadJob;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.Map;

public class HadoopLoadEtlTask extends LoadEtlTask {
    private static final Logger LOG = LogManager.getLogger(HadoopLoadEtlTask.class);
    private static final String MAP_COMPLETION = "map() completion";
    private static final String REDUCE_COMPLETION = "reduce() completion";
    private static final double FILTER_RATIO_DELTA = 0.02;

    public HadoopLoadEtlTask(LoadJob job) {
        super(job);
    }

    @Override
    protected boolean updateJobEtlStatus() {
        // get etl status
        DppScheduler dppScheduler = new DppScheduler(job.getHadoopDppConfig());
        EtlStatus status = dppScheduler.getEtlJobStatus(job.getHadoopEtlJobId());
        LOG.info("job status: {}. job: {}", status, job.toString());
        
        // update load job etl status
        job.setEtlJobStatus(status);
        return true;
    }
    
    @Override
    protected void processEtlRunning() throws LoadException {
        Map<String, String> stats = job.getEtlJobStatus().getStats();
        boolean isMapCompleted = false;
        if (stats.containsKey(MAP_COMPLETION) && stats.containsKey(REDUCE_COMPLETION)) {
            float mapProgress = Float.parseFloat(stats.get(MAP_COMPLETION));
            float reduceProgress = Float.parseFloat(stats.get(REDUCE_COMPLETION));
            int progress = (int) (100 * (mapProgress + reduceProgress) / 2);
            if (progress >= 100) {
                // hadoop job status result:
                // [map() completion] and [reduce() completion] are not accurate,
                // etl job state must be depend on [job state] 
                progress = 99;
            }
            job.setProgress(progress);
            
            if (mapProgress >= 1) {
                isMapCompleted = true;
            }
        }
        
        // check data quality when map complete
        if (isMapCompleted) {
            // [map() completion] is not accurate
            double maxFilterRatio = job.getMaxFilterRatio() + FILTER_RATIO_DELTA;
            if (!checkDataQuality(maxFilterRatio)) {
                throw new LoadException(QUALITY_FAIL_MSG);
            }
        }
    }

    @Override
    protected Map<String, Pair<String, Long>> getFilePathMap() throws LoadException {
        DppConfig dppConfig = job.getHadoopDppConfig();
        // get etl files
        DppScheduler dppScheduler = new DppScheduler(dppConfig);
        long dbId = job.getDbId();
        String loadLabel = job.getLabel();
        String outputPath = DppScheduler.getEtlOutputPath(dppConfig.getFsDefaultName(), dppConfig.getOutputPath(),
                dbId, loadLabel, job.getHadoopEtlOutputDir());
        Map<String, Long> fileMap = dppScheduler.getEtlFiles(outputPath);
        if (fileMap == null) {
            throw new LoadException("get etl files error");
        }

        // create file map
        Map<String, Pair<String, Long>> filePathMap = Maps.newHashMap();
        String httpServer = String.format("http://%s:%s", dppConfig.getNameNodeHost(), dppConfig.getHttpPort());
        String ugi = String.format("ugi=%s", dppConfig.getHadoopJobUgiStr());
        for (Map.Entry<String, Long> entry : fileMap.entrySet()) {
            String filePath = entry.getKey();
            String partitionIndexBucket = getPartitionIndexBucketString(filePath);
            filePath = String.format("%s/data%s?%s", httpServer, filePath, ugi);
            filePathMap.put(partitionIndexBucket, Pair.create(filePath, entry.getValue()));
        }

        return filePathMap;
    }
}
