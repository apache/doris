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

package org.apache.doris.nereids.jobs.load;

import org.apache.doris.catalog.Database;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.InsertJob;

import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * label manager
 */
public class LabelProcessor {
    private final Map<Long, Map<String, List<InsertJob>>> dbIdToLabelToLoadJobs = new ConcurrentHashMap<>();
    private final MonitoredReentrantReadWriteLock lock = new MonitoredReentrantReadWriteLock(true);

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    /**
     * get jobs with label
     * @param db db
     * @return jobs
     * @throws JobException e
     */
    public List<InsertJob> getJobs(Database db) throws JobException {
        readLock();
        try {
            Map<String, List<InsertJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(db.getId());
            if (labelToLoadJobs == null) {
                throw new JobException("Load job does not exist");
            }
            return labelToLoadJobs.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        } finally {
            readUnlock();
        }
    }

    /**
     * add job with label
     *
     * @param job job with label
     * @throws LabelAlreadyUsedException e
     */
    public void addJob(InsertJob job) throws LabelAlreadyUsedException {
        writeLock();
        try {
            Map<String, List<InsertJob>> labelToLoadJobs;
            if (!dbIdToLabelToLoadJobs.containsKey(job.getDbId())) {
                labelToLoadJobs = new ConcurrentHashMap<>();
                dbIdToLabelToLoadJobs.put(job.getDbId(), labelToLoadJobs);
            }
            labelToLoadJobs = dbIdToLabelToLoadJobs.get(job.getDbId());
            if (labelToLoadJobs.containsKey(job.getLabelName())) {
                throw new LabelAlreadyUsedException(job.getLabelName());
            } else {
                labelToLoadJobs.put(job.getLabelName(), new ArrayList<>());
            }
            labelToLoadJobs.get(job.getLabelName()).add(job);
        } finally {
            writeUnlock();
        }
    }

    /**
     * support remove label job
     * @param dbId db id
     * @param labelName label name
     */
    public void removeJob(long dbId, String labelName) {
        writeLock();
        try {
            if (dbIdToLabelToLoadJobs.containsKey(dbId)) {
                dbIdToLabelToLoadJobs.get(dbId).remove(labelName);
            }
        } finally {
            writeUnlock();
        }
    }

    public void cleanOldLabels() throws JobException {
        // TODO: remain this method to implement label cleaner
    }

    /**
     * filterJobs with label and support quick match label
     * @param dbId dbId
     * @param labelValue label
     * @param accurateMatch direct find label from map
     * @return jobs with label
     */
    public List<InsertJob> filterJobs(long dbId, String labelValue, boolean accurateMatch)
            throws AnalysisException {
        List<InsertJob> loadJobList = new ArrayList<>();
        readLock();
        try {
            Map<String, List<InsertJob>> labelToLoadJobs = this.dbIdToLabelToLoadJobs.get(dbId);
            if (Strings.isNullOrEmpty(labelValue)) {
                loadJobList.addAll(
                        labelToLoadJobs.values().stream().flatMap(Collection::stream).collect(Collectors.toList()));
            } else {
                // check label value
                if (accurateMatch) {
                    if (!labelToLoadJobs.containsKey(labelValue)) {
                        return new ArrayList<>();
                    }
                    loadJobList.addAll(labelToLoadJobs.get(labelValue));
                } else {
                    // non-accurate match
                    PatternMatcher matcher =
                            PatternMatcherWrapper.createMysqlPattern(labelValue,
                                    CaseSensibility.LABEL.getCaseSensibility());
                    for (Map.Entry<String, List<InsertJob>> entry : labelToLoadJobs.entrySet()) {
                        if (matcher.match(entry.getKey())) {
                            loadJobList.addAll(entry.getValue());
                        }
                    }
                }
            }
        } finally {
            readUnlock();
        }
        return loadJobList;
    }

    /**
     * check jobs in database
     * @param dbId dbId
     * @return has jobs
     */
    public boolean existJobs(long dbId) {
        readLock();
        try {
            return dbIdToLabelToLoadJobs.containsKey(dbId);
        } finally {
            readUnlock();
        }
    }
}
