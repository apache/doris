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

package org.apache.doris.backup;

import org.apache.doris.backup.Status.ErrCode;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.fs.remote.S3FileSystem;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/*
 * A manager to manage all backup repositories
 */
public class RepositoryMgr extends Daemon implements Writable {
    private static final Logger LOG = LogManager.getLogger(RepositoryMgr.class);

    // all key should be in lower case
    private Map<String, Repository> repoNameMap = Maps.newConcurrentMap();
    private Map<Long, Repository> repoIdMap = Maps.newConcurrentMap();

    private ReentrantLock lock = new ReentrantLock();

    public RepositoryMgr() {
        super(Repository.class.getSimpleName(), 600 * 1000 /* 10min */);
    }

    @Override
    protected void runOneCycle() {
        for (Repository repo : repoNameMap.values()) {
            if (!repo.ping()) {
                LOG.warn("Failed to connect repository {}. msg: {}", repo.getName(), repo.getErrorMsg());
            }
        }
    }

    public Status addAndInitRepoIfNotExist(Repository repo, boolean isReplay) {
        lock.lock();
        try {
            if (!repoNameMap.containsKey(repo.getName())) {
                if (!isReplay) {
                    // create repository path and repo info file in remote storage
                    Status st = repo.initRepository();
                    if (!st.ok()) {
                        return st;
                    }
                }
                repoNameMap.put(repo.getName(), repo);
                repoIdMap.put(repo.getId(), repo);

                if (!isReplay) {
                    // write log
                    Env.getCurrentEnv().getEditLog().logCreateRepository(repo);
                }

                LOG.info("successfully adding repo {} to repository mgr. is replay: {}",
                         repo.getName(), isReplay);
                return Status.OK;
            }
            return new Status(ErrCode.COMMON_ERROR, "repository with same name already exist: " + repo.getName());
        } finally {
            lock.unlock();
        }
    }

    public Repository getRepo(String repoName) {
        return repoNameMap.get(repoName);
    }

    public Repository getRepo(long repoId) {
        return repoIdMap.get(repoId);
    }

    public Status alterRepo(Repository newRepo, boolean isReplay) {
        lock.lock();
        try {
            Repository repo = repoNameMap.get(newRepo.getName());
            if (repo != null) {
                if (repo.getRemoteFileSystem() instanceof S3FileSystem) {
                    repoNameMap.put(repo.getName(), newRepo);
                    repoIdMap.put(repo.getId(), newRepo);

                    if (!isReplay) {
                        // log
                        Env.getCurrentEnv().getEditLog().logAlterRepository(newRepo);
                    }
                    LOG.info("successfully alter repo {}, isReplay {}", newRepo.getName(), isReplay);
                    return Status.OK;
                } else {
                    return new Status(ErrCode.COMMON_ERROR, "Only support alter s3 repository");
                }
            }
            return new Status(ErrCode.NOT_FOUND, "repository does not exist");
        } finally {
            lock.unlock();
        }
    }

    public Status removeRepo(String repoName, boolean isReplay) {
        lock.lock();
        try {
            Repository repo = repoNameMap.remove(repoName);
            if (repo != null) {
                repoIdMap.remove(repo.getId());

                if (!isReplay) {
                    // log
                    Env.getCurrentEnv().getEditLog().logDropRepository(repoName);
                }
                LOG.info("successfully removing repo {} from repository mgr", repoName);
                return Status.OK;
            }
            return new Status(ErrCode.NOT_FOUND, "repository does not exist");
        } finally {
            lock.unlock();
        }
    }

    public List<List<String>> getReposInfo() {
        List<List<String>> infos = Lists.newArrayList();
        for (Repository repo : repoIdMap.values()) {
            infos.add(repo.getInfo());
        }
        return infos;
    }

    public static RepositoryMgr read(DataInput in) throws IOException {
        RepositoryMgr mgr = new RepositoryMgr();
        mgr.readFields(in);
        return mgr;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(repoNameMap.size());
        for (Repository repo : repoNameMap.values()) {
            repo.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            Repository repo = Repository.read(in);
            repoNameMap.put(repo.getName(), repo);
            repoIdMap.put(repo.getId(), repo);
        }
    }
}
