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

package org.apache.doris.job;

import org.apache.doris.analysis.AlterResourceQueueNameStmt;
import org.apache.doris.analysis.AlterResourceQueueStmt;
import org.apache.doris.analysis.CreateResourceQueueStmt;
import org.apache.doris.analysis.DropResourceQueueStmt;
import org.apache.doris.analysis.ShowResourceQueueStmt;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ShowResultSet;

import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ResourceQueueMgr {
    private static volatile ResourceQueueMgr INSTANCE = null;

    public static ResourceQueueMgr get() {
        if (INSTANCE == null) {
            synchronized (ResourceQueueMgr.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ResourceQueueMgr();
                }
            }
        }
        return INSTANCE;
    }

    private final Map<Long, ResourceQueue> id2Queue = new HashMap<>();
    private final Map<String, Long> name2id = new HashMap<>();
    private final ReadWriteLock rwlock = new ReentrantReadWriteLock();
    private final Lock rlock = rwlock.readLock();
    private final Lock wlock = rwlock.writeLock();

    public ResourceQueue matchQueue(AsyncJob job) {
        rlock.lock();
        try {
            String queueName = job.getResourceQueue();
            if (queueName != null) {
                Long queueId = name2id.get(queueName);
                if (queueId != null) {
                    return id2Queue.get(queueId);
                }
            }
            ResourceQueue result = null;
            for (ResourceQueue queue : id2Queue.values()) {
                if (queue.match(job)) {
                    if (result == null) {
                        result = queue;
                    } else {
                        if (queue.canAddJob() && !result.canAddJob()) {
                            result = queue;
                        } else if (queue.canAddJob() && queue.numPendingJobs() < result.numPendingJobs()) {
                            result = queue;
                        }
                    }
                }
            }
            return result;
        } finally {
            rlock.unlock();
        }
    }

    public void addQueue(ResourceQueue queue, boolean ifNotExists) throws UserException {
        wlock.lock();
        try {
            if (name2id.containsKey(queue.getName())) {
                if (ifNotExists) {
                    return;
                }
                throw new UserException("Add the same resource queue with name: " + queue.getName());
            }
            if (id2Queue.putIfAbsent(queue.queueId(), queue) != null) {
                throw new UserException(
                        String.format("Add the same resource queue with name: %s, queueId: %d", queue.getName(),
                                queue.queueId()));
            } else {
                name2id.put(queue.getName(), queue.queueId());
                queue.run();
            }
        } finally {
            wlock.unlock();
        }
    }

    public void addQueue(CreateResourceQueueStmt stmt) throws UserException {
        addQueue(new ResourceQueue(stmt.getName(), new ResourceQueueConfig(stmt.getConfig()),
                new MatchingPolicy(stmt.getPolicy())), stmt.isIfNotExists());
    }

    public void renameQueue(AlterResourceQueueNameStmt stmt) throws UserException {
        wlock.lock();
        try {
            if (!name2id.containsKey(stmt.getQueueName())) {
                throw new UserException("No resource queue found with name: " + stmt.getQueueName());
            }
            if (name2id.containsKey(stmt.getNewQueueName())) {
                throw new UserException("Rename to an existed resource queue with name: " + stmt.getNewQueueName());
            }
            name2id.put(stmt.getNewQueueName(), name2id.remove(stmt.getQueueName()));
            id2Queue.get(name2id.get(stmt.getNewQueueName())).setName(stmt.getNewQueueName());
        } finally {
            wlock.unlock();
        }
    }

    public void alterQueue(AlterResourceQueueStmt stmt) throws UserException {
        wlock.lock();
        try {
            if (!name2id.containsKey(stmt.getQueueName())) {
                throw new UserException("No resource queue found with name: " + stmt.getQueueName());
            }
            id2Queue.get(name2id.get(stmt.getQueueName()))
                    .alterQueue(new ResourceQueueConfig(stmt.getNewConfig()), new MatchingPolicy(stmt.getNewPolicy()));
        } finally {
            wlock.unlock();
        }
    }

    public void dropQueue(long queueId) throws UserException {
        wlock.lock();
        try {
            ResourceQueue queue = id2Queue.remove(queueId);
            if (queue == null) {
                throw new UserException("No resource queue found with id: " + queueId);
            } else {
                name2id.remove(queue.getName());
            }
        } finally {
            wlock.unlock();
        }
    }

    public void dropQueue(String name, boolean ifExists) throws UserException {
        wlock.lock();
        try {
            Long queueId = name2id.get(name);
            if (queueId != null) {
                if (ifExists) {
                    return;
                }
                throw new UserException("No resource queue found with name: " + name);
            } else {
                id2Queue.get(queueId).dropQueue();
                name2id.remove(name);
                id2Queue.remove(queueId);
            }
        } finally {
            wlock.unlock();
        }
    }

    public void dropQueue(DropResourceQueueStmt stmt) throws UserException {
        dropQueue(stmt.getQueueName(), stmt.isSetIfExists());
    }

    public ResourceQueue getQueue(long queueId) {
        rlock.lock();
        try {
            return id2Queue.get(queueId);
        } finally {
            rlock.unlock();
        }
    }

    public ResourceQueue getQueue(String name) {
        rlock.lock();
        try {
            Long queueId = name2id.get(name);
            if (queueId == null) {
                return null;
            }
            return id2Queue.get(queueId);
        } finally {
            rlock.unlock();
        }
    }

    public ShowResultSet showResourceQueue(ShowResourceQueueStmt stmt) throws AnalysisException {
        List<List<String>> rows = Lists.newArrayList();
        rlock.lock();
        try {
            if (stmt.getQueueName() == null) {
                for (ResourceQueue queue : id2Queue.values()) {
                    rows.add(queue.showQueueInfo());
                }
            } else {
                Long queueId = name2id.get(stmt.getQueueName());
                if (queueId == null) {
                    throw new AnalysisException("No resource queue found with name: " + stmt.getQueueName());
                }
                rows.add(id2Queue.get(queueId).showQueueInfo());
            }
            return new ShowResultSet(stmt.getMetaData(), rows);
        } finally {
            rlock.unlock();
        }
    }
}
