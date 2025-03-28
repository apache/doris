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

package org.apache.doris.nereids.hint;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Writable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Outline manager used to manage read write and cached of outline
 */
public class OutlineMgr implements Writable {
    public static final OutlineMgr INSTANCE = new OutlineMgr();
    private static final Logger LOG = LogManager.getLogger(OutlineMgr.class);
    private static final Map<String, OutlineInfo> outlineMap = new HashMap<>();

    private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private static void writeLock() {
        lock.writeLock().lock();
    }

    private static void writeUnlock() {
        lock.writeLock().unlock();
    }

    public static Optional<OutlineInfo> getOutline(String outlineName) {
        if (outlineMap.containsKey(outlineName)) {
            return Optional.of(outlineMap.get(outlineName));
        }
        return Optional.empty();
    }

    public static Map<String, OutlineInfo> getOutlineMap() {
        return outlineMap;
    }

    /**
     * createOutlineInternal
     * @param outlineInfo outline info used to create outline
     * @param ignoreIfExists if we add or replace to create outline statement, it would be true
     * @param isReplay when it is replay mode, editlog would not be written
     * @throws DdlException should throw exception when meeting problem
     */
    public static void createOutlineInternal(OutlineInfo outlineInfo, boolean ignoreIfExists, boolean isReplay)
            throws DdlException {
        writeLock();
        try {
            if (!ignoreIfExists && OutlineMgr.getOutline(outlineInfo.getOutlineName()).isPresent()) {
                LOG.info("outline already exists, ignored to create outline: {}, is replay: {}",
                        outlineInfo.getOutlineName(), isReplay);
                throw new DdlException(outlineInfo.getOutlineName() + " already exists");
            }

            createOutline(outlineInfo);
            if (!isReplay) {
                Env.getCurrentEnv().getEditLog().logCreateOutline(outlineInfo);
            }
        } finally {
            writeUnlock();
        }
        LOG.info("finished to create outline: {}, is replay: {}", outlineInfo.getOutlineName(), isReplay);
    }

    private static void createOutline(OutlineInfo outlineInfo) {
        outlineMap.put(outlineInfo.getOutlineName(), outlineInfo);
    }

    /**
     * createOutlineInternal
     * @param outlineName outline info used to create outline
     * @param ifExists if we add if exists to create outline statement, it would be true
     * @param isReplay when it is replay mode, editlog would not be written
     * @throws DdlException should throw exception when meeting problem
     */
    public static void dropOutlineInternal(String outlineName, boolean ifExists, boolean isReplay)
            throws DdlException {
        writeLock();
        try {
            boolean isPresent = outlineMap.containsKey(outlineName);
            if (!ifExists && !isPresent) {
                LOG.info("outline not exists, ignored to drop outline: {}, is replay: {}",
                        outlineName, isReplay);
                throw new DdlException(outlineName + " not exists");
            }

            OutlineInfo outlineInfo = outlineMap.get(outlineName);
            dropOutline(outlineName);
            if (!isReplay && isPresent) {
                Env.getCurrentEnv().getEditLog().logDropOutline(outlineInfo);
            }
        } finally {
            writeUnlock();
        }
        LOG.info("finished to create outline: {}, is replay: {}", outlineName, isReplay);
    }

    private static void dropOutline(String outlineName) {
        outlineMap.remove(outlineName);
    }

    public static String fastParamization(String sql) {
        return sql;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(outlineMap.size());
        for (OutlineInfo outlineInfo : outlineMap.values()) {
            outlineInfo.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            OutlineInfo outlineInfo = OutlineInfo.read(in);
            outlineMap.put(outlineInfo.getOutlineName(), outlineInfo);
        }
    }
}
