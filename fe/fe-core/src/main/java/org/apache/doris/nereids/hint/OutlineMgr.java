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
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Writable;
import org.apache.doris.nereids.trees.plans.PlaceholderId;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    private static final Map<String, OutlineInfo> visibleSignatureMap = new HashMap<>();

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

    public static Optional<OutlineInfo> getOutlineByVisibleSignature(String visibleSignature) {
        if (visibleSignatureMap.containsKey(visibleSignature)) {
            return Optional.of(visibleSignatureMap.get(visibleSignature));
        }
        return Optional.empty();
    }

    public static Map<String, OutlineInfo> getVisibleSignatureMap() {
        return visibleSignatureMap;
    }

    /**
     * replace constant by place holder
     * @param originalQuery original query input by create outline command
     * @param constantMap constant map collected by logicalPlanBuilder
     * @param startIndex a shift of create outline command
     * @return query replace constant by place holder
     */
    public static String replaceConstant(String originalQuery, Map<PlaceholderId,
            Pair<Integer, Integer>> constantMap, int startIndex) {
        List<Pair<Integer, Integer>> sortedKeys = new ArrayList<>(constantMap.values());

        // Sort by start index in descending order to avoid shifting problems
        sortedKeys.sort((a, b) -> b.first.compareTo(a.first));

        StringBuilder sb = new StringBuilder(originalQuery);

        for (Pair<Integer, Integer> range : sortedKeys) {
            if (range.first == 0 && range.second == 0) {
                continue;
            }
            int start = range.first - startIndex;
            int end = range.second - startIndex + 1;
            sb.replace(start, end, "?");
        }

        return sb.toString();
    }

    /**
     * create outline data which include some hints
     * @param sessionVariable sessionVariables used to generate corresponding plan
     * @return string include many hints
     */
    public static String createOutlineData(SessionVariable sessionVariable) {
        StringBuilder sb = new StringBuilder();
        sb.append("/*+ ");
        // add set_var hint
        List<List<String>> changedVars = VariableMgr.dumpChangedVars(sessionVariable);
        if (!changedVars.isEmpty()) {
            sb.append("set_var(");
            for (List<String> changedVar : changedVars) {
                sb.append(changedVar.get(0));
                sb.append("=");
                sb.append(changedVar.get(1));
                sb.append(" ");
            }
            sb.append(") ");
        }
        sb.append("*/ ");
        return sb.toString();
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

            if (OutlineMgr.getOutlineByVisibleSignature(outlineInfo.getVisibleSignature()).isPresent()) {
                if (!ignoreIfExists) {
                    LOG.info("outline already exists, ignored to create outline with signature: {}, is replay: {}",
                            outlineInfo.getVisibleSignature(), isReplay);
                    throw new DdlException(outlineInfo.getVisibleSignature() + " already exists");
                } else {
                    dropOutline(outlineInfo);
                }
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
        visibleSignatureMap.put(outlineInfo.getVisibleSignature(), outlineInfo);
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
            dropOutline(outlineInfo);
            if (!isReplay && isPresent) {
                Env.getCurrentEnv().getEditLog().logDropOutline(outlineInfo);
            }
        } finally {
            writeUnlock();
        }
        LOG.info("finished to create outline: {}, is replay: {}", outlineName, isReplay);
    }

    private static void dropOutline(OutlineInfo outlineInfo) {
        outlineMap.remove(outlineInfo.getOutlineName());
        visibleSignatureMap.remove(outlineInfo.getVisibleSignature());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(outlineMap.size());
        for (OutlineInfo outlineInfo : outlineMap.values()) {
            outlineInfo.write(out);
        }
    }

    /**
     * read fields from disk
     * @param in data source of disk
     * @throws IOException maybe throw ioexception
     */
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            OutlineInfo outlineInfo = OutlineInfo.read(in);
            outlineMap.put(outlineInfo.getOutlineName(), outlineInfo);
            visibleSignatureMap.put(outlineInfo.getVisibleSignature(), outlineInfo);
        }
    }
}
