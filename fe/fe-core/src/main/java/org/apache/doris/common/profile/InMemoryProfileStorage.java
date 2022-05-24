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

package org.apache.doris.common.profile;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.common.util.ProfileManager.ProfileType;
import org.apache.doris.common.util.RuntimeProfile;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * An inMemory impl of profile storage. It will only keep 100 records in memory.
 * If you want to keep more query records, you can use other impl of profile storage.
 */
public class InMemoryProfileStorage implements ProfileStorage {
    private static final Logger LOG = LogManager.getLogger(InMemoryProfileStorage.class);

    private static final int ARRAY_SIZE = 100;
    // private static final int TOTAL_LEN = 1000 * ARRAY_SIZE ;

    private final ReadLock readLock;
    private final WriteLock writeLock;

    // record the order of profiles by queryId
    private final Deque<String> queryIdDeque;

    /**
     * Store the base element of each query.
     */
    private static class ProfileElement {
        public Map<String, String> infoStrings = Maps.newHashMap();
        public String profileContent = "";
        public MultiProfileTreeBuilder builder = null;
        public String errMsg = "";
    }

    private final Map<String, ProfileElement> queryIdToProfileMap; // from QueryId to RuntimeProfile

    /**
     * InMemoryProfileStorage.
     */
    public InMemoryProfileStorage() {
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
        readLock = lock.readLock();
        writeLock = lock.writeLock();
        queryIdDeque = new LinkedList<>();
        queryIdToProfileMap = new ConcurrentHashMap<>();
    }

    @Override
    public String getProfileContent(String queryID) {
        readLock.lock();
        try {
            ProfileElement element = queryIdToProfileMap.get(queryID);
            if (element == null) {
                return null;
            }
            return element.profileContent;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public MultiProfileTreeBuilder getProfileBuilder(String queryID) throws AnalysisException {
        readLock.lock();
        try {
            ProfileElement element = queryIdToProfileMap.get(queryID);
            if (element == null || element.builder == null) {
                throw new AnalysisException("failed to get fragment profile tree. err: "
                        + (element == null ? "not found" : element.errMsg));
            }

            return element.builder;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public List<List<String>> getAllProfiles(ProfileType type) {
        List<List<String>> result = Lists.newArrayList();
        readLock.lock();
        try {
            Iterator<String> reverse = queryIdDeque.descendingIterator();
            while (reverse.hasNext()) {
                String  queryId = reverse.next();
                ProfileElement profileElement = queryIdToProfileMap.get(queryId);
                if (profileElement == null) {
                    continue;
                }
                Map<String, String> infoStrings = profileElement.infoStrings;
                if (type != null && !infoStrings.get(ProfileManager.QUERY_TYPE).equalsIgnoreCase(type.name())) {
                    continue;
                }

                List<String> row = Lists.newArrayList();
                for (String str : ProfileManager.PROFILE_HEADERS) {
                    row.add(infoStrings.get(str));
                }
                result.add(row);
            }
        } finally {
            readLock.unlock();
        }
        return result;
    }

    @Override
    public void pushProfile(RuntimeProfile profile) {
        if (profile == null) {
            return;
        }

        ProfileElement element = createElement(profile);
        String queryId = element.infoStrings.get(ProfileManager.QUERY_ID);
        // check when push in, which can ensure every element in the list has QUERY_ID column,
        // so there is no need to check when remove element from list.
        if (Strings.isNullOrEmpty(queryId)) {
            LOG.warn("the key or value of Map is null, "
                    + "may be forget to insert 'QUERY_ID' column into infoStrings");
        }

        // a profile may be updated multiple times in queryIdToProfileMap,
        // and only needs to be inserted into the queryIdDeque for the first time.
        queryIdToProfileMap.put(queryId, element);
        writeLock.lock();
        try {
            if (!queryIdDeque.contains(queryId)) {
                if (queryIdDeque.size() >= ARRAY_SIZE) {
                    queryIdToProfileMap.remove(queryIdDeque.getFirst());
                    queryIdDeque.removeFirst();
                }
                queryIdDeque.addLast(queryId);
            }
        } finally {
            writeLock.unlock();
        }
    }

    private ProfileElement createElement(RuntimeProfile profile) {
        ProfileElement element = new ProfileElement();
        RuntimeProfile summaryProfile = profile.getChildList().get(0).first;
        for (String header : ProfileManager.PROFILE_HEADERS) {
            element.infoStrings.put(header, summaryProfile.getInfoString(header));
        }
        element.profileContent = profile.toString();

        MultiProfileTreeBuilder builder = new MultiProfileTreeBuilder(profile);
        try {
            builder.build();
        } catch (Exception e) {
            element.errMsg = e.getMessage();
            LOG.debug("failed to build profile tree", e);
            return element;
        }
        element.builder = builder;
        return element;
    }
}
