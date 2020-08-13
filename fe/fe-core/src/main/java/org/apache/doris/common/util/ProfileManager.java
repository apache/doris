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

package org.apache.doris.common.util;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/*
 * if you want to visit the atrribute(such as queryID,defaultDb) 
 * you can use profile.getInfoStrings("queryId")
 * All attributes can be seen from the above.
 * 
 * why the element in the finished profile arary is not RuntimeProfile,
 * the purpose is let coordinator can destruct earlier(the fragment profile is in Coordinator) 
 * 
 */
public class ProfileManager {
    private static final Logger LOG = LogManager.getLogger(ProfileManager.class);
    private static ProfileManager INSTANCE = null;
    private static final int ARRAY_SIZE = 100;
    // private static final int TOTAL_LEN = 1000 * ARRAY_SIZE ;
    public static final String QUERY_ID = "Query ID";
    public static final String START_TIME = "Start Time";
    public static final String END_TIME = "End Time";
    public static final String TOTAL_TIME = "Total";
    public static final String QUERY_TYPE = "Query Type";
    public static final String QUERY_STATE = "Query State";
    public static final String SQL_STATEMENT = "Sql Statement";
    public static final String USER = "User";
    public static final String DEFAULT_DB = "Default Db";
    
    public static final ArrayList<String> PROFILE_HEADERS = new ArrayList(
            Arrays.asList(QUERY_ID, USER, DEFAULT_DB, SQL_STATEMENT, QUERY_TYPE,
                    START_TIME, END_TIME, TOTAL_TIME, QUERY_STATE));
    
    private class ProfileElement {
        public Map<String, String> infoStrings = Maps.newHashMap();  
        public String profileContent;
    }
    
    // only protect profileDeque; profileMap is concurrent, no need to protect
    private ReentrantReadWriteLock lock; 
    private ReadLock readLock;
    private WriteLock writeLock;

    private Deque<ProfileElement> profileDeque;
    private Map<String, ProfileElement> profileMap; // from QueryId to RuntimeProfile
    
    public static ProfileManager getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ProfileManager();
        }
        return INSTANCE;
    }
    
    private ProfileManager() {
        lock = new ReentrantReadWriteLock(true); 
        readLock = lock.readLock();
        writeLock = lock.writeLock();
        profileDeque = new LinkedList<ProfileElement>();
        profileMap = new ConcurrentHashMap<String, ProfileElement>();
    }
    
    public ProfileElement createElement(RuntimeProfile profile) {
        ProfileElement element = new ProfileElement();
        RuntimeProfile summaryProfile = profile.getChildList().get(0).first;
        for (String header : PROFILE_HEADERS) {
            element.infoStrings.put(header, summaryProfile.getInfoString(header));
        }
        element.profileContent = profile.toString();
        return element;
    }
    
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
        
        profileMap.put(queryId, element);
        writeLock.lock();
        try { 
            if (profileDeque.size() >= ARRAY_SIZE) {
                profileMap.remove(profileDeque.getFirst().infoStrings.get(QUERY_ID));
                profileDeque.removeFirst();
            }
            profileDeque.addLast(element);
        } finally {
            writeLock.unlock();
        }
    }
    
    public List<List<String>> getAllQueries() {
        List<List<String>> result = Lists.newArrayList();
        readLock.lock();
        try {
            Iterator reverse = profileDeque.descendingIterator();
            while (reverse.hasNext()) {
                ProfileElement element = (ProfileElement) reverse.next();
                Map<String, String> infoStrings = element.infoStrings;
                
                List<String> row = Lists.newArrayList();
                for (String str : PROFILE_HEADERS ) {
                    row.add(infoStrings.get(str));
                }
                result.add(row);
            }
        } finally {
            readLock.unlock();
        }
        return result;
    }
    
    public String getProfile(String queryID) {
        readLock.lock();
        try {
            ProfileElement element = profileMap.get(queryID);
            if (element == null) {
                return null;
            }
            
            return element.profileContent;
        } finally {
            readLock.unlock();
        }
    }
}
