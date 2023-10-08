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

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Used for collecting information obtained from the profile.
 */
public class ProfileStatistics {
    // Record statistical information based on nodeid.
    private HashMap<Integer, ArrayList<String>> statisticalInfo;

    // Record statistical information based on fragment ID.
    // "Currently used to record sink nodes.
    private HashMap<Integer, ArrayList<String>> fragmentInfo;

    private int fragmentId;

    private boolean isDataSink;

    public ProfileStatistics() {
        statisticalInfo = new HashMap<Integer, ArrayList<String>>();
        fragmentInfo = new HashMap<Integer, ArrayList<String>>();
        fragmentId = 0;
        isDataSink = false;
    }

    private void addPlanNodeInfo(int id, String info) {
        if (!statisticalInfo.containsKey(id)) {
            statisticalInfo.put(id, new ArrayList<String>());
        }
        statisticalInfo.get(id).add(info);
    }

    private void addDataSinkInfo(String info) {
        if (fragmentInfo.get(fragmentId) == null) {
            fragmentInfo.put(fragmentId, new ArrayList<String>());
        }
        fragmentInfo.get(fragmentId).add(info);
    }

    public void addInfoFromProfile(RuntimeProfile profile, String info) {
        if (isDataSink) {
            addDataSinkInfo(info);
        } else {
            addPlanNodeInfo(profile.nodeId(), info);
        }
    }

    public boolean hasInfo(int id) {
        return statisticalInfo.containsKey(id);
    }

    public void getInfoById(int id, String prefix, StringBuilder str) {
        if (!hasInfo(id)) {
            return;
        }
        ArrayList<String> infos = statisticalInfo.get(id);
        for (String info : infos) {
            str.append(prefix + info + "\n");
        }
    }

    public void getDataSinkInfo(int fragmentIdx, String prefix, StringBuilder str) {
        if (!fragmentInfo.containsKey(fragmentIdx)) {
            return;
        }
        ArrayList<String> infos = fragmentInfo.get(fragmentIdx);
        for (String info : infos) {
            str.append(prefix + info + "\n");
        }
    }

    public void setFragmentId(int id) {
        this.fragmentId = id;
    }

    public void setIsDataSink(boolean dataSink) {
        this.isDataSink = dataSink;
    }

}
