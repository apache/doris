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
import java.util.Collections;
import java.util.HashMap;

/**
 * Used for collecting information obtained from the profile.
 */
public class ProfileStatistics {
    class PipelineXStatistics {
        ArrayList<String> sinkInfos;
        ArrayList<String> infos;

        PipelineXStatistics() {
            sinkInfos = new ArrayList<String>();
            infos = new ArrayList<String>();
        }

        void add(boolean isSink, String str) {
            if (isSink) {
                sinkInfos.add(str);
            } else {
                infos.add(str);
            }
        }

        String to_String() {
            return null;
        }

        void getInfo(ArrayList<String> infos, String prefix, StringBuilder str) {
            Collections.sort(infos);
            for (String info : infos) {
                str.append(prefix + info + "\n");
            }
        }

        void getOperator(String prefix, StringBuilder str) {
            getInfo(infos, prefix, str);
        }

        void getSink(String prefix, StringBuilder str) {
            getInfo(sinkInfos, prefix, str);
        }
    }

    // Record statistical information based on nodeid.
    private HashMap<Integer, ArrayList<String>> statisticalInfo;

    // Record statistical information based on nodeid(use in pipelineX).
    private HashMap<Integer, PipelineXStatistics> pipelineXStatisticalInfo;

    // Record statistical information based on fragment ID.
    // "Currently used to record sink nodes.
    private HashMap<Integer, ArrayList<String>> fragmentInfo;

    private int fragmentId;

    private boolean isDataSink;

    private boolean isPipelineX;

    public ProfileStatistics(boolean isPipelineX) {
        statisticalInfo = new HashMap<Integer, ArrayList<String>>();
        fragmentInfo = new HashMap<Integer, ArrayList<String>>();
        pipelineXStatisticalInfo = new HashMap<Integer, PipelineXStatistics>();
        fragmentId = 0;
        isDataSink = false;
        this.isPipelineX = isPipelineX;
    }

    private void addPlanNodeInfo(int id, String info) {
        if (!statisticalInfo.containsKey(id)) {
            statisticalInfo.put(id, new ArrayList<String>());
        }
        statisticalInfo.get(id).add(info);
    }

    private void addPipelineXPlanNodeInfo(boolean isSink, int id, String info) {
        if (!pipelineXStatisticalInfo.containsKey(id)) {
            pipelineXStatisticalInfo.put(id, new PipelineXStatistics());
        }
        pipelineXStatisticalInfo.get(id).add(isSink, info);
    }

    private void addDataSinkInfo(String info) {
        if (fragmentInfo.get(fragmentId) == null) {
            fragmentInfo.put(fragmentId, new ArrayList<String>());
        }
        fragmentInfo.get(fragmentId).add(info);
    }

    public void addInfoFromProfile(RuntimeProfile profile, String name, String info) {
        if (isPipelineX) {
            addPipelineXPlanNodeInfo(profile.sinkOperator(), profile.nodeId(), name + ": " + info);
        } else {
            if (isDataSink) {
                addDataSinkInfo(name + ": " + info);
            } else {
                addPlanNodeInfo(profile.nodeId(), name + ": " + info);
            }
        }
    }

    public boolean hasInfo(int id) {
        if (isPipelineX) {
            return pipelineXStatisticalInfo.containsKey(id);
        } else {
            return statisticalInfo.containsKey(id);
        }
    }

    public void getInfoById(int id, String prefix, StringBuilder str) {
        if (!hasInfo(id)) {
            return;
        }
        if (isPipelineX) {
            getPipelineXInfoById(id, prefix, str);
        } else {
            ArrayList<String> infos = statisticalInfo.get(id);
            Collections.sort(infos);
            for (String info : infos) {
                str.append(prefix + info + "\n");
            }
        }
    }

    private void getPipelineXInfoById(int id, String prefix, StringBuilder str) {
        PipelineXStatistics statistics = pipelineXStatisticalInfo.get(id);
        str.append(prefix + "Operator: \n");
        statistics.getOperator(prefix + "  ", str);

        str.append(prefix + "Sink: \n");
        statistics.getSink(prefix + "  ", str);
    }

    public void getDataSinkInfo(int fragmentIdx, String prefix, StringBuilder str) {
        if (!fragmentInfo.containsKey(fragmentIdx)) {
            return;
        }
        ArrayList<String> infos = fragmentInfo.get(fragmentIdx);
        Collections.sort(infos);
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
