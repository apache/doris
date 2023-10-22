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

        int sinkInstance = 0;
        int operatorInstance = 0;

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

        void updateInstance(boolean isSink, int instance) {
            if (isSink) {
                sinkInstance = Math.max(sinkInstance, instance);
            } else {
                operatorInstance = Math.max(operatorInstance, instance);
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
            str.append(prefix + "Instance num : " + operatorInstance + "\n");
            getInfo(infos, prefix, str);
        }

        void getSink(String prefix, StringBuilder str) {
            str.append(prefix + "Instance num : " + sinkInstance + "\n");
            getInfo(sinkInfos, prefix, str);
        }
    }

    // Record statistical information based on nodeid.
    private HashMap<Integer, ArrayList<String>> statisticalInfo;
    private HashMap<Integer, Integer> statisticalInfoInstance;

    // Record statistical information based on nodeid(use in pipelineX).
    private HashMap<Integer, PipelineXStatistics> pipelineXStatisticalInfo;

    // Record statistical information based on fragment ID.
    // "Currently used to record sink nodes.
    private HashMap<Integer, ArrayList<String>> fragmentInfo;
    private HashMap<Integer, Integer> fragmentInfoInstance;
    private int fragmentId;

    private boolean isDataSink;

    private boolean isPipelineX;

    public ProfileStatistics(boolean isPipelineX) {
        statisticalInfo = new HashMap<Integer, ArrayList<String>>();
        statisticalInfoInstance = new HashMap<Integer, Integer>();

        fragmentInfo = new HashMap<Integer, ArrayList<String>>();
        pipelineXStatisticalInfo = new HashMap<Integer, PipelineXStatistics>();
        fragmentInfoInstance = new HashMap<Integer, Integer>();
        fragmentId = 0;
        isDataSink = false;
        this.isPipelineX = isPipelineX;
    }

    private void addPipelineXPlanNodeInfo(boolean isSink, int id, String info, int instance) {
        if (!pipelineXStatisticalInfo.containsKey(id)) {
            pipelineXStatisticalInfo.put(id, new PipelineXStatistics());
        }
        pipelineXStatisticalInfo.get(id).add(isSink, info);
        pipelineXStatisticalInfo.get(id).updateInstance(isSink, instance);
    }

    private void addPlanNodeInfo(int id, String info, int instance) {
        if (!statisticalInfo.containsKey(id)) {
            statisticalInfo.put(id, new ArrayList<String>());
            statisticalInfoInstance.put(id, new Integer(0));
        }
        statisticalInfo.get(id).add(info);
        int ins = statisticalInfoInstance.get(id);
        ins = Math.max(ins, instance);
        statisticalInfoInstance.put(id, ins);
    }

    private void addDataSinkInfo(String info, int instance) {
        if (fragmentInfo.get(fragmentId) == null) {
            fragmentInfo.put(fragmentId, new ArrayList<String>());
            fragmentInfoInstance.put(fragmentId, new Integer(0));
        }
        fragmentInfo.get(fragmentId).add(info);
        int ins = fragmentInfoInstance.get(fragmentId);
        ins = Math.max(ins, instance);
        fragmentInfoInstance.put(fragmentId, ins);
    }

    public void addInfoFromProfile(RuntimeProfile profile, String name, String info, int instance) {
        if (isPipelineX) {
            addPipelineXPlanNodeInfo(profile.sinkOperator(), profile.nodeId(), name + ": " + info, instance);
        } else {
            if (isDataSink) {
                addDataSinkInfo(name + ": " + info, instance);
            } else {
                addPlanNodeInfo(profile.nodeId(), name + ": " + info, instance);
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
            str.append(prefix + "Instance num :" + statisticalInfoInstance.get(id) + "\n");
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
        str.append(prefix + "Instance num :" + fragmentInfoInstance.get(fragmentIdx) + "\n");
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
