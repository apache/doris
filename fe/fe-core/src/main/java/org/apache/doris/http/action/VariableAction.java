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

package org.apache.doris.http.action;

import org.apache.doris.analysis.SetType;
import org.apache.doris.common.Config;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;
import org.apache.doris.qe.VariableMgr;

import com.google.common.collect.Lists;

import io.netty.handler.codec.http.HttpMethod;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class VariableAction extends WebBaseAction {

    public VariableAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/variable", new VariableAction(controller));
    }

    @Override
    public void executeGet(BaseRequest request, BaseResponse response) {
        getPageHeader(request, response.getContent());
        
        appendConfigureInfo(response.getContent());
        appendVariableInfo(response.getContent());
        
        getPageFooter(response.getContent());
        writeResponse(request, response);
    }
    
    public void appendConfigureInfo(StringBuilder buffer) {
        buffer.append("<h2>Configure Info</h2>");
        buffer.append("<pre>");
        HashMap<String, String> confmap;
        try {
            confmap = Config.dump();
            List<String> keyList = Lists.newArrayList(confmap.keySet());
            Collections.sort(keyList);
            for (String key : keyList) {
                buffer.append(key + "=" + confmap.get(key) + "\n");
            }
        } catch (Exception e) {
            buffer.append("read conf exception" + e.toString());
        }
        buffer.append("</pre>");
    }
    
    private void appendVariableInfo(StringBuilder buffer) {
        buffer.append("<h2>Variable Info</h2>");
        buffer.append("<pre>");
        List<List<String>> variableInfo = VariableMgr.dump(SetType.GLOBAL, null, null);
        for (List<String> list : variableInfo) {
            buffer.append(list.get(0) + "=" + list.get(1) + "\n");
        }
        buffer.append("</pre>");
    }

}
