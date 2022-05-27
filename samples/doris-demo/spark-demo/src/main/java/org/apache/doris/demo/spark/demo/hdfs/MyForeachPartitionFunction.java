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

package org.apache.doris.demo.spark.demo.hdfs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.demo.spark.util.DorisStreamLoad;
import org.apache.doris.demo.spark.vo.TestVo;
import scala.runtime.AbstractFunction1;
import scala.collection.AbstractIterator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


@Slf4j
public class MyForeachPartitionFunction extends AbstractFunction1 implements Serializable {
    DorisStreamLoad dorisStreamLoad;

    public MyForeachPartitionFunction(Map<String, Object> parameters) {
        dorisStreamLoad = new DorisStreamLoad(parameters.get("hostPort").toString(), parameters.get("db").toString(), parameters.get("tbl").toString(), parameters.get("user").toString(), parameters.get("password").toString(), "", "");
    }

    @Override
    public Object apply(Object v1) {
        List usersList = new ArrayList<TestVo>();
        while (((AbstractIterator) v1).hasNext()) {
            String[] dataSplit = ((AbstractIterator) v1).next().toString().split(",");
            TestVo testVo = new TestVo();
            testVo.setName(dataSplit[0]);
            testVo.setAge(Integer.parseInt(dataSplit[1]));
            usersList.add(testVo);
        }
        DorisStreamLoad.LoadResponse loadResponse = dorisStreamLoad.loadBatch(JSONArray.parseArray(JSON.toJSONString(usersList)).toString());
        log.info(loadResponse.respContent);
        return null;
    }

    @Override
    public String toString() {
        return "";
    }

    @Override
    public Function1 andThen(Function1 g) {
        return null;
    }

    @Override
    public Function1 compose(Function1 g) {
        return null;
    }
}
