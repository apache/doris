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

package org.apache.doris.service;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class ExecuteEnvTest {
    int threadMaxNum = 10;
    int[] oids = new int[threadMaxNum];

    @Test
    public void testGetInstance() {
        Set<Thread> tds = new HashSet<Thread>();
        for (int i = 0; i < threadMaxNum; i++) {
            Thread td = new Thread(new MyTest(i, oids));
            tds.add(td);
            td.start();
        }

        for (Thread td : tds) {
            try {
                td.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        for (int i = 1; i < threadMaxNum; i++) {
            Assert.assertEquals(oids[i - 1], oids[i]);
        }
    }

    static class MyTest implements Runnable {
        public int index;
        public int[] oids;

        MyTest(int index, int[] oids) {
            this.index = index;
            this.oids = oids;
        }

        @Override
        public void run() {
            ExecuteEnv instance = ExecuteEnv.getInstance();
            int oid = instance.hashCode();
            oids[index] = oid;
        }
    }
}
