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

package org.apache.doris.qe;

import org.apache.doris.common.Config;

import java.lang.reflect.Field;
import java.util.Random;

/**
 * Handler for fuzzy variable
 */
public class FuzzyVarHandlers {
    public interface VarHandler {
        void handle(Field field, Object obj, Random random) throws Exception;
    }

    static class DefaultVarHandler implements VarHandler {
        @Override
        public void handle(Field field, Object obj, Random random) throws Exception {
        }
    }

    static class FuzzyRandomBool implements VarHandler {
        @Override
        public void handle(Field f, Object obj, Random random) throws Exception {
            boolean val = random.nextBoolean();
            setField(obj, f, String.valueOf(val));
        }
    }

    static class FuzzyPartitionedHashNodeRowsThreshold implements VarHandler {
        @Override
        public void handle(Field f, Object obj, Random random) throws Exception {
            int val = random.nextBoolean() ? 8 : 1048576;
            setField(obj, f, String.valueOf(val));
        }
    }

    static class FuzzyParallelExecNum implements VarHandler {
        @Override
        public void handle(Field f, Object obj, Random random) throws Exception {
            int val = random.nextInt(8) + 1;
            switch (f.getType().getSimpleName()) {
                case "int":
                    f.setInt(obj, val);
                    break;
                default:
                    throw new Exception("invalid field type: " + f.getType() + ", var: " + f.getName());
            }
        }
    }

    static class FuzzyExternalSortBytesThreshold implements VarHandler {
        @Override
        public void handle(Field f, Object obj, Random random) throws Exception {
            int randomInt = random.nextInt(4);
            long val;
            switch (randomInt) {
                case 0:
                    val = 0;
                    break;
                case 1:
                    val = 1;
                    break;
                case 2:
                    val = 1024 * 1024;
                    break;
                default:
                    val = 100 * 1024 * 1024 * 1024;
                    break;
            }
            setField(obj, f, String.valueOf(val));
        }
    }

    static class FuzzyPullRequestIdTrue implements VarHandler {
        @Override
        public void handle(Field f, Object obj, Random random) throws Exception {
            boolean val = Config.pull_request_id % 2 == 1;
            setField(obj, f, String.valueOf(val));
        }
    }

    static class FuzzyPullRequestIdFalse implements VarHandler {
        @Override
        public void handle(Field f, Object obj, Random random) throws Exception {
            boolean val = Config.pull_request_id % 2 != 1;
            setField(obj, f, String.valueOf(val));
        }
    }

    static class FuzzyRewriteOrToInPredicateThreshold implements VarHandler {
        @Override
        public void handle(Field f, Object obj, Random random) throws Exception {
            int randomInt = random.nextInt(4);
            int val;
            if (randomInt % 2 == 0) {
                val = 100000;
            } else {
                val = 2;
            }
            setField(obj, f, String.valueOf(val));
        }
    }

    static void setField(Object instance, Field f, String val) throws Exception {
        switch (f.getType().getSimpleName()) {
            case "boolean":
                f.setBoolean(instance, Boolean.valueOf(val));
                break;
            case "int":
                f.setInt(instance, Integer.valueOf(val));
                break;
            case "long":
                f.setLong(instance, Long.valueOf(val));
                break;
            default:
                throw new Exception("invalid field type: " + f.getType() + ", var: " + f.getName());
        }
    }
}
