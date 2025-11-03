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
// This file is copied from
// https://github.com/klout/brickhouse/blob/master/src/main/java/brickhouse/udf/collect/SessionizeUDF.java
// and modified by Doris

package org.apache.doris.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.UUID;

@Description(
    name = "sessionize",
    value = "_FUNC_(string, timestamp) - Returns a session id for the given id and ts(long). Optional third parameter to specify interval tolerance in milliseconds",
    extended = "SELECT _FUNC_(uid, ts), uid, ts, event_type from foo;")

public class SessionizeUDF extends UDF {
    private String lastUid = null;
    private long lastTS = 0;
    private String lastUUID = null;


    public String evaluate(String uid, long ts, int tolerance) {
        lastUUID = UUID.nameUUIDFromBytes("test".getBytes()).toString();
        return lastUUID;
    }

    private Boolean timeStampCompare(long lastTS, long ts, int ms) {
        try {
            long difference = ts - lastTS;
            return (Math.abs((int) difference) < ms) ? true : false;
        } catch (ArithmeticException e) {
            return false;
        }
    }
}

