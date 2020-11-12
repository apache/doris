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

import java.io.Closeable;

/*
 * This class is used to control the behavior of certain toSql() methods. The usage is as follows:
 *
 * try (ToSqlContext toSqlContext =ToSqlContext.getOrNewThreadLocalContext()) {
 *     toSqlContext.setNeedSlotRefId(false); // set some property
 *     inlineViewDef = viewDefStmt.toSql(); // call some toSql() methods
 * }
 *
 * This class implements "Closable" interface, and it should be closed right after being used.
 * To prevent it from affecting other following logic.
 *
 */
public class ToSqlContext implements Closeable {

    // Used to control whether to output the slotId in the toSql() method of SlotRef.
    private boolean needSlotRefId;

    private static ThreadLocal<ToSqlContext> threadLocalInfo = new ThreadLocal<ToSqlContext>();

    public ToSqlContext() {

    }

    public void setNeedSlotRefId(boolean needSlotRefId) {
        this.needSlotRefId = needSlotRefId;
    }

    public boolean isNeedSlotRefId() {
        return needSlotRefId;
    }

    public static ToSqlContext get() {
        return threadLocalInfo.get();
    }

    public static ToSqlContext getOrNewThreadLocalContext() {
        ToSqlContext toSqlContext = threadLocalInfo.get();
        if (toSqlContext == null) {
            toSqlContext = new ToSqlContext();
            threadLocalInfo.set(toSqlContext);
        }
        return toSqlContext;
    }

    @Override
    public void close() {
        threadLocalInfo.remove();
    }
}
