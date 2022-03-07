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

package org.apache.doris.meta;

import org.apache.doris.common.FeMetaVersion;

/*
 * MetaContext saved the current meta version.
 * And we need to create a thread local meta context for all threads which are about to reading meta.
 */
public class MetaContext {

    private int metaVersion;

    private static ThreadLocal<MetaContext> threadLocalInfo = new ThreadLocal<MetaContext>();

    public MetaContext() {

    }

    public void setMetaVersion(int metaVersion) {
        if (metaVersion < FeMetaVersion.MINIMUM_VERSION_REQUIRED) {
            throw new IllegalArgumentException("Could not set meta version to " 
                    + metaVersion + " since it is lower than minimum required version " 
                    + FeMetaVersion.MINIMUM_VERSION_REQUIRED);
        }
        this.metaVersion = metaVersion;
    }

    public int getMetaVersion() {
        return metaVersion;
    }

    public void setThreadLocalInfo() {
        threadLocalInfo.set(this);
    }
    
    public static MetaContext get() {
        return threadLocalInfo.get();
    }

    public static void remove() {
        threadLocalInfo.remove();
    }
}
