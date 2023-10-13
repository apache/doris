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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.PatternMatcherException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;

public class GlobalPrivEntry extends PrivEntry {
    private static final Logger LOG = LogManager.getLogger(GlobalPrivEntry.class);

    @Deprecated
    protected byte[] password;
    @Deprecated
    protected UserIdentity domainUserIdent;

    protected GlobalPrivEntry() {
    }

    protected GlobalPrivEntry(PrivBitSet privSet) {
        super(privSet);
    }

    public static GlobalPrivEntry create(PrivBitSet privs) {
        return new GlobalPrivEntry(privs);
    }

    @Override
    public int compareTo(PrivEntry other) {
        if (!(other instanceof GlobalPrivEntry)) {
            throw new ClassCastException("cannot cast " + other.getClass().toString() + " to " + this.getClass());
        }

        return 0;
    }

    @Override
    public boolean keyMatch(PrivEntry other) {
        if (!(other instanceof GlobalPrivEntry)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return privSet.toString();
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int passwordLen = in.readInt();
        password = new byte[passwordLen];
        in.readFully(password);
    }

    @Override
    protected PrivEntry copy() throws AnalysisException, PatternMatcherException {
        return GlobalPrivEntry.create(this.getPrivSet().copy());
    }
}
