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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherException;
import org.apache.doris.common.io.Text;

import java.io.DataInput;
import java.io.IOException;

public class WorkloadGroupPrivEntry extends PrivEntry {

    protected PatternMatcher workloadGroupPattern;
    protected String origWorkloadGroupName;

    protected WorkloadGroupPrivEntry(PatternMatcher workloadGroupPattern,
            String origWorkloadGroupName, PrivBitSet privSet) {
        super(privSet);
        this.workloadGroupPattern = workloadGroupPattern;
        this.origWorkloadGroupName = origWorkloadGroupName;
    }

    public static WorkloadGroupPrivEntry create(String workloadGroupName, PrivBitSet privs)
            throws AnalysisException, PatternMatcherException {
        PatternMatcher workloadGroupPattern = PatternMatcher.createMysqlPattern(workloadGroupName,
                CaseSensibility.WORKLOAD_GROUP.getCaseSensibility());
        if (privs.containsNodePriv() || privs.containsDbTablePriv()) {
            throw new AnalysisException(
                    "Workload group privilege can not contains node or db table privileges: " + privs);
        }
        return new WorkloadGroupPrivEntry(workloadGroupPattern,
                workloadGroupName, privs);
    }

    public PatternMatcher getWorkloadGroupPattern() {
        return workloadGroupPattern;
    }

    public String getOrigWorkloadGroupName() {
        return origWorkloadGroupName;
    }

    @Override
    public int compareTo(PrivEntry other) {
        if (!(other instanceof WorkloadGroupPrivEntry)) {
            throw new ClassCastException("cannot cast " + other.getClass().toString() + " to " + this.getClass());
        }

        WorkloadGroupPrivEntry otherEntry = (WorkloadGroupPrivEntry) other;

        return origWorkloadGroupName.compareTo(otherEntry.origWorkloadGroupName);
    }

    @Override
    protected PrivEntry copy() throws AnalysisException, PatternMatcherException {
        return WorkloadGroupPrivEntry.create(this.getOrigWorkloadGroupName(), this.getPrivSet().copy());
    }

    @Override
    public boolean keyMatch(PrivEntry other) {
        if (!(other instanceof WorkloadGroupPrivEntry)) {
            return false;
        }

        WorkloadGroupPrivEntry otherEntry = (WorkloadGroupPrivEntry) other;
        return origWorkloadGroupName.equals(otherEntry.origWorkloadGroupName);
    }

    @Override
    public String toString() {
        return "origWorkloadGroup:" + origWorkloadGroupName + ", priv:" + privSet;
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        origWorkloadGroupName = Text.readString(in);
        try {
            workloadGroupPattern = PatternMatcher.createMysqlPattern(origWorkloadGroupName,
                    CaseSensibility.WORKLOAD_GROUP.getCaseSensibility());
        } catch (PatternMatcherException e) {
            throw new IOException(e);
        }
    }
}
