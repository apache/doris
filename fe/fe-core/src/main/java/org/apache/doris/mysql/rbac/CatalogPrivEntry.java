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

package org.apache.doris.mysql.rbac;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.mysql.privilege.PrivBitSet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CatalogPrivEntry extends PrivEntry {
    protected static final String ANY_CTL = "*";

    protected PatternMatcher ctlPattern;
    protected String origCtl;
    protected boolean isAnyCtl;

    protected CatalogPrivEntry() {
    }

    protected CatalogPrivEntry(
            PatternMatcher ctlPattern, String origCtl,
            PrivBitSet privSet) {
        super(privSet);
        this.ctlPattern = ctlPattern;
        this.origCtl = origCtl;
        if (origCtl.equals(ANY_CTL)) {
            isAnyCtl = true;
        }
    }

    public static CatalogPrivEntry create(String ctl, PrivBitSet privs)
            throws AnalysisException {
        PatternMatcher ctlPattern = createCtlPatternMatcher(ctl);

        if (privs.containsNodePriv() || privs.containsResourcePriv()) {
            throw new AnalysisException("Catalog privilege can not contains node or resource privileges: " + privs);
        }

        return new CatalogPrivEntry(ctlPattern, ctl, privs);
    }

    private static PatternMatcher createCtlPatternMatcher(String ctl) throws AnalysisException {
        boolean ctlCaseSensibility = CaseSensibility.CATALOG.getCaseSensibility();
        return PatternMatcher.createFlatPattern(ctl, ctlCaseSensibility, ctl.equals(ANY_CTL));
    }

    public PatternMatcher getCtlPattern() {
        return ctlPattern;
    }

    public String getOrigCtl() {
        return origCtl;
    }

    public boolean isAnyCtl() {
        return isAnyCtl;
    }

    @Override
    public int compareTo(PrivEntry other) {
        if (!(other instanceof CatalogPrivEntry)) {
            throw new ClassCastException("cannot cast " + other.getClass().toString() + " to " + this.getClass());
        }

        CatalogPrivEntry otherEntry = (CatalogPrivEntry) other;
        return compareAssist(origCtl, otherEntry.origCtl);
    }

    @Override
    public boolean keyMatch(PrivEntry other) {
        if (!(other instanceof CatalogPrivEntry)) {
            return false;
        }
        CatalogPrivEntry otherEntry = (CatalogPrivEntry) other;
        return origCtl.equals(otherEntry.origCtl);
    }

    @Override
    public String toString() {
        return String.format("catalog privilege. ctl: %s, priv: %s",
                origCtl, privSet.toString());
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
    }

}
