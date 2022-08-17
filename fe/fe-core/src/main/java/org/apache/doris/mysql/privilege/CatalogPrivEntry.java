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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.io.Text;
import org.apache.doris.datasource.InternalCatalog;

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

    protected CatalogPrivEntry(PatternMatcher userPattern, String user,
                               PatternMatcher hostPattern, String origHost,
                               PatternMatcher ctlPattern, String origCtl,
                               boolean isDomain, PrivBitSet privSet) {
        super(hostPattern, origHost, userPattern, user, isDomain, privSet);
        this.ctlPattern = ctlPattern;
        this.origCtl = origCtl;
        if (origCtl.equals(ANY_CTL)) {
            isAnyCtl = true;
        }
    }

    public static CatalogPrivEntry create(String user, String host, String ctl, boolean isDomain, PrivBitSet privs)
            throws AnalysisException {
        PatternMatcher hostPattern = PatternMatcher.createMysqlPattern(host, CaseSensibility.HOST.getCaseSensibility());

        PatternMatcher ctlPattern = createCtlPatternMatcher(ctl);

        PatternMatcher userPattern = PatternMatcher.createFlatPattern(user, CaseSensibility.USER.getCaseSensibility());

        if (privs.containsNodePriv() || privs.containsResourcePriv()) {
            throw new AnalysisException("Catalog privilege can not contains node or resource privileges: " + privs);
        }

        return new CatalogPrivEntry(userPattern, user, hostPattern, host, ctlPattern, ctl, isDomain, privs);
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
        return compareAssist(origUser, otherEntry.origUser,
                             origHost, otherEntry.origHost,
                             origCtl, otherEntry.origCtl);
    }

    @Override
    public boolean keyMatch(PrivEntry other) {
        if (!(other instanceof CatalogPrivEntry)) {
            return false;
        }

        CatalogPrivEntry otherEntry = (CatalogPrivEntry) other;
        return origUser.equals(otherEntry.origUser) && origHost.equals(otherEntry.origHost)
                && origCtl.equals(otherEntry.origCtl) && isDomain == otherEntry.isDomain;
    }

    @Override
    public String toString() {
        return String.format("catalog privilege. user: %s, host: %s, ctl: %s, priv: %s, set by resolver: %b",
                origUser, origHost, origCtl, privSet.toString(), isSetByDomainResolver);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = CatalogPrivEntry.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }
        super.write(out);
        Text.writeString(out, origCtl);
        isClassNameWrote = false;
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_111) {
            origCtl = Text.readString(in);
        } else {
            origCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
        }
        try {
            ctlPattern = createCtlPatternMatcher(origCtl);
        } catch (AnalysisException e) {
            throw new IOException(e);
        }
        isAnyCtl = origCtl.equals(ANY_CTL);
    }

}
