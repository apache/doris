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

public class TablePrivEntry extends DbPrivEntry {
    private static final String ANY_TBL = "*";

    private PatternMatcher tblPattern;
    private String origTbl;
    private boolean isAnyTbl;

    protected TablePrivEntry() {
    }

    private TablePrivEntry(
            PatternMatcher ctlPattern, String origCtl,
            PatternMatcher dbPattern, String origDb,
            PatternMatcher tblPattern, String origTbl,
            PrivBitSet privSet) {
        super(ctlPattern, origCtl, dbPattern, origDb, privSet);
        this.tblPattern = tblPattern;
        this.origTbl = origTbl;
        if (origTbl.equals(ANY_TBL)) {
            isAnyTbl = true;
        }
    }

    public static TablePrivEntry create(
            String ctl, String db, String tbl,
            PrivBitSet privs) throws AnalysisException {
        PatternMatcher dbPattern = PatternMatcher.createFlatPattern(
                db, CaseSensibility.DATABASE.getCaseSensibility(), db.equals(ANY_DB));
        PatternMatcher ctlPattern = PatternMatcher.createFlatPattern(
                ctl, CaseSensibility.CATALOG.getCaseSensibility(), ctl.equals(ANY_CTL));

        PatternMatcher tblPattern = PatternMatcher.createFlatPattern(
                tbl, CaseSensibility.TABLE.getCaseSensibility(), tbl.equals(ANY_TBL));

        if (privs.containsNodePriv() || privs.containsResourcePriv()) {
            throw new AnalysisException("Table privilege can not contains global or resource privileges: " + privs);
        }

        return new TablePrivEntry(
                ctlPattern, ctl, dbPattern, db, tblPattern, tbl, privs);
    }

    public PatternMatcher getTblPattern() {
        return tblPattern;
    }

    public String getOrigTbl() {
        return origTbl;
    }

    public boolean isAnyTbl() {
        return isAnyTbl;
    }

    @Override
    public int compareTo(PrivEntry other) {
        if (!(other instanceof TablePrivEntry)) {
            throw new ClassCastException("cannot cast " + other.getClass().toString() + " to " + this.getClass());
        }

        TablePrivEntry otherEntry = (TablePrivEntry) other;
        return compareAssist(
                origCtl, otherEntry.origCtl,
                origDb, otherEntry.origDb,
                origTbl, otherEntry.origTbl);
    }

    @Override
    public boolean keyMatch(PrivEntry other) {
        if (!(other instanceof TablePrivEntry)) {
            return false;
        }

        TablePrivEntry otherEntry = (TablePrivEntry) other;
        return origCtl.equals(otherEntry.origCtl) && origDb.equals(otherEntry.origDb)
                && origTbl.equals(otherEntry.origTbl);
    }

    @Override
    public String toString() {
        return String.format("table privilege.ctl: %s, db: %s, tbl: %s, priv: %s", origCtl, origDb, origTbl,
                privSet.toString());
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        origTbl = Text.readString(in);
        try {
            tblPattern = PatternMatcher.createMysqlPattern(origTbl, CaseSensibility.TABLE.getCaseSensibility());
        } catch (PatternMatcherException e) {
            throw new IOException(e);
        }
        isAnyTbl = origTbl.equals(ANY_TBL);
    }

    @Override
    protected PrivEntry copy() throws AnalysisException, PatternMatcherException {
        return TablePrivEntry.create(this.getOrigCtl(), this.getOrigDb(), this.getOrigTbl(), this.getPrivSet().copy());
    }
}
