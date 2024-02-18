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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.PatternMatcherException;
import org.apache.doris.common.io.Text;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public abstract class PrivTable {
    private static final Logger LOG = LogManager.getLogger(PrivTable.class);

    protected List<PrivEntry> entries = Lists.newArrayList();

    // see PrivEntry for more detail
    protected boolean isClassNameWrote = false;

    /*
     * Check if user@host has specified privilege
     */
    public boolean hasPriv(PrivPredicate wanted) {
        for (PrivEntry entry : entries) {
            // check priv
            if (entry.privSet.satisfy(wanted)) {
                return true;
            }
        }
        return false;
    }

    /*
     * Add an entry to priv table.
     * If entry already exists and errOnExist is false, we try to reset or merge the new priv entry with existing one.
     * NOTICE, this method does not set password for the newly added entry if this is a user priv table, the caller
     * need to set password later.
     */
    public PrivEntry addEntry(PrivEntry newEntry,
            boolean errOnExist, boolean errOnNonExist) throws DdlException {
        return addEntry(newEntry, errOnExist, errOnNonExist, false);
    }

    public PrivEntry addEntry(PrivEntry entry, boolean errOnExist, boolean errOnNonExist, boolean isMerge)
            throws DdlException {
        PrivEntry newEntry = entry;
        if (isMerge) {
            try {
                newEntry = entry.copy();
            } catch (AnalysisException | PatternMatcherException e) {
                LOG.error("exception when copy PrivEntry", e);
            }
        }

        PrivEntry existingEntry = getExistingEntry(newEntry);
        if (existingEntry == null) {
            if (errOnNonExist) {
                throw new DdlException("entry does not exist");
            }
            entries.add(newEntry);
            Collections.sort(entries);
            LOG.info("add priv entry: {}", newEntry);
            return newEntry;
        } else {
            if (errOnExist) {
                throw new DdlException("entry already exist");
            } else {
                mergePriv(existingEntry, newEntry);
                LOG.debug("merge priv entry: {}", existingEntry);
            }
        }
        return existingEntry;
    }


    public List<PrivEntry> getEntries() {
        return entries;
    }

    public void dropEntry(PrivEntry entry) {
        Iterator<PrivEntry> iter = entries.iterator();
        while (iter.hasNext()) {
            PrivEntry privEntry = iter.next();
            if (privEntry.keyMatch(entry)) {
                iter.remove();
                LOG.info("drop priv entry: {}", privEntry);
                break;
            }
        }
    }

    public void revoke(PrivEntry entry, boolean errOnNonExist,
            boolean deleteEntryWhenEmpty) throws DdlException {
        PrivEntry existingEntry = getExistingEntry(entry);
        if (existingEntry == null) {
            if (errOnNonExist) {
                ErrorReport.reportDdlException(ErrorCode.ERR_NONEXISTING_GRANT);
            }
            return;
        }

        // check if privs to be revoked exist in priv entry.
        PrivBitSet tmp = existingEntry.getPrivSet().copy();
        tmp.and(entry.getPrivSet());
        if (tmp.isEmpty()) {
            if (errOnNonExist) {
                ErrorReport.reportDdlException(ErrorCode.ERR_NONEXISTING_GRANT);
            }
            // there is no such priv, nothing need to be done
            return;
        }

        // revoke privs from existing priv entry
        LOG.debug("before revoke: {}, privs to be revoked: {}",
                existingEntry.getPrivSet(), entry.getPrivSet());
        tmp = existingEntry.getPrivSet().copy();
        tmp.xor(entry.getPrivSet());
        existingEntry.getPrivSet().and(tmp);
        LOG.debug("after revoke: {}", existingEntry);

        if (existingEntry.getPrivSet().isEmpty() && deleteEntryWhenEmpty) {
            // no priv exists in this entry, remove it
            dropEntry(existingEntry);
        }
    }


    // Get existing entry which is the keys match the given entry
    protected PrivEntry getExistingEntry(PrivEntry entry) {
        for (PrivEntry existingEntry : entries) {
            if (existingEntry.keyMatch(entry)) {
                return existingEntry;
            }
        }
        return null;
    }

    private void mergePriv(
            PrivEntry first, PrivEntry second) {
        first.getPrivSet().or(second.getPrivSet());
    }

    // for test only
    public void clear() {
        entries.clear();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Deprecated
    public static PrivTable read(DataInput in) throws IOException {
        String className = Text.readString(in);
        PrivTable privTable = null;
        try {
            Class<? extends PrivTable> derivedClass = (Class<? extends PrivTable>) Class.forName(className);
            privTable = derivedClass.newInstance();
            Class[] paramTypes = {DataInput.class};
            Method readMethod = derivedClass.getMethod("readFields", paramTypes);
            Object[] params = {in};
            readMethod.invoke(privTable, params);

            return privTable;
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException
                | SecurityException | IllegalArgumentException | InvocationTargetException e) {
            throw new IOException("failed read PrivTable", e);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("\n");
        for (PrivEntry privEntry : entries) {
            sb.append(privEntry).append("\n");
        }
        return sb.toString();
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            PrivEntry entry = PrivEntry.read(in);
            entries.add(entry);
        }
        Collections.sort(entries);
    }

    public void merge(PrivTable privTable) {
        for (PrivEntry entry : privTable.entries) {
            try {
                addEntry(entry, false, false, true);
            } catch (DdlException e) {
                //will no exception
                LOG.debug(e.getMessage());
            }
        }
    }
}
