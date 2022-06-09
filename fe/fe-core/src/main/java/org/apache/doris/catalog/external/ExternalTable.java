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

package org.apache.doris.catalog.external;

import org.apache.doris.alter.AlterCancelException;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * External table represent tables that are not self-managed by Doris.
 * Such as tables from hive, iceberg, es, etc.
 */
public class ExternalTable implements TableIf {

    @Override
    public void readLock() {

    }

    @Override
    public boolean tryReadLock(long timeout, TimeUnit unit) {
        return false;
    }

    @Override
    public void readUnlock() {

    }

    @Override
    public void writeLock() {

    }

    @Override
    public boolean writeLockIfExist() {
        return false;
    }

    @Override
    public boolean tryWriteLock(long timeout, TimeUnit unit) {
        return false;
    }

    @Override
    public void writeUnlock() {

    }

    @Override
    public boolean isWriteLockHeldByCurrentThread() {
        return false;
    }

    @Override
    public <E extends Exception> void writeLockOrException(E e) throws E {

    }

    @Override
    public void writeLockOrDdlException() throws DdlException {

    }

    @Override
    public void writeLockOrMetaException() throws MetaNotFoundException {

    }

    @Override
    public void writeLockOrAlterCancelException() throws AlterCancelException {

    }

    @Override
    public boolean tryWriteLockOrMetaException(long timeout, TimeUnit unit) throws MetaNotFoundException {
        return false;
    }

    @Override
    public <E extends Exception> boolean tryWriteLockOrException(long timeout, TimeUnit unit, E e) throws E {
        return false;
    }

    @Override
    public boolean tryWriteLockIfExist(long timeout, TimeUnit unit) {
        return false;
    }

    @Override
    public long getId() {
        return 0;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public Table.TableType getType() {
        return null;
    }

    @Override
    public List<Column> getFullSchema() {
        return null;
    }

    @Override
    public List<Column> getBaseSchema() {
        return null;
    }

    @Override
    public List<Column> getBaseSchema(boolean full) {
        return null;
    }

    @Override
    public void setNewFullSchema(List<Column> newSchema) {

    }

    @Override
    public Column getColumn(String name) {
        return null;
    }
}
