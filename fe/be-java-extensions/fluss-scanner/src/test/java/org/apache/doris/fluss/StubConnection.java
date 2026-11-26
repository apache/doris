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

package org.apache.doris.fluss;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.MultiTable;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;

/** A connection nobody uses for anything but lending it out and closing it. */
final class StubConnection implements Connection {

    interface CloseAction {
        void run() throws Exception;
    }

    private final CloseAction onClose;

    StubConnection(CloseAction onClose) {
        this.onClose = onClose;
    }

    @Override
    public Configuration getConfiguration() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Admin getAdmin() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Table getTable(TablePath tablePath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MultiTable getMultiTable() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Exception {
        onClose.run();
    }
}
