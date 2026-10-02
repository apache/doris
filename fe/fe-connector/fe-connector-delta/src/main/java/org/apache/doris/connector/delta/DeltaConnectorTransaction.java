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

package org.apache.doris.connector.delta;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTransaction;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;
import org.apache.doris.thrift.TConnectorFileCommitData;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/** Statement transaction bridging BE file reports to one atomic Kernel append or replacement. */
final class DeltaConnectorTransaction implements ConnectorTransaction {
    private enum State {
        NEW, OPEN, COMMITTING, COMMITTED, ROLLED_BACK
    }

    private final long transactionId;
    private final DeltaConnectorMetadata metadata;
    // BE report threads may race with one another. Only state and report bookkeeping hold this monitor;
    // catalog, storage, and Kernel operations always run outside it.
    private final Map<String, TConnectorFileCommitData> reports = new LinkedHashMap<>();
    private State state = State.NEW;
    private DeltaInsertHandle insert;
    private long writtenRows;

    DeltaConnectorTransaction(long transactionId, DeltaConnectorMetadata metadata) {
        this.transactionId = transactionId;
        this.metadata = metadata;
    }

    DeltaWriteConfig beginWrite(ConnectorSession session, ConnectorWriteHandle handle) {
        synchronized (this) {
            requireState(State.NEW);
        }
        DeltaWriteConfig config = metadata.getWriteConfig(session, handle.getTableHandle(), handle.getColumns());
        DeltaInsertHandle opened = handle.isOverwrite()
                ? metadata.beginInsertOverwrite(session, handle.getTableHandle(), handle.getColumns())
                : metadata.beginInsert(session, handle.getTableHandle(), handle.getColumns());
        try {
            synchronized (this) {
                requireState(State.NEW);
                insert = opened;
                state = State.OPEN;
            }
        } catch (RuntimeException e) {
            // Statement cancellation can close the transaction while Kernel is opening its resources.
            // An opened handle that lost the publication race still owns those resources and must close.
            try {
                metadata.abortInsert(null, opened);
            } catch (RuntimeException cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            throw e;
        }
        return config;
    }

    @Override
    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public void addCommitData(byte[] fragment) {
        TConnectorFileCommitData report = new TConnectorFileCommitData();
        try {
            new TDeserializer(new TBinaryProtocol.Factory()).deserialize(report, fragment);
        } catch (TException e) {
            throw new DorisConnectorException("Invalid Delta file report for transaction " + transactionId, e);
        }
        DeltaFileCommitInfo file = toFileCommitInfo(report);
        synchronized (this) {
            requireState(State.OPEN);
            TConnectorFileCommitData previous = reports.get(file.getFilePath());
            if (previous != null) {
                if (!previous.equals(report)) {
                    throw new DorisConnectorException("Conflicting Delta file reports for " + file.getFilePath());
                }
                return;
            }
            writtenRows = Math.addExact(writtenRows, file.getRowCount());
            reports.put(file.getFilePath(), report);
        }
    }

    @Override
    public synchronized long getUpdateCnt() {
        return writtenRows;
    }

    @Override
    public synchronized OptionalLong getOriginalRowCount() {
        return insert == null ? OptionalLong.empty() : insert.getOriginalRowCount();
    }

    @Override
    public void commit() {
        List<TConnectorFileCommitData> committedReports;
        DeltaInsertHandle committing;
        synchronized (this) {
            requireState(State.OPEN);
            state = State.COMMITTING;
            committing = insert;
            committedReports = new ArrayList<>(reports.values());
        }
        try {
            List<DeltaFileCommitInfo> files = new ArrayList<>(committedReports.size());
            for (TConnectorFileCommitData report : committedReports) {
                files.add(toFileCommitInfo(report));
            }
            metadata.finishFileInsert(null, committing, files);
            synchronized (this) {
                state = State.COMMITTED;
            }
        } catch (RuntimeException e) {
            try {
                metadata.abortInsert(null, committing);
            } catch (RuntimeException cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            } finally {
                synchronized (this) {
                    state = State.ROLLED_BACK;
                }
            }
            throw e;
        }
    }

    @Override
    public void rollback() {
        DeltaInsertHandle aborting;
        synchronized (this) {
            if (state == State.ROLLED_BACK || state == State.COMMITTED) {
                return;
            }
            if (state == State.COMMITTING) {
                throw new DorisConnectorException("Delta transaction " + transactionId + " is committing");
            }
            aborting = insert;
            state = State.ROLLED_BACK;
            reports.clear();
        }
        if (aborting != null) {
            metadata.abortInsert(null, aborting);
        }
    }

    @Override
    public void close() {
        rollback();
        synchronized (this) {
            reports.clear();
        }
    }

    @Override
    public String profileLabel() {
        return "DELTA";
    }

    private void requireState(State expected) {
        if (state != expected) {
            throw new DorisConnectorException("Delta transaction " + transactionId
                    + " is " + state + ", expected " + expected);
        }
    }

    private static DeltaFileCommitInfo toFileCommitInfo(TConnectorFileCommitData report) {
        return new DeltaFileCommitInfo(report.getFilePath(), report.getRowCount(),
                report.getFileSize(), report.getModificationTime(),
                report.isSetPartitionValues() ? report.getPartitionValues() : Map.of(),
                report.isSetNullPartitionColumns() ? report.getNullPartitionColumns() : java.util.Set.of());
    }
}
