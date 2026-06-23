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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TIcebergCommitData;

import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.util.ArrayList;
import java.util.List;

/**
 * Iceberg connector transaction (ports the legacy
 * {@code org.apache.doris.datasource.iceberg.IcebergTransaction} write lifecycle to the connector SPI).
 *
 * <p>Holds a single SDK {@link Transaction} / {@link Table} for one SQL statement and the accumulated
 * commit fragments ({@link TIcebergCommitData}, fed back from BE via {@link #addCommitData}). The SDK
 * transaction is opened — through the {@link IcebergCatalogOps} seam wrapped in the auth context — by
 * {@link #beginWrite(String, String)}; {@link #commit()} flushes it with {@code commitTransaction()}.</p>
 *
 * <p><b>P6.3-T03 = skeleton.</b> This task wires the transaction holder, the 14-field commit-fragment
 * accumulation, {@link #getUpdateCnt()}, and the commit/rollback shells. The op selection (AppendFiles
 * / ReplacePartitions / OverwriteFiles / RowDelta) and the {@code IcebergWriterHelper} equivalents that
 * stage files onto the transaction <em>before</em> {@link #commit()} — together with the begin-time
 * guards (format-version &ge; 2 for delete/merge, branch validation, {@code baseSnapshotId} capture) and
 * the commit-time validation suite — land in P6.3-T04/T05.</p>
 *
 * <p><b>Gate-closed / dormant.</b> Iceberg is not in {@code SPI_READY_TYPES} until the P6.6 cutover, so
 * nothing routes plugin-driven iceberg writes through this class yet. The txn-id is the engine-allocated
 * Doris global id, so the generic {@code PluginDrivenTransactionManager} registers it in both the
 * per-manager map and {@code GlobalExternalTransactionInfoMgr} (the BE&rarr;FE report path finds the txn
 * by this id) — no per-connector registration code is needed, mirroring maxcompute.</p>
 */
public class IcebergConnectorTransaction implements ConnectorTransaction {

    private static final Logger LOG = LogManager.getLogger(IcebergConnectorTransaction.class);

    private final long transactionId;
    private final IcebergCatalogOps catalogOps;
    private final ConnectorContext context;
    private final List<TIcebergCommitData> commitDataList = new ArrayList<>();

    // The single SDK transaction / table, opened lazily by beginWrite (the write plan binds the target
    // table only when the sink is planned). volatile: addCommitData / commit may run on different threads.
    private volatile Transaction transaction;
    private volatile Table table;

    public IcebergConnectorTransaction(long transactionId, IcebergCatalogOps catalogOps,
            ConnectorContext context) {
        this.transactionId = transactionId;
        this.catalogOps = catalogOps;
        this.context = context;
    }

    /**
     * Opens the single SDK transaction for {@code db.tableName}, loading the table through the
     * {@link IcebergCatalogOps} seam wrapped in the FE-injected auth context (Kerberos UGI), mirroring
     * {@code IcebergConnectorMetadata.loadTable}. Op-specific guards (format-version, branch validation)
     * and {@code baseSnapshotId} capture are P6.3-T04.
     *
     * <p>Both {@code loadTable} AND {@code newTransaction()} run inside {@code executeAuthenticated},
     * mirroring legacy {@code IcebergTransaction.beginInsert} (which wraps the same two calls in one
     * authenticated block): {@code BaseTable.newTransaction()} issues an unconditional
     * {@code TableOperations.refresh()} — a remote metastore call (HMS Thrift / Glue / REST) — so it must
     * carry the same auth context as the load, or a Kerberized catalog write would fail. (UT-invisible:
     * the offline {@link InMemoryCatalog} needs no auth; verified at P6.6 docker on a Kerberized HMS.)</p>
     */
    public void beginWrite(String db, String tableName) {
        try {
            context.executeAuthenticated(() -> {
                Table loaded = catalogOps.loadTable(db, tableName);
                this.table = loaded;
                this.transaction = loaded.newTransaction();
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to load iceberg table for write: " + e.getMessage(), e);
        }
    }

    @Override
    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public void addCommitData(byte[] commitFragment) {
        TIcebergCommitData data = new TIcebergCommitData();
        try {
            new TDeserializer(new TBinaryProtocol.Factory()).deserialize(data, commitFragment);
        } catch (TException e) {
            throw new DorisConnectorException("failed to deserialize Iceberg commit data", e);
        }
        synchronized (this) {
            commitDataList.add(data);
        }
    }

    /**
     * Affected-row count for the statement, ported verbatim from legacy
     * {@code IcebergTransaction.getUpdateCnt}: prefer {@code affected_rows} over {@code row_count}, split
     * data-file rows from delete-file rows (position deletes / deletion vectors), and return the data
     * rows when present, else the delete rows. For UPDATE/MERGE the data rows already equal the affected
     * rows; the internal position deletes must not be double-counted.
     */
    @Override
    public long getUpdateCnt() {
        long dataRows = 0;
        long deleteRows = 0;
        for (TIcebergCommitData commitData : commitDataList) {
            long affectedRows = commitData.isSetAffectedRows()
                    ? commitData.getAffectedRows()
                    : commitData.getRowCount();
            if (commitData.isSetFileContent()
                    && (commitData.getFileContent() == TFileContent.POSITION_DELETES
                    || commitData.getFileContent() == TFileContent.DELETION_VECTOR)) {
                deleteRows += affectedRows;
            } else {
                dataRows += affectedRows;
            }
        }
        return dataRows > 0 ? dataRows : deleteRows;
    }

    @Override
    public String profileLabel() {
        return "ICEBERG";
    }

    @Override
    public void commit() {
        if (transaction == null) {
            throw new DorisConnectorException("no active iceberg transaction to commit");
        }
        try {
            transaction.commitTransaction();
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to commit iceberg transaction: " + e.getMessage(), e);
        }
    }

    @Override
    public void rollback() {
        // Insert-mode: nothing to undo on the FE side — an uncommitted SDK transaction simply discards
        // its pending manifests (legacy IcebergTransaction.rollback no-ops the insert path). The rewrite
        // path's file-list cleanup is a P6.4 procedure concern, out of scope here.
        LOG.info("Iceberg transaction {} rollback called; uncommitted manifests will be discarded.",
                transactionId);
    }

    @Override
    public void close() {
        // No resources to release: the SDK transaction holds no connections of its own.
    }

    /** Package-visible accessors for the op-selection / commit logic in P6.3-T04 (and the unit tests). */
    Transaction getTransaction() {
        return transaction;
    }

    Table getTable() {
        return table;
    }

    List<TIcebergCommitData> getCommitDataList() {
        return commitDataList;
    }
}
