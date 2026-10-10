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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;
import org.apache.doris.thrift.TPaimonCommitMessage;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Commit-state handling of {@link PaimonConnectorTransaction}, ported from the branch-4.x
 * {@code PaimonTransactionTest}: a commit whose outcome was lost is reconciled against the snapshots
 * Paimon actually published, a transaction whose outcome stays unknown is never aborted, a failure
 * before the commit started stays abortable, and the commit payloads the BEs report are
 * de-duplicated by their exact content.
 *
 * <p>No mocking framework (this module has none by convention). The table is a real on-disk Paimon
 * table, so reconciliation reads real snapshot files; only the committer it hands out is scripted,
 * to fail or to record where a test needs it.
 */
public class PaimonConnectorTransactionTest {

    private static final long TRANSACTION_ID = 12345L;
    private static final DataField ID = new DataField(0, "id", DataTypes.INT());

    @TempDir
    Path warehouse;

    private FileStoreTable table;

    @BeforeEach
    public void createTable() throws Exception {
        org.apache.paimon.fs.Path location =
                new org.apache.paimon.fs.Path("file://" + warehouse + "/db.db/tbl");
        LocalFileIO fileIO = LocalFileIO.create();
        new SchemaManager(fileIO, location).createTable(new Schema(
                Collections.singletonList(ID), Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), ""));
        table = FileStoreTableFactory.create(fileIO, location);
    }

    @Test
    public void publishedSnapshotReconcilesAsCommitted() throws Exception {
        // The first attempt publishes its snapshot but the response is lost, and the retry fails as
        // well. The snapshot carrying this commit user and transaction id proves the commit landed.
        ScriptedTable target = new ScriptedTable(table);
        target.failingAttempts = 2;
        target.publishOnFirstAttempt = true;
        PaimonConnectorTransaction transaction = boundTransaction(target);
        transaction.addCommitData(commitFragment(commitPayload(0)));

        transaction.commit();

        Assertions.assertEquals(2, target.commitAttempts);
        transaction.rollback();
        Assertions.assertEquals(0, target.aborts, "a committed transaction must keep its files");
    }

    @Test
    public void missingSnapshotLeavesOutcomeUnknown() throws Exception {
        // Both attempts fail and no snapshot carries this transaction: the commit may still land
        // later, so the files must not be aborted.
        ScriptedTable target = new ScriptedTable(table);
        target.failingAttempts = 2;
        PaimonConnectorTransaction transaction = boundTransaction(target);
        transaction.addCommitData(commitFragment(commitPayload(0)));

        DorisConnectorException failure =
                Assertions.assertThrows(DorisConnectorException.class, transaction::commit);

        Assertions.assertEquals("commit attempt 2 failed", failure.getCause().getMessage());
        Assertions.assertEquals("commit attempt 1 failed",
                failure.getCause().getSuppressed()[0].getMessage(),
                "the first failure must be reported together with the retry failure");
        transaction.rollback();
        Assertions.assertEquals(0, target.aborts,
                "a transaction whose outcome is unknown must not abort its files");
    }

    @Test
    public void preCommitFailureRemainsAbortable() throws Exception {
        ScriptedTable target = new ScriptedTable(table);
        target.newCommitFailures = 1;
        PaimonConnectorTransaction transaction = boundTransaction(target);
        transaction.addCommitData(commitFragment(commitPayload(0)));

        Assertions.assertThrows(DorisConnectorException.class, transaction::commit);

        Assertions.assertEquals(0, target.commitAttempts, "a failed committer open must not be retried");
        transaction.rollback();
        Assertions.assertEquals(1, target.aborts);
        Assertions.assertEquals(1, target.abortedMessages.size());
    }

    @Test
    public void closeFailureAfterCommitDoesNotChangeOutcome() throws Exception {
        ScriptedTable target = new ScriptedTable(table);
        target.closeFailure = new IllegalStateException("committer close failed");
        PaimonConnectorTransaction transaction = boundTransaction(target);
        transaction.addCommitData(commitFragment(commitPayload(0)));

        transaction.commit();

        Assertions.assertEquals(1, target.newCommitCalls, "a committed transaction must not be retried");
        Assertions.assertEquals(1, target.commitAttempts);
        transaction.rollback();
        Assertions.assertEquals(0, target.aborts);
    }

    @Test
    public void commitPayloadDedupUsesExactContent() throws Exception {
        byte[] first = new byte[] {0, 31};
        byte[] sameHash = new byte[] {1, 0};
        Assertions.assertEquals(Arrays.hashCode(first), Arrays.hashCode(sameHash));
        PaimonConnectorTransaction transaction = boundTransaction(new ScriptedTable(table));

        transaction.addCommitData(commitFragment(first));
        transaction.addCommitData(commitFragment(sameHash));
        transaction.addCommitData(commitFragment(Arrays.copyOf(first, first.length)));

        Assertions.assertEquals(2, storedPayloads(transaction),
                "a re-reported payload is dropped, a different payload with the same hash is kept");
    }

    @Test
    public void reReportedPayloadsCommitOnce() throws Exception {
        ScriptedTable target = new ScriptedTable(table);
        PaimonConnectorTransaction transaction = boundTransaction(target);
        byte[] payload = commitPayload(0);

        transaction.addCommitData(commitFragment(payload));
        transaction.addCommitData(commitFragment(Arrays.copyOf(payload, payload.length)));
        transaction.addCommitData(commitFragment(commitPayload(1)));
        transaction.commit();

        Assertions.assertEquals(2, target.committedMessages.size());
    }

    @Test
    public void commitUserIsNamespacedByDorisCluster() {
        Assertions.assertEquals(
                PaimonConnectorTransaction.commitUser(10001, TRANSACTION_ID),
                PaimonConnectorTransaction.commitUser(10001, TRANSACTION_ID));
        Assertions.assertNotEquals(
                PaimonConnectorTransaction.commitUser(10001, TRANSACTION_ID),
                PaimonConnectorTransaction.commitUser(10002, TRANSACTION_ID));
        RecordingConnectorContext context = new RecordingConnectorContext();
        Assertions.assertEquals(
                PaimonConnectorTransaction.commitUser(context.getClusterId(), TRANSACTION_ID),
                new PaimonConnectorTransaction(TRANSACTION_ID, context).getCommitUser());
    }

    private static PaimonConnectorTransaction boundTransaction(FileStoreTable target) {
        PaimonConnectorTransaction transaction =
                new PaimonConnectorTransaction(TRANSACTION_ID, new RecordingConnectorContext());
        transaction.bind(PaimonWriteBinding.create(
                new PaimonTableHandle("db", "tbl", Collections.emptyList(), Collections.emptyList()),
                target, Collections.emptyMap(), plainInsert()));
        return transaction;
    }

    /** An INSERT without a PARTITION clause. */
    private static ConnectorWriteHandle plainInsert() {
        return new ConnectorWriteHandle() {
            @Override
            public ConnectorTableHandle getTableHandle() {
                return null;
            }

            @Override
            public List<ConnectorColumn> getColumns() {
                return Collections.emptyList();
            }

            @Override
            public boolean isOverwrite() {
                return false;
            }

            @Override
            public Map<String, String> getStaticPartitionSpec() {
                return Collections.emptyMap();
            }
        };
    }

    /** A DPCM-framed payload with one empty commit message, as the BE writer reports it. */
    private static byte[] commitPayload(int bucket) throws Exception {
        CommitMessage message = new CommitMessageImpl(BinaryRow.EMPTY_ROW, bucket, 2,
                DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement());
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        serializer.serializeList(Collections.singletonList(message), new DataOutputViewStreamWrapper(output));
        byte[] serialized = output.toByteArray();
        ByteBuffer framed = ByteBuffer.allocate(12 + serialized.length);
        framed.put(new byte[] {'D', 'P', 'C', 'M'});
        framed.putInt(serializer.getVersion());
        framed.putInt(serialized.length);
        framed.put(serialized);
        return framed.array();
    }

    /** One entry of the connector commit data a BE reports: a binary-protocol TPaimonCommitMessage. */
    private static byte[] commitFragment(byte[] payload) throws Exception {
        return new TSerializer(new TBinaryProtocol.Factory())
                .serialize(new TPaimonCommitMessage().setPayload(payload));
    }

    @SuppressWarnings("unchecked")
    private static int storedPayloads(PaimonConnectorTransaction transaction) throws ReflectiveOperationException {
        Field field = PaimonConnectorTransaction.class.getDeclaredField("commitPayloads");
        field.setAccessible(true);
        return ((List<byte[]>) field.get(transaction)).size();
    }

    /**
     * The real table, except that the committer it opens follows the test's script. Reconciliation
     * still reads the real snapshot manager. The script is transient because the write binding
     * serializes the table it is given.
     */
    private static final class ScriptedTable extends DelegatedFileStoreTable {
        transient int newCommitFailures;
        transient int failingAttempts;
        transient boolean publishOnFirstAttempt;
        transient RuntimeException closeFailure;
        transient int newCommitCalls;
        transient int commitAttempts;
        transient int aborts;
        transient List<CommitMessage> abortedMessages = new ArrayList<>();
        transient List<CommitMessage> committedMessages = new ArrayList<>();

        ScriptedTable(FileStoreTable real) {
            super(real);
        }

        @Override
        public TableCommitImpl newCommit(String commitUser) {
            newCommitCalls++;
            if (newCommitFailures > 0) {
                newCommitFailures--;
                throw new IllegalStateException("cannot open the Paimon committer");
            }
            return new ScriptedCommit(this, commitUser);
        }

        @Override
        public FileStoreTable copy(Map<String, String> dynamicOptions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStoreTable copy(TableSchema newTableSchema) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStoreTable copyWithLatestSchema() {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStoreTable switchToBranch(String branchName) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class ScriptedCommit extends TableCommitImpl {
        private final ScriptedTable target;
        private final String commitUser;

        ScriptedCommit(ScriptedTable target, String commitUser) {
            super(target.wrapped().store().newCommit(commitUser, target.wrapped()), null, null, null, null,
                    new ConsumerManager(target.fileIO(), target.location()),
                    CoreOptions.ExpireExecutionMode.SYNC, target.name(), false, 1);
            this.target = target;
            this.commitUser = commitUser;
        }

        @Override
        public int filterAndCommit(Map<Long, List<CommitMessage>> commitIdentifiersAndMessages) {
            int attempt = ++target.commitAttempts;
            if (attempt > target.failingAttempts) {
                commitIdentifiersAndMessages.values().forEach(target.committedMessages::addAll);
                return commitIdentifiersAndMessages.size();
            }
            if (attempt == 1 && target.publishOnFirstAttempt) {
                try (TableCommitImpl real = target.wrapped().newCommit(commitUser)) {
                    real.ignoreEmptyCommit(false);
                    for (Long identifier : commitIdentifiersAndMessages.keySet()) {
                        real.commit(identifier, Collections.emptyList());
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
            throw new IllegalStateException("commit attempt " + attempt + " failed");
        }

        @Override
        public void abort(List<CommitMessage> commitMessages) {
            target.aborts++;
            target.abortedMessages.addAll(commitMessages);
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (target.closeFailure != null) {
                throw target.closeFailure;
            }
        }
    }
}
