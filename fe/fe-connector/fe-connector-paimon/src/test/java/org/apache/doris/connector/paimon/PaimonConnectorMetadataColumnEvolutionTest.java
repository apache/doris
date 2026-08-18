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
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.ddl.ConnectorColumnPath;
import org.apache.doris.connector.spi.ddl.ConnectorColumnPosition;

import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataTypeRoot;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Column-evolution tests for the {@code ConnectorColumnEvolutionOps} group implemented by
 * {@link PaimonConnectorMetadata}, pinning:
 * (1) each neutral SPI op maps to the right paimon {@link SchemaChange} subtype and field path,
 * (2) a composite op (MODIFY, ADD COLUMNS, REORDER) is committed as ONE atomic change list rather
 *     than N separate {@code alterTable} calls,
 * (3) the {@code isNullableSpecified} / {@code isCommentSpecified} gates decide whether the
 *     corresponding change is emitted at all — an unspecified clause must not silently reset the
 *     column's existing nullability or comment,
 * (4) {@code null} position emits NO reposition change (rather than a no-op Move.last()),
 * (5) a single-part path degrades to the flat op, and
 * (6) D7=B: every remote call runs INSIDE
 *     {@link org.apache.doris.connector.spi.ConnectorContext#executeAuthenticated}, with paimon's
 *     checked exceptions wrapped as {@link DorisConnectorException} naming the table.
 *
 * <p>All tests run offline against the recording seam fake (null real Catalog).
 */
public class PaimonConnectorMetadataColumnEvolutionTest {

    private static PaimonConnectorMetadata metadata(RecordingPaimonCatalogOps ops,
            RecordingConnectorContext ctx) {
        return new PaimonConnectorMetadata(ops, PaimonCatalogProperties.of(Collections.emptyMap()), ctx);
    }

    private static PaimonTableHandle handle() {
        return new PaimonTableHandle("db", "tbl", Collections.emptyList(), Collections.emptyList());
    }

    /** A column with neither NULL nor COMMENT specified (the bare {@code MODIFY COLUMN c INT} case). */
    private static ConnectorColumn column(String name, String typeName) {
        return new ConnectorColumn(name, ConnectorType.of(typeName), null, true, null);
    }

    private static ConnectorColumn column(String name, String typeName, String comment, boolean nullable) {
        return new ConnectorColumn(name, ConnectorType.of(typeName), comment, nullable, null);
    }

    // ==================== ADD COLUMN ====================

    @Test
    public void addColumnAppendsWithoutMoveWhenPositionIsNull() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        metadata(ops, ctx).addColumn(null, handle(), column("c_new", "STRING"), null);

        // WHY: a null position means "no position clause". Paimon's AddColumn carries a nullable Move,
        // so the absence must be encoded as move==null, NOT as Move.last() — the latter would rewrite
        // the column order on a table where the user asked for no repositioning at all.
        // MUTATION: emitting Move.last() here flips the assertNull below red.
        Assertions.assertEquals(1, ops.lastSchemaChanges.size());
        SchemaChange.AddColumn add = (SchemaChange.AddColumn) ops.lastSchemaChanges.get(0);
        Assertions.assertArrayEquals(new String[] {"c_new"}, add.fieldNames());
        Assertions.assertNull(add.move(), "a null SPI position must not synthesize a Move");
        Assertions.assertEquals(DataTypeRoot.VARCHAR, add.dataType().getTypeRoot());
        Assertions.assertEquals(1, ctx.authCount, "the remote alter must run inside executeAuthenticated");
        Assertions.assertEquals(Arrays.asList("alterTable:db.tbl,changes=1"), ops.log);
    }

    @Test
    public void addColumnFirstAndAfterMapToPaimonMoves() {
        RecordingPaimonCatalogOps first = new RecordingPaimonCatalogOps();
        metadata(first, new RecordingConnectorContext())
                .addColumn(null, handle(), column("c1", "INT"), ConnectorColumnPosition.FIRST);
        SchemaChange.Move firstMove = ((SchemaChange.AddColumn) first.lastSchemaChanges.get(0)).move();
        Assertions.assertEquals(SchemaChange.Move.MoveType.FIRST, firstMove.type());
        Assertions.assertEquals("c1", firstMove.fieldName());

        RecordingPaimonCatalogOps after = new RecordingPaimonCatalogOps();
        metadata(after, new RecordingConnectorContext())
                .addColumn(null, handle(), column("c2", "INT"), ConnectorColumnPosition.after("anchor"));
        SchemaChange.Move afterMove = ((SchemaChange.AddColumn) after.lastSchemaChanges.get(0)).move();
        // WHY: Doris only expresses FIRST | AFTER (there is no BEFORE), so AFTER must carry BOTH the
        // moved column and the reference column. MUTATION: swapping the two arguments in toMove()
        // makes referenceFieldName "c2" and flips the last assertion red.
        Assertions.assertEquals(SchemaChange.Move.MoveType.AFTER, afterMove.type());
        Assertions.assertEquals("c2", afterMove.fieldName());
        Assertions.assertEquals("anchor", afterMove.referenceFieldName());
    }

    @Test
    public void addColumnCarriesNullabilityOnTheType() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();

        metadata(ops, new RecordingConnectorContext())
                .addColumn(null, handle(), column("c_nn", "INT", null, false), null);

        // WHY: column-level nullability rides on the paimon DataType via copy(nullable), exactly as the
        // CREATE TABLE path does in PaimonSchemaBuilder — paimon's AddColumn has no separate nullability
        // argument. MUTATION: dropping the .copy(isNullable()) leaves the paimon default (nullable) and
        // flips this red.
        SchemaChange.AddColumn add = (SchemaChange.AddColumn) ops.lastSchemaChanges.get(0);
        Assertions.assertFalse(add.dataType().isNullable());
    }

    @Test
    public void addColumnsCommitsAllColumnsAsOneChangeList() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        metadata(ops, ctx).addColumns(null, handle(),
                Arrays.asList(column("a", "INT"), column("b", "STRING"), column("c", "BIGINT")));

        // WHY: three columns must reach paimon as ONE alterTable so a mid-list failure cannot leave the
        // table with a partially-applied ADD. MUTATION: looping applySchemaChanges per column would make
        // the log 3 entries and authCount 3.
        Assertions.assertEquals(3, ops.lastSchemaChanges.size());
        Assertions.assertEquals(Arrays.asList("alterTable:db.tbl,changes=3"), ops.log);
        Assertions.assertEquals(1, ctx.authCount);
    }

    // ==================== DROP / RENAME ====================

    @Test
    public void dropColumnEmitsSinglePartPath() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        metadata(ops, ctx).dropColumn(null, handle(), "gone");

        SchemaChange.DropColumn drop = (SchemaChange.DropColumn) ops.lastSchemaChanges.get(0);
        Assertions.assertArrayEquals(new String[] {"gone"}, drop.fieldNames());
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void renameColumnCarriesOldPathAndNewLeafName() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();

        metadata(ops, new RecordingConnectorContext()).renameColumn(null, handle(), "old", "new");

        SchemaChange.RenameColumn rename = (SchemaChange.RenameColumn) ops.lastSchemaChanges.get(0);
        Assertions.assertArrayEquals(new String[] {"old"}, rename.fieldNames());
        Assertions.assertEquals("new", rename.newName());
    }

    // ==================== MODIFY COLUMN ====================

    @Test
    public void modifyColumnOmitsNullabilityAndCommentWhenUnspecified() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        // A bare "MODIFY COLUMN c BIGINT": neither NULL/NOT NULL nor COMMENT was written.
        metadata(ops, ctx).modifyColumn(null, handle(), column("c", "BIGINT"), null);

        // WHY: isNullableSpecified()/isCommentSpecified() distinguish "clause absent" from "clause set to
        // the default". Emitting updateColumnNullability / updateColumnComment unconditionally would
        // silently reset a column that the statement never mentioned — a real data-visible regression
        // (a NOT NULL column would become nullable, an existing comment would be wiped).
        // MUTATION: dropping either gate adds a change here and flips the size assertion red.
        Assertions.assertEquals(1, ops.lastSchemaChanges.size(),
                "only the type change may be emitted when nothing else was specified");
        SchemaChange.UpdateColumnType type =
                (SchemaChange.UpdateColumnType) ops.lastSchemaChanges.get(0);
        // WHY: paimon's 2-arg updateColumnType hardcodes keepNullability=false, which would make the type
        // change ALSO reset nullability to the new type's default (paimon types are nullable by default).
        // A bare "MODIFY COLUMN c BIGINT" on a NOT NULL column would then silently make it nullable —
        // data-visible, and invisible in the statement the user wrote. MUTATION: switching to the 2-arg
        // overload flips this red.
        Assertions.assertTrue(type.keepNullability(),
                "an unspecified nullability must survive a type-only MODIFY");
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void modifyColumnEmitsEverySpecifiedAspectAsOneCommit() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        ConnectorColumn col = column("c", "BIGINT", "the comment", false)
                .withSpecified(/*nullableSpecified*/ true, /*commentSpecified*/ true);
        metadata(ops, ctx).modifyColumn(null, handle(), col, ConnectorColumnPosition.after("anchor"));

        // WHY: what Doris expresses as ONE statement, paimon splits across four SchemaChanges. They must
        // land in a single alterTable so the table cannot end up with the new type but the old
        // nullability. MUTATION: four separate applySchemaChanges calls make authCount 4.
        List<SchemaChange> changes = ops.lastSchemaChanges;
        Assertions.assertEquals(4, changes.size());
        Assertions.assertTrue(changes.get(0) instanceof SchemaChange.UpdateColumnType);
        Assertions.assertTrue(changes.get(1) instanceof SchemaChange.UpdateColumnNullability);
        Assertions.assertTrue(changes.get(2) instanceof SchemaChange.UpdateColumnComment);
        Assertions.assertTrue(changes.get(3) instanceof SchemaChange.UpdateColumnPosition);
        Assertions.assertFalse(((SchemaChange.UpdateColumnNullability) changes.get(1)).newNullability());
        Assertions.assertEquals("the comment",
                ((SchemaChange.UpdateColumnComment) changes.get(2)).newDescription());
        Assertions.assertEquals(1, ctx.authCount, "a composite MODIFY must be ONE atomic schema commit");
        Assertions.assertEquals(Arrays.asList("alterTable:db.tbl,changes=4"), ops.log);
    }

    @Test
    public void modifyColumnClearsCommentWithEmptyStringWhenSpecified() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();

        ConnectorColumn col = column("c", "INT", "", true).withSpecified(false, true);
        metadata(ops, new RecordingConnectorContext()).modifyColumn(null, handle(), col, null);

        // WHY: COMMENT '' is an explicit clear, not an absent clause — it must reach paimon.
        SchemaChange.UpdateColumnComment comment = ops.lastSchemaChanges.stream()
                .filter(c -> c instanceof SchemaChange.UpdateColumnComment)
                .map(c -> (SchemaChange.UpdateColumnComment) c)
                .findFirst()
                .orElseThrow(() -> new AssertionError("COMMENT '' must emit an UpdateColumnComment"));
        Assertions.assertEquals("", comment.newDescription());
    }

    // ==================== REORDER ====================

    @Test
    public void reorderColumnsChainsFirstThenAfterMoves() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        metadata(ops, ctx).reorderColumns(null, handle(), Arrays.asList("c", "a", "b"));

        // WHY: paimon has no "set the whole order" primitive, so the target order is expressed as a chain:
        // c FIRST, a AFTER c, b AFTER a. Committing the chain as one list means an invalid name cannot
        // leave the table half-reordered. MUTATION: anchoring every move on newOrder.get(0) would make
        // the third move's reference "c" instead of "a".
        List<SchemaChange> changes = ops.lastSchemaChanges;
        Assertions.assertEquals(3, changes.size());
        SchemaChange.Move m0 = ((SchemaChange.UpdateColumnPosition) changes.get(0)).move();
        SchemaChange.Move m1 = ((SchemaChange.UpdateColumnPosition) changes.get(1)).move();
        SchemaChange.Move m2 = ((SchemaChange.UpdateColumnPosition) changes.get(2)).move();
        Assertions.assertEquals(SchemaChange.Move.MoveType.FIRST, m0.type());
        Assertions.assertEquals("c", m0.fieldName());
        Assertions.assertEquals("a", m1.fieldName());
        Assertions.assertEquals("c", m1.referenceFieldName());
        Assertions.assertEquals("b", m2.fieldName());
        Assertions.assertEquals("a", m2.referenceFieldName());
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void reorderColumnsRejectsEmptyOrderBeforeAnyRemoteCall() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        // WHY: an empty order is a pure local arg error; it must not reach the authenticator or the seam.
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).reorderColumns(null, handle(), Collections.emptyList()));
        Assertions.assertEquals(0, ctx.authCount);
        Assertions.assertTrue(ops.log.isEmpty());
    }

    // ==================== Nested (dotted-path) ops ====================

    @Test
    public void nestedOpsCarryTheFullFieldPath() {
        RecordingPaimonCatalogOps addOps = new RecordingPaimonCatalogOps();
        metadata(addOps, new RecordingConnectorContext()).addNestedColumn(null, handle(),
                ConnectorColumnPath.of(Arrays.asList("s", "leaf")), column("leaf", "INT"), null);
        Assertions.assertArrayEquals(new String[] {"s", "leaf"},
                ((SchemaChange.AddColumn) addOps.lastSchemaChanges.get(0)).fieldNames());

        RecordingPaimonCatalogOps dropOps = new RecordingPaimonCatalogOps();
        metadata(dropOps, new RecordingConnectorContext()).dropNestedColumn(null, handle(),
                ConnectorColumnPath.of(Arrays.asList("s", "leaf")));
        Assertions.assertArrayEquals(new String[] {"s", "leaf"},
                ((SchemaChange.DropColumn) dropOps.lastSchemaChanges.get(0)).fieldNames());

        RecordingPaimonCatalogOps renameOps = new RecordingPaimonCatalogOps();
        metadata(renameOps, new RecordingConnectorContext()).renameNestedColumn(null, handle(),
                ConnectorColumnPath.of(Arrays.asList("s", "leaf")), "leaf2");
        SchemaChange.RenameColumn rename =
                (SchemaChange.RenameColumn) renameOps.lastSchemaChanges.get(0);
        Assertions.assertArrayEquals(new String[] {"s", "leaf"}, rename.fieldNames());
        Assertions.assertEquals("leaf2", rename.newName());
    }

    @Test
    public void nestedModifyKeepsNullabilityOnTheTypeChange() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();

        metadata(ops, new RecordingConnectorContext()).modifyNestedColumn(null, handle(),
                ConnectorColumnPath.of(Arrays.asList("s", "leaf")), column("leaf", "BIGINT"), null);

        // WHY: nullability is owned by the explicit UpdateColumnNullability change (emitted only when the
        // statement specified it), so the type update must pass keepNullability=true or it would reset a
        // nested NOT NULL field the user never mentioned. MUTATION: passing false flips this red.
        SchemaChange.UpdateColumnType type =
                (SchemaChange.UpdateColumnType) ops.lastSchemaChanges.get(0);
        Assertions.assertTrue(type.keepNullability());
        Assertions.assertEquals(1, ops.lastSchemaChanges.size(),
                "an unspecified nullability/comment must not emit extra changes");
    }

    @Test
    public void singlePartPathDegradesToTheFlatOp() {
        // WHY: the fe-core bridge routes only nested paths to the *NestedColumn ops, but a direct call
        // with a one-part path must still behave exactly like the flat op rather than building a
        // one-element nested path with different semantics.
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        metadata(ops, new RecordingConnectorContext()).dropNestedColumn(null, handle(),
                ConnectorColumnPath.of("top"));
        Assertions.assertArrayEquals(new String[] {"top"},
                ((SchemaChange.DropColumn) ops.lastSchemaChanges.get(0)).fieldNames());

        RecordingPaimonCatalogOps renameOps = new RecordingPaimonCatalogOps();
        metadata(renameOps, new RecordingConnectorContext()).renameNestedColumn(null, handle(),
                ConnectorColumnPath.of("top"), "top2");
        Assertions.assertEquals("top2",
                ((SchemaChange.RenameColumn) renameOps.lastSchemaChanges.get(0)).newName());
    }

    @Test
    public void modifyColumnCommentAcceptsFlatAndNestedPaths() {
        // WHY: modifyColumnComment is the SOLE entrypoint for MODIFY COLUMN ... COMMENT and receives both
        // flat and nested paths, so it uses the String[] overload unconditionally — a one-element array
        // IS the flat case in paimon's API.
        RecordingPaimonCatalogOps flat = new RecordingPaimonCatalogOps();
        metadata(flat, new RecordingConnectorContext())
                .modifyColumnComment(null, handle(), ConnectorColumnPath.of("c"), "flat comment");
        SchemaChange.UpdateColumnComment flatChange =
                (SchemaChange.UpdateColumnComment) flat.lastSchemaChanges.get(0);
        Assertions.assertArrayEquals(new String[] {"c"}, flatChange.fieldNames());
        Assertions.assertEquals("flat comment", flatChange.newDescription());

        RecordingPaimonCatalogOps nested = new RecordingPaimonCatalogOps();
        metadata(nested, new RecordingConnectorContext()).modifyColumnComment(null, handle(),
                ConnectorColumnPath.of(Arrays.asList("s", "leaf")), "");
        SchemaChange.UpdateColumnComment nestedChange =
                (SchemaChange.UpdateColumnComment) nested.lastSchemaChanges.get(0);
        Assertions.assertArrayEquals(new String[] {"s", "leaf"}, nestedChange.fieldNames());
        Assertions.assertEquals("", nestedChange.newDescription(), "empty string clears the comment");
    }

    // ==================== Error handling ====================

    @Test
    public void tableNotExistIsWrappedWithTheQualifiedTableName() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.throwTableNotExistOnAlter = true;
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).dropColumn(null, handle(), "c"));

        // WHY: paimon's checked TableNotExistException must not escape the connector boundary, and the
        // message must name both the operation and the table so a user with many external tables can act
        // on it. MUTATION: rethrowing raw or dropping the identifier flips these red.
        Assertions.assertTrue(ex.getMessage().contains("drop column c"), ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("db.tbl"), ex.getMessage());
    }

    @Test
    public void columnLevelPaimonExceptionsAreWrappedWithTheOperation() {
        // WHY: alterTable declares THREE checked exceptions, and the two column-level ones are the most
        // likely failures in practice (adding a duplicate name, altering a column that is not there).
        // They must surface as DorisConnectorException naming the operation, not escape raw or get
        // flattened into a generic message. MUTATION: declaring only TableNotExistException on the seam
        // does not even compile; catching and swallowing them here flips these red.
        RecordingPaimonCatalogOps dup = new RecordingPaimonCatalogOps();
        dup.throwColumnAlreadyExist = true;
        dup.alterColumnName = "c_dup";
        DorisConnectorException dupEx = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(dup, new RecordingConnectorContext())
                        .addColumn(null, handle(), column("c_dup", "INT"), null));
        Assertions.assertTrue(dupEx.getMessage().contains("add column c_dup"), dupEx.getMessage());

        RecordingPaimonCatalogOps missing = new RecordingPaimonCatalogOps();
        missing.throwColumnNotExist = true;
        missing.alterColumnName = "c_missing";
        DorisConnectorException missingEx = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(missing, new RecordingConnectorContext())
                        .renameColumn(null, handle(), "c_missing", "c2"));
        Assertions.assertTrue(missingEx.getMessage().contains("rename column c_missing to c2"),
                missingEx.getMessage());
    }

    @Test
    public void authFailureIsWrappedAndSkipsTheRemoteCall() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;

        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).addColumn(null, handle(), column("c", "INT"), null));

        // WHY: when the authenticator itself fails the task must never run — a schema change that
        // bypassed auth would be a security hole, not just a bug.
        Assertions.assertTrue(ops.log.isEmpty(), "the seam must not be reached when auth fails");
    }

    @Test
    public void unrepresentableTypeIsRejectedBeforeTheRemoteCall() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        // WHY: the neutral->paimon type conversion is a PURE step that runs OUTSIDE executeAuthenticated
        // (mirroring the iceberg connector), so a type paimon cannot represent fails before any remote
        // call and cannot leave a half-applied schema. MUTATION: building the type inside the
        // executeAuthenticated lambda makes authCount 1 and flips the assertion below.
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).addColumn(null, handle(), column("c", "TIME"), null));
        Assertions.assertEquals(0, ctx.authCount);
        Assertions.assertTrue(ops.log.isEmpty());
    }
}
