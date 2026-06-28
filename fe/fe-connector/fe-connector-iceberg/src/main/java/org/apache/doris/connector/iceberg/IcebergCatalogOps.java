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

import org.apache.doris.connector.api.ConnectorViewDefinition;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.BranchChange;
import org.apache.doris.connector.api.ddl.ConnectorColumnPosition;
import org.apache.doris.connector.api.ddl.DropRefChange;
import org.apache.doris.connector.api.ddl.PartitionFieldChange;
import org.apache.doris.connector.api.ddl.TagChange;

import com.google.common.base.Splitter;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewVersion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Injection seam over the remote Iceberg {@link Catalog} calls.
 *
 * <p>The default {@link CatalogBackedIcebergCatalogOps} simply delegates to a real {@code Catalog},
 * which requires a live remote catalog (REST / HMS / Glue / Hadoop / JDBC / S3Tables / DLF). By
 * depending on this interface instead of {@code Catalog} directly, {@link IcebergConnectorMetadata}
 * becomes unit-testable offline with a hand-written recording fake (no Mockito) — mirroring the paimon
 * connector's {@link org.apache.doris.connector.iceberg.IcebergCatalogFactory} sibling seam pattern
 * {@code PaimonCatalogOps}.
 *
 * <p>P6.1 (this task) declares only the read subset the metadata layer needs. Write / DDL / MVCC
 * methods land in later phases, with signatures mirroring the real Iceberg {@code Catalog}. The
 * {@code SupportsNamespaces}-vs-plain-{@code Catalog} branch (which the skeleton leaked into the
 * metadata layer) is kept INTERNAL to the default impl.
 */
public interface IcebergCatalogOps {

    /**
     * Lists the top-level database (namespace) names, or an empty list when the catalog does not
     * support namespaces. Returns the LAST level of each namespace (mirrors the legacy skeleton).
     */
    List<String> listDatabaseNames();

    /** Returns {@code true} iff the database (namespace) exists; {@code false} when no namespace support. */
    boolean databaseExists(String dbName);

    /** Lists the table names in {@code dbName}. */
    List<String> listTableNames(String dbName);

    /** Lists the view names in {@code dbName}; empty when the catalog is not a (view-enabled) ViewCatalog. */
    List<String> listViewNames(String dbName);

    /** Returns {@code true} iff {@code dbName.tableName} exists. */
    boolean tableExists(String dbName, String tableName);

    /** Returns {@code true} iff the view {@code dbName.viewName} exists; {@code false} when no view support. */
    boolean viewExists(String dbName, String viewName);

    /**
     * Loads the stored SQL definition + dialect of the view {@code dbName.viewName}. Requires a
     * (view-enabled) {@link ViewCatalog}; otherwise fails loud.
     */
    ConnectorViewDefinition loadViewDefinition(String dbName, String viewName);

    /** Drops the view {@code dbName.viewName}. Requires a (view-enabled) {@link ViewCatalog}; else fails loud. */
    void dropView(String dbName, String viewName);

    /** Loads the Iceberg {@link Table} for {@code dbName.tableName}. */
    Table loadTable(String dbName, String tableName);

    // ---- DDL writes (B1) — thin delegations to the real Catalog / SupportsNamespaces ----

    /** Creates the database (namespace) {@code dbName} with {@code properties}. */
    void createDatabase(String dbName, Map<String, String> properties);

    /** Drops the (already-emptied) database (namespace) {@code dbName}. */
    void dropDatabase(String dbName);

    /**
     * Creates {@code dbName.tableName} with the given Iceberg {@code schema} / {@code partitionSpec} /
     * {@code properties} and, when non-null and sorted, {@code sortOrder}.
     */
    void createTable(String dbName, String tableName, Schema schema, PartitionSpec partitionSpec,
            SortOrder sortOrder, Map<String, String> properties);

    /** Drops {@code dbName.tableName}; {@code purge} requests deletion of the underlying data + metadata. */
    void dropTable(String dbName, String tableName, boolean purge);

    /** Renames {@code dbName.oldName} to {@code dbName.newName} (same database). */
    void renameTable(String dbName, String oldName, String newName);

    /** The table's storage location, or empty when blank — read BEFORE a drop to prune empty dirs. */
    Optional<String> loadTableLocation(String dbName, String tableName);

    /** The database (namespace)'s {@code location} metadata, or empty when absent/blank. */
    Optional<String> loadNamespaceLocation(String dbName);

    // ---- Column evolution (B2) — build + commit an UpdateSchema; thin delegations to the real Table ----

    /** Adds {@code column} to {@code dbName.tableName} at {@code position} (null = append at the end). */
    void addColumn(String dbName, String tableName, IcebergColumnChange column, ConnectorColumnPosition position);

    /** Adds {@code columns} to {@code dbName.tableName}, appended in order, in a single schema update. */
    void addColumns(String dbName, String tableName, List<IcebergColumnChange> columns);

    /** Drops {@code columnName} from {@code dbName.tableName}. */
    void dropColumn(String dbName, String tableName, String columnName);

    /** Renames {@code oldName} to {@code newName} in {@code dbName.tableName}. */
    void renameColumn(String dbName, String tableName, String oldName, String newName);

    /** Modifies a primitive {@code column} (type/comment/nullable) of {@code dbName.tableName}, optional move. */
    void modifyColumn(String dbName, String tableName, IcebergColumnChange column, ConnectorColumnPosition position);

    /** Reorders the columns of {@code dbName.tableName} to match {@code newOrder} (full ordered name list). */
    void reorderColumns(String dbName, String tableName, List<String> newOrder);

    // ---- Branch / tag refs (B4) — build + commit a ManageSnapshots; needs the live Table ----

    /** Creates or replaces the branch described by {@code branch} on {@code dbName.tableName}. */
    void createOrReplaceBranch(String dbName, String tableName, BranchChange branch);

    /** Creates or replaces the tag described by {@code tag} on {@code dbName.tableName}. */
    void createOrReplaceTag(String dbName, String tableName, TagChange tag);

    /** Drops the branch named by {@code branch} from {@code dbName.tableName} (no-op when absent + ifExists). */
    void dropBranch(String dbName, String tableName, DropRefChange branch);

    /** Drops the tag named by {@code tag} from {@code dbName.tableName} (no-op when absent + ifExists). */
    void dropTag(String dbName, String tableName, DropRefChange tag);

    // ---- Partition evolution (B5) — build + commit an UpdatePartitionSpec; needs the live Table ----

    /** Adds the partition field described by {@code change} to {@code dbName.tableName}'s spec. */
    void addPartitionField(String dbName, String tableName, PartitionFieldChange change);

    /** Drops the partition field described by {@code change} from {@code dbName.tableName}'s spec. */
    void dropPartitionField(String dbName, String tableName, PartitionFieldChange change);

    /** Replaces a partition field (remove old + add new) per {@code change} in {@code dbName.tableName}'s spec. */
    void replacePartitionField(String dbName, String tableName, PartitionFieldChange change);

    void close() throws IOException;

    /**
     * Default implementation backing the seam with a real Iceberg {@link Catalog}. Each method is a
     * thin delegation; the {@code Catalog} is the only state. Keeps the {@code SupportsNamespaces}
     * branch internal.
     */
    class CatalogBackedIcebergCatalogOps implements IcebergCatalogOps {

        private static final Logger LOG = LogManager.getLogger(CatalogBackedIcebergCatalogOps.class);

        // The iceberg namespace-metadata key carrying the database location (legacy NAMESPACE_LOCATION_PROP).
        private static final String NAMESPACE_LOCATION_PROP = "location";

        private final Catalog catalog;
        // Listing-parity gating mirrored from legacy IcebergMetadataOps (threaded from IcebergConnector):
        private final boolean restFlavor;
        private final boolean nestedNamespaceEnabled;
        private final boolean viewEnabled;
        private final Optional<String> externalCatalogName;

        public CatalogBackedIcebergCatalogOps(Catalog catalog) {
            this(catalog, false, false, true, Optional.empty());
        }

        public CatalogBackedIcebergCatalogOps(Catalog catalog, boolean restFlavor,
                boolean nestedNamespaceEnabled, boolean viewEnabled, Optional<String> externalCatalogName) {
            this.catalog = catalog;
            this.restFlavor = restFlavor;
            this.nestedNamespaceEnabled = nestedNamespaceEnabled;
            this.viewEnabled = viewEnabled;
            this.externalCatalogName = externalCatalogName;
        }

        @Override
        public List<String> listDatabaseNames() {
            if (!(catalog instanceof SupportsNamespaces)) {
                LOG.warn("Iceberg catalog does not support namespaces");
                return Collections.emptyList();
            }
            return listNestedNamespaces(rootNamespace());
        }

        /**
         * Lists databases under {@code parentNs}, mirroring legacy {@code IcebergMetadataOps}: for a REST
         * flavor with {@code iceberg.rest.nested-namespace-enabled=true} it RECURSES, emitting each child's
         * dotted {@code toString()} followed by its descendants; otherwise it returns each child's last
         * level only. Assumes the catalog is a {@link SupportsNamespaces} (guarded by the callers).
         */
        private List<String> listNestedNamespaces(Namespace parentNs) {
            SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
            if (restFlavor && nestedNamespaceEnabled) {
                return nsCatalog.listNamespaces(parentNs).stream()
                        .flatMap(childNs -> Stream.concat(
                                Stream.of(childNs.toString()),
                                listNestedNamespaces(childNs).stream()))
                        .collect(Collectors.toList());
            }
            return nsCatalog.listNamespaces(parentNs).stream()
                    .map(ns -> ns.level(ns.length() - 1))
                    .collect(Collectors.toList());
        }

        @Override
        public boolean databaseExists(String dbName) {
            if (!(catalog instanceof SupportsNamespaces)) {
                return false;
            }
            return ((SupportsNamespaces) catalog).namespaceExists(toNamespace(dbName));
        }

        @Override
        public List<String> listTableNames(String dbName) {
            Namespace ns = toNamespace(dbName);
            List<String> tableNames = catalog.listTables(ns).stream()
                    .map(TableIdentifier::name)
                    .collect(Collectors.toList());
            // iceberg's listTables also returns views, so subtract the view names when the catalog is a
            // (view-enabled) ViewCatalog — mirrors legacy IcebergMetadataOps.listTableNames.
            if (!isViewCatalogEnabled()) {
                return tableNames;
            }
            List<String> views = ((ViewCatalog) catalog).listViews(ns).stream()
                    .map(TableIdentifier::name)
                    .collect(Collectors.toList());
            if (views.isEmpty()) {
                return tableNames;
            }
            return tableNames.stream()
                    .filter(name -> !views.contains(name))
                    .collect(Collectors.toList());
        }

        @Override
        public List<String> listViewNames(String dbName) {
            // Mirrors legacy IcebergMetadataOps.listViewNames: empty unless the catalog is a
            // (view-enabled) ViewCatalog. The auth wrapping / exception normalization is in
            // IcebergConnectorMetadata.listViewNames, keeping this a thin catalog delegation.
            if (!isViewCatalogEnabled()) {
                return Collections.emptyList();
            }
            return ((ViewCatalog) catalog).listViews(toNamespace(dbName)).stream()
                    .map(TableIdentifier::name)
                    .collect(Collectors.toList());
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            return catalog.tableExists(toTableIdentifier(dbName, tableName));
        }

        @Override
        public boolean viewExists(String dbName, String viewName) {
            // Mirrors legacy IcebergMetadataOps.viewExists: false unless the catalog is a
            // (view-enabled) ViewCatalog. Auth wrapping is in IcebergConnectorMetadata.viewExists.
            if (!isViewCatalogEnabled()) {
                return false;
            }
            return ((ViewCatalog) catalog).viewExists(toTableIdentifier(dbName, viewName));
        }

        @Override
        public ConnectorViewDefinition loadViewDefinition(String dbName, String viewName) {
            // Mirrors legacy IcebergExternalTable.getViewText + getSqlDialect, consolidated into one remote
            // load: the dialect is the summary's "engine-name", and the SQL is that dialect's representation.
            // Auth wrapping is in IcebergConnectorMetadata.getViewDefinition.
            if (!isViewCatalogEnabled()) {
                throw new DorisConnectorException("View is not supported with not view catalog.");
            }
            View icebergView = ((ViewCatalog) catalog).loadView(toTableIdentifier(dbName, viewName));
            ViewVersion viewVersion = icebergView.currentVersion();
            if (viewVersion == null) {
                throw new DorisConnectorException(String.format("Cannot get view version for view '%s'", icebergView));
            }
            Map<String, String> summary = viewVersion.summary();
            if (summary == null) {
                throw new DorisConnectorException(String.format("Cannot get summary for view '%s'", icebergView));
            }
            // "engine-name" is the iceberg view-version summary key the writing engine (e.g. spark) records.
            String engineName = summary.get("engine-name");
            if (engineName == null || engineName.isEmpty()) {
                throw new DorisConnectorException(String.format("Cannot get engine-name for view '%s'", icebergView));
            }
            String dialect = engineName.toLowerCase(Locale.ROOT);
            SQLViewRepresentation sqlViewRepresentation = icebergView.sqlFor(dialect);
            if (sqlViewRepresentation == null) {
                throw new DorisConnectorException("Cannot get view text from iceberg view");
            }
            return new ConnectorViewDefinition(sqlViewRepresentation.sql(), dialect);
        }

        @Override
        public void dropView(String dbName, String viewName) {
            // Mirrors legacy IcebergMetadataOps.performDropView: requires a (view-enabled) ViewCatalog;
            // otherwise fails loud. Auth wrapping is in IcebergConnectorMetadata.dropView.
            if (!isViewCatalogEnabled()) {
                throw new DorisConnectorException("Drop Iceberg view is not supported with not view catalog.");
            }
            ((ViewCatalog) catalog).dropView(toTableIdentifier(dbName, viewName));
        }

        @Override
        public Table loadTable(String dbName, String tableName) {
            return catalog.loadTable(toTableIdentifier(dbName, tableName));
        }

        @Override
        public void createDatabase(String dbName, Map<String, String> properties) {
            requireNamespaces().createNamespace(toNamespace(dbName), properties);
        }

        @Override
        public void dropDatabase(String dbName) {
            requireNamespaces().dropNamespace(toNamespace(dbName));
        }

        @Override
        public void createTable(String dbName, String tableName, Schema schema, PartitionSpec partitionSpec,
                SortOrder sortOrder, Map<String, String> properties) {
            TableIdentifier id = toTableIdentifier(dbName, tableName);
            // Mirror legacy IcebergMetadataOps.performCreateTable: the buildTable path is only needed to
            // attach a sort order; otherwise the plain createTable overload is used.
            if (sortOrder != null && !sortOrder.isUnsorted()) {
                catalog.buildTable(id, schema)
                        .withPartitionSpec(partitionSpec)
                        .withProperties(properties)
                        .withSortOrder(sortOrder)
                        .create();
            } else {
                catalog.createTable(id, schema, partitionSpec, properties);
            }
        }

        @Override
        public void dropTable(String dbName, String tableName, boolean purge) {
            catalog.dropTable(toTableIdentifier(dbName, tableName), purge);
        }

        @Override
        public void renameTable(String dbName, String oldName, String newName) {
            catalog.renameTable(toTableIdentifier(dbName, oldName), toTableIdentifier(dbName, newName));
        }

        @Override
        public Optional<String> loadTableLocation(String dbName, String tableName) {
            String location = catalog.loadTable(toTableIdentifier(dbName, tableName)).location();
            return isBlank(location) ? Optional.empty() : Optional.of(location);
        }

        @Override
        public Optional<String> loadNamespaceLocation(String dbName) {
            Map<String, String> metadata = requireNamespaces().loadNamespaceMetadata(toNamespace(dbName));
            String location = metadata.get(NAMESPACE_LOCATION_PROP);
            return isBlank(location) ? Optional.empty() : Optional.of(location);
        }

        @Override
        public void addColumn(String dbName, String tableName, IcebergColumnChange column,
                ConnectorColumnPosition position) {
            UpdateSchema updateSchema = loadTable(dbName, tableName).updateSchema();
            updateSchema.addColumn(column.getName(), column.getType(), column.getComment(),
                    column.getDefaultValue());
            applyPosition(updateSchema, position, column.getName());
            updateSchema.commit();
        }

        @Override
        public void addColumns(String dbName, String tableName, List<IcebergColumnChange> columns) {
            UpdateSchema updateSchema = loadTable(dbName, tableName).updateSchema();
            for (IcebergColumnChange column : columns) {
                updateSchema.addColumn(column.getName(), column.getType(), column.getComment(),
                        column.getDefaultValue());
            }
            updateSchema.commit();
        }

        @Override
        public void dropColumn(String dbName, String tableName, String columnName) {
            UpdateSchema updateSchema = loadTable(dbName, tableName).updateSchema();
            updateSchema.deleteColumn(columnName);
            updateSchema.commit();
        }

        @Override
        public void renameColumn(String dbName, String tableName, String oldName, String newName) {
            UpdateSchema updateSchema = loadTable(dbName, tableName).updateSchema();
            updateSchema.renameColumn(oldName, newName);
            updateSchema.commit();
        }

        @Override
        public void modifyColumn(String dbName, String tableName, IcebergColumnChange column,
                ConnectorColumnPosition position) {
            Table table = loadTable(dbName, tableName);
            Types.NestedField current = table.schema().findField(column.getName());
            if (current == null) {
                throw new DorisConnectorException("Column " + column.getName() + " does not exist");
            }
            // Iceberg can widen required -> optional but never optional -> required (existing data may hold
            // nulls), so a NOT NULL request on an already-nullable column fails loud — legacy parity
            // (IcebergMetadataOps.validateForModifyColumn / validateForModifyComplexColumn).
            if (current.isOptional() && !column.isNullable()) {
                throw new DorisConnectorException(
                        "Can not change nullable column " + column.getName() + " to not null");
            }
            UpdateSchema updateSchema = table.updateSchema();
            Type newType = column.getType();
            if (newType.isPrimitiveType()) {
                updateSchema.updateColumn(column.getName(), newType.asPrimitiveType(), column.getComment());
            } else {
                // A complex (STRUCT/ARRAY/MAP) modify diffs the new type against the current one field-by-field
                // (IcebergComplexTypeDiff); the top-level column doc is updated separately, as in legacy.
                if (current.type().isPrimitiveType()) {
                    throw new DorisConnectorException("Modify column type from non-complex to complex is not"
                            + " supported: " + column.getName());
                }
                IcebergComplexTypeDiff.apply(updateSchema, column.getName(), current.type(), newType);
                if (!Objects.equals(current.doc(), column.getComment())) {
                    updateSchema.updateColumnDoc(column.getName(), column.getComment());
                }
            }
            if (column.isNullable()) {
                updateSchema.makeColumnOptional(column.getName());
            }
            applyPosition(updateSchema, position, column.getName());
            updateSchema.commit();
        }

        @Override
        public void reorderColumns(String dbName, String tableName, List<String> newOrder) {
            UpdateSchema updateSchema = loadTable(dbName, tableName).updateSchema();
            updateSchema.moveFirst(newOrder.get(0));
            for (int i = 1; i < newOrder.size(); i++) {
                updateSchema.moveAfter(newOrder.get(i), newOrder.get(i - 1));
            }
            updateSchema.commit();
        }

        @Override
        public void createOrReplaceBranch(String dbName, String tableName, BranchChange branch) {
            Table icebergTable = loadTable(dbName, tableName);
            String branchName = branch.getName();
            if (branchName == null || branchName.trim().isEmpty()) {
                throw new DorisConnectorException("Branch name cannot be empty");
            }
            // null snapshotId == "use the table's current snapshot" (may itself be null for an empty table),
            // mirroring legacy IcebergMetadataOps.createOrReplaceBranchImpl.
            Long snapshotId = resolveSnapshotId(branch.getSnapshotId(), icebergTable);
            boolean refExists = icebergTable.refs().get(branchName) != null;
            ManageSnapshots manageSnapshots = icebergTable.manageSnapshots();
            if (branch.isCreate() && branch.isReplace() && !refExists) {
                createBranch(manageSnapshots, branchName, snapshotId);
            } else if (branch.isReplace()) {
                if (snapshotId == null) {
                    throw new DorisConnectorException("Cannot complete replace branch operation on "
                            + icebergTable.name() + " , main has no snapshot");
                }
                manageSnapshots.replaceBranch(branchName, snapshotId);
            } else {
                if (refExists && branch.isIfNotExists()) {
                    return;
                }
                createBranch(manageSnapshots, branchName, snapshotId);
            }
            if (branch.getMaxSnapshotAgeMs() != null) {
                manageSnapshots.setMaxSnapshotAgeMs(branchName, branch.getMaxSnapshotAgeMs());
            }
            if (branch.getMinSnapshotsToKeep() != null) {
                manageSnapshots.setMinSnapshotsToKeep(branchName, branch.getMinSnapshotsToKeep());
            }
            if (branch.getMaxRefAgeMs() != null) {
                manageSnapshots.setMaxRefAgeMs(branchName, branch.getMaxRefAgeMs());
            }
            manageSnapshots.commit();
        }

        @Override
        public void createOrReplaceTag(String dbName, String tableName, TagChange tag) {
            Table icebergTable = loadTable(dbName, tableName);
            Long snapshotId = resolveSnapshotId(tag.getSnapshotId(), icebergTable);
            if (snapshotId == null) {
                // Creating a tag on an empty table is not allowed (legacy parity, incl. the legacy message text).
                throw new DorisConnectorException("Cannot complete replace branch operation on "
                        + icebergTable.name() + " , main has no snapshot");
            }
            String tagName = tag.getName();
            if (tagName == null || tagName.trim().isEmpty()) {
                throw new DorisConnectorException("Tag name cannot be empty");
            }
            boolean refExists = icebergTable.refs().get(tagName) != null;
            ManageSnapshots manageSnapshots = icebergTable.manageSnapshots();
            if (tag.isCreate() && tag.isReplace() && !refExists) {
                manageSnapshots.createTag(tagName, snapshotId);
            } else if (tag.isReplace()) {
                manageSnapshots.replaceTag(tagName, snapshotId);
            } else {
                if (refExists && tag.isIfNotExists()) {
                    return;
                }
                manageSnapshots.createTag(tagName, snapshotId);
            }
            if (tag.getMaxRefAgeMs() != null) {
                manageSnapshots.setMaxRefAgeMs(tagName, tag.getMaxRefAgeMs());
            }
            manageSnapshots.commit();
        }

        @Override
        public void dropBranch(String dbName, String tableName, DropRefChange branch) {
            Table icebergTable = loadTable(dbName, tableName);
            SnapshotRef ref = icebergTable.refs().get(branch.getName());
            if (ref != null || !branch.isIfExists()) {
                icebergTable.manageSnapshots().removeBranch(branch.getName()).commit();
            }
        }

        @Override
        public void dropTag(String dbName, String tableName, DropRefChange tag) {
            Table icebergTable = loadTable(dbName, tableName);
            SnapshotRef ref = icebergTable.refs().get(tag.getName());
            if (ref != null || !tag.isIfExists()) {
                icebergTable.manageSnapshots().removeTag(tag.getName()).commit();
            }
        }

        /** The explicit snapshot id, else the table's current snapshot id, else {@code null} (empty table). */
        private static Long resolveSnapshotId(Long explicitSnapshotId, Table icebergTable) {
            if (explicitSnapshotId != null) {
                return explicitSnapshotId;
            }
            Snapshot current = icebergTable.currentSnapshot();
            return current == null ? null : current.snapshotId();
        }

        /** {@code createBranch(name)} when no snapshot is pinned, else {@code createBranch(name, id)}. */
        private static void createBranch(ManageSnapshots manageSnapshots, String branchName, Long snapshotId) {
            if (snapshotId == null) {
                manageSnapshots.createBranch(branchName);
            } else {
                manageSnapshots.createBranch(branchName, snapshotId);
            }
        }

        @Override
        public void addPartitionField(String dbName, String tableName, PartitionFieldChange change) {
            UpdatePartitionSpec updateSpec = loadTable(dbName, tableName).updateSpec();
            Term transform = getTransform(change.getTransformName(), change.getColumnName(),
                    change.getTransformArg());
            // A non-null partitionFieldName is the AS alias (mirroring IcebergMetadataOps.addPartitionField).
            if (change.getPartitionFieldName() != null) {
                updateSpec.addField(change.getPartitionFieldName(), transform);
            } else {
                updateSpec.addField(transform);
            }
            updateSpec.commit();
        }

        @Override
        public void dropPartitionField(String dbName, String tableName, PartitionFieldChange change) {
            UpdatePartitionSpec updateSpec = loadTable(dbName, tableName).updateSpec();
            // Remove by field name when given, else by the transform that identifies the field (legacy parity).
            if (change.getPartitionFieldName() != null) {
                updateSpec.removeField(change.getPartitionFieldName());
            } else {
                Term transform = getTransform(change.getTransformName(), change.getColumnName(),
                        change.getTransformArg());
                updateSpec.removeField(transform);
            }
            updateSpec.commit();
        }

        @Override
        public void replacePartitionField(String dbName, String tableName, PartitionFieldChange change) {
            UpdatePartitionSpec updateSpec = loadTable(dbName, tableName).updateSpec();
            // Remove the old field first, then add the new one — both in one spec update (legacy parity).
            if (change.getOldPartitionFieldName() != null) {
                updateSpec.removeField(change.getOldPartitionFieldName());
            } else {
                Term oldTransform = getTransform(change.getOldTransformName(), change.getOldColumnName(),
                        change.getOldTransformArg());
                updateSpec.removeField(oldTransform);
            }
            Term newTransform = getTransform(change.getTransformName(), change.getColumnName(),
                    change.getTransformArg());
            if (change.getPartitionFieldName() != null) {
                updateSpec.addField(change.getPartitionFieldName(), newTransform);
            } else {
                updateSpec.addField(newTransform);
            }
            updateSpec.commit();
        }

        /**
         * Builds an iceberg partition {@link Term} from a neutral transform spec, mirroring legacy
         * {@code IcebergMetadataOps.getTransform}: a {@code null} transform name is identity ({@code ref}),
         * {@code bucket}/{@code truncate} require a width, and {@code year}/{@code month}/{@code day}/
         * {@code hour} take none. An unknown transform fails loud.
         */
        private static Term getTransform(String transformName, String columnName, Integer transformArg) {
            if (columnName == null) {
                throw new DorisConnectorException("Column name is required for partition transform");
            }
            if (transformName == null) {
                return Expressions.ref(columnName);
            }
            switch (transformName.toLowerCase(Locale.ROOT)) {
                case "bucket":
                    if (transformArg == null) {
                        throw new DorisConnectorException("Bucket transform requires a bucket count argument");
                    }
                    return Expressions.bucket(columnName, transformArg);
                case "truncate":
                    if (transformArg == null) {
                        throw new DorisConnectorException("Truncate transform requires a width argument");
                    }
                    return Expressions.truncate(columnName, transformArg);
                case "year":
                    return Expressions.year(columnName);
                case "month":
                    return Expressions.month(columnName);
                case "day":
                    return Expressions.day(columnName);
                case "hour":
                    return Expressions.hour(columnName);
                default:
                    throw new DorisConnectorException("Unsupported partition transform: " + transformName);
            }
        }

        /** Applies the (nullable) position to a not-yet-committed schema update: FIRST / AFTER / no-op. */
        private void applyPosition(UpdateSchema updateSchema, ConnectorColumnPosition position,
                String columnName) {
            if (position == null) {
                return;
            }
            if (position.isFirst()) {
                updateSchema.moveFirst(columnName);
            } else {
                updateSchema.moveAfter(columnName, position.getAfterColumn());
            }
        }

        /** The catalog as a {@link SupportsNamespaces}, or a fail-loud error (legacy cast unconditionally). */
        private SupportsNamespaces requireNamespaces() {
            if (!(catalog instanceof SupportsNamespaces)) {
                throw new DorisConnectorException("Iceberg catalog does not support databases (namespaces)");
            }
            return (SupportsNamespaces) catalog;
        }

        /** View filtering is on iff the catalog is a {@link ViewCatalog} and (for REST) views are enabled. */
        private boolean isViewCatalogEnabled() {
            if (!(catalog instanceof ViewCatalog)) {
                return false;
            }
            return !restFlavor || viewEnabled;
        }

        /** The root namespace to start database listing from: the external-catalog level, else empty. */
        private Namespace rootNamespace() {
            return externalCatalogName.map(Namespace::of).orElseGet(Namespace::empty);
        }

        /**
         * Builds the multi-level namespace for {@code dbName}, mirroring legacy {@code getNamespace}: split
         * on {@code '.'} (omit empties / trim), then append the external-catalog level last when present.
         */
        private Namespace toNamespace(String dbName) {
            // Use the SAME Guava splitter as legacy IcebergMetadataOps.getNamespace so splitting is
            // byte-faithful — including trimResults()==CharMatcher.whitespace(), which trims Unicode
            // whitespace above U+0020 (e.g. U+3000) that String.trim() would leave behind.
            List<String> levels = new ArrayList<>(
                    Splitter.on('.').omitEmptyStrings().trimResults().splitToList(dbName));
            externalCatalogName.ifPresent(levels::add);
            return Namespace.of(levels.toArray(new String[0]));
        }

        private TableIdentifier toTableIdentifier(String dbName, String tableName) {
            return TableIdentifier.of(toNamespace(dbName), tableName);
        }

        private static boolean isBlank(String s) {
            return s == null || s.trim().isEmpty();
        }

        @Override
        public void close() throws IOException {
            if (catalog instanceof java.io.Closeable) {
                ((java.io.Closeable) catalog).close();
            }
        }
    }
}
