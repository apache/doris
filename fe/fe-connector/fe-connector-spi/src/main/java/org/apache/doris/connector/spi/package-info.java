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

/**
 * The connector plugin contract: everything on the engine &harr; connector boundary, in both directions.
 *
 * <p>Three kinds of type live here, and the difference is who implements them:</p>
 *
 * <ul>
 * <li>{@link ConnectorProvider} — <b>implemented by the connector</b>, and the single entry point the engine
 *     discovers, via Java ServiceLoader or directory-based plugin loading; a connector registers it in
 *     {@code META-INF/services}.</li>
 * <li>{@link Connector}, {@link ConnectorMetadata} with its Ops sub-interfaces, the scan / write / procedure
 *     providers, the handle / pushdown / mvcc / ddl / event / rest types, {@link ConnectorCapability} —
 *     <b>implemented by the connector and consumed by the engine</b>. This is the bulk of the module, and the
 *     javadoc here <b>is</b> the API reference.</li>
 * <li>{@link ConnectorContext}, {@link ConnectorStorageContext}, {@link ConnectorSession},
 *     {@link ConnectorHttpSecurityHook}, {@link ConnectorValidationContext} — <b>implemented (or produced) by
 *     the engine</b> and handed to the connector: the storage, filesystem, configuration and authentication
 *     services a plugin may use without depending on the engine.</li>
 * </ul>
 *
 * <p>Read this page before adding a connector or extending these interfaces. It records the rules the
 * existing connectors follow, and — where a rule is not yet fully honored — says so explicitly instead of
 * describing the target state as if it were already true. Every rule below can be checked against the code
 * that is cited with it; when the two disagree, the code wins and this page is the bug.</p>
 *
 * <h2>Rule 1 — declare a capability in exactly one of three layers</h2>
 *
 * <ol>
 * <li><b>A whole subsystem is present or absent</b> &rarr; a getter on {@link Connector} that returns
 *     {@code null} when the subsystem is absent: {@code getScanPlanProvider()},
 *     {@code getWritePlanProvider()}, {@code getProcedureOps()}, {@code getEventSource()},
 *     {@code getRestPassthrough()}. These stay {@code null}-returning rather than {@code Optional}: the
 *     engine already branches on {@code null}, and changing the shape would be pure churn.</li>
 * <li><b>A switch inside one subsystem</b> &rarr; a {@code supportsXxx()} / {@code requiresXxx()} method on
 *     THAT subsystem's provider, which is the single and ONLY source of truth. {@link Connector} used to
 *     carry null-safe mirrors of the write switches so the engine never had to null-check; they were deleted,
 *     because a mirror is a second overridable answer to one question and a connector overriding one while
 *     leaving the provider at its default would produce two divergent answers with no compile error. The
 *     engine fetches the provider and asks it, treating a {@code null} provider as "not supported".</li>
 * <li><b>A gate the engine must evaluate during static planning, before any provider is reachable</b> &rarr;
 *     a constant in {@link ConnectorCapability}, resolved additively as
 *     (connector-level set) &cup; (per-table set). A switch that corresponds one-to-one with a provider does
 *     NOT belong here. Worked example, already written into the code: {@code SUPPORTS_SORT_ORDER} is an
 *     enum constant because the engine must decide during analysis whether {@code CREATE TABLE ... ORDER BY}
 *     is accepted, and no write-plan provider exists at that point; the runtime row-ordering sink trait
 *     {@code ConnectorWritePlanProvider.requiresFullSchemaWriteOrder()} lives on the provider. Both mention
 *     "write", but one gates a DDL clause and the other shapes the write path.</li>
 * </ol>
 *
 * <p>Use {@code Optional} for a <em>missing value</em> and {@code null} only for an <em>absent
 * subsystem</em>; they are not two competing idioms for the same thing.</p>
 *
 * <p><b>Current deviations.</b> (a) A new switch must default to {@code false} (opt-in), so that adding a
 * capability can never change an existing connector's behavior. One legacy switch violates this and defaults
 * to {@code true}: {@code ConnectorPushdownOps.supportsCastPredicatePushdown(ConnectorSession)} — see the
 * risk paragraph in its own javadoc, and do not copy its polarity. (b) The {@code supportsXxx()} shape also
 * appears on non-provider interfaces ({@code ConnectorPushdownOps}, {@code ConnectorSchemaOps},
 * {@code ConnectorTableOps}); those are per-operation switches on the interface that owns the operation,
 * which is consistent with layer 2, but there is no provider object to attach them to. (c) Per-table
 * refinement of {@link ConnectorCapability} is honored for only five of the thirteen constants today, and
 * each constant's own javadoc states its scope (catalog-only, or catalog &cup; per-table) and why; do not
 * assume a newly added constant can be refined per table.</p>
 *
 * <h2>Rule 2 — which exception to throw, and when to fail loud</h2>
 *
 * <p>Two engine paths translate connector exceptions into user-facing errors, and they accept different
 * types. Getting this wrong does not break anything visibly; it only degrades the error the user sees into an
 * internal FE exception.</p>
 *
 * <ul>
 * <li><b>Metadata, DDL, DML and scan-planning paths</b> &rarr; throw {@link DorisConnectorException} (or a
 *     subclass). It is the family the engine catches and re-wraps (for example into {@code DdlException}) on
 *     those paths.</li>
 * <li><b>Catalog property validation</b> ({@code ConnectorProvider.validateProperties}) &rarr; throw
 *     {@code IllegalArgumentException}. It is the ONLY type the engine unwraps there
 *     ({@code PluginDrivenExternalCatalog.checkProperties} catches it and re-throws its message as
 *     {@code DdlException}); several connectors depend on this and say so at their throw sites. Do not
 *     "correct" these to {@link DorisConnectorException}.</li>
 * </ul>
 *
 * <p>Everything else ({@code UnsupportedOperationException}, {@code IllegalStateException}, plain
 * {@code RuntimeException}) is untranslated and surfaces as an internal error. The one accepted use of
 * {@code UnsupportedOperationException} is the "this optional SPI method is not implemented" idiom.</p>
 *
 * <p><b>Subclass {@link DorisConnectorException} when the CALLER must distinguish outcomes</b>, not to build
 * a taxonomy for its own sake. Shipped precedent: {@code HiveDirectoryListingException} exists so the scan
 * path can catch <em>only</em> a skippable listing failure while a plain {@link DorisConnectorException}
 * still fails the query loud.</p>
 *
 * <p><b>Fail loud vs. degrade silently.</b> Anything the user explicitly asked for — DDL, writes, an explicit
 * {@code ANALYZE ... WITH SAMPLE} — must fail loud, because a swallowed error there produces a wrong result
 * or a wrong statistic that then looks authoritative. Anything the engine attempts opportunistically for
 * optimization must degrade silently (return empty / a default, and log), because it must never be able to
 * fail a query that would otherwise succeed.</p>
 *
 * <h2>Rule 3 — thrift types only at the BE protocol boundary</h2>
 *
 * <p>BE is C++ and has no plugin mechanism, so a payload destined for BE has to be a thrift type. That is the
 * only reason thrift appears in this module, and the complete list of places it does is:</p>
 *
 * <ul>
 * <li>{@code scan/ConnectorScanPlanProvider} — {@code TFileCompressType}, {@code TFileScanRangeParams},
 *     {@code TTableFormatFileDesc}</li>
 * <li>{@code scan/ConnectorScanRange} — {@code TFileRangeDesc}, {@code TTableFormatFileDesc}</li>
 * <li>{@code handle/ConnectorWriteHandle} — {@code TSortInfo}</li>
 * <li>{@code write/ConnectorSinkPlan} — {@code TDataSink}</li>
 * <li>{@link ConnectorTableMetadataOps#buildTableDescriptor} — {@code TTableDescriptor} (written as an inline
 *     fully-qualified name, not an import)</li>
 * </ul>
 *
 * <p>Do not add a method that takes a thrift type as a PARAMETER. A new return value that must carry a BE
 * payload may only reuse a type already on that list. {@code fe-thrift} is a {@code provided} dependency
 * here: it is supplied by the parent classloader at runtime and is not part of a plugin package.</p>
 *
 * <p><b>Local convention.</b> The engine-service types ({@link ConnectorContext},
 * {@link ConnectorStorageContext}, {@link ConnectorConf}, {@link ConnectorBrokerAddress}) avoid thrift even
 * for data that ends up at BE: they pass an enum NAME as a {@code String}, a neutral broker-address type, and
 * a thrift enum's integer VALUE as an {@code int}. Keep it that way when extending them — but do not read it
 * as a module-wide guarantee, since the list above does use thrift at the BE protocol boundary.</p>
 *
 * <h2>Rule 4 — what belongs in this module</h2>
 *
 * <p>Everything on the engine &harr; connector boundary belongs here, in <em>both</em> directions: the types a
 * connector implements and the engine calls, and the services the engine implements and a connector calls.
 * They are deliberately NOT split into two modules by "who implements", because the boundary is genuinely
 * bidirectional — {@link ConnectorProvider#create(java.util.Map, ConnectorContext)} takes a
 * {@link ConnectorContext}, and {@link ConnectorContext#createSiblingConnector} hands back a
 * {@link Connector} — so such a split would be circular. Trino makes the same call: its whole plugin
 * surface is one {@code trino-spi} module.</p>
 *
 * <p>What does NOT belong here: a type only one connector uses (it belongs in that connector's own module),
 * and anything engine-internal (it stays in fe-core). Nothing here may reference fe-core, and a build gate
 * enforces it — see {@code AGENTS.md} next to this directory's README.</p>
 *
 * <p>Layout: the root package holds the core contract and the engine services; sub-packages group by concern
 * ({@code ddl}, {@code event}, {@code handle}, {@code mvcc}, {@code procedure}, {@code pushdown},
 * {@code rest}, {@code scan}, {@code write}).</p>
 *
 * <h2>Rule 5 — lifecycle, and which methods run off the query thread</h2>
 *
 * <ul>
 * <li>{@link Connector} — one instance per catalog, created once (see its own class javadoc).</li>
 * <li>{@link ConnectorMetadata} — <b>exactly one instance per catalog per statement</b>, closed
 *     deterministically when the statement ends. The engine's single entry point for obtaining one
 *     ({@code PluginDrivenMetadata}) states this contract; {@link Connector#getMetadata} is not called
 *     anywhere else in the engine. {@link ConnectorMetadata#close()} must be idempotent, and one instance
 *     must not be shared across threads.</li>
 * <li>{@link ConnectorStatisticsOps} — <b>may run on engine background pools</b>, and the engine pins no
 *     thread context classloader on any of them. See that interface's class javadoc for the pools and for
 *     what a connector has to do about it. This is the one contract on this page that causes a crash rather
 *     than a wrong answer when it is ignored.</li>
 * </ul>
 *
 * <h2>Rule 6 — a javadoc statement about ENGINE behavior must cite the engine code</h2>
 *
 * <p>These interfaces are the only behavioral contract a connector author has: connectors are packaged and
 * class-loaded separately and cannot read the engine. A javadoc line that describes what the engine does with
 * a value, and is wrong, is worse than no line at all — it produces an implementation that compiles, runs,
 * and silently returns wrong results. So when documenting engine behavior, name the engine class (and
 * ideally the method) that does it, so the next reader can re-verify instead of trusting the sentence.</p>
 *
 * <h2>Rule 7 — where a connector's tunable knobs live</h2>
 *
 * <p>Pick the channel by the SCOPE of the value. None of them requires an engine change any more, so a
 * knob that can be catalog-scoped should be, and one that cannot belongs in the plugin's own conf file.</p>
 *
 * <ul>
 * <li><b>Per catalog</b> &rarr; a key in {@code CREATE CATALOG ... PROPERTIES(...)}. The engine hands the
 *     whole property map to {@code ConnectorProvider.create} and exposes the same map per query through
 *     {@link ConnectorSession#getCatalogProperties()}; reject a bad value in
 *     {@code ConnectorProvider.validateProperties} (see Rule 2 for the exception type). Key names, parsing
 *     and defaults stay entirely inside the connector — <b>prefix the key with the connector's type name</b>
 *     so two connectors cannot collide. Shipped precedent: {@code hive.ignore_absent_partitions},
 *     {@code hive.enable_hms_events_incremental_sync} and {@code hive.hms_events_batch_size_per_rpc} are
 *     declared in {@code HiveCatalogProperties} and read in {@code HiveScanPlanProvider} /
 *     {@code HiveConnector}; those key strings appear nowhere in {@code fe-core}.</li>
 * <li><b>Per FE process</b> (one deployment-level value for every catalog, e.g. a driver directory)
 *     &rarr; a key in the plugin's own {@code <name>.conf}, read with {@code ConnectorConf.get}. The engine
 *     locates and parses {@code <pluginDir>/<name>.conf} generically
 *     ({@code ConnectorPluginManager.loadPlugins}) and serves it back through
 *     {@code ConnectorContext.getConnectorConfig()}, so no key name of yours reaches {@code fe-core} and
 *     adding one costs the engine nothing. Do <b>not</b> prefix these keys — the file name already
 *     namespaces them. Ship {@code src/main/resources/<name>.conf.template} and add it to your assembly's
 *     {@code <files>} at the zip root; {@code build.sh} seeds the live {@code .conf} from it. {@code <name>}
 *     is {@code ConnectorProvider.name()} and need not equal your plugin directory name
 *     ({@code plugins/connector/hive/} holds {@code hms.conf}).</li>
 * <li><b>Per FE process, legacy</b> &rarr; an {@code fe.conf} field forwarded through
 *     {@code ConnectorContext.getEnvironment()} by {@code DefaultConnectorContext.buildEnvironment}. This is
 *     the one shape that requires an engine change per key, and it is <b>closed to new keys</b>. What is
 *     still there is either shared by several connectors or not a connector setting at all
 *     ({@code doris_home}, {@code doris_version}); the connector settings among them are kept as the
 *     fallback {@code ConnectorConf.get} consults after the plugin conf, so deployments configured before
 *     the conf files existed keep working untouched.</li>
 * <li><b>Per session</b> &rarr; read the query's session variables from
 *     {@link ConnectorSession#getSessionProperties()}. The connector does not declare them; it looks up the
 *     names it cares about.</li>
 * </ul>
 *
 * <p>There is deliberately <b>no declarative property-descriptor mechanism</b> here. One was carried over
 * from Trino ({@code ConnectorPropertyMetadata} plus two {@link Connector} getters) and shipped for several
 * releases with zero implementors and zero consumers; it was removed rather than wired up, because in Doris
 * it would not have solved the thing it looks like it solves — there is no {@code SET SESSION
 * catalog.property} syntax to feed, and per-catalog values already arrive through the map above. If you find
 * yourself wanting the engine to validate unknown catalog property keys, that belongs in
 * {@code ConnectorProvider.validateProperties}, which already exists and already runs.</p>
 */
package org.apache.doris.connector.spi;
