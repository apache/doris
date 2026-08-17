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

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AccessDeniedException;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.authorization.ResourceKind;
import org.apache.doris.authorization.RowFilterSpec;
import org.apache.doris.authorization.spi.AuthorizationPlugin;
import org.apache.doris.authorization.spi.AuthorizationPluginFactory;
import org.apache.doris.catalog.AuthorizationInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AuthorizationException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.ClassLoaderUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.extension.loader.ApiVersionGate;
import org.apache.doris.extension.loader.ClassLoadingPolicy;
import org.apache.doris.extension.loader.DirectoryPluginRuntimeManager;
import org.apache.doris.extension.loader.LoadFailure;
import org.apache.doris.extension.loader.LoadReport;
import org.apache.doris.extension.loader.PluginHandle;
import org.apache.doris.extension.loader.PluginRegistry;
import org.apache.doris.plugin.PropertiesUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

/**
 * AccessControllerManager is the entry point of privilege authentication.
 *
 * <p>Access is decided by authorization sources - the built-in privilege model, or a plugin standing for an
 * external system - and this class decides only which one to ask: system-wide objects and catalog level
 * grants go to the source {@code access_controller_type} installs, everything inside a catalog goes to the
 * source that catalog is bound to. Whatever that source answers is the answer. The manager establishes no
 * privilege of its own beforehand and never combines two sources' verdicts, so which policies apply to a
 * resource is readable from which source the catalog is bound to.
 *
 * <p><b>The exemptions to that.</b> Three places grant access without asking any source, and they are the
 * whole of the list:
 * <ul>
 *   <li>{@link ConnectContext#isSkipAuth()} - a statement the engine is running on its own behalf inside
 *       another statement the caller was already authorized for. Honoured by {@link #checkTblPriv} and
 *       {@link #checkColumnsPriv}, and only by the overloads that take a {@code ConnectContext};</li>
 *   <li>{@link Config#skip_catalog_priv_check} on a catalog bound to a source of its own, in
 *       {@link #checkCtlPriv}: such a catalog stores no catalog level grant anywhere, so with the check
 *       switched off there is nobody left to ask;</li>
 *   <li>the literal {@code root} and {@code admin} accounts are exempt from row filters and column masks -
 *       not here but in the planner, see
 *       {@code LogicalCheckPolicy#findPolicy}. That one is the engine's own and is not offered to a source
 *       to decide.</li>
 * </ul>
 */
public class AccessControllerManager {
    private static final Logger LOG = LogManager.getLogger(AccessControllerManager.class);

    /**
     * The authorization plugin API contract this FE serves. Built from the version filtered into
     * fe-authorization-spi at build time, anchored on {@link AuthorizationPluginFactory} so that it is read
     * from the very artifact carrying the SPI. A missing or malformed resource is a build defect and fails
     * class initialization loudly rather than degrading into a check that admits everything.
     */
    private static final ApiVersionGate API_VERSION_GATE =
            ApiVersionGate.forFamily("authorization", AuthorizationPluginFactory.class);

    /**
     * Loaded from the FE rather than from the plugin jar, so that the types crossing the boundary - the
     * decision vocabulary in {@code org.apache.doris.authorization} and the contract in its {@code .spi}
     * sub-package - exist exactly once. A plugin carrying its own copy would hand back objects the engine
     * refuses to recognise as the types it asked for.
     */
    private static final List<String> AUTHORIZATION_PARENT_FIRST_PREFIXES =
            Collections.singletonList("org.apache.doris.authorization.");

    /** Family label in the process-wide {@link PluginRegistry}, i.e. in information_schema.extensions. */
    private static final String PLUGIN_FAMILY = "AUTHORIZATION";

    /**
     * Authorization sources that used to be published by a factory class of fe-core's own, and the name each
     * is published under now.
     *
     * <p>A catalog names its source in {@code access_controller.class}, and that value is persisted with the
     * catalog: an FE upgraded across the release that moved a source out of fe-core reads back the class name
     * written when the catalog was created. {@link #accessControllerClassNameMapping} answers for every
     * factory class that still exists, being filled from the classes actually registered; this table answers
     * for the ones that no longer do.
     *
     * <p>It lives here, and not in the plugin, because the class it names was fe-core's: whoever owned an
     * identifier owns remembering it. Trino, renaming its Hive connector, put the superseded name in the Hive
     * plugin for that same reason - there the identifier that went stale was the plugin's own.
     */
    private static final Map<String, String> SOURCES_THAT_LEFT_THE_KERNEL = ImmutableMap.of(
            "org.apache.doris.mysql.privilege.RangerDorisAccessControllerFactory", "ranger-doris");

    private Auth auth;
    // Governs everything no catalog-bound source governs; the built-in model unless configured otherwise
    private AuthorizationPlugin defaultAccessController;
    // A catalog name can be reused after DROP. Keep the catalog id next to the source so cleanup from
    // an old catalog generation can never remove or close the replacement generation's source.
    private Map<String, CatalogAccessControllerEntry> ctlToCtlAccessController = Maps.newConcurrentMap();
    // Factories publishing an authorization source under the current contract, by the name it is selected by
    private ConcurrentHashMap<String, AuthorizationPluginFactory> authorizationPluginFactories
            = new ConcurrentHashMap<>();
    // Cache of loaded access controller factories for quick creation of new access controllers
    private ConcurrentHashMap<String, AccessControllerFactory> accessControllerFactoriesCache
            = new ConcurrentHashMap<>();
    // Mapping between access controller class names and their identifiers for easy lookup of factory identifiers
    private ConcurrentHashMap<String, String> accessControllerClassNameMapping = new ConcurrentHashMap<>();
    // Holds the classloader of every plugin loaded from a directory, for the lifetime of the FE
    private final DirectoryPluginRuntimeManager<AuthorizationPluginFactory> pluginDirectoryRuntime =
            new DirectoryPluginRuntimeManager<>();
    /**
     * Plugins the startup sweep refused, whatever it refused them on.
     *
     * <p>Kept because the refusal and the complaint happen in different places: the load is a startup sweep
     * that logs and carries on, while what an operator sees is "no authorization plugin factory found for
     * {@code <name>}" from whoever asked for that name. Without this, a plugin that was refused would be
     * indistinguishable from one that was never installed - which is just as true of a plugin whose jars
     * would not resolve or whose factory would not instantiate as it is of one built against another
     * release, so every stage is collected here and each entry says which stage it came from.
     */
    private final List<String> pluginLoadRejections = new CopyOnWriteArrayList<>();

    /**
     * A manager that authorizes nothing loads nothing: see {@link #AccessControllerManager(Auth, boolean)}.
     * Kept as a field because the catalog-bound sources are built lazily, long after the constructor.
     */
    private final boolean authorizesNothing;

    public AccessControllerManager(Auth auth) {
        this(auth, false);
    }

    /**
     * @param isCheckpointCatalog whether this belongs to a checkpoint {@link Env}, which only replays
     *         metadata to write an image and authorizes nothing. Such an {@code Env} must neither sweep the
     *         plugin directory nor build an authorization source: a source starts threads - a Ranger one
     *         starts a policy refresher and a policy download timer - that nothing ever stops, and a
     *         checkpoint {@code Env} is built and discarded twice per checkpoint round, forever.
     */
    public AccessControllerManager(Auth auth, boolean isCheckpointCatalog) {
        this.auth = auth;
        this.authorizesNothing = isCheckpointCatalog;
        if (isCheckpointCatalog) {
            this.defaultAccessController = new InternalAuthorizationPlugin(auth);
        } else {
            loadAccessControllerPlugins();
            String accessControllerName = Config.access_controller_type;
            this.defaultAccessController = loadAccessControllerOrThrow(accessControllerName);
        }
        ctlToCtlAccessController.put(InternalCatalog.INTERNAL_CATALOG_NAME,
                new CatalogAccessControllerEntry(
                        InternalCatalog.INTERNAL_CATALOG_ID, defaultAccessController, false));
    }

    private static final class CatalogAccessControllerEntry {
        private final long catalogId;
        private final AuthorizationPlugin accessController;
        // The default source is shared with the internal catalog. Catalog aliases must detach it but never
        // close it when an external catalog is reset or dropped.
        private final boolean owned;

        private CatalogAccessControllerEntry(
                long catalogId, AuthorizationPlugin accessController, boolean owned) {
            this.catalogId = catalogId;
            this.accessController = accessController;
            this.owned = owned;
        }
    }

    private AuthorizationPlugin loadAccessControllerOrThrow(String accessControllerName) {
        if (accessControllerName.equalsIgnoreCase(InternalAuthorizationPlugin.NAME)) {
            return new InternalAuthorizationPlugin(auth);
        }
        if (!isKnownAuthorizationSource(accessControllerName)) {
            throw new RuntimeException("No authorization plugin factory found for " + accessControllerName
                    + "." + pluginLocationHint() + pluginLoadRejectionHint());
        }
        Map<String, String> prop;
        try {
            prop = PropertiesUtils.loadAccessControllerPropertiesOrNull();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load authorization properties."
                    + "Please check the configuration file, authorization name is " + accessControllerName, e);
        }
        return create(accessControllerName, prop);
    }

    /**
     * Builds the authorization source published under {@code name}, whichever contract publishes it.
     *
     * <p>A source written against the current contract is built with the context it may put questions to the
     * engine through. It cannot be handed that context any earlier than this - the context has to name the
     * source it belongs to, and the source does not exist until its factory has run.
     *
     * <p>The factory runs under its own plugin's classloader as the thread context one. A plugin that bundles
     * a library which resolves class names through the context classloader - Hadoop's Configuration is the
     * one both Ranger sources drag in - would otherwise load the name from the engine's copy of that library
     * and get back a class the plugin's own copy does not recognise as implementing its interface. The
     * refresher threads such a library starts inherit this classloader too, which is what keeps them working
     * after the swap is undone.
     */
    private AuthorizationPlugin create(String name, Map<String, String> properties) {
        if (InternalAuthorizationPlugin.NAME.equalsIgnoreCase(name)) {
            // The built-in model is published by no factory, so it is named here rather than looked up.
            return new InternalAuthorizationPlugin(auth);
        }
        AuthorizationPluginFactory factory = authorizationPluginFactories.get(name);
        if (factory == null) {
            return adapt(name, accessControllerFactoriesCache.get(name).createAccessController(properties));
        }
        EngineAuthorizationContext context = new EngineAuthorizationContext(this, auth);
        Map<String, String> pluginProperties = properties == null ? Collections.emptyMap() : properties;
        AuthorizationPlugin plugin = inClassLoaderOf(factory,
                () -> factory.create(pluginProperties, context));
        context.servedBy(plugin);
        return plugin;
    }

    /**
     * Runs one call into a plugin with that plugin's own classloader as the thread context one, and puts back
     * whatever was there afterwards.
     *
     * <p>Every crossing into plugin code goes through here or through {@link #checkInClassLoaderOf}, not just
     * the one that builds it. A plugin that bundles a library resolving class names through the context
     * classloader - Hadoop's Configuration is the one both Ranger sources drag in - would otherwise load the
     * name from the engine's copy of that library and get back a class its own copy does not recognise as
     * implementing its interface. The same reasoning makes the connector family wrap all of its boundaries;
     * leaving one unwrapped is a defect that only shows up with a plugin that happens to resolve something
     * there.
     */
    private static <T> T inClassLoaderOf(Object plugin, Supplier<T> call) {
        Thread current = Thread.currentThread();
        ClassLoader callerLoader = current.getContextClassLoader();
        try {
            current.setContextClassLoader(plugin.getClass().getClassLoader());
            return call.get();
        } finally {
            current.setContextClassLoader(callerLoader);
        }
    }

    /** {@link #inClassLoaderOf} for the one boundary that may refuse. */
    private static void checkInClassLoaderOf(AuthorizationPlugin plugin, PluginCheck check)
            throws AccessDeniedException {
        Thread current = Thread.currentThread();
        ClassLoader callerLoader = current.getContextClassLoader();
        try {
            current.setContextClassLoader(plugin.getClass().getClassLoader());
            check.run();
        } finally {
            current.setContextClassLoader(callerLoader);
        }
    }

    /** One check inside a plugin; refusing is the only thing it may fail with. */
    @FunctionalInterface
    private interface PluginCheck {
        void run() throws AccessDeniedException;
    }

    /** Presents a controller written against the older per-scope interface as an authorization source. */
    private AuthorizationPlugin adapt(String name, CatalogAccessController controller) {
        // The global verdict the older interface was handed alongside every scoped question. Routed like any
        // other global check, which is where the engine read it from before.
        return new LegacyAccessControllerPlugin(name, controller,
                (subject, requirement) -> decide(AccessTranslation.userIdentityOf(subject),
                        AuthorizedResource.global(), requirement));
    }

    private boolean isKnownAuthorizationSource(String name) {
        return InternalAuthorizationPlugin.NAME.equalsIgnoreCase(name)
                || authorizationPluginFactories.containsKey(name)
                || accessControllerFactoriesCache.containsKey(name);
    }

    /**
     * Loads every source this FE can select, class path before directory in both contracts.
     *
     * <p>The order is what makes "a jar dropped into the plugin directory can never displace a source shipped
     * with the FE" true: the directory sweep skips a name that is taken, so every name the class path
     * publishes has to be registered before it runs - under either contract, since the two share one
     * namespace.
     */
    private void loadAccessControllerPlugins() {
        // The built-in model is published by no factory, so nothing else would give it an inventory row -
        // leaving information_schema.extensions listing the sources that are installed but idle, and not the
        // one that decides every check on a default FE.
        PluginRegistry.getInstance().register(PLUGIN_FAMILY, InternalAuthorizationPlugin.NAME,
                PluginRegistry.implementationVersionOf(InternalAuthorizationPlugin.class),
                "The privilege model Doris ships with: users, roles and GRANT statements",
                PluginRegistry.PluginSource.BUILTIN);
        // Sources on the FE's class path. Held to the plugin API version like the directory channel: nothing
        // in this source tree publishes an AuthorizationPluginFactory here any more - the Ranger sources are
        // installed as plugins - so in a release package the only thing this channel can find is a jar
        // someone added to fe/lib, built against some other Doris release.
        Iterator<ServiceLoader.Provider<AuthorizationPluginFactory>> providers =
                ServiceLoader.load(AuthorizationPluginFactory.class).stream().iterator();
        while (providers.hasNext()) {
            ServiceLoader.Provider<AuthorizationPluginFactory> descriptor = providers.next();
            Class<? extends AuthorizationPluginFactory> factoryClass = descriptor.type();
            String rejection = API_VERSION_GATE.rejectionReasonForClass(factoryClass);
            if (rejection != null) {
                // Refuse the descriptor before get(): an incompatible factory's constructor may link against
                // an API this FE must never execute.
                LOG.warn("Skip authorization plugin factory {} from class path: {}",
                        factoryClass.getName(), rejection);
                pluginLoadRejections.add(factoryClass.getName() + " (class path): " + rejection);
                continue;
            }
            AuthorizationPluginFactory factory = descriptor.get();
            LOG.info("Found authorization plugin factory: {} from class path.", factory.name());
            if (registerPluginFactory(factory)) {
                PluginRegistry.getInstance().registerBuiltin(PLUGIN_FAMILY, factory);
            }
        }
        ServiceLoader<AccessControllerFactory> loaderFromClasspath = ServiceLoader.load(AccessControllerFactory.class);
        for (AccessControllerFactory factory : loaderFromClasspath) {
            LOG.info("Found Access Controller Plugin Factory: {} from class path.", factory.factoryIdentifier());
            registerLegacyFactory(factory);
        }
        loadAuthorizationPluginsFromDirectory();
        List<AccessControllerFactory> loader = null;
        try {
            loader = ClassLoaderUtils.loadServicesFromDirectory(AccessControllerFactory.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Authentication Plugin Factories", e);
        }
        for (AccessControllerFactory factory : loader) {
            LOG.info("Found Access Controller Plugin Factory: {} from directory.", factory.factoryIdentifier());
            registerLegacyFactory(factory);
        }
    }

    /**
     * Loads authorization plugins from {@code authorization_plugins_dir}, laid out one plugin per
     * subdirectory: {@code <dir>/*.jar} plus {@code <dir>/lib/*.jar}.
     *
     * <p>A directory that fails is logged and skipped: one unusable plugin must not stop an FE from
     * starting, and if the failed one is the very source {@code access_controller_type} names, the
     * constructor refuses right afterwards anyway - with the reason attached, see
     * {@link #pluginLoadRejectionHint()}.
     *
     * <p>This is a different layout from the one the deprecated {@link AccessControllerFactory} channel
     * reads out of the same directory, which takes jars lying loose at its root. The two cannot collide:
     * that channel lists only files, this one lists only subdirectories.
     */
    private void loadAuthorizationPluginsFromDirectory() {
        List<Path> pluginRoots = new ArrayList<>();
        for (Path root : ClassLoaderUtils.parsePluginRootDirectories(Config.authorization_plugins_dir)) {
            if (Files.isDirectory(root)) {
                pluginRoots.add(root);
            } else {
                // Having nowhere to put plugins is the normal state of an FE with none, so this is not a
                // warning: one that fires on every start teaches operators to skip the ones that matter.
                LOG.info("No authorization plugin directory at {}; skipping the directory channel.", root);
            }
        }
        if (pluginRoots.isEmpty()) {
            return;
        }
        LoadReport<AuthorizationPluginFactory> report = pluginDirectoryRuntime.loadAll(
                pluginRoots,
                AccessControllerManager.class.getClassLoader(),
                AuthorizationPluginFactory.class,
                new ClassLoadingPolicy(AUTHORIZATION_PARENT_FIRST_PREFIXES),
                API_VERSION_GATE);

        for (LoadFailure failure : report.getFailures()) {
            LOG.warn("Skip authorization plugin directory: pluginDir={}, stage={}, message={}",
                    failure.getPluginDir(), failure.getStage(), failure.getMessage(), failure.getCause());
            pluginLoadRejections.add(failure.getPluginDir() + " (" + failure.getStage() + "): "
                    + failure.getMessage());
        }

        for (PluginHandle<AuthorizationPluginFactory> handle : report.getSuccesses()) {
            String name = handle.getPluginName();
            if (isKnownAuthorizationSource(name)) {
                // Whatever is already installed under this name keeps it, so that dropping a jar into the
                // plugin directory can never displace a source shipped with the FE. Both contracts publish
                // into the same namespace, so both tables have to be consulted - and so does the name the
                // built-in model is selected by.
                LOG.warn("Skip authorization plugin '{}' from {}: that name is already taken by a source on"
                        + " the class path", name, handle.getPluginDir());
                pluginLoadRejections.add(handle.getPluginDir() + " (name): '" + name
                        + "' is already taken by a source on the class path");
                pluginDirectoryRuntime.discard(name);
                continue;
            }
            if (!registerPluginFactory(handle.getFactory())) {
                pluginDirectoryRuntime.discard(name);
                continue;
            }
            // Only a plugin that was actually admitted gets an inventory row, so
            // information_schema.extensions never lists an authorization source nothing can reach.
            PluginRegistry.getInstance().registerExternal(PLUGIN_FAMILY, handle);
            LOG.info("Loaded authorization plugin: name={}, pluginDir={}, jarCount={}",
                    name, handle.getPluginDir(), handle.getResolvedJars().size());
        }
    }

    /** @return whether the source was registered; a source claiming a reserved name is refused. */
    private boolean registerPluginFactory(AuthorizationPluginFactory factory) {
        String name = factory.name();
        if (isReservedSourceName(name)) {
            LOG.warn("Skip authorization plugin factory {}: '{}' is the name the built-in privilege model is"
                    + " selected by and cannot be published by a plugin.", factory.getClass().getName(), name);
            pluginLoadRejections.add(factory.getClass().getName() + " (name): '" + name + "' is reserved");
            return false;
        }
        authorizationPluginFactories.put(name, factory);
        // Keeps `access_controller.class = <factory class name>` working for a source published this way,
        // which is how a catalog written before plugin names existed still names its source.
        accessControllerClassNameMapping.put(factory.getClass().getName(), name);
        return true;
    }

    private void registerLegacyFactory(AccessControllerFactory factory) {
        String name = factory.factoryIdentifier();
        if (isReservedSourceName(name)) {
            LOG.warn("Skip access controller factory {}: '{}' is the name the built-in privilege model is"
                    + " selected by and cannot be published by a plugin.", factory.getClass().getName(), name);
            pluginLoadRejections.add(factory.getClass().getName() + " (name): '" + name + "' is reserved");
            return;
        }
        if (authorizationPluginFactories.containsKey(name)) {
            // Both were found, so say which one answers rather than letting the loser look installed.
            LOG.warn("Authorization source {} is published both as a plugin and as an access controller"
                    + " factory; the plugin is the one used.", name);
        } else {
            LOG.warn("Authorization source {} implements the deprecated {} interface. It keeps working, but"
                            + " that interface will be removed: implement {} instead and ship the plugin as"
                            + " a subdirectory of {}, declaring {}={} in its jar manifest.",
                    name, AccessControllerFactory.class.getName(), AuthorizationPluginFactory.class.getName(),
                    Config.authorization_plugins_dir, API_VERSION_GATE.getManifestAttribute(),
                    API_VERSION_GATE.getExpectedVersion());
            // An inventory row of its own, so that information_schema.extensions answers "which sources can
            // this FE select" rather than "which of them happen to use the current contract".
            PluginRegistry.getInstance().register(PLUGIN_FAMILY, name,
                    PluginRegistry.implementationVersionOf(factory.getClass()),
                    "Access controller implementing the deprecated " + AccessControllerFactory.class.getName(),
                    PluginRegistry.PluginSource.EXTERNAL);
        }
        accessControllerFactoriesCache.put(name, factory);
        accessControllerClassNameMapping.put(factory.getClass().getName(), name);
    }

    /**
     * Whether {@code name} is the one the built-in privilege model answers to.
     *
     * <p>Reserved in both selectors, not just in {@code access_controller_type}: a plugin publishing it would
     * be selectable by a catalog's {@code access_controller.class} and unselectable for the instance, which
     * is the same name meaning two different sources depending on where it is written.
     */
    private static boolean isReservedSourceName(String name) {
        return InternalAuthorizationPlugin.NAME.equalsIgnoreCase(name);
    }

    /**
     * A clause naming whatever the startup sweep refused, or the empty string when it refused nothing.
     *
     * <p>Appended to "no authorization plugin factory found for {@code <name>}". A refused plugin never reaches
     * the factory table, and the sweep itself does not fail, so without this a refusal would only ever be an
     * FE log line nobody correlates with the failure they are looking at.
     */
    private String pluginLoadRejectionHint() {
        if (pluginLoadRejections.isEmpty()) {
            return "";
        }
        return " Note that " + pluginLoadRejections.size()
                + " plugin(s) were refused while loading: "
                + String.join("; ", pluginLoadRejections);
    }

    /** The authorization source governing the objects inside {@code ctl}. */
    public AuthorizationPlugin getAccessControllerOrDefault(String ctl) {
        if (InternalCatalog.INTERNAL_CATALOG_NAME.equals(ctl)) {
            return defaultAccessController;
        }
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(ctl);
        if (catalog != null && catalog instanceof ExternalCatalog) {
            CatalogAccessControllerEntry entry = ctlToCtlAccessController.get(ctl);
            if (entry != null && entry.catalogId == catalog.getId()) {
                return entry.accessController;
            }
            lazyLoadCtlAccessController((ExternalCatalog) catalog);
            entry = ctlToCtlAccessController.get(ctl);
            if (entry != null && entry.catalogId == catalog.getId()) {
                return entry.accessController;
            }
        }

        return defaultAccessController;
    }

    private void lazyLoadCtlAccessController(ExternalCatalog catalog) {
        if (authorizesNothing) {
            // Same reason the constructor loaded nothing: a checkpoint Env replays a CREATE CATALOG but never
            // authorizes against it, and the source it would build here starts threads nothing stops.
            return;
        }
        CatalogAccessControllerEntry staleEntry = null;
        synchronized (this) {
            if (!isCurrentCatalog(catalog)) {
                return;
            }
            CatalogAccessControllerEntry entry = ctlToCtlAccessController.get(catalog.getName());
            if (entry != null && entry.catalogId == catalog.getId()) {
                return;
            }
            if (entry != null && ctlToCtlAccessController.remove(catalog.getName(), entry)) {
                staleEntry = entry;
            }
        }
        closeEntry(catalog.getName(), staleEntry);

        catalog.initAccessController(false);

        CatalogAccessControllerEntry displaced = null;
        boolean stillCurrent;
        synchronized (this) {
            stillCurrent = isCurrentCatalog(catalog);
            if (stillCurrent) {
                CatalogAccessControllerEntry entry = ctlToCtlAccessController.get(catalog.getName());
                if (entry == null || entry.catalogId != catalog.getId()) {
                    displaced = ctlToCtlAccessController.put(catalog.getName(),
                            new CatalogAccessControllerEntry(catalog.getId(), defaultAccessController, false));
                }
            }
        }
        closeEntry(catalog.getName(), displaced);
        if (!stillCurrent) {
            // A DROP can complete while initAccessController() is constructing the plugin. The custom
            // publication path performs the same post-publication check; this also covers the fallback path.
            removeAccessController(catalog.getName(), catalog.getId());
            return;
        }
        // If DROP won immediately after the synchronized publication, its onClose() removes this id. If DROP
        // already completed before publication, this final identity check removes the orphan ourselves.
        if (!isCurrentCatalog(catalog)) {
            removeAccessController(catalog.getName(), catalog.getId());
        }
    }

    public boolean checkIfAccessControllerExist(String ctl) {
        return ctlToCtlAccessController.containsKey(ctl);
    }

    public void createAccessController(ExternalCatalog catalog, String acFactoryClassName, Map<String, String> prop,
                                       boolean isDryRun) {
        String pluginIdentifier = getPluginIdentifierForAccessController(acFactoryClassName);
        AuthorizationPlugin accessController = create(pluginIdentifier, prop);
        if (isDryRun) {
            closeAccessController(catalog.getName(), accessController);
            return;
        }

        CatalogAccessControllerEntry displaced = null;
        boolean installed = false;
        synchronized (this) {
            if (isCurrentCatalog(catalog)) {
                CatalogAccessControllerEntry current = ctlToCtlAccessController.get(catalog.getName());
                if (current == null || current.catalogId != catalog.getId()) {
                    displaced = ctlToCtlAccessController.put(catalog.getName(),
                            new CatalogAccessControllerEntry(catalog.getId(), accessController, true));
                    installed = true;
                }
            }
        }
        closeEntry(catalog.getName(), displaced);
        if (!installed) {
            closeAccessController(catalog.getName(), accessController);
            return;
        }
        LOG.info("create access controller {} for catalog {}:{}",
                acFactoryClassName, catalog.getName(), catalog.getId());
        if (!isCurrentCatalog(catalog)) {
            removeAccessController(catalog.getName(), catalog.getId());
        }
    }

    private boolean isCurrentCatalog(ExternalCatalog catalog) {
        CatalogIf currentCatalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalog.getName());
        return currentCatalog == catalog && currentCatalog.getId() == catalog.getId();
    }

    /**
     * The authorization source a catalog's {@code access_controller.class} names, whichever way it names it:
     * by the name the source is published under, by the class name of the factory publishing it, or - for a
     * source that has since moved out of fe-core - by the class name that used to publish it.
     *
     * <p>Package-private so that the strings older releases accepted here can be pinned by a test; this is
     * the only place that decides what any of them mean.
     */
    String getPluginIdentifierForAccessController(String acClassName) {
        String pluginIdentifier = null;
        if (accessControllerClassNameMapping.containsKey(acClassName)) {
            pluginIdentifier = accessControllerClassNameMapping.get(acClassName);
        }
        if (isKnownAuthorizationSource(acClassName)) {
            pluginIdentifier = acClassName;
        }
        if (pluginIdentifier == null) {
            pluginIdentifier = SOURCES_THAT_LEFT_THE_KERNEL.get(acClassName);
            if (pluginIdentifier != null) {
                LOG.warn("Catalog property {} = {} names a class this FE no longer has; that source is now"
                                + " published as '{}'. It is still resolved, but set the property to '{}'.",
                        CatalogMgr.ACCESS_CONTROLLER_CLASS_PROP, acClassName, pluginIdentifier,
                        pluginIdentifier);
            }
        }
        if (null == pluginIdentifier || !isKnownAuthorizationSource(pluginIdentifier)) {
            throw new RuntimeException("Access Controller Plugin Factory not found for " + acClassName
                    + "." + pluginLocationHint() + pluginLoadRejectionHint());
        }
        return pluginIdentifier;
    }

    /**
     * Where an authorization plugin has to be for this FE to find it.
     *
     * <p>Spelled out because the commonest way to reach either "not found" is an upgrade rather than a typo:
     * sources that used to be part of the FE itself are installed like any other plugin now, so a deployment
     * whose lib directory was assembled by hand loses them with nothing else having changed.
     */
    private static String pluginLocationHint() {
        return " An authorization plugin is installed as a subdirectory of " + Config.authorization_plugins_dir
                + " holding the plugin jar and its lib/ directory, and the release package ships the sources"
                + " that used to be part of the FE there. An FE upgraded in place needs that directory"
                + " copied across too.";
    }

    public void removeAccessController(String ctl, long catalogId) {
        detachAccessController(ctl, catalogId).run();
    }

    /**
     * Atomically detach the controller owned by one catalog generation. The returned cleanup can be executed
     * after the caller releases CatalogMgr's global lock, so a slow plugin close never blocks unrelated DDL.
     */
    public Runnable detachAccessController(String ctl, long catalogId) {
        if (StringUtils.isBlank(ctl)) {
            return () -> { };
        }
        CatalogAccessControllerEntry entry = ctlToCtlAccessController.get(ctl);
        if (entry == null || entry.catalogId != catalogId || !ctlToCtlAccessController.remove(ctl, entry)) {
            return () -> { };
        }
        LOG.info("detach access controller for catalog {}:{}", ctl, catalogId);
        return () -> closeEntry(ctl, entry);
    }

    private void closeEntry(String ctl, CatalogAccessControllerEntry entry) {
        if (entry == null || !entry.owned) {
            return;
        }
        closeAccessController(ctl, entry.accessController);
    }

    private void closeAccessController(String ctl, AuthorizationPlugin accessController) {
        if (isSharedDefault(accessController)) {
            // The instance-wide source is shared with the internal catalog and outlives every catalog, so
            // detaching one must never close it. The guard lives here rather than in closeEntry() alone
            // because the dry run of a CREATE CATALOG closes what it built without going through an entry.
            return;
        }
        try {
            inClassLoaderOf(accessController, () -> {
                accessController.close();
                return null;
            });
        } catch (Throwable e) {
            // Access-controller plugins are external code. A faulty cleanup must not prevent the catalog
            // lifecycle from releasing its own resources.
            LOG.warn("Failed to close access controller for catalog {}", ctl, e);
        }
    }

    /**
     * Whether closing {@code candidate} would close the source governing instance scope.
     *
     * <p>Object identity is not enough: a source published by the deprecated factory is handed out wrapped,
     * and the wrapper is built fresh per binding, so two bindings of one legacy source share the controller
     * while their wrappers never compare equal. Closing the wrapper closes that shared controller.
     */
    private boolean isSharedDefault(AuthorizationPlugin candidate) {
        if (candidate == defaultAccessController) {
            return true;
        }
        return candidate instanceof LegacyAccessControllerPlugin
                && defaultAccessController instanceof LegacyAccessControllerPlugin
                && ((LegacyAccessControllerPlugin) candidate).getController()
                        == ((LegacyAccessControllerPlugin) defaultAccessController).getController();
    }

    public Auth getAuth() {
        return this.auth;
    }

    /**
     * Answers whether {@code subject} may act on {@code resource} as {@code requirement} demands.
     *
     * <p>This is the one place a check is routed. Which source is asked follows from the resource alone -
     * system-wide objects and catalog-level grants go to the source {@code access_controller_type}
     * installs, everything inside a catalog goes to the source that catalog is bound to - and whatever it
     * answers is the answer. Combining two sources, or granting anything before asking, would have to
     * happen here, and deliberately does not.
     *
     * <p>Columns are not decided here: see {@link #decideColumns}.
     */
    public boolean decide(UserIdentity subject, AuthorizedResource resource, AccessRequirement requirement) {
        return decide(subject, resource, requirement, ConnectionAccessContext.current());
    }

    /**
     * As {@link #decide(UserIdentity, AuthorizedResource, AccessRequirement)}, with the circumstances of the
     * check stated rather than read off the thread.
     *
     * <p>Worth stating wherever the caller holds them: a check can run before the connection it belongs to is
     * installed on the thread - the HTTP cookie path does exactly that - and a thread from a pool carries
     * whatever the request before it left there, so the fallback is not merely "no circumstances" but
     * possibly another client's.
     */
    public boolean decide(UserIdentity subject, AuthorizedResource resource, AccessRequirement requirement,
            AccessContext context) {
        if (resource.getKind() == ResourceKind.COLUMNS) {
            throw new IllegalArgumentException("column access is decided by decideColumns(), which"
                    + " reports which column was refused instead of a yes or no");
        }
        try {
            ask(subject, resource, requirement, context);
            return true;
        } catch (AccessDeniedException e) {
            if (LOG.isDebugEnabled()) {
                // Which source refused is the first question a deployment with both an instance-wide source
                // and a catalog-bound one has to answer, and the boolean facades throw it away: each phrases
                // its own error message. Building this costs nothing while debug logging is off - the
                // exception carries no stack and composes its message lazily.
                LOG.debug("Access denied by source {}: subject={}, resource={}, requirement={}, reason={}",
                        e.getDeniedBy(), subject, resource, requirement, e.getMessage());
            }
            return false;
        }
    }

    /**
     * Checks access to named columns, reporting the column that was refused rather than a yes or no.
     *
     * <p>Kept apart from {@link #decide} because the answer has a different shape, not because the routing
     * differs: it is the same source the table itself would be asked about.
     */
    public void decideColumns(UserIdentity subject, AuthorizedResource.Columns columns,
            AccessRequirement requirement) throws AuthorizationException {
        decideColumns(subject, columns, requirement, ConnectionAccessContext.current());
    }

    /** As {@link #decideColumns}, with the circumstances of the check stated rather than read off the thread. */
    public void decideColumns(UserIdentity subject, AuthorizedResource.Columns columns,
            AccessRequirement requirement, AccessContext context) throws AuthorizationException {
        try {
            ask(subject, columns, requirement, context);
        } catch (AccessDeniedException e) {
            throw new AuthorizationException(e.getMessage());
        }
    }

    private void ask(UserIdentity subject, AuthorizedResource resource, AccessRequirement requirement,
            AccessContext context) throws AccessDeniedException {
        AuthorizationPlugin controller = controllerOf(resource);
        checkInClassLoaderOf(controller, () ->
                controller.checkPrivilege(AccessTranslation.subjectOf(subject), resource, requirement, context));
    }

    /**
     * The authorization source that answers for {@code resource}. This is the whole of the routing: system
     * wide objects and catalog level grants belong to the source installed for the instance, everything
     * inside a catalog to the source that catalog is bound to.
     */
    private AuthorizationPlugin controllerOf(AuthorizedResource resource) {
        switch (resource.getKind()) {
            case GLOBAL:
            case RESOURCE:
            case WORKLOAD_GROUP:
            case STORAGE_VAULT:
            case CLOUD_GENERAL:
            case CLOUD_COMPUTE_GROUP:
            case CLOUD_STAGE:
            case CLOUD_STORAGE_VAULT:
                return systemScopeController();
            case CATALOG:
                // Catalog level grants are only ever stored by the system scope source, so it answers for
                // every catalog, including those bound to a source of their own.
                return systemScopeController();
            case DATABASE:
                return getAccessControllerOrDefault(((AuthorizedResource.Database) resource).getCatalog());
            case TABLE:
                return getAccessControllerOrDefault(((AuthorizedResource.Table) resource).getCatalog());
            case COLUMNS:
                return getAccessControllerOrDefault(((AuthorizedResource.Columns) resource).getCatalog());
            default:
                throw new IllegalStateException("no route for resource kind " + resource.getKind());
        }
    }

    /**
     * The source governing everything that is not inside a catalog: global privileges, resources,
     * workload groups, cloud objects, storage vaults - and catalog level grants, which only it stores.
     */
    private AuthorizationPlugin systemScopeController() {
        return defaultAccessController;
    }

    /**
     * Whether {@code candidate} is itself the source governing instance scope.
     *
     * <p>Asked on behalf of a source that would otherwise defer to that authority, so that it does not ask
     * itself a question it is about to answer - the two would agree, at the price of evaluating the same
     * policies twice.
     */
    boolean isGlobalScopeAuthority(AuthorizationPlugin candidate) {
        return systemScopeController() == candidate;
    }

    // ==== Global ====
    public boolean checkGlobalPriv(ConnectContext ctx, PrivPredicate wanted) {
        return decide(ctx.getCurrentUserIdentity(), AuthorizedResource.global(),
                AccessTranslation.requirementOf(wanted), ConnectionAccessContext.of(ctx));
    }

    public boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted) {
        return decide(currentUser, AuthorizedResource.global(), AccessTranslation.requirementOf(wanted));
    }

    // ==== Catalog ====
    public boolean checkCtlPriv(ConnectContext ctx, String ctl, PrivPredicate wanted) {
        return checkCtlPriv(ctx.getCurrentUserIdentity(), ctl, wanted, ConnectionAccessContext.of(ctx));
    }

    private boolean canSkipCatalogPrivCheck(PrivPredicate wanted) {
        return wanted == PrivPredicate.SHOW || wanted == PrivPredicate.SELECT;
    }

    private boolean shouldSkipCatalogPrivCheck(PrivPredicate wanted) {
        return Config.skip_catalog_priv_check && canSkipCatalogPrivCheck(wanted);
    }

    public boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted) {
        return checkCtlPriv(currentUser, ctl, wanted, ConnectionAccessContext.current());
    }

    private boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted,
            AccessContext context) {
        if (shouldSkipCatalogPrivCheck(wanted)) {
            CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(ctl);
            if (catalog == null) {
                return false;
            }
            // An external catalog bound to a controller of its own keeps no catalog level grants anywhere,
            // so with the check switched off there is nobody left to ask. Every other catalog still goes
            // through the normal route below.
            String className = catalog.isInternalCatalog() ? ""
                    : (String) catalog.getProperties().getOrDefault(CatalogMgr.ACCESS_CONTROLLER_CLASS_PROP, "");
            if (!Strings.isNullOrEmpty(className)) {
                return true;
            }
        }
        return decide(currentUser, AuthorizedResource.catalog(ctl), AccessTranslation.requirementOf(wanted),
                context);
    }

    // ==== Database ====
    public boolean checkDbPriv(ConnectContext ctx, String ctl, String db, PrivPredicate wanted) {
        return decide(ctx.getCurrentUserIdentity(), AuthorizedResource.database(ctl, db),
                AccessTranslation.requirementOf(wanted), ConnectionAccessContext.of(ctx));
    }

    public boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted) {
        return decide(currentUser, AuthorizedResource.database(ctl, db), AccessTranslation.requirementOf(wanted));
    }

    // ==== Table ====
    public boolean checkTblPriv(ConnectContext ctx, TableNameInfo tableName, PrivPredicate wanted) {
        Preconditions.checkState(tableName.isFullyQualified());
        return checkTblPriv(ctx, tableName.getCtl(), tableName.getDb(), tableName.getTbl(), wanted);
    }

    public boolean checkTblPriv(ConnectContext ctx, String qualifiedCtl,
                                String qualifiedDb, String tbl, PrivPredicate wanted) {
        if (ctx.isSkipAuth()) {
            // One of the exemptions listed on this class: the engine is running this statement on behalf of
            // one the caller was already authorized for.
            return true;
        }
        return decide(ctx.getCurrentUserIdentity(), AuthorizedResource.table(qualifiedCtl, qualifiedDb, tbl),
                AccessTranslation.requirementOf(wanted), ConnectionAccessContext.of(ctx));
    }

    public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl, PrivPredicate wanted) {
        return decide(currentUser, AuthorizedResource.table(ctl, db, tbl), AccessTranslation.requirementOf(wanted));
    }

    // ==== Column ====
    // If param has ctx, we can skip auth by isSkipAuth field in ctx
    public void checkColumnsPriv(ConnectContext ctx, String ctl, String qualifiedDb, String tbl, Set<String> cols,
                                 PrivPredicate wanted) throws UserException {
        if (ctx.isSkipAuth()) {
            return;
        }
        checkColumnsPriv(ctx.getCurrentUserIdentity(), ctl, qualifiedDb, tbl, cols, wanted,
                ConnectionAccessContext.of(ctx));
    }

    public void checkColumnsPriv(UserIdentity currentUser, String
            ctl, String qualifiedDb, String tbl, Set<String> cols,
                                 PrivPredicate wanted) throws UserException {
        checkColumnsPriv(currentUser, ctl, qualifiedDb, tbl, cols, wanted, ConnectionAccessContext.current());
    }

    private void checkColumnsPriv(UserIdentity currentUser, String ctl, String qualifiedDb, String tbl,
            Set<String> cols, PrivPredicate wanted, AccessContext context) throws UserException {
        long start = System.currentTimeMillis();
        decideColumns(currentUser, AuthorizedResource.columns(ctl, qualifiedDb, tbl, cols),
                AccessTranslation.requirementOf(wanted), context);
        if (LOG.isDebugEnabled()) {
            LOG.debug("checkColumnsPriv use {} mills, user: {}, ctl: {}, db: {}, table: {}, cols: {}",
                    System.currentTimeMillis() - start, currentUser, ctl, qualifiedDb, tbl, cols);
        }
    }

    // ==== Resource ====
    public boolean checkResourcePriv(ConnectContext ctx, String resourceName, PrivPredicate wanted) {
        return decide(ctx.getCurrentUserIdentity(), AuthorizedResource.resource(resourceName),
                AccessTranslation.requirementOf(wanted), ConnectionAccessContext.of(ctx));
    }

    public boolean checkResourcePriv(UserIdentity currentUser, String resourceName, PrivPredicate wanted) {
        return decide(currentUser, AuthorizedResource.resource(resourceName),
                AccessTranslation.requirementOf(wanted));
    }

    // ==== Cloud ====
    public boolean checkCloudPriv(ConnectContext ctx, String cloudName, PrivPredicate wanted, ResourceTypeEnum type) {
        return decide(ctx.getCurrentUserIdentity(),
                AuthorizedResource.cloud(AccessTranslation.cloudKindOf(type), cloudName),
                AccessTranslation.requirementOf(wanted), ConnectionAccessContext.of(ctx));
    }

    public boolean checkCloudPriv(UserIdentity currentUser, String cloudName,
                                  PrivPredicate wanted, ResourceTypeEnum type) {
        return decide(currentUser, AuthorizedResource.cloud(AccessTranslation.cloudKindOf(type), cloudName),
                AccessTranslation.requirementOf(wanted));
    }

    public boolean checkStorageVaultPriv(ConnectContext ctx, String storageVaultName, PrivPredicate wanted) {
        return decide(ctx.getCurrentUserIdentity(), AuthorizedResource.storageVault(storageVaultName),
                AccessTranslation.requirementOf(wanted), ConnectionAccessContext.of(ctx));
    }

    public boolean checkStorageVaultPriv(UserIdentity currentUser, String storageVaultName, PrivPredicate wanted) {
        return decide(currentUser, AuthorizedResource.storageVault(storageVaultName),
                AccessTranslation.requirementOf(wanted));
    }

    public boolean checkWorkloadGroupPriv(ConnectContext ctx, String workloadGroupName, PrivPredicate wanted) {
        return decide(ctx.getCurrentUserIdentity(), AuthorizedResource.workloadGroup(workloadGroupName),
                AccessTranslation.requirementOf(wanted), ConnectionAccessContext.of(ctx));
    }

    public boolean checkWorkloadGroupPriv(UserIdentity currentUser, String workloadGroupName, PrivPredicate wanted) {
        return decide(currentUser, AuthorizedResource.workloadGroup(workloadGroupName),
                AccessTranslation.requirementOf(wanted));
    }

    // ==== Other ====
    public boolean checkPrivByAuthInfo(ConnectContext ctx, AuthorizationInfo authInfo, PrivPredicate wanted) {
        if (authInfo == null) {
            return false;
        }
        if (authInfo.getDbName() == null) {
            return false;
        }
        if (authInfo.getTableNameList() == null || authInfo.getTableNameList().isEmpty()) {
            return checkDbPriv(ctx, InternalCatalog.INTERNAL_CATALOG_NAME, authInfo.getDbName(), wanted);
        }
        for (String tblName : authInfo.getTableNameList()) {
            if (!checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, authInfo.getDbName(),
                    tblName, wanted)) {
                return false;
            }
        }
        return true;
    }

    public Optional<DataMaskSpec> evalDataMaskPolicy(UserIdentity currentUser, String
            ctl, String db, String tbl, String col) {
        Objects.requireNonNull(col, "require col object");
        return Optional.ofNullable(evalDataMaskPolicies(currentUser, ctl, db, tbl,
                Collections.singleton(col)).get(col.toLowerCase()));
    }

    /**
     * The masks in force on {@code cols} of one table, asked for in one call.
     *
     * <p>One call rather than one per column because that is what the contract offers and why it offers it:
     * a source answering over the network would otherwise be reached once per column of every table in the
     * statement. Keyed by the lower-cased column name, which is how the sources that store policies per
     * column have them written and therefore how they are asked.
     */
    public Map<String, DataMaskSpec> evalDataMaskPolicies(UserIdentity currentUser, String ctl, String db,
            String tbl, Set<String> cols) {
        Objects.requireNonNull(currentUser, "require currentUser object");
        Objects.requireNonNull(ctl, "require ctl object");
        Objects.requireNonNull(db, "require db object");
        Objects.requireNonNull(tbl, "require tbl object");
        Objects.requireNonNull(cols, "require cols object");
        if (cols.isEmpty()) {
            return Collections.emptyMap();
        }
        Set<String> columns = new LinkedHashSet<>();
        for (String col : cols) {
            columns.add(col.toLowerCase());
        }
        AuthorizedResource.Table table = AuthorizedResource.table(ctl, db, tbl);
        AuthorizationPlugin controller = controllerOf(table);
        AccessContext context = ConnectionAccessContext.current();
        return inClassLoaderOf(controller, () -> controller.getDataMasks(
                AccessTranslation.subjectOf(currentUser), table, columns, context));
    }

    public List<RowFilterSpec> evalRowFilterPolicies(UserIdentity currentUser, String
            ctl, String db, String tbl) {
        Objects.requireNonNull(currentUser, "require currentUser object");
        Objects.requireNonNull(ctl, "require ctl object");
        Objects.requireNonNull(db, "require db object");
        Objects.requireNonNull(tbl, "require tbl object");
        AuthorizedResource.Table table = AuthorizedResource.table(ctl, db, tbl);
        AuthorizationPlugin controller = controllerOf(table);
        AccessContext context = ConnectionAccessContext.current();
        return inClassLoaderOf(controller, () -> controller.getRowFilters(
                AccessTranslation.subjectOf(currentUser), table, context));
    }
}
