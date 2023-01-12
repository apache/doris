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

// Copy from
// https://github.com/apache/hive/blob/rel/release-2.3.7/metastore/src/java/org/apache/hadoop/hive/metastore/HiveMetaStoreClient.java

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.HMSResource;
import org.apache.doris.datasource.hive.HiveVersionUtil.HiveVersion;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.auth.HiveAuthUtils;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceAudience.Public;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Unstable;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveConfUtil;
import org.apache.hadoop.hive.metastore.DefaultHiveMetaHook;
import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.AddForeignKeyRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsResult;
import org.apache.hadoop.hive.metastore.api.AddPrimaryKeyRequest;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.CacheFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.CacheFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.ClearFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.ClientCapabilities;
import org.apache.hadoop.hive.metastore.api.ClientCapability;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropConstraintRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsExpr;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataByExprRequest;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataByExprResult;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokeRoleRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokeRoleResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokeType;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PutFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.RequestPartsSpec;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TableStatsRequest;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.security.auth.login.LoginException;

/**
 * Hive Metastore Client.
 * The public implementation of IMetaStoreClient. Methods not inherited from IMetaStoreClient
 * are not public and can change. Hence this is marked as unstable.
 * For users who require retry mechanism when the connection between metastore and client is
 * broken, RetryingMetaStoreClient class should be used.
 * <p>
 * Doris Modification.
 * To support different type of hive, copy this file from hive repo and modify some method.
 * 1. getSchema
 * 2. tableExists
 * 3. getTable
 */
@Public
@Unstable
public class HiveMetaStoreClient implements IMetaStoreClient {
    private static final org.apache.logging.log4j.Logger LOG = LogManager.getLogger(HiveMetaStoreClient.class);
    /**
     * Capabilities of the current client. If this client talks to a MetaStore server in a manner
     * implying the usage of some expanded features that require client-side support that this client
     * doesn't have (e.g. a getting a table of a new type), it will get back failures when the
     * capability checking is enabled (the default).
     */
    public static final ClientCapabilities VERSION = null; // No capabilities.
    public static final ClientCapabilities TEST_VERSION = new ClientCapabilities(
            Lists.newArrayList(ClientCapability.TEST_CAPABILITY)); // Test capability for tests.

    ThriftHiveMetastore.Iface client = null;
    private TTransport transport = null;
    private boolean isConnected = false;
    private URI[] metastoreUris;
    private final HiveMetaHookLoader hookLoader;
    protected final HiveConf conf;
    // Keep a copy of HiveConf so if Session conf changes, we may need to get a new HMS client.
    protected boolean fastpath = false;
    private String tokenStrForm;
    private final boolean localMetaStore;
    private final MetaStoreFilterHook filterHook;
    private final int fileMetadataBatchSize;

    private Map<String, String> currentMetaVars;

    private static final AtomicInteger connCount = new AtomicInteger(0);

    // for thrift connects
    private int retries = 5;
    private long retryDelaySeconds = 0;
    private final ClientCapabilities version;

    private final HiveVersion hiveVersion;

    public HiveMetaStoreClient(HiveConf conf) throws MetaException {
        this(conf, null, true);
    }

    public HiveMetaStoreClient(HiveConf conf, HiveMetaHookLoader hookLoader) throws MetaException {
        this(conf, hookLoader, true);
    }

    public HiveMetaStoreClient(HiveConf conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded)
            throws MetaException {

        this.hookLoader = hookLoader;
        if (conf == null) {
            conf = new HiveConf(HiveMetaStoreClient.class);
            this.conf = conf;
        } else {
            this.conf = new HiveConf(conf);
        }

        hiveVersion = HiveVersionUtil.getVersion(conf.get(HMSResource.HIVE_VERSION));

        version = HiveConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST) ? TEST_VERSION : VERSION;
        filterHook = loadFilterHooks();
        fileMetadataBatchSize = HiveConf.getIntVar(
                conf, HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_OBJECTS_MAX);

        String msUri = conf.getVar(ConfVars.METASTOREURIS);
        localMetaStore = HiveConfUtil.isEmbeddedMetaStore(msUri);
        if (localMetaStore) {
            if (!allowEmbedded) {
                throw new MetaException("Embedded metastore is not allowed here. Please configure "
                        + ConfVars.METASTOREURIS.varname + "; it is currently set to [" + msUri + "]");
            }
            // instantiate the metastore server handler directly instead of connecting
            // through the network
            if (conf.getBoolVar(ConfVars.METASTORE_FASTPATH)) {
                client = new HiveMetaStore.HMSHandler("hive client", this.conf, true);
                fastpath = true;
            } else {
                client = HiveMetaStore.newRetryingHMSHandler("hive client", this.conf, true);
            }
            isConnected = true;
            snapshotActiveConf();
            return;
        } else {
            if (conf.getBoolVar(ConfVars.METASTORE_FASTPATH)) {
                throw new RuntimeException("You can't set hive.metastore.fastpath to true when you're "
                        + "talking to the thrift metastore service.  You must run the metastore locally.");
            }
        }

        // get the number retries
        retries = HiveConf.getIntVar(conf, HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES);
        retryDelaySeconds = conf.getTimeVar(
                ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, TimeUnit.SECONDS);

        // user wants file store based configuration
        if (conf.getVar(HiveConf.ConfVars.METASTOREURIS) != null) {
            String[] metastoreUrisString = conf.getVar(
                    HiveConf.ConfVars.METASTOREURIS).split(",");
            metastoreUris = new URI[metastoreUrisString.length];
            try {
                int i = 0;
                for (String s : metastoreUrisString) {
                    URI tmpUri = new URI(s);
                    if (tmpUri.getScheme() == null) {
                        throw new IllegalArgumentException("URI: " + s
                                + " does not have a scheme");
                    }
                    metastoreUris[i++] = tmpUri;

                }
                // make metastore URIS random
                List<URI> uriList = Arrays.asList(metastoreUris);
                Collections.shuffle(uriList);
                metastoreUris = uriList.toArray(new URI[uriList.size()]);
            } catch (IllegalArgumentException e) {
                throw (e);
            } catch (Exception e) {
                throw new MetaException(e.getMessage());
            }
        } else {
            LOG.error("NOT getting uris from conf");
            throw new MetaException("MetaStoreURIs not found in conf file");
        }

        //If HADOOP_PROXY_USER is set in env or property,
        //then need to create metastore client that proxies as that user.
        String hadoopProxyUser = "HADOOP_PROXY_USER";
        String proxyUser = System.getenv(hadoopProxyUser);
        if (proxyUser == null) {
            proxyUser = System.getProperty(hadoopProxyUser);
        }
        //if HADOOP_PROXY_USER is set, create DelegationToken using real user
        if (proxyUser != null) {
            LOG.info(hadoopProxyUser + " is set. Using delegation "
                    + "token for HiveMetaStore connection.");
            try {
                UserGroupInformation.getLoginUser().getRealUser().doAs(
                        new PrivilegedExceptionAction<Void>() {
                            @Override
                            public Void run() throws Exception {
                                open();
                                return null;
                            }
                        });
                String delegationTokenPropString = "DelegationTokenForHiveMetaStoreServer";
                String delegationTokenStr = getDelegationToken(proxyUser, proxyUser);
                Utils.setTokenStr(UserGroupInformation.getCurrentUser(), delegationTokenStr,
                        delegationTokenPropString);
                this.conf.setVar(ConfVars.METASTORE_TOKEN_SIGNATURE, delegationTokenPropString);
                close();
            } catch (Exception e) {
                LOG.error("Error while setting delegation token for " + proxyUser, e);
                if (e instanceof MetaException) {
                    throw (MetaException) e;
                } else {
                    throw new MetaException(e.getMessage());
                }
            }
        }
        // finally open the store
        open();
    }

    private MetaStoreFilterHook loadFilterHooks() throws IllegalStateException {
        Class<? extends MetaStoreFilterHook> authProviderClass = conf.getClass(
                HiveConf.ConfVars.METASTORE_FILTER_HOOK.varname,
                DefaultMetaStoreFilterHookImpl.class,
                MetaStoreFilterHook.class);
        String msg = "Unable to create instance of " + authProviderClass.getName() + ": ";
        try {
            Constructor<? extends MetaStoreFilterHook> constructor =
                    authProviderClass.getConstructor(HiveConf.class);
            return constructor.newInstance(conf);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(msg + e.getMessage(), e);
        } catch (SecurityException e) {
            throw new IllegalStateException(msg + e.getMessage(), e);
        } catch (InstantiationException e) {
            throw new IllegalStateException(msg + e.getMessage(), e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(msg + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(msg + e.getMessage(), e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException(msg + e.getMessage(), e);
        }
    }

    /**
     * Swaps the first element of the metastoreUris array with a random element from the
     * remainder of the array.
     */
    private void promoteRandomMetaStoreURI() {
        if (metastoreUris.length <= 1) {
            return;
        }
        Random rng = new Random();
        int index = rng.nextInt(metastoreUris.length - 1) + 1;
        URI tmp = metastoreUris[0];
        metastoreUris[0] = metastoreUris[index];
        metastoreUris[index] = tmp;
    }

    @VisibleForTesting
    public TTransport getTTransport() {
        return transport;
    }

    @Override
    public boolean isLocalMetaStore() {
        return localMetaStore;
    }

    @Override
    public boolean isCompatibleWith(HiveConf conf) {
        // Make a copy of currentMetaVars, there is a race condition that
        // currentMetaVars might be changed during the execution of the method
        Map<String, String> currentMetaVarsCopy = currentMetaVars;
        if (currentMetaVarsCopy == null) {
            return false; // recreate
        }
        boolean compatible = true;
        for (ConfVars oneVar : HiveConf.metaVars) {
            // Since metaVars are all of different types, use string for comparison
            String oldVar = currentMetaVarsCopy.get(oneVar.varname);
            String newVar = conf.get(oneVar.varname, "");
            if (oldVar == null
                    || (oneVar.isCaseSensitive() ? !oldVar.equals(newVar) : !oldVar.equalsIgnoreCase(newVar))) {
                LOG.info("Metastore configuration " + oneVar.varname
                        + " changed from " + oldVar + " to " + newVar);
                compatible = false;
            }
        }
        return compatible;
    }

    @Override
    public void setHiveAddedJars(String addedJars) {
        HiveConf.setVar(conf, ConfVars.HIVEADDEDJARS, addedJars);
    }

    @Override
    public void reconnect() throws MetaException {
        if (localMetaStore) {
            // For direct DB connections we don't yet support reestablishing connections.
            throw new MetaException("For direct MetaStore DB connections, we don't support retries"
                    + " at the client level.");
        } else {
            close();
            // Swap the first element of the metastoreUris[] with a random element from the rest
            // of the array. Rationale being that this method will generally be called when the default
            // connection has died and the default connection is likely to be the first array element.
            promoteRandomMetaStoreURI();
            open();
        }
    }

    /**
     * @param dbname
     * @param tblName
     * @param newTbl
     * @throws InvalidOperationException
     * @throws MetaException
     * @throws TException
     * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#alter_table(
     *java.lang.String, java.lang.String,
     * org.apache.hadoop.hive.metastore.api.Table)
     */
    @Override
    public void alter_table(String dbname, String tblName, Table newTbl)
            throws InvalidOperationException, MetaException, TException {
        alter_table_with_environmentContext(dbname, tblName, newTbl, null);
    }

    @Override
    public void alter_table(String defaultDatabaseName, String tblName, Table table,
            boolean cascade) throws InvalidOperationException, MetaException, TException {
        EnvironmentContext environmentContext = new EnvironmentContext();
        if (cascade) {
            environmentContext.putToProperties(StatsSetupConst.CASCADE, StatsSetupConst.TRUE);
        }
        alter_table_with_environmentContext(defaultDatabaseName, tblName, table, environmentContext);
    }

    @Override
    public void alter_table_with_environmentContext(String dbname, String tblName, Table newTbl,
            EnvironmentContext envContext) throws InvalidOperationException, MetaException, TException {
        client.alter_table_with_environment_context(dbname, tblName, newTbl, envContext);
    }

    /**
     * @param dbname
     * @param name
     * @param partVals
     * @param newPart
     * @throws InvalidOperationException
     * @throws MetaException
     * @throws TException
     * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#rename_partition(
     *java.lang.String, java.lang.String, java.util.List, org.apache.hadoop.hive.metastore.api.Partition)
     */
    @Override
    public void renamePartition(final String dbname, final String name, final List<String> partVals,
            final Partition newPart)
            throws InvalidOperationException, MetaException, TException {
        client.rename_partition(dbname, name, partVals, newPart);
    }

    private void open() throws MetaException {
        isConnected = false;
        TTransportException tte = null;
        boolean useSSL = conf.getBoolVar(ConfVars.HIVE_METASTORE_USE_SSL);
        boolean useSasl = conf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL);
        boolean useFramedTransport = conf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_FRAMED_TRANSPORT);
        boolean useCompactProtocol = conf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_COMPACT_PROTOCOL);
        int clientSocketTimeout = (int) conf.getTimeVar(
                ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);

        for (int attempt = 0; !isConnected && attempt < retries; ++attempt) {
            for (URI store : metastoreUris) {
                LOG.debug("Trying to connect to metastore with URI " + store);

                try {
                    if (useSasl) {
                        // Wrap thrift connection with SASL for secure connection.
                        try {
                            HadoopThriftAuthBridge.Client authBridge =
                                    ShimLoader.getHadoopThriftAuthBridge().createClient();

                            // check if we should use delegation tokens to authenticate
                            // the call below gets hold of the tokens if they are set up by hadoop
                            // this should happen on the map/reduce tasks if the client added the
                            // tokens into hadoop's credential store in the front end during job
                            // submission.
                            String tokenSig = conf.getVar(ConfVars.METASTORE_TOKEN_SIGNATURE);
                            // tokenSig could be null
                            tokenStrForm = Utils.getTokenStrForm(tokenSig);
                            transport = new TSocket(store.getHost(), store.getPort(), clientSocketTimeout);

                            if (tokenStrForm != null) {
                                // authenticate using delegation tokens via the "DIGEST" mechanism
                                transport = authBridge.createClientTransport(null, store.getHost(),
                                        "DIGEST", tokenStrForm, transport,
                                        MetaStoreUtils.getMetaStoreSaslProperties(conf));
                            } else {
                                String principalConfig =
                                        conf.getVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL);
                                transport = authBridge.createClientTransport(
                                        principalConfig, store.getHost(), "KERBEROS", null,
                                        transport, MetaStoreUtils.getMetaStoreSaslProperties(conf));
                            }
                        } catch (IOException ioe) {
                            LOG.error("Couldn't create client transport", ioe);
                            throw new MetaException(ioe.toString());
                        }
                    } else {
                        if (useSSL) {
                            try {
                                String trustStorePath = conf.getVar(ConfVars.HIVE_METASTORE_SSL_TRUSTSTORE_PATH).trim();
                                if (trustStorePath.isEmpty()) {
                                    throw new IllegalArgumentException(
                                            ConfVars.HIVE_METASTORE_SSL_TRUSTSTORE_PATH.varname
                                                    + " Not configured for SSL connection");
                                }
                                String trustStorePassword = ShimLoader.getHadoopShims().getPassword(conf,
                                        HiveConf.ConfVars.HIVE_METASTORE_SSL_TRUSTSTORE_PASSWORD.varname);

                                // Create an SSL socket and connect
                                transport = HiveAuthUtils.getSSLSocket(store.getHost(), store.getPort(),
                                        clientSocketTimeout, trustStorePath, trustStorePassword);
                                LOG.info("Opened an SSL connection to metastore, current connections: "
                                        + connCount.incrementAndGet());
                            } catch (IOException e) {
                                throw new IllegalArgumentException(e);
                            } catch (TTransportException e) {
                                tte = e;
                                throw new MetaException(e.toString());
                            }
                        } else {
                            transport = new TSocket(store.getHost(), store.getPort(), clientSocketTimeout);
                        }

                        if (useFramedTransport) {
                            transport = new TFramedTransport(transport);
                        }
                    }

                    final TProtocol protocol;
                    if (useCompactProtocol) {
                        protocol = new TCompactProtocol(transport);
                    } else {
                        protocol = new TBinaryProtocol(transport);
                    }
                    client = new ThriftHiveMetastore.Client(protocol);
                    try {
                        if (!transport.isOpen()) {
                            transport.open();
                            LOG.info("Opened a connection to metastore, current connections: "
                                    + connCount.incrementAndGet());
                        }
                        isConnected = true;
                    } catch (TTransportException e) {
                        tte = e;
                        if (LOG.isDebugEnabled()) {
                            LOG.warn("Failed to connect to the MetaStore Server...", e);
                        } else {
                            // Don't print full exception trace if DEBUG is not on.
                            LOG.warn("Failed to connect to the MetaStore Server...");
                        }
                    }

                    if (isConnected && !useSasl && conf.getBoolVar(ConfVars.METASTORE_EXECUTE_SET_UGI)) {
                        // Call set_ugi, only in unsecure mode.
                        try {
                            UserGroupInformation ugi = Utils.getUGI();
                            client.set_ugi(ugi.getUserName(), Arrays.asList(ugi.getGroupNames()));
                        } catch (LoginException e) {
                            LOG.warn("Failed to do login. set_ugi() is not successful, "
                                    + "Continuing without it.", e);
                        } catch (IOException e) {
                            LOG.warn("Failed to find ugi of client set_ugi() is not successful, "
                                    + "Continuing without it.", e);
                        } catch (TException e) {
                            LOG.warn("set_ugi() not successful, Likely cause: new client talking to old server. "
                                    + "Continuing without it.", e);
                        }
                    }
                } catch (MetaException e) {
                    LOG.error("Unable to connect to metastore with URI " + store
                            + " in attempt " + attempt, e);
                }
                if (isConnected) {
                    break;
                }
            }
            // Wait before launching the next round of connection retries.
            if (!isConnected && retryDelaySeconds > 0) {
                try {
                    LOG.info("Waiting " + retryDelaySeconds + " seconds before next connection attempt.");
                    Thread.sleep(retryDelaySeconds * 1000);
                } catch (InterruptedException ignore) {
                    ignore.printStackTrace();
                }
            }
        }

        if (!isConnected) {
            throw new MetaException("Could not connect to meta store using any of the URIs provided."
                    + " Most recent failure: " + StringUtils.stringifyException(tte));
        }

        snapshotActiveConf();

        LOG.info("Connected to metastore.");
    }

    private void snapshotActiveConf() {
        currentMetaVars = new HashMap<String, String>(HiveConf.metaVars.length);
        for (ConfVars oneVar : HiveConf.metaVars) {
            currentMetaVars.put(oneVar.varname, conf.get(oneVar.varname, ""));
        }
    }

    @Override
    public String getTokenStrForm() throws IOException {
        return tokenStrForm;
    }

    @Override
    public void close() {
        isConnected = false;
        currentMetaVars = null;
        try {
            if (null != client) {
                client.shutdown();
            }
        } catch (TException e) {
            LOG.debug("Unable to shutdown metastore client. Will try closing transport directly.", e);
        }
        // Transport would have got closed via client.shutdown(), so we dont need this, but
        // just in case, we make this call.
        if ((transport != null) && transport.isOpen()) {
            transport.close();
            LOG.info("Closed a connection to metastore, current connections: " + connCount.decrementAndGet());
        }
    }

    @Override
    public void setMetaConf(String key, String value) throws TException {
        client.setMetaConf(key, value);
    }

    @Override
    public String getMetaConf(String key) throws TException {
        return client.getMetaConf(key);
    }

    /**
     * @param newPart
     * @return the added partition
     * @throws InvalidObjectException
     * @throws AlreadyExistsException
     * @throws MetaException
     * @throws TException
     * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#add_partition
     * (org.apache.hadoop.hive.metastore.api.Partition)
     */
    @Override
    public Partition add_partition(Partition newPart)
            throws InvalidObjectException, AlreadyExistsException, MetaException,
            TException {
        return add_partition(newPart, null);
    }

    public Partition add_partition(Partition newPart, EnvironmentContext envContext)
            throws InvalidObjectException, AlreadyExistsException, MetaException,
            TException {
        Partition p = client.add_partition_with_environment_context(newPart, envContext);
        return fastpath ? p : deepCopy(p);
    }

    /**
     * @param newParts
     * @throws InvalidObjectException
     * @throws AlreadyExistsException
     * @throws MetaException
     * @throws TException
     * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#add_partitions(List)
     */
    @Override
    public int add_partitions(List<Partition> newParts)
            throws InvalidObjectException, AlreadyExistsException, MetaException,
            TException {
        return client.add_partitions(newParts);
    }

    @Override
    public List<Partition> add_partitions(
            List<Partition> parts, boolean ifNotExists, boolean needResults)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        if (parts.isEmpty()) {
            return needResults ? new ArrayList<Partition>() : null;
        }
        Partition part = parts.get(0);
        AddPartitionsRequest req = new AddPartitionsRequest(
                part.getDbName(), part.getTableName(), parts, ifNotExists);
        req.setNeedResult(needResults);
        AddPartitionsResult result = client.add_partitions_req(req);
        return needResults ? filterHook.filterPartitions(result.getPartitions()) : null;
    }

    @Override
    public int add_partitions_pspec(PartitionSpecProxy partitionSpec) throws TException {
        return client.add_partitions_pspec(partitionSpec.toPartitionSpec());
    }

    /**
     * @param tableName
     * @param dbName
     * @param partVals
     * @return the appended partition
     * @throws InvalidObjectException
     * @throws AlreadyExistsException
     * @throws MetaException
     * @throws TException
     * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#append_partition(java.lang.String,
     * java.lang.String, java.util.List)
     */
    @Override
    public Partition appendPartition(String dbName, String tableName,
            List<String> partVals) throws InvalidObjectException,
            AlreadyExistsException, MetaException, TException {
        return appendPartition(dbName, tableName, partVals, null);
    }

    public Partition appendPartition(String dbName, String tableName, List<String> partVals,
            EnvironmentContext envContext) throws InvalidObjectException, AlreadyExistsException,
            MetaException, TException {
        Partition p = client.append_partition_with_environment_context(dbName, tableName,
                partVals, envContext);
        return fastpath ? p : deepCopy(p);
    }

    @Override
    public Partition appendPartition(String dbName, String tableName, String partName)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return appendPartition(dbName, tableName, partName, null);
    }

    public Partition appendPartition(String dbName, String tableName, String partName,
            EnvironmentContext envContext) throws InvalidObjectException, AlreadyExistsException,
            MetaException, TException {
        Partition p = client.append_partition_by_name_with_environment_context(dbName, tableName,
                partName, envContext);
        return fastpath ? p : deepCopy(p);
    }

    /**
     * Exchange the partition between two tables
     *
     * @param partitionSpecs partitions specs of the parent partition to be exchanged
     * @param destDb the db of the destination table
     * @param destinationTableName the destination table name
     * @ @return new partition after exchanging
     */
    @Override
    public Partition exchange_partition(Map<String, String> partitionSpecs,
            String sourceDb, String sourceTable, String destDb,
            String destinationTableName) throws MetaException,
            NoSuchObjectException, InvalidObjectException, TException {
        return client.exchange_partition(partitionSpecs, sourceDb, sourceTable,
                destDb, destinationTableName);
    }

    /**
     * Exchange the partitions between two tables
     *
     * @param partitionSpecs partitions specs of the parent partition to be exchanged
     * @param destDb the db of the destination table
     * @param destinationTableName the destination table name
     * @ @return new partitions after exchanging
     */
    @Override
    public List<Partition> exchange_partitions(Map<String, String> partitionSpecs,
            String sourceDb, String sourceTable, String destDb,
            String destinationTableName) throws MetaException,
            NoSuchObjectException, InvalidObjectException, TException {
        return client.exchange_partitions(partitionSpecs, sourceDb, sourceTable,
                destDb, destinationTableName);
    }

    @Override
    public void validatePartitionNameCharacters(List<String> partVals)
            throws TException, MetaException {
        client.partition_name_has_valid_characters(partVals, true);
    }

    /**
     * Create a new Database
     *
     * @param db
     * @throws AlreadyExistsException
     * @throws InvalidObjectException
     * @throws MetaException
     * @throws TException
     * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_database(Database)
     */
    @Override
    public void createDatabase(Database db)
            throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
        client.create_database(db);
    }

    /**
     * @param tbl
     * @throws MetaException
     * @throws NoSuchObjectException
     * @throws TException
     * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_table
     * (org.apache.hadoop.hive.metastore.api.Table)
     */
    @Override
    public void createTable(Table tbl) throws AlreadyExistsException,
            InvalidObjectException, MetaException, NoSuchObjectException, TException {
        createTable(tbl, null);
    }

    public void createTable(Table tbl, EnvironmentContext envContext) throws AlreadyExistsException,
            InvalidObjectException, MetaException, NoSuchObjectException, TException {
        HiveMetaHook hook = getHook(tbl);
        if (hook != null) {
            hook.preCreateTable(tbl);
        }
        boolean success = false;
        try {
            // Subclasses can override this step (for example, for temporary tables)
            create_table_with_environment_context(tbl, envContext);
            if (hook != null) {
                hook.commitCreateTable(tbl);
            }
            success = true;
        } finally {
            if (!success && (hook != null)) {
                try {
                    hook.rollbackCreateTable(tbl);
                } catch (Exception e) {
                    LOG.error("Create rollback failed with", e);
                }
            }
        }
    }

    @Override
    public void createTableWithConstraints(Table tbl,
            List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys)
            throws AlreadyExistsException, InvalidObjectException,
            MetaException, NoSuchObjectException, TException {
        HiveMetaHook hook = getHook(tbl);
        if (hook != null) {
            hook.preCreateTable(tbl);
        }
        boolean success = false;
        try {
            // Subclasses can override this step (for example, for temporary tables)
            client.create_table_with_constraints(tbl, primaryKeys, foreignKeys);
            if (hook != null) {
                hook.commitCreateTable(tbl);
            }
            success = true;
        } finally {
            if (!success && (hook != null)) {
                hook.rollbackCreateTable(tbl);
            }
        }
    }

    @Override
    public void dropConstraint(String dbName, String tableName, String constraintName) throws
            NoSuchObjectException, MetaException, TException {
        client.drop_constraint(new DropConstraintRequest(dbName, tableName, constraintName));
    }

    @Override
    public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols) throws
            NoSuchObjectException, MetaException, TException {
        client.add_primary_key(new AddPrimaryKeyRequest(primaryKeyCols));
    }

    @Override
    public void addForeignKey(List<SQLForeignKey> foreignKeyCols) throws
            NoSuchObjectException, MetaException, TException {
        client.add_foreign_key(new AddForeignKeyRequest(foreignKeyCols));
    }

    /**
     * @param type
     * @return true or false
     * @throws AlreadyExistsException
     * @throws InvalidObjectException
     * @throws MetaException
     * @throws TException
     * @see
     * org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_type(
     *org.apache.hadoop.hive.metastore.api.Type)
     */
    public boolean createType(Type type) throws AlreadyExistsException,
            InvalidObjectException, MetaException, TException {
        return client.create_type(type);
    }

    /**
     * @param name
     * @throws NoSuchObjectException
     * @throws InvalidOperationException
     * @throws MetaException
     * @throws TException
     * @see
     * org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_database(java.lang.String, boolean, boolean)
     */
    @Override
    public void dropDatabase(String name)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        dropDatabase(name, true, false, false);
    }

    @Override
    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        dropDatabase(name, deleteData, ignoreUnknownDb, false);
    }

    @Override
    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        try {
            getDatabase(name);
        } catch (NoSuchObjectException e) {
            if (!ignoreUnknownDb) {
                throw e;
            }
            return;
        }

        if (cascade) {
            List<String> tableList = getAllTables(name);
            for (String table : tableList) {
                try {
                    // Subclasses can override this step (for example, for temporary tables)
                    dropTable(name, table, deleteData, true);
                } catch (UnsupportedOperationException e) {
                    // Ignore Index tables, those will be dropped with parent tables
                }
            }
        }
        client.drop_database(name, deleteData, cascade);
    }

    /**
     * @param tblName
     * @param dbName
     * @param partVals
     * @return true or false
     * @throws NoSuchObjectException
     * @throws MetaException
     * @throws TException
     * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_partition(java.lang.String,
     * java.lang.String, java.util.List, boolean)
     */
    public boolean dropPartition(String dbName, String tblName,
            List<String> partVals) throws NoSuchObjectException, MetaException,
            TException {
        return dropPartition(dbName, tblName, partVals, true, null);
    }

    public boolean dropPartition(String dbName, String tblName, List<String> partVals,
            EnvironmentContext envContext) throws NoSuchObjectException, MetaException, TException {
        return dropPartition(dbName, tblName, partVals, true, envContext);
    }

    @Override
    public boolean dropPartition(String dbName, String tableName, String partName, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        return dropPartition(dbName, tableName, partName, deleteData, null);
    }

    private static EnvironmentContext getEnvironmentContextWithIfPurgeSet() {
        Map<String, String> warehouseOptions = new HashMap<String, String>();
        warehouseOptions.put("ifPurge", "TRUE");
        return new EnvironmentContext(warehouseOptions);
    }

    public boolean dropPartition(String dbName, String tableName, String partName, boolean deleteData,
            EnvironmentContext envContext) throws NoSuchObjectException, MetaException, TException {
        return client.drop_partition_by_name_with_environment_context(dbName, tableName, partName,
                deleteData, envContext);
    }

    /**
     * @param dbName
     * @param tblName
     * @param partVals
     * @param deleteData delete the underlying data or just delete the table in metadata
     * @return true or false
     * @throws NoSuchObjectException
     * @throws MetaException
     * @throws TException
     * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_partition(java.lang.String,
     * java.lang.String, java.util.List, boolean)
     */
    @Override
    public boolean dropPartition(String dbName, String tblName,
            List<String> partVals, boolean deleteData) throws NoSuchObjectException,
            MetaException, TException {
        return dropPartition(dbName, tblName, partVals, deleteData, null);
    }

    @Override
    public boolean dropPartition(String dbName, String tblName,
            List<String> partVals, PartitionDropOptions options) throws TException {
        return dropPartition(dbName, tblName, partVals, options.deleteData,
                options.purgeData ? getEnvironmentContextWithIfPurgeSet() : null);
    }

    public boolean dropPartition(String dbName, String tblName, List<String> partVals,
            boolean deleteData, EnvironmentContext envContext) throws NoSuchObjectException,
            MetaException, TException {
        return client.drop_partition_with_environment_context(dbName, tblName, partVals, deleteData,
                envContext);
    }

    @Override
    public List<Partition> dropPartitions(String dbName, String tblName,
            List<ObjectPair<Integer, byte[]>> partExprs, PartitionDropOptions options)
            throws TException {
        RequestPartsSpec rps = new RequestPartsSpec();
        List<DropPartitionsExpr> exprs = new ArrayList<DropPartitionsExpr>(partExprs.size());
        for (ObjectPair<Integer, byte[]> partExpr : partExprs) {
            DropPartitionsExpr dpe = new DropPartitionsExpr();
            dpe.setExpr(partExpr.getSecond());
            dpe.setPartArchiveLevel(partExpr.getFirst());
            exprs.add(dpe);
        }
        rps.setExprs(exprs);
        DropPartitionsRequest req = new DropPartitionsRequest(dbName, tblName, rps);
        req.setDeleteData(options.deleteData);
        req.setNeedResult(options.returnResults);
        req.setIfExists(options.ifExists);
        if (options.purgeData) {
            LOG.info("Dropped partitions will be purged!");
            req.setEnvironmentContext(getEnvironmentContextWithIfPurgeSet());
        }
        return client.drop_partitions_req(req).getPartitions();
    }

    @Override
    public List<Partition> dropPartitions(String dbName, String tblName,
            List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData,
            boolean ifExists, boolean needResult) throws NoSuchObjectException, MetaException, TException {

        return dropPartitions(dbName, tblName, partExprs,
                PartitionDropOptions.instance()
                        .deleteData(deleteData)
                        .ifExists(ifExists)
                        .returnResults(needResult));

    }

    @Override
    public List<Partition> dropPartitions(String dbName, String tblName,
            List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData,
            boolean ifExists) throws NoSuchObjectException, MetaException, TException {
        // By default, we need the results from dropPartitions();
        return dropPartitions(dbName, tblName, partExprs,
                PartitionDropOptions.instance()
                        .deleteData(deleteData)
                        .ifExists(ifExists));
    }

    /**
     * {@inheritDoc}
     *
     * @see #dropTable(String, String, boolean, boolean, EnvironmentContext)
     */
    @Override
    public void dropTable(String dbname, String name, boolean deleteData,
            boolean ignoreUnknownTab) throws MetaException, TException,
            NoSuchObjectException, UnsupportedOperationException {
        dropTable(dbname, name, deleteData, ignoreUnknownTab, null);
    }

    /**
     * Drop the table and choose whether to save the data in the trash.
     *
     * @param ifPurge completely purge the table (skipping trash) while removing
     * data from warehouse
     * @see #dropTable(String, String, boolean, boolean, EnvironmentContext)
     */
    @Override
    public void dropTable(String dbname, String name, boolean deleteData,
            boolean ignoreUnknownTab, boolean ifPurge)
            throws MetaException, TException, NoSuchObjectException, UnsupportedOperationException {
        //build new environmentContext with ifPurge;
        EnvironmentContext envContext = null;
        if (ifPurge) {
            Map<String, String> warehouseOptions = null;
            warehouseOptions = new HashMap<String, String>();
            warehouseOptions.put("ifPurge", "TRUE");
            envContext = new EnvironmentContext(warehouseOptions);
        }
        dropTable(dbname, name, deleteData, ignoreUnknownTab, envContext);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Deprecated
    public void dropTable(String tableName, boolean deleteData)
            throws MetaException, UnknownTableException, TException, NoSuchObjectException {
        dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName, deleteData, false, null);
    }

    /**
     * @see #dropTable(String, String, boolean, boolean, EnvironmentContext)
     */
    @Override
    public void dropTable(String dbname, String name)
            throws NoSuchObjectException, MetaException, TException {
        dropTable(dbname, name, true, true, null);
    }

    /**
     * Drop the table and choose whether to: delete the underlying table data;
     * throw if the table doesn't exist; save the data in the trash.
     *
     * @param dbname
     * @param name
     * @param deleteData delete the underlying data or just delete the table in metadata
     * @param ignoreUnknownTab don't throw if the requested table doesn't exist
     * @param envContext for communicating with thrift
     * @throws MetaException could not drop table properly
     * @throws NoSuchObjectException the table wasn't found
     * @throws TException a thrift communication error occurred
     * @throws UnsupportedOperationException dropping an index table is not allowed
     * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_table(java.lang.String,
     * java.lang.String, boolean)
     */
    public void dropTable(String dbname, String name, boolean deleteData,
            boolean ignoreUnknownTab, EnvironmentContext envContext) throws MetaException, TException,
            NoSuchObjectException, UnsupportedOperationException {
        Table tbl;
        try {
            tbl = getTable(dbname, name);
        } catch (NoSuchObjectException e) {
            if (!ignoreUnknownTab) {
                throw e;
            }
            return;
        }
        if (MetaStoreUtils.isIndexTable(tbl)) {
            throw new UnsupportedOperationException("Cannot drop index tables");
        }
        HiveMetaHook hook = getHook(tbl);
        if (hook != null) {
            hook.preDropTable(tbl);
        }
        boolean success = false;
        try {
            drop_table_with_environment_context(dbname, name, deleteData, envContext);
            if (hook != null) {
                hook.commitDropTable(tbl,
                        deleteData || (envContext != null && "TRUE".equals(envContext.getProperties().get("ifPurge"))));
            }
            success = true;
        } catch (NoSuchObjectException e) {
            if (!ignoreUnknownTab) {
                throw e;
            }
        } finally {
            if (!success && (hook != null)) {
                hook.rollbackDropTable(tbl);
            }
        }
    }

    /**
     * @param type
     * @return true if the type is dropped
     * @throws MetaException
     * @throws TException
     * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_type(java.lang.String)
     */
    public boolean dropType(String type) throws NoSuchObjectException, MetaException, TException {
        return client.drop_type(type);
    }

    /**
     * @param name
     * @return map of types
     * @throws MetaException
     * @throws TException
     * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_type_all(java.lang.String)
     */
    public Map<String, Type> getTypeAll(String name) throws MetaException,
            TException {
        Map<String, Type> result = null;
        Map<String, Type> fromClient = client.get_type_all(name);
        if (fromClient != null) {
            result = new LinkedHashMap<String, Type>();
            for (String key : fromClient.keySet()) {
                result.put(key, deepCopy(fromClient.get(key)));
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getDatabases(String databasePattern)
            throws MetaException {
        try {
            return filterHook.filterDatabases(client.get_databases(databasePattern));
        } catch (Exception e) {
            throw new MetaException(e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getAllDatabases() throws MetaException {
        try {
            return filterHook.filterDatabases(client.get_all_databases());
        } catch (Exception e) {
            throw new MetaException(e.getMessage());
        }
    }

    /**
     * @param tblName
     * @param dbName
     * @param maxParts
     * @return list of partitions
     * @throws NoSuchObjectException
     * @throws MetaException
     * @throws TException
     */
    @Override
    public List<Partition> listPartitions(String dbName, String tblName,
            short maxParts) throws NoSuchObjectException, MetaException, TException {
        List<Partition> parts = client.get_partitions(dbName, tblName, maxParts);
        return fastpath ? parts : deepCopyPartitions(filterHook.filterPartitions(parts));
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(String dbName, String tableName, int maxParts) throws TException {
        return PartitionSpecProxy.Factory.get(filterHook.filterPartitionSpecs(
                client.get_partitions_pspec(dbName, tableName, maxParts)));
    }

    @Override
    public List<Partition> listPartitions(String dbName, String tblName,
            List<String> partVals, short maxParts)
            throws NoSuchObjectException, MetaException, TException {
        List<Partition> parts = client.get_partitions_ps(dbName, tblName, partVals, maxParts);
        return fastpath ? parts : deepCopyPartitions(filterHook.filterPartitions(parts));
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String dbName,
            String tblName, short maxParts, String userName, List<String> groupNames)
            throws NoSuchObjectException, MetaException, TException {
        List<Partition> parts = client.get_partitions_with_auth(dbName, tblName, maxParts,
                userName, groupNames);
        return fastpath ? parts : deepCopyPartitions(filterHook.filterPartitions(parts));
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String dbName,
            String tblName, List<String> partVals, short maxParts,
            String userName, List<String> groupNames) throws NoSuchObjectException,
            MetaException, TException {
        List<Partition> parts = client.get_partitions_ps_with_auth(dbName,
                tblName, partVals, maxParts, userName, groupNames);
        return fastpath ? parts : deepCopyPartitions(filterHook.filterPartitions(parts));
    }

    /**
     * Get list of partitions matching specified filter
     *
     * @param dbName the database name
     * @param tblName the table name
     * @param filter the filter string,
     * for example "part1 = \"p1_abc\" and part2 <= "\p2_test\"". Filtering can
     * be done only on string partition keys.
     * @param maxParts the maximum number of partitions to return,
     * all partitions are returned if -1 is passed
     * @return list of partitions
     * @throws MetaException
     * @throws NoSuchObjectException
     * @throws TException
     */
    @Override
    public List<Partition> listPartitionsByFilter(String dbName, String tblName,
            String filter, short maxParts) throws MetaException,
            NoSuchObjectException, TException {
        List<Partition> parts = client.get_partitions_by_filter(dbName, tblName, filter, maxParts);
        return fastpath ? parts : deepCopyPartitions(filterHook.filterPartitions(parts));
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(String dbName, String tblName,
            String filter, int maxParts) throws MetaException,
            NoSuchObjectException, TException {
        return PartitionSpecProxy.Factory.get(filterHook.filterPartitionSpecs(
                client.get_part_specs_by_filter(dbName, tblName, filter, maxParts)));
    }

    @Override
    public boolean listPartitionsByExpr(String dbName, String tblName, byte[] expr,
            String defaultPartitionName, short maxParts, List<Partition> result)
            throws TException {
        assert result != null;
        PartitionsByExprRequest req = new PartitionsByExprRequest(
                dbName, tblName, ByteBuffer.wrap(expr));
        if (defaultPartitionName != null) {
            req.setDefaultPartitionName(defaultPartitionName);
        }
        if (maxParts >= 0) {
            req.setMaxParts(maxParts);
        }
        PartitionsByExprResult r = null;
        try {
            r = client.get_partitions_by_expr(req);
        } catch (TApplicationException te) {
            // TODO: backward compat for Hive <= 0.12. Can be removed later.
            if (te.getType() != TApplicationException.UNKNOWN_METHOD
                    && te.getType() != TApplicationException.WRONG_METHOD_NAME) {
                throw te;
            }
            throw new MetaException(
                    "Metastore doesn't support listPartitionsByExpr: " + te.getMessage());
        }
        if (fastpath) {
            result.addAll(r.getPartitions());
        } else {
            r.setPartitions(filterHook.filterPartitions(r.getPartitions()));
            // TODO: in these methods, do we really need to deepcopy?
            deepCopyPartitions(r.getPartitions(), result);
        }
        return !r.isSetHasUnknownPartitions() || r.isHasUnknownPartitions(); // Assume the worst.
    }

    /**
     * @param name
     * @return the database
     * @throws NoSuchObjectException
     * @throws MetaException
     * @throws TException
     * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_database(java.lang.String)
     */
    @Override
    public Database getDatabase(String name) throws NoSuchObjectException,
            MetaException, TException {
        Database d = client.get_database(name);
        return fastpath ? d : deepCopy(filterHook.filterDatabase(d));
    }

    /**
     * @param tblName
     * @param dbName
     * @param partVals
     * @return the partition
     * @throws MetaException
     * @throws TException
     * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_partition(java.lang.String,
     * java.lang.String, java.util.List)
     */
    @Override
    public Partition getPartition(String dbName, String tblName,
            List<String> partVals) throws NoSuchObjectException, MetaException, TException {
        Partition p = client.get_partition(dbName, tblName, partVals);
        return fastpath ? p : deepCopy(filterHook.filterPartition(p));
    }

    @Override
    public List<Partition> getPartitionsByNames(String dbName, String tblName,
            List<String> partNames) throws NoSuchObjectException, MetaException, TException {
        List<Partition> parts = client.get_partitions_by_names(dbName, tblName, partNames);
        return fastpath ? parts : deepCopyPartitions(filterHook.filterPartitions(parts));
    }

    @Override
    public PartitionValuesResponse listPartitionValues(PartitionValuesRequest request)
            throws MetaException, TException, NoSuchObjectException {
        return client.get_partition_values(request);
    }

    @Override
    public Partition getPartitionWithAuthInfo(String dbName, String tblName,
            List<String> partVals, String userName, List<String> groupNames)
            throws MetaException, UnknownTableException, NoSuchObjectException,
            TException {
        Partition p = client.get_partition_with_auth(dbName, tblName, partVals, userName,
                groupNames);
        return fastpath ? p : deepCopy(filterHook.filterPartition(p));
    }

    /**
     * @param name
     * @param dbname
     * @return the table
     * @throws NoSuchObjectException
     * @throws MetaException
     * @throws TException
     * @throws NoSuchObjectException
     * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_table(java.lang.String,
     * java.lang.String)
     */
    @Override
    public Table getTable(String dbname, String name) throws MetaException,
            TException, NoSuchObjectException {
        Table t;
        if (hiveVersion == HiveVersion.V1_0) {
            t = client.get_table(dbname, name);
        } else {
            GetTableRequest req = new GetTableRequest(dbname, name);
            req.setCapabilities(version);
            t = client.get_table_req(req).getTable();
        }
        return fastpath ? t : deepCopy(filterHook.filterTable(t));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Deprecated
    public Table getTable(String tableName) throws MetaException, TException,
            NoSuchObjectException {
        Table t = getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
        return fastpath ? t : filterHook.filterTable(t);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
            throws MetaException, InvalidOperationException, UnknownDBException, TException {
        GetTablesRequest req = new GetTablesRequest(dbName);
        req.setTblNames(tableNames);
        req.setCapabilities(version);
        List<Table> tabs = client.get_table_objects_by_name_req(req).getTables();
        return fastpath ? tabs : deepCopyTables(filterHook.filterTables(tabs));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables)
            throws MetaException, TException, InvalidOperationException, UnknownDBException {
        return filterHook.filterTableNames(dbName,
                client.get_table_names_by_filter(dbName, filter, maxTables));
    }

    /**
     * @param name
     * @return the type
     * @throws MetaException
     * @throws TException
     * @throws NoSuchObjectException
     * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_type(java.lang.String)
     */
    public Type getType(String name) throws NoSuchObjectException, MetaException, TException {
        return deepCopy(client.get_type(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getTables(String dbname, String tablePattern) throws MetaException {
        try {
            return filterHook.filterTableNames(dbname, client.get_tables(dbname, tablePattern));
        } catch (Exception e) {
            throw new MetaException(e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getTables(String dbname, String tablePattern, TableType tableType) throws MetaException {
        try {
            return filterHook.filterTableNames(dbname,
                    client.get_tables_by_type(dbname, tablePattern, tableType.toString()));
        } catch (Exception e) {
            throw new MetaException(e.getMessage());
        }
    }

    @Override
    public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes)
            throws MetaException {
        try {
            return filterNames(client.get_table_meta(dbPatterns, tablePatterns, tableTypes));
        } catch (Exception e) {
            throw new MetaException(e.getMessage());
        }
    }

    private List<TableMeta> filterNames(List<TableMeta> metas) throws MetaException {
        Map<String, TableMeta> sources = new LinkedHashMap<>();
        Map<String, List<String>> dbTables = new LinkedHashMap<>();
        for (TableMeta meta : metas) {
            sources.put(meta.getDbName() + "." + meta.getTableName(), meta);
            List<String> tables = dbTables.get(meta.getDbName());
            if (tables == null) {
                dbTables.put(meta.getDbName(), tables = new ArrayList<String>());
            }
            tables.add(meta.getTableName());
        }
        List<TableMeta> filtered = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : dbTables.entrySet()) {
            for (String table : filterHook.filterTableNames(entry.getKey(), entry.getValue())) {
                filtered.add(sources.get(entry.getKey() + "." + table));
            }
        }
        return filtered;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getAllTables(String dbname) throws MetaException {
        try {
            return filterHook.filterTableNames(dbname, client.get_all_tables(dbname));
        } catch (Exception e) {
            throw new MetaException(e.getMessage());
        }
    }

    @Override
    public boolean tableExists(String databaseName, String tableName) throws MetaException,
            TException, UnknownDBException {
        try {
            Table t;
            if (hiveVersion == HiveVersion.V1_0) {
                t = client.get_table(databaseName, tableName);
            } else {
                GetTableRequest req = new GetTableRequest(databaseName, tableName);
                req.setCapabilities(version);
                t = client.get_table_req(req).getTable();
            }
            return filterHook.filterTable(t) != null;
        } catch (NoSuchObjectException e) {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Deprecated
    public boolean tableExists(String tableName) throws MetaException,
            TException, UnknownDBException {
        return tableExists(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName,
            short max) throws MetaException, TException {
        return filterHook.filterPartitionNames(dbName, tblName,
                client.get_partition_names(dbName, tblName, max));
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName,
            List<String> partVals, short maxParts)
            throws MetaException, TException, NoSuchObjectException {
        return filterHook.filterPartitionNames(dbName, tblName,
                client.get_partition_names_ps(dbName, tblName, partVals, maxParts));
    }

    /**
     * Get number of partitions matching specified filter
     *
     * @param dbName the database name
     * @param tblName the table name
     * @param filter the filter string,
     * for example "part1 = \"p1_abc\" and part2 <= "\p2_test\"". Filtering can
     * be done only on string partition keys.
     * @return number of partitions
     * @throws MetaException
     * @throws NoSuchObjectException
     * @throws TException
     */
    @Override
    public int getNumPartitionsByFilter(String dbName, String tblName,
            String filter) throws MetaException,
            NoSuchObjectException, TException {
        return client.get_num_partitions_by_filter(dbName, tblName, filter);
    }

    @Override
    public void alter_partition(String dbName, String tblName, Partition newPart)
            throws InvalidOperationException, MetaException, TException {
        client.alter_partition_with_environment_context(dbName, tblName, newPart, null);
    }

    @Override
    public void alter_partition(String dbName, String tblName, Partition newPart, EnvironmentContext environmentContext)
            throws InvalidOperationException, MetaException, TException {
        client.alter_partition_with_environment_context(dbName, tblName, newPart, environmentContext);
    }

    @Override
    public void alter_partitions(String dbName, String tblName, List<Partition> newParts)
            throws InvalidOperationException, MetaException, TException {
        client.alter_partitions_with_environment_context(dbName, tblName, newParts, null);
    }

    @Override
    public void alter_partitions(String dbName, String tblName, List<Partition> newParts,
            EnvironmentContext environmentContext)
            throws InvalidOperationException, MetaException, TException {
        client.alter_partitions_with_environment_context(dbName, tblName, newParts, environmentContext);
    }

    @Override
    public void alterDatabase(String dbName, Database db)
            throws MetaException, NoSuchObjectException, TException {
        client.alter_database(dbName, db);
    }

    /**
     * @param db
     * @param tableName
     * @throws UnknownTableException
     * @throws UnknownDBException
     * @throws MetaException
     * @throws TException
     * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_fields(java.lang.String,
     * java.lang.String)
     */
    @Override
    public List<FieldSchema> getFields(String db, String tableName)
            throws MetaException, TException, UnknownTableException,
            UnknownDBException {
        List<FieldSchema> fields = client.get_fields(db, tableName);
        return fastpath ? fields : deepCopyFieldSchemas(fields);
    }

    /**
     * create an index
     *
     * @param index the index object
     * @param indexTable which stores the index data
     * @throws InvalidObjectException
     * @throws MetaException
     * @throws NoSuchObjectException
     * @throws TException
     * @throws AlreadyExistsException
     */
    @Override
    public void createIndex(Index index, Table indexTable)
            throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
        client.add_index(index, indexTable);
    }

    /**
     * @param dbname
     * @param baseTblName
     * @param idxName
     * @param newIdx
     * @throws InvalidOperationException
     * @throws MetaException
     * @throws TException
     * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#alter_index(java.lang.String,
     * java.lang.String, java.lang.String, org.apache.hadoop.hive.metastore.api.Index)
     */
    @Override
    public void alter_index(String dbname, String baseTblName, String idxName, Index newIdx)
            throws InvalidOperationException, MetaException, TException {
        client.alter_index(dbname, baseTblName, idxName, newIdx);
    }

    /**
     * @param dbName
     * @param tblName
     * @param indexName
     * @return the index
     * @throws MetaException
     * @throws UnknownTableException
     * @throws NoSuchObjectException
     * @throws TException
     */
    @Override
    public Index getIndex(String dbName, String tblName, String indexName)
            throws MetaException, UnknownTableException, NoSuchObjectException,
            TException {
        return deepCopy(filterHook.filterIndex(client.get_index_by_name(dbName, tblName, indexName)));
    }

    /**
     * list indexes of the give base table
     *
     * @param dbName
     * @param tblName
     * @param max
     * @return the list of indexes
     * @throws NoSuchObjectException
     * @throws MetaException
     * @throws TException
     */
    @Override
    public List<String> listIndexNames(String dbName, String tblName, short max)
            throws MetaException, TException {
        return filterHook.filterIndexNames(dbName, tblName, client.get_index_names(dbName, tblName, max));
    }

    /**
     * list all the index names of the give base table.
     *
     * @param dbName
     * @param tblName
     * @param max
     * @return list of indexes
     * @throws MetaException
     * @throws TException
     */
    @Override
    public List<Index> listIndexes(String dbName, String tblName, short max)
            throws NoSuchObjectException, MetaException, TException {
        return filterHook.filterIndexes(client.get_indexes(dbName, tblName, max));
    }

    @Override
    public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest req)
            throws MetaException, NoSuchObjectException, TException {
        return client.get_primary_keys(req).getPrimaryKeys();
    }

    @Override
    public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest req) throws MetaException,
            NoSuchObjectException, TException {
        return client.get_foreign_keys(req).getForeignKeys();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    @Deprecated
    //use setPartitionColumnStatistics instead
    public boolean updateTableColumnStatistics(ColumnStatistics statsObj)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
            InvalidInputException {
        return client.update_table_column_statistics(statsObj);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Deprecated
    //use setPartitionColumnStatistics instead
    public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
            InvalidInputException {
        return client.update_partition_column_statistics(statsObj);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
            InvalidInputException {
        return client.set_aggr_stats_for(request);
    }

    @Override
    public void flushCache() {
        try {
            client.flushCache();
        } catch (TException e) {
            // Not much we can do about it honestly
            LOG.warn("Got error flushing the cache", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
            List<String> colNames) throws NoSuchObjectException, MetaException, TException,
            InvalidInputException, InvalidObjectException {
        return client.get_table_statistics_req(
                new TableStatsRequest(dbName, tableName, colNames)).getTableStats();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            String dbName, String tableName, List<String> partNames, List<String> colNames)
            throws NoSuchObjectException, MetaException, TException {
        return client.get_partitions_statistics_req(
                new PartitionsStatsRequest(dbName, tableName, colNames, partNames)).getPartStats();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName,
            String colName) throws NoSuchObjectException, InvalidObjectException, MetaException,
            TException, InvalidInputException {
        return client.delete_partition_column_statistics(dbName, tableName, partName, colName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
            InvalidInputException {
        return client.delete_table_column_statistics(dbName, tableName, colName);
    }

    /**
     * @param db
     * @param tableName
     * @throws UnknownTableException
     * @throws UnknownDBException
     * @throws MetaException
     * @throws TException
     * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_schema(java.lang.String,
     * java.lang.String)
     */
    @Override
    public List<FieldSchema> getSchema(String db, String tableName)
            throws MetaException, TException, UnknownTableException,
            UnknownDBException {
        List<FieldSchema> fields;
        if (hiveVersion == HiveVersion.V1_0) {
            fields = client.get_schema(db, tableName);
        } else {
            EnvironmentContext envCxt = null;
            String addedJars = conf.getVar(ConfVars.HIVEADDEDJARS);
            if (org.apache.commons.lang.StringUtils.isNotBlank(addedJars)) {
                Map<String, String> props = new HashMap<String, String>();
                props.put("hive.added.jars.path", addedJars);
                envCxt = new EnvironmentContext(props);
            }
            fields = client.get_schema_with_environment_context(db, tableName, envCxt);
        }
        return fastpath ? fields : deepCopyFieldSchemas(fields);
    }

    @Override
    public String getConfigValue(String name, String defaultValue)
            throws TException, ConfigValSecurityException {
        return client.get_config_value(name, defaultValue);
    }

    @Override
    public Partition getPartition(String db, String tableName, String partName)
            throws MetaException, TException, UnknownTableException, NoSuchObjectException {
        Partition p = client.get_partition_by_name(db, tableName, partName);
        return fastpath ? p : deepCopy(filterHook.filterPartition(p));
    }

    public Partition appendPartitionByName(String dbName, String tableName, String partName)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return appendPartitionByName(dbName, tableName, partName, null);
    }

    public Partition appendPartitionByName(String dbName, String tableName, String partName,
            EnvironmentContext envContext) throws InvalidObjectException, AlreadyExistsException,
            MetaException, TException {
        Partition p = client.append_partition_by_name_with_environment_context(dbName, tableName,
                partName, envContext);
        return fastpath ? p : deepCopy(p);
    }

    public boolean dropPartitionByName(String dbName, String tableName, String partName,
            boolean deleteData) throws NoSuchObjectException, MetaException, TException {
        return dropPartitionByName(dbName, tableName, partName, deleteData, null);
    }

    public boolean dropPartitionByName(String dbName, String tableName, String partName,
            boolean deleteData, EnvironmentContext envContext) throws NoSuchObjectException,
            MetaException, TException {
        return client.drop_partition_by_name_with_environment_context(dbName, tableName, partName,
                deleteData, envContext);
    }

    private HiveMetaHook getHook(Table tbl) throws MetaException {
        if (hookLoader == null) {
            return null;
        }
        return hookLoader.getHook(tbl);
    }

    @Override
    public List<String> partitionNameToVals(String name) throws MetaException, TException {
        return client.partition_name_to_vals(name);
    }

    @Override
    public Map<String, String> partitionNameToSpec(String name) throws MetaException, TException {
        return client.partition_name_to_spec(name);
    }

    /**
     * @param partition
     * @return
     */
    private Partition deepCopy(Partition partition) {
        Partition copy = null;
        if (partition != null) {
            copy = new Partition(partition);
        }
        return copy;
    }

    private Database deepCopy(Database database) {
        Database copy = null;
        if (database != null) {
            copy = new Database(database);
        }
        return copy;
    }

    protected Table deepCopy(Table table) {
        Table copy = null;
        if (table != null) {
            copy = new Table(table);
        }
        return copy;
    }

    private Index deepCopy(Index index) {
        Index copy = null;
        if (index != null) {
            copy = new Index(index);
        }
        return copy;
    }

    private Type deepCopy(Type type) {
        Type copy = null;
        if (type != null) {
            copy = new Type(type);
        }
        return copy;
    }

    private FieldSchema deepCopy(FieldSchema schema) {
        FieldSchema copy = null;
        if (schema != null) {
            copy = new FieldSchema(schema);
        }
        return copy;
    }

    private Function deepCopy(Function func) {
        Function copy = null;
        if (func != null) {
            copy = new Function(func);
        }
        return copy;
    }

    protected PrincipalPrivilegeSet deepCopy(PrincipalPrivilegeSet pps) {
        PrincipalPrivilegeSet copy = null;
        if (pps != null) {
            copy = new PrincipalPrivilegeSet(pps);
        }
        return copy;
    }

    private List<Partition> deepCopyPartitions(List<Partition> partitions) {
        return deepCopyPartitions(partitions, null);
    }

    private List<Partition> deepCopyPartitions(
            Collection<Partition> src, List<Partition> dest) {
        if (src == null) {
            return dest;
        }
        if (dest == null) {
            dest = new ArrayList<Partition>(src.size());
        }
        for (Partition part : src) {
            dest.add(deepCopy(part));
        }
        return dest;
    }

    private List<Table> deepCopyTables(List<Table> tables) {
        List<Table> copy = null;
        if (tables != null) {
            copy = new ArrayList<Table>();
            for (Table tab : tables) {
                copy.add(deepCopy(tab));
            }
        }
        return copy;
    }

    protected List<FieldSchema> deepCopyFieldSchemas(List<FieldSchema> schemas) {
        List<FieldSchema> copy = null;
        if (schemas != null) {
            copy = new ArrayList<FieldSchema>();
            for (FieldSchema schema : schemas) {
                copy.add(deepCopy(schema));
            }
        }
        return copy;
    }

    @Override
    public boolean dropIndex(String dbName, String tblName, String name,
            boolean deleteData) throws NoSuchObjectException, MetaException,
            TException {
        return client.drop_index_by_name(dbName, tblName, name, deleteData);
    }

    @Override
    public boolean grant_role(String roleName, String userName,
            PrincipalType principalType, String grantor, PrincipalType grantorType,
            boolean grantOption) throws MetaException, TException {
        GrantRevokeRoleRequest req = new GrantRevokeRoleRequest();
        req.setRequestType(GrantRevokeType.GRANT);
        req.setRoleName(roleName);
        req.setPrincipalName(userName);
        req.setPrincipalType(principalType);
        req.setGrantor(grantor);
        req.setGrantorType(grantorType);
        req.setGrantOption(grantOption);
        GrantRevokeRoleResponse res = client.grant_revoke_role(req);
        if (!res.isSetSuccess()) {
            throw new MetaException("GrantRevokeResponse missing success field");
        }
        return res.isSuccess();
    }

    @Override
    public boolean create_role(Role role)
            throws MetaException, TException {
        return client.create_role(role);
    }

    @Override
    public boolean drop_role(String roleName) throws MetaException, TException {
        return client.drop_role(roleName);
    }

    @Override
    public List<Role> list_roles(String principalName,
            PrincipalType principalType) throws MetaException, TException {
        return client.list_roles(principalName, principalType);
    }

    @Override
    public List<String> listRoleNames() throws MetaException, TException {
        return client.get_role_names();
    }

    @Override
    public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest req)
            throws MetaException, TException {
        return client.get_principals_in_role(req);
    }

    @Override
    public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
            GetRoleGrantsForPrincipalRequest getRolePrincReq) throws MetaException, TException {
        return client.get_role_grants_for_principal(getRolePrincReq);
    }

    @Override
    public boolean grant_privileges(PrivilegeBag privileges)
            throws MetaException, TException {
        GrantRevokePrivilegeRequest req = new GrantRevokePrivilegeRequest();
        req.setRequestType(GrantRevokeType.GRANT);
        req.setPrivileges(privileges);
        GrantRevokePrivilegeResponse res = client.grant_revoke_privileges(req);
        if (!res.isSetSuccess()) {
            throw new MetaException("GrantRevokePrivilegeResponse missing success field");
        }
        return res.isSuccess();
    }

    @Override
    public boolean revoke_role(String roleName, String userName,
            PrincipalType principalType, boolean grantOption) throws MetaException, TException {
        GrantRevokeRoleRequest req = new GrantRevokeRoleRequest();
        req.setRequestType(GrantRevokeType.REVOKE);
        req.setRoleName(roleName);
        req.setPrincipalName(userName);
        req.setPrincipalType(principalType);
        req.setGrantOption(grantOption);
        GrantRevokeRoleResponse res = client.grant_revoke_role(req);
        if (!res.isSetSuccess()) {
            throw new MetaException("GrantRevokeResponse missing success field");
        }
        return res.isSuccess();
    }

    @Override
    public boolean revoke_privileges(PrivilegeBag privileges, boolean grantOption) throws MetaException,
            TException {
        GrantRevokePrivilegeRequest req = new GrantRevokePrivilegeRequest();
        req.setRequestType(GrantRevokeType.REVOKE);
        req.setPrivileges(privileges);
        req.setRevokeGrantOption(grantOption);
        GrantRevokePrivilegeResponse res = client.grant_revoke_privileges(req);
        if (!res.isSetSuccess()) {
            throw new MetaException("GrantRevokePrivilegeResponse missing success field");
        }
        return res.isSuccess();
    }

    @Override
    public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject,
            String userName, List<String> groupNames) throws MetaException,
            TException {
        return client.get_privilege_set(hiveObject, userName, groupNames);
    }

    @Override
    public List<HiveObjectPrivilege> list_privileges(String principalName,
            PrincipalType principalType, HiveObjectRef hiveObject)
            throws MetaException, TException {
        return client.list_privileges(principalName, principalType, hiveObject);
    }

    public String getDelegationToken(String renewerKerberosPrincipalName) throws
            MetaException, TException, IOException {
        //a convenience method that makes the intended owner for the delegation
        //token request the current user
        String owner = conf.getUser();
        return getDelegationToken(owner, renewerKerberosPrincipalName);
    }

    @Override
    public String getDelegationToken(String owner, String renewerKerberosPrincipalName) throws
            MetaException, TException {
        // This is expected to be a no-op, so we will return null when we use local metastore.
        if (localMetaStore) {
            return null;
        }
        return client.get_delegation_token(owner, renewerKerberosPrincipalName);
    }

    @Override
    public long renewDelegationToken(String tokenStrForm) throws MetaException, TException {
        if (localMetaStore) {
            return 0;
        }
        return client.renew_delegation_token(tokenStrForm);

    }

    @Override
    public void cancelDelegationToken(String tokenStrForm) throws MetaException, TException {
        if (localMetaStore) {
            return;
        }
        client.cancel_delegation_token(tokenStrForm);
    }

    @Override
    public boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
        return client.add_token(tokenIdentifier, delegationToken);
    }

    @Override
    public boolean removeToken(String tokenIdentifier) throws TException {
        return client.remove_token(tokenIdentifier);
    }

    @Override
    public String getToken(String tokenIdentifier) throws TException {
        return client.get_token(tokenIdentifier);
    }

    @Override
    public List<String> getAllTokenIdentifiers() throws TException {
        return client.get_all_token_identifiers();
    }

    @Override
    public int addMasterKey(String key) throws MetaException, TException {
        return client.add_master_key(key);
    }

    @Override
    public void updateMasterKey(Integer seqNo, String key)
            throws NoSuchObjectException, MetaException, TException {
        client.update_master_key(seqNo, key);
    }

    @Override
    public boolean removeMasterKey(Integer keySeq) throws TException {
        return client.remove_master_key(keySeq);
    }

    @Override
    public String[] getMasterKeys() throws TException {
        List<String> keyList = client.get_master_keys();
        return keyList.toArray(new String[keyList.size()]);
    }

    @Override
    public ValidTxnList getValidTxns() throws TException {
        return TxnUtils.createValidReadTxnList(client.get_open_txns(), 0);
    }

    @Override
    public ValidTxnList getValidTxns(long currentTxn) throws TException {
        return TxnUtils.createValidReadTxnList(client.get_open_txns(), currentTxn);
    }

    @Override
    public long openTxn(String user) throws TException {
        OpenTxnsResponse txns = openTxns(user, 1);
        return txns.getTxn_ids().get(0);
    }

    @Override
    public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
        String hostname = null;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOG.error("Unable to resolve my host name " + e.getMessage());
            throw new RuntimeException(e);
        }
        return client.open_txns(new OpenTxnRequest(numTxns, user, hostname));
    }

    @Override
    public void rollbackTxn(long txnid) throws NoSuchTxnException, TException {
        client.abort_txn(new AbortTxnRequest(txnid));
    }

    @Override
    public void commitTxn(long txnid)
            throws NoSuchTxnException, TxnAbortedException, TException {
        client.commit_txn(new CommitTxnRequest(txnid));
    }

    @Override
    public GetOpenTxnsInfoResponse showTxns() throws TException {
        return client.get_open_txns_info();
    }

    @Override
    public void abortTxns(List<Long> txnids) throws NoSuchTxnException, TException {
        client.abort_txns(new AbortTxnsRequest(txnids));
    }

    @Override
    public LockResponse lock(LockRequest request)
            throws NoSuchTxnException, TxnAbortedException, TException {
        return client.lock(request);
    }

    @Override
    public LockResponse checkLock(long lockid)
            throws NoSuchTxnException, TxnAbortedException, NoSuchLockException,
            TException {
        return client.check_lock(new CheckLockRequest(lockid));
    }

    @Override
    public void unlock(long lockid)
            throws NoSuchLockException, TxnOpenException, TException {
        client.unlock(new UnlockRequest(lockid));
    }

    @Override
    @Deprecated
    public ShowLocksResponse showLocks() throws TException {
        return client.show_locks(new ShowLocksRequest());
    }

    @Override
    public ShowLocksResponse showLocks(ShowLocksRequest request) throws TException {
        return client.show_locks(request);
    }

    @Override
    public void heartbeat(long txnid, long lockid)
            throws NoSuchLockException, NoSuchTxnException, TxnAbortedException,
            TException {
        HeartbeatRequest hb = new HeartbeatRequest();
        hb.setLockid(lockid);
        hb.setTxnid(txnid);
        client.heartbeat(hb);
    }

    @Override
    public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max)
            throws NoSuchTxnException, TxnAbortedException, TException {
        HeartbeatTxnRangeRequest rqst = new HeartbeatTxnRangeRequest(min, max);
        return client.heartbeat_txn_range(rqst);
    }

    @Override
    @Deprecated
    public void compact(String dbname, String tableName, String partitionName, CompactionType type)
            throws TException {
        CompactionRequest cr = new CompactionRequest();
        if (dbname == null) {
            cr.setDbname(MetaStoreUtils.DEFAULT_DATABASE_NAME);
        } else {
            cr.setDbname(dbname);
        }
        cr.setTablename(tableName);
        if (partitionName != null) {
            cr.setPartitionname(partitionName);
        }
        cr.setType(type);
        client.compact(cr);
    }

    @Deprecated
    @Override
    public void compact(String dbname, String tableName, String partitionName, CompactionType type,
            Map<String, String> tblproperties) throws TException {
        compact2(dbname, tableName, partitionName, type, tblproperties);
    }

    @Override
    public CompactionResponse compact2(String dbname, String tableName, String partitionName, CompactionType type,
            Map<String, String> tblproperties) throws TException {
        CompactionRequest cr = new CompactionRequest();
        if (dbname == null) {
            cr.setDbname(MetaStoreUtils.DEFAULT_DATABASE_NAME);
        } else {
            cr.setDbname(dbname);
        }
        cr.setTablename(tableName);
        if (partitionName != null) {
            cr.setPartitionname(partitionName);
        }
        cr.setType(type);
        cr.setProperties(tblproperties);
        return client.compact2(cr);
    }

    @Override
    public ShowCompactResponse showCompactions() throws TException {
        return client.show_compact(new ShowCompactRequest());
    }

    @Deprecated
    @Override
    public void addDynamicPartitions(long txnId, String dbName, String tableName,
            List<String> partNames) throws TException {
        client.add_dynamic_partitions(new AddDynamicPartitions(txnId, dbName, tableName, partNames));
    }

    @Override
    public void addDynamicPartitions(long txnId, String dbName, String tableName,
            List<String> partNames, DataOperationType operationType) throws TException {
        AddDynamicPartitions adp = new AddDynamicPartitions(txnId, dbName, tableName, partNames);
        adp.setOperationType(operationType);
        client.add_dynamic_partitions(adp);
    }

    @Override
    public void insertTable(Table table, boolean overwrite) throws MetaException {
        boolean failed = true;
        HiveMetaHook hook = getHook(table);
        if (hook == null || !(hook instanceof DefaultHiveMetaHook)) {
            return;
        }
        DefaultHiveMetaHook hiveMetaHook = (DefaultHiveMetaHook) hook;
        try {
            hiveMetaHook.commitInsertTable(table, overwrite);
            failed = false;
        } finally {
            if (failed) {
                hiveMetaHook.rollbackInsertTable(table, overwrite);
            }
        }
    }

    @InterfaceAudience.LimitedPrivate({"HCatalog"})
    @Override
    public NotificationEventResponse getNextNotification(long lastEventId, int maxEvents,
            NotificationFilter filter) throws TException {
        NotificationEventRequest rqst = new NotificationEventRequest(lastEventId);
        rqst.setMaxEvents(maxEvents);
        NotificationEventResponse rsp = client.get_next_notification(rqst);
        LOG.debug("Got back " + rsp.getEventsSize() + " events");
        if (filter == null) {
            return rsp;
        } else {
            NotificationEventResponse filtered = new NotificationEventResponse();
            if (rsp != null && rsp.getEvents() != null) {
                for (NotificationEvent e : rsp.getEvents()) {
                    if (filter.accept(e)) {
                        filtered.addToEvents(e);
                    }
                }
            }
            return filtered;
        }
    }

    @InterfaceAudience.LimitedPrivate({"HCatalog"})
    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
        return client.get_current_notificationEventId();
    }

    @InterfaceAudience.LimitedPrivate({"Apache Hive, HCatalog"})
    @Override
    public FireEventResponse fireListenerEvent(FireEventRequest rqst) throws TException {
        return client.fire_listener_event(rqst);
    }

    /**
     * Creates a synchronized wrapper for any {@link IMetaStoreClient}.
     * This may be used by multi-threaded applications until we have
     * fixed all reentrancy bugs.
     *
     * @param client unsynchronized client
     * @return synchronized client
     */
    public static IMetaStoreClient newSynchronizedClient(
            IMetaStoreClient client) {
        return (IMetaStoreClient) Proxy.newProxyInstance(
                HiveMetaStoreClient.class.getClassLoader(),
                new Class[] {IMetaStoreClient.class},
                new SynchronizedHandler(client));
    }

    private static class SynchronizedHandler implements InvocationHandler {
        private final IMetaStoreClient client;

        SynchronizedHandler(IMetaStoreClient client) {
            this.client = client;
        }

        @Override
        public synchronized Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable {
            try {
                return method.invoke(client, args);
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        }
    }

    @Override
    public void markPartitionForEvent(String dbName, String tblName, Map<String, String> partKVs,
            PartitionEventType eventType)
            throws MetaException, TException, NoSuchObjectException, UnknownDBException,
            UnknownTableException,
            InvalidPartitionException, UnknownPartitionException {
        assert dbName != null;
        assert tblName != null;
        assert partKVs != null;
        client.markPartitionForEvent(dbName, tblName, partKVs, eventType);
    }

    @Override
    public boolean isPartitionMarkedForEvent(String dbName, String tblName, Map<String, String> partKVs,
            PartitionEventType eventType)
            throws MetaException, NoSuchObjectException, UnknownTableException, UnknownDBException, TException,
            InvalidPartitionException, UnknownPartitionException {
        assert dbName != null;
        assert tblName != null;
        assert partKVs != null;
        return client.isPartitionMarkedForEvent(dbName, tblName, partKVs, eventType);
    }

    @Override
    public void createFunction(Function func) throws InvalidObjectException,
            MetaException, TException {
        client.create_function(func);
    }

    @Override
    public void alterFunction(String dbName, String funcName, Function newFunction)
            throws InvalidObjectException, MetaException, TException {
        client.alter_function(dbName, funcName, newFunction);
    }

    @Override
    public void dropFunction(String dbName, String funcName)
            throws MetaException, NoSuchObjectException, InvalidObjectException,
            InvalidInputException, TException {
        client.drop_function(dbName, funcName);
    }

    @Override
    public Function getFunction(String dbName, String funcName)
            throws MetaException, TException {
        Function f = client.get_function(dbName, funcName);
        return fastpath ? f : deepCopy(f);
    }

    @Override
    public List<String> getFunctions(String dbName, String pattern)
            throws MetaException, TException {
        return client.get_functions(dbName, pattern);
    }

    @Override
    public GetAllFunctionsResponse getAllFunctions()
            throws MetaException, TException {
        return client.get_all_functions();
    }

    protected void create_table_with_environment_context(Table tbl, EnvironmentContext envContext)
            throws AlreadyExistsException, InvalidObjectException,
            MetaException, NoSuchObjectException, TException {
        client.create_table_with_environment_context(tbl, envContext);
    }

    protected void drop_table_with_environment_context(String dbname, String name,
            boolean deleteData, EnvironmentContext envContext) throws MetaException, TException,
            NoSuchObjectException, UnsupportedOperationException {
        client.drop_table_with_environment_context(dbname, name, deleteData, envContext);
    }

    @Override
    public AggrStats getAggrColStatsFor(String dbName, String tblName,
            List<String> colNames, List<String> partNames) throws NoSuchObjectException, MetaException, TException {
        if (colNames.isEmpty() || partNames.isEmpty()) {
            LOG.debug("Columns is empty or partNames is empty : Short-circuiting stats eval on client side.");
            return new AggrStats(new ArrayList<ColumnStatisticsObj>(), 0); // Nothing to aggregate
        }
        PartitionsStatsRequest req = new PartitionsStatsRequest(dbName, tblName, colNames, partNames);
        return client.get_aggr_stats_for(req);
    }

    @Override
    public Iterable<Entry<Long, ByteBuffer>> getFileMetadata(
            final List<Long> fileIds) throws TException {
        return new MetastoreMapIterable<Long, ByteBuffer>() {
            private int listIndex = 0;

            @Override
            protected Map<Long, ByteBuffer> fetchNextBatch() throws TException {
                if (listIndex == fileIds.size()) {
                    return null;
                }
                int endIndex = Math.min(listIndex + fileMetadataBatchSize, fileIds.size());
                List<Long> subList = fileIds.subList(listIndex, endIndex);
                GetFileMetadataResult resp = sendGetFileMetadataReq(subList);
                // TODO: we could remember if it's unsupported and stop sending calls; although, it might
                //       be a bad idea for HS2+standalone metastore that could be updated with support.
                //       Maybe we should just remember this for some time.
                if (!resp.isIsSupported()) {
                    return null;
                }
                listIndex = endIndex;
                return resp.getMetadata();
            }
        };
    }

    private GetFileMetadataResult sendGetFileMetadataReq(List<Long> fileIds) throws TException {
        return client.get_file_metadata(new GetFileMetadataRequest(fileIds));
    }

    @Override
    public Iterable<Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(
            final List<Long> fileIds, final ByteBuffer sarg, final boolean doGetFooters)
            throws TException {
        return new MetastoreMapIterable<Long, MetadataPpdResult>() {
            private int listIndex = 0;

            @Override
            protected Map<Long, MetadataPpdResult> fetchNextBatch() throws TException {
                if (listIndex == fileIds.size()) {
                    return null;
                }
                int endIndex = Math.min(listIndex + fileMetadataBatchSize, fileIds.size());
                List<Long> subList = fileIds.subList(listIndex, endIndex);
                GetFileMetadataByExprResult resp = sendGetFileMetadataBySargReq(
                        sarg, subList, doGetFooters);
                if (!resp.isIsSupported()) {
                    return null;
                }
                listIndex = endIndex;
                return resp.getMetadata();
            }
        };
    }

    private GetFileMetadataByExprResult sendGetFileMetadataBySargReq(
            ByteBuffer sarg, List<Long> fileIds, boolean doGetFooters) throws TException {
        GetFileMetadataByExprRequest req = new GetFileMetadataByExprRequest(fileIds, sarg);
        req.setDoGetFooters(doGetFooters); // No need to get footers
        return client.get_file_metadata_by_expr(req);
    }

    public abstract static class MetastoreMapIterable<K, V>
            implements Iterable<Entry<K, V>>, Iterator<Entry<K, V>> {
        private Iterator<Entry<K, V>> currentIter;

        protected abstract Map<K, V> fetchNextBatch() throws TException;

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            ensureCurrentBatch();
            return currentIter != null;
        }

        private void ensureCurrentBatch() {
            if (currentIter != null && currentIter.hasNext()) {
                return;
            }
            currentIter = null;
            Map<K, V> currentBatch;
            do {
                try {
                    currentBatch = fetchNextBatch();
                } catch (TException ex) {
                    throw new RuntimeException(ex);
                }
                if (currentBatch == null) {
                    return; // No more data.
                }
            } while (currentBatch.isEmpty());
            currentIter = currentBatch.entrySet().iterator();
        }

        @Override
        public Entry<K, V> next() {
            ensureCurrentBatch();
            if (currentIter == null) {
                throw new NoSuchElementException();
            }
            return currentIter.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void clearFileMetadata(List<Long> fileIds) throws TException {
        ClearFileMetadataRequest req = new ClearFileMetadataRequest();
        req.setFileIds(fileIds);
        client.clear_file_metadata(req);
    }

    @Override
    public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException {
        PutFileMetadataRequest req = new PutFileMetadataRequest();
        req.setFileIds(fileIds);
        req.setMetadata(metadata);
        client.put_file_metadata(req);
    }

    @Override
    public boolean isSameConfObj(HiveConf c) {
        return conf == c;
    }

    @Override
    public boolean cacheFileMetadata(
            String dbName, String tableName, String partName, boolean allParts) throws TException {
        CacheFileMetadataRequest req = new CacheFileMetadataRequest();
        req.setDbName(dbName);
        req.setTblName(tableName);
        if (partName != null) {
            req.setPartName(partName);
        } else {
            req.setIsAllParts(allParts);
        }
        CacheFileMetadataResult result = client.cache_file_metadata(req);
        return result.isIsSupported();
    }
}
