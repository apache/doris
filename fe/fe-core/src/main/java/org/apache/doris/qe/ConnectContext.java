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

package org.apache.doris.qe;

import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TimeStampNsLiteral;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.arrowflight.protocol.FlightProtocolAdapter;
import org.apache.doris.arrowflight.protocol.FlightProtocolAdapter.PreparedQuery;
import org.apache.doris.arrowflight.results.FlightSqlChannel;
import org.apache.doris.arrowflight.results.FlightSqlEndpointsLocation;
import org.apache.doris.authentication.Principal;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.catalog.NameSpaceContext;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.mysql.DummyMysqlChannel;
import org.apache.doris.mysql.MysqlCapability;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlSslContext;
import org.apache.doris.mysql.ProxyMysqlChannel;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.protocol.MysqlProtocolAdapter;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.util.MoreFieldsThread;
import org.apache.doris.plugin.AuditEvent.AuditEventBuilder;
import org.apache.doris.qe.protocol.ProtocolAdapter;
import org.apache.doris.qe.protocol.ResultSender;
import org.apache.doris.resource.BackendSelection;
import org.apache.doris.resource.BackendSelectionManager;
import org.apache.doris.resource.BackendSelectionProfile;
import org.apache.doris.resource.Tag;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.resource.computegroup.ComputeGroupMgr;
import org.apache.doris.system.Backend;
import org.apache.doris.task.LoadTaskInfo;
import org.apache.doris.thrift.TResultSinkType;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionEntry;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xnio.StreamConnection;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;


/**
 * Owns session state, serial execution and final cleanup. ProtocolAdapter owns protocol resources;
 * ConnectPoolMgr maintains local indexes and quotas. Neither GC nor removing a thread-local binding
 * closes a session. There is no activation step or reference-counted operation API.
 *
 * <h2>Creation and use</h2>
 * Configure a new context privately before publishing it. Client protocol entries wrap initialization
 * in {@link #runCommand(ContextAction)} and call {@link #registerClientSession(ConnectPoolMgr)} after
 * authentication. Registration returns -1 on success and a nonnegative connection count on quota
 * refusal. The creator must close abandoned initialization, including failed registration or login
 * responses. Install a transport close listener, then check whether the transport had already closed.
 * MySQL COM_CHANGE_USER first calls {@link #resetConnection()} and authenticates, even for the same
 * user, then calls {@link #changeUser(UserIdentity, String)} inside its command owner. This validates
 * fresh target-user settings and the namespace before atomically committing quota ownership and
 * identity. Failure sends an error before AFTER_RESPONSE closes the session; reset resources cannot
 * be restored. Direct identity assignment is limited to initialization and same-user refreshes, so
 * unregister always releases the quota belonging to the registered identity.
 *
 * <p>MySQL's initialization and read listeners use {@link #runCommand(ContextAction, Runnable)}.
 * Its idle action only rearms nonblocking reads, after the response, business finally blocks and
 * thread-local restoration. Flight's runCommand/callCommand entry uses
 * {@link #executeCommand(long, ContextCommand)} to wait a bounded time for the same execution slot.
 * Flight DoGet uses {@link #executeResult(long, ContextCommand)} to continue an existing response;
 * it validates and consumes its ticket under that ownership. Business code needs no lifecycle calls.
 * Helpers borrow the current context; they must not recursively enter a top-level command. An
 * unregistered internal context can be used privately and closed by its creating scope without
 * activation or per-statement bookkeeping. {@link AutoCloseConnectContext} binds and closes a
 * privately owned internal context and restores the caller's binding; it must not wrap a borrowed session.
 *
 * <p>Only one initialization, request, expiry maintenance or cleanup owns execution at a time.
 * Queued requests acquire ownership when they actually start, not when submitted. A stale pool
 * lookup grants no admission.
 * Rejected or discarded initialization/request tasks remain their submitter's responsibility.
 * Asynchronous work must finish borrowing session resources before the request exits, or take
 * ownership of independent query resources. Thread-local binding alone does not grant ownership.
 *
 * <h2>Closing and handoff</h2>
 * CloseMode specifies the caller's request; SessionState records the session's current state.
 * SessionState moves monotonically:
 * <pre>
 * OPEN -- CloseMode.AFTER_RESPONSE -------&gt; DRAINING
 * OPEN / DRAINING -- CloseMode.IMMEDIATE --&gt; STOPPING
 * DRAINING -- response released ----------&gt; STOPPING
 * </pre>
 * STOPPING is terminal; it does not promise that resource release has completed. There is no
 * separate CLOSED state, cleanup phase or completion-wait API. Both close modes immediately reject
 * new commands. IMMEDIATE revokes pool registration immediately; AFTER_RESPONSE enters DRAINING
 * and keeps registration until finalization so Flight can still authenticate the token, resolve tickets
 * and run timeout checks.
 * A draining session continues to occupy its pool quota and cannot be registered again.
 *
 * <p>{@link #requestClose(CloseMode)} with AFTER_RESPONSE preserves the accepted response. MySQL
 * finishes flushing it before yielding. Flight returns its normal FlightInfo and retains local results
 * for a later DoGet; GetFlightInfo returning is not result delivery. Only result continuations may
 * enter a draining session. Their success or failure consumes the ticket, and requests that fail before
 * returning FlightInfo discard their results. Flight's backend DoGet has no completion callback to the
 * frontend: deferred query resources remain bounded by their existing query/session expiry, rather
 * than being treated as delivered when FlightInfo returns. None of these boundaries promises receipt.
 *
 * <p>Before yielding, the owner checks whether the protocol still holds response resources. With no
 * such resources and the session still DRAINING, it atomically claims STOPPING and unregisters,
 * retaining execution to close transport and release resources directly. An idle close borrows
 * execution to make the same decision. No thread retains execution between RPCs or waits for a client.
 * An immediate close that wins before this claim takes responsibility for interruption and cleanup.
 *
 * <p>{@link #cleanup()} requests IMMEDIATE closure. Its unique winner closes transport and cancels
 * execution outside the handoff lock. MySQL socket closure unblocks incomplete packet reads. Flight
 * has no per-session gRPC transport to close. Cancellation can invoke synchronous callbacks, so the
 * winner hands off cleanup only in its finally, after interruption and thread-local restoration.
 * An exiting request must not clean up just because it sees STOPPING: cancellation may still be using
 * resources. A late executor published by the current owner observes STOPPING and is cancelled too.
 *
 * <p>Interruption completion and owner exit share lifecycleLock. If an owner remains, the closer sets
 * cleanupPending; that owner consumes the flag and keeps execution to clean up on exit. Otherwise,
 * the closer takes execution and cleans up inline. This handles either ordering without a task object,
 * waiting for the request, counting references or submitting work to another executor. Cleanup
 * releases protocol state, rolls back transactions, invokes the
 * existing temporary-table/session hooks, closes the remaining StatementContext and clears statement state.
 * Each step is attempted once;
 * failures are logged and remaining steps continue. Reentrant close callbacks see STOPPING and return.
 * Cleanup binds this context for rollback helpers, then restores an unrelated caller's binding.
 * Its finally releases execution directly, without rechecking protocol resources or entering yield again.
 *
 * <h2>Concurrency boundaries</h2>
 * Session state, execution ownership, cleanupPending, registration and idle-expiry decisions
 * share lifecycleLock. Lock order is lifecycleLock then the pool's admissionLock; pool changes only
 * update local indexes and quotas. User-property lookups run outside lifecycleLock; timeout checks
 * revalidate the observed statement after the lookup. Cancellation and resource release run outside both locks. The
 * MySQL idle action is exclusively a short read-scheduling operation under the handoff lock; it must
 * neither block nor run request code inline. No old request accesses session resources after handoff.
 *
 * <p>The pool timeout checker requests closure on session idle expiry. Flight metadata may report
 * COM_SLEEP while owning execution and is not idle. A MySQL incomplete-packet reader still needs
 * wait_timeout to interrupt the socket. A draining Flight session also obeys wait_timeout while DoGet
 * owns execution; result continuations never renew that bound. The checker nonblockingly borrows
 * execution to expire local results and deferred queries, then its yield can finalize a drained session.
 * Expiry cannot close Arrow buffers concurrently with DoGet. KILL QUERY and query timeout retain
 * the session: their query-owned cancellation handles must keep a stable target and support concurrent termination,
 * rather than rereading the session's current query identity. Safe final release requires the request
 * and the close interruption to return; GC and forced release cannot replace either responsibility.
 */
public class ConnectContext {
    private static final Logger LOG = LogManager.getLogger(ConnectContext.class);

    private static final int INITIAL_PREPARED_STMT_ID = Integer.MIN_VALUE;

    public enum ConnectType {
        MYSQL("MySQL"),
        ARROW_FLIGHT_SQL("ArrowFlightSQL");

        // The name SHOW PROCESSLIST, information_schema.processlist and the audit log report the
        // protocol under (their Protocol column / field).
        private final String protocolName;

        ConnectType(String protocolName) {
            this.protocolName = protocolName;
        }

        public String protocolName() {
            return protocolName;
        }
    }

    private final ProtocolAdapter protocolAdapter;

    /** How a caller requests closure; the accepted request determines the session state. */
    public enum CloseMode {
        IMMEDIATE, AFTER_RESPONSE
    }

    /** DRAINING preserves the current response; STOPPING does not imply cleanup has finished. */
    private enum SessionState {
        OPEN, DRAINING, STOPPING
    }

    // Lock order: lifecycleLock -> ConnectPoolMgr.admissionLock. No cancellation or cleanup
    // callbacks run under these locks. Only the MySQL nonblocking read rearm runs at handoff.
    private final Object lifecycleLock = new Object();
    private volatile SessionState sessionState = SessionState.OPEN;
    // Guarded by lifecycleLock. Ownership covers the entire request, including its finally blocks.
    private Thread owner;
    // Interruption has finished; the current owner must clean up when it exits. Guarded by lifecycleLock.
    private boolean cleanupPending;
    private ConnectPoolMgr registeredPool;
    // set this id before analyze
    protected volatile long stmtId;
    protected volatile long forwardedStmtId;

    // set for http_stream
    protected volatile TUniqueId loadId;
    protected volatile long backendId;
    // range [Integer.MIN_VALUE, Integer.MAX_VALUE]
    protected int preparedStmtId = INITIAL_PREPARED_STMT_ID;
    protected volatile LoadTaskInfo streamLoadInfo;

    protected volatile TUniqueId queryId = null;
    // only be active one time. tell Coordinator to regenerate instance ids for certain query(when retry).
    protected volatile TUniqueId needRegenerateInstanceId = null;
    protected volatile AtomicInteger instanceIdGenerator = new AtomicInteger();
    protected volatile String traceId;
    protected volatile TUniqueId lastQueryId = null;
    // id for this connection
    protected volatile int connectionId;
    // Timestamp when the connection is make
    protected volatile long loginTime;
    // state
    protected volatile QueryState state;
    protected volatile long returnRows;
    // Db
    protected volatile String currentDb = "";
    protected volatile long currentDbId = -1;
    // Transaction
    protected volatile TransactionEntry txnEntry = null;
    // used for ShowSqlAction which don't allow a user account
    protected volatile boolean noAuth = false;
    // LDAP authenticated but the Doris account does not exist,
    // set the flag, and the user login Doris as Temporary user.
    protected volatile boolean isTempUser = false;
    // username@host combination for the Doris account
    // that the server used to authenticate the current client.
    // In other word, currentUserIdentity is the entry that matched in Doris auth table.
    // This account determines user's access privileges.
    protected volatile UserIdentity currentUserIdentity;
    // Authenticated external principal captured during the login flow.
    protected volatile Principal authenticatedPrincipal;
    // Roles granted during authentication and bound to the current session.
    protected volatile Set<String> authenticatedRoles = Collections.emptySet();
    // Variables belong to this session.
    protected volatile SessionVariable sessionVariable;
    // Store user variable in this connection
    private Map<String, LiteralExpr> userVars = new HashMap<>();
    // Connection attributes provided by the MySQL client during handshake.
    private Map<String, String> connectAttributes = new HashMap<>();
    // Scheduler this connection belongs to
    protected volatile ConnectScheduler connectScheduler;
    // Executor
    protected volatile StmtExecutor executor;
    // Command this connection is processing.
    protected volatile MysqlCommand command;
    // Timestamp in millisecond last command starts at
    protected volatile long startTime;
    // Timestamp with nanosecond precision when the current command starts.
    protected volatile Instant startTimeInstant = Instant.now();
    // Cache thread info for this connection.
    protected volatile ThreadInfo threadInfo;

    // Catalog: put catalog here is convenient for unit test,
    // because catalog is singleton, hard to mock
    protected Env env;
    protected String defaultCatalog = InternalCatalog.INTERNAL_CATALOG_NAME;

    // record last used database of every catalog
    private final Map<String, String> lastDBOfCatalog = Maps.newConcurrentMap();

    protected AuditEventBuilder auditEventBuilder = new AuditEventBuilder();

    protected String remoteIP;

    // This is used to statistic the current query details.
    // This property will only be set when the query starts to execute.
    // So in the query planning stage, do not use any value in this attribute.
    protected QueryDetail queryDetail = null;

    // cloud cluster name
    protected volatile String cloudCluster = null;
    // The compute group selected for the statement currently being executed. Unlike cloudCluster,
    // this value is query-scoped and remains available after a per-query SET_VAR is reverted.
    protected volatile String effectiveCloudCluster = null;

    // If set to true, the nondeterministic function will not be rewrote to constant.
    private boolean notEvalNondeterministicFunction = false;
    // The compute group tag is used to limit the node resources that the user can use for query.
    // The default is empty, that is, unlimited.
    // This property is obtained from UserProperty when the client connection is created.
    // Only when the connection is created again, the new resource tags will be retrieved from the UserProperty
    private ComputeGroup computeGroup = null;

    private String sqlHash;

    // The FE ip current connected
    private String currentConnectedFEIp = "";
    private transient String connectingFeLocalResourceGroup = "";

    private InsertResult insertResult;

    private SessionContext sessionContext = SessionContext.empty();

    private StatsErrorEstimator statsErrorEstimator;

    private List<Map<String, String>> resultAttachedInfo = Lists.newArrayList();

    private String workloadGroupName = "";
    private boolean isGroupCommit;
    private BackendSelection.SelectionHint queryBackendSelectionDecision;
    private BackendSelection.SelectionHint loadBackendSelectionDecision;
    // A replayed async load owns this hint independently of the statement lifecycle. The
    // statement-level decision is reset by setStartTime(), but the persisted load intent must
    // remain available while the load planner is being rebuilt.
    private BackendSelection.SelectionHint loadBackendSelectionHint;
    private final BackendSelectionProfile backendSelectionProfile = new BackendSelectionProfile();

    private Map<String, Set<String>> dbToTempTableNamesMap = new HashMap<>();

    // unique session id in the doris cluster
    private String sessionId;

    // internal call like `insert overwrite` need skipAuth
    // For example, `insert overwrite` only requires load permission,
    // but the internal implementation will call the logic of `AlterTable`.
    // In this case, `skipAuth` needs to be set to `true` to skip the permission check of `AlterTable`
    private boolean skipAuth = false;

    // isProxy used for forward request from other FE and used in one thread
    // it's default thread-safe
    private boolean isProxy = false;

    public void setUserQueryTimeout(int queryTimeout) {
        if (queryTimeout > 0) {
            sessionVariable.setQueryTimeoutS(queryTimeout);
        }
    }

    public void setUserInsertTimeout(int insertTimeout) {
        if (insertTimeout > 0) {
            sessionVariable.setInsertTimeoutS(insertTimeout);
        }
    }

    private StatementContext statementContext;
    // Internal flag to expose a connector's synthetic write column (the hidden row-identity column a
    // row-level DML write needs) for a SINGLE target table during analysis/planning. When set to a valid
    // table ID (>= 0), only that table's getFullSchema() injects its synthetic write column (today the
    // only consumer is iceberg's __DORIS_ICEBERG_ROWID_COL__). Scoping it to one table prevents ambiguity
    // in MERGE INTO when the source table is also a write-capable table of the same format.
    private long syntheticWriteColTargetTableId = -1;

    // new planner
    private Map<String, PreparedStatementContext> preparedStatementContextMap = Maps.newHashMap();

    public SessionContext getSessionContext() {
        return sessionContext;
    }

    public void setSessionContext(SessionContext sessionContext) {
        this.sessionContext = sessionContext == null ? SessionContext.empty() : sessionContext;
    }

    public ProtocolAdapter getProtocolAdapter() {
        return protocolAdapter;
    }

    /** How a statement's result reaches this connection's client. */
    public ResultSender getResultSender() {
        return protocolAdapter.resultSender(this);
    }

    public MysqlSslContext getMysqlSslContext() {
        return MysqlProtocolAdapter.of(this).getSslContext();
    }

    public TResultSinkType getResultSinkType() {
        return protocolAdapter.resultSinkType();
    }

    public void setOrUpdateInsertResult(long txnId, String label, String db, String tbl,
            TransactionStatus txnStatus, long loadedRows, int filteredRows) {
        if (isTxnModel() && insertResult != null) {
            insertResult.updateResult(txnStatus, loadedRows, filteredRows);
        } else {
            insertResult = new InsertResult(txnId, label, db, Util.getTempTableDisplayName(tbl),
                txnStatus, loadedRows, filteredRows);
        }
    }

    public InsertResult getInsertResult() {
        return insertResult;
    }

    public static ConnectContext get() {
        return MoreFieldsThread.getConnectContext();
    }

    public static void remove() {
        MoreFieldsThread.removeConnectContext();
    }

    public void addLastDBOfCatalog(String catalog, String db) {
        lastDBOfCatalog.put(catalog, db);
    }

    public String getLastDBOfCatalog(String catalog) {
        return lastDBOfCatalog.get(catalog);
    }

    public String removeLastDBOfCatalog(String catalog) {
        return lastDBOfCatalog.get(catalog);
    }

    // Used by COM_RESET_CONNECTION
    public void clearLastDBOfCatalog() {
        lastDBOfCatalog.clear();
    }

    public void resetConnection() throws UserException {
        closeTxnForConnectionReset();
        if (!dbToTempTableNamesMap.isEmpty()) {
            cleanupTemporaryTables(true);
            dbToTempTableNamesMap.clear();
        }
        resetSessionVariable();
        userVars = new HashMap<>();
        preparedStatementContextMap.clear();
        queryId = null;
        lastQueryId = null;
        setTraceId(null);
        insertResult = null;
        command = MysqlCommand.COM_SLEEP;
        returnRows = 0;
    }

    private void resetSessionVariable() {
        sessionVariable = VariableMgr.newSessionVariable();
        applyUserSessionVariableDefaults();
        if (Config.use_fuzzy_session_variable) {
            sessionVariable.initFuzzyModeVariables();
        }
    }

    private void applyUserSessionVariableDefaults() {
        String qualifiedUser = getQualifiedUser();
        if (Strings.isNullOrEmpty(qualifiedUser)) {
            return;
        }
        Env currentEnv = env == null ? Env.getCurrentEnv() : env;
        Auth auth = currentEnv == null ? null : currentEnv.getAuth();
        if (auth == null) {
            return;
        }
        setUserQueryTimeout(auth.getQueryTimeout(qualifiedUser));
        setUserInsertTimeout(auth.getInsertTimeout(qualifiedUser));
    }

    public void setNotEvalNondeterministicFunction(boolean notEvalNondeterministicFunction) {
        this.notEvalNondeterministicFunction = notEvalNondeterministicFunction;
    }

    public boolean notEvalNondeterministicFunction() {
        return notEvalNondeterministicFunction;
    }

    public ConnectType getConnectType() {
        return protocolAdapter.type();
    }

    private void init() {
        try {
            state = new QueryState();
            returnRows = 0;
            resetSessionVariable();
            userVars = new HashMap<>();
            command = MysqlCommand.COM_SLEEP;

            sessionId = UUID.randomUUID().toString();
            if (!FeConstants.runningUnitTest) {
                Env.getCurrentEnv().registerSessionInfo(sessionId);
            }
        } catch (Throwable failure) {
            cleanup();
            throw failure;
        }
    }

    /** An internal context: it speaks MySQL to a channel that discards everything. */
    public ConnectContext() {
        this(null, false);
    }

    public ConnectContext(StreamConnection connection) {
        this(connection, false);
    }

    public ConnectContext(StreamConnection connection, boolean isProxy, String sessionId) {
        this(connection, isProxy);
        // used for binding new created temporary table with its original session
        this.sessionId = sessionId;
    }

    public ConnectContext(StreamConnection connection, boolean isProxy) {
        if (connection != null) {
            protocolAdapter = new MysqlProtocolAdapter(new MysqlChannel(connection, this));
        } else if (isProxy) {
            protocolAdapter = new MysqlProtocolAdapter(new ProxyMysqlChannel());
            this.isProxy = isProxy;
        } else {
            protocolAdapter = new MysqlProtocolAdapter(new DummyMysqlChannel());
        }
        init();
    }

    public ConnectContext(ProtocolAdapter protocolAdapter) {
        this.protocolAdapter = protocolAdapter;
        init();
    }

    /** The context of a MySQL client connection. */
    public static ConnectContext forMysql(StreamConnection connection) {
        return new ConnectContext(connection);
    }

    /**
     * The context the master builds to run a statement forwarded by another frontend. Its
     * channel collects the MySQL packets of the result so they can be handed back to the
     * frontend the client is connected to.
     */
    public static ConnectContext forMysqlProxy(String sessionId) {
        return new ConnectContext(null, true, sessionId);
    }

    /** The context of an Arrow Flight SQL session, identified by its bearer token. */
    public static ConnectContext forFlight(String peerIdentity) {
        return new ConnectContext(new FlightProtocolAdapter(peerIdentity));
    }

    public Map<String, String> getConnectAttributes() {
        return connectAttributes;
    }

    public void setConnectAttributes(Map<String, String> connectAttributes) {
        if (connectAttributes == null || connectAttributes.isEmpty()) {
            this.connectAttributes = new HashMap<>();
            return;
        }
        this.connectAttributes = new HashMap<>(connectAttributes);
    }

    public boolean isCursorFetchRequested() {
        return MysqlProtocolAdapter.of(this).isCursorFetchRequested();
    }

    public void setCursorFetchRequested(boolean cursorFetchRequested) {
        MysqlProtocolAdapter.of(this).setCursorFetchRequested(cursorFetchRequested);
    }

    public ByteBuffer getPrepareExecuteBuffer() {
        return MysqlProtocolAdapter.of(this).getPrepareExecuteBuffer();
    }

    public void setPrepareExecuteBuffer(ByteBuffer prepareExecuteBuffer) {
        MysqlProtocolAdapter.of(this).setPrepareExecuteBuffer(prepareExecuteBuffer);
    }

    public boolean isTxnModel() {
        return txnEntry != null && txnEntry.isTxnModel();
    }

    public boolean isInsertValuesTxnIniting() {
        return txnEntry != null && txnEntry.isInsertValuesTxnIniting();
    }

    public void addPreparedStatementContext(String stmtName, PreparedStatementContext ctx) throws UserException {
        if (!sessionVariable.enableServeSidePreparedStatement) {
            throw new UserException("Failed to do prepared command, server side prepared statement is disabled");
        }
        if (this.preparedStatementContextMap.size() > sessionVariable.maxPreparedStmtCount) {
            throw new UserException("Failed to create a server prepared statement"
                    + " possibly because there are too many active prepared statements on server already."
                    + "set max_prepared_stmt_count with larger number than " + sessionVariable.maxPreparedStmtCount);
        }
        this.preparedStatementContextMap.put(stmtName, ctx);
        incPreparedStmtId();
    }

    public void removePrepareStmt(String stmtName) {
        this.preparedStatementContextMap.remove(stmtName);
    }

    public PreparedStatementContext getPreparedStementContext(String stmtName) {
        return this.preparedStatementContextMap.get(stmtName);
    }

    public void closeTxn() {
        if (isTxnModel()) {
            try {
                txnEntry.abortTransaction();
            } catch (Exception e) {
                LOG.error("db: {}, txnId: {}, rollback error.", currentDb,
                        txnEntry.getTransactionId(), e);
            }
            txnEntry = null;
        }
    }

    private void closeTxnForConnectionReset() throws DdlException {
        if (isTxnModel()) {
            try {
                txnEntry.abortTransaction();
            } catch (Exception e) {
                throw new DdlException(String.format("rollback transaction failed, db: %s, txnId: %s",
                        currentDb, txnEntry.getTransactionId()), e);
            }
            txnEntry = null;
        }
    }

    public long getStmtId() {
        return stmtId;
    }

    public long getPreparedStmtId() {
        return preparedStmtId;
    }

    public void incPreparedStmtId() {
        ++preparedStmtId;
    }

    public long getBackendId() {
        return backendId;
    }

    public void setBackendId(long backendId) {
        this.backendId = backendId;
    }

    public TUniqueId getLoadId() {
        return loadId;
    }

    public void setLoadId(TUniqueId loadId) {
        this.loadId = loadId;
    }

    public void setStreamLoadInfo(LoadTaskInfo streamLoadInfo) {
        this.streamLoadInfo = streamLoadInfo;
    }

    public LoadTaskInfo getStreamLoadInfo() {
        return streamLoadInfo;
    }

    public void setStmtId(long stmtId) {
        this.stmtId = stmtId;
    }

    public long getForwardedStmtId() {
        return forwardedStmtId;
    }

    public void setForwardedStmtId(long forwardedStmtId) {
        this.forwardedStmtId = forwardedStmtId;
    }

    public String getRemoteIP() {
        return remoteIP;
    }

    public void setRemoteIP(String remoteIP) {
        this.remoteIP = remoteIP;
    }

    public void setQueryDetail(QueryDetail queryDetail) {
        this.queryDetail = queryDetail;
    }

    public QueryDetail getQueryDetail() {
        return queryDetail;
    }

    public AuditEventBuilder getAuditEventBuilder() {
        return auditEventBuilder;
    }

    public void setThreadLocalInfo() {
        MoreFieldsThread.setConnectContext(this);
    }

    public long getCurrentDbId() {
        return currentDbId;
    }

    public TransactionEntry getTxnEntry() {
        return txnEntry;
    }

    public void setTxnEntry(TransactionEntry txnEntry) {
        this.txnEntry = txnEntry;
    }

    public void setEnv(Env env) {
        this.env = env;
        defaultCatalog = env.getInternalCatalog().getName();
    }

    public void setUserVar(String name, LiteralExpr value) {
        userVars.put(name.toLowerCase(), value);
    }

    public @Nullable Literal getLiteralForUserVar(String varName) {
        varName = varName.toLowerCase();
        if (userVars.containsKey(varName)) {
            LiteralExpr literalExpr = userVars.get(varName);
            if (literalExpr instanceof BoolLiteral) {
                return Literal.of(((BoolLiteral) literalExpr).getValue());
            } else if (literalExpr instanceof IntLiteral || literalExpr instanceof LargeIntLiteral) {
                // the value in the IntLiteral should be int, but now is long in old planner literalExpr
                // so type coercion to generate right new planner int Literal
                switch (literalExpr.getType().getPrimitiveType()) {
                    case LARGEINT:
                        return Literal.of(new BigInteger(literalExpr.getStringValue()));
                    case BIGINT:
                        return Literal.of(((IntLiteral) literalExpr).getValue());
                    case INT:
                        return Literal.of((int) ((IntLiteral) literalExpr).getValue());
                    case SMALLINT:
                        return Literal.of((short) ((IntLiteral) literalExpr).getValue());
                    case TINYINT:
                        return Literal.of((byte) ((IntLiteral) literalExpr).getValue());
                    default:
                        return Literal.of((int) ((IntLiteral) literalExpr).getValue());
                }
            } else if (literalExpr instanceof FloatLiteral) {
                return Literal.of(((FloatLiteral) literalExpr).getValue());
            } else if (literalExpr instanceof DecimalLiteral) {
                return Literal.of(((DecimalLiteral) literalExpr).getValue());
            } else if (literalExpr instanceof DateLiteral) {
                return Literal.fromLegacyLiteral(literalExpr, literalExpr.getType());
            } else if (literalExpr instanceof StringLiteral) {
                return Literal.of(((StringLiteral) literalExpr).getValue());
            } else if (literalExpr instanceof NullLiteral) {
                return Literal.of(null);
            } else if (literalExpr instanceof TimeStampNsLiteral) {
                return Literal.fromLegacyLiteral(literalExpr, literalExpr.getType());
            } else {
                return Literal.of(literalExpr.getStringValue());
            }
        } else {
            // If there are no such user defined var, just return the NULL value.
            return Literal.of(null);
        }
    }

    public Env getEnv() {
        return env;
    }

    public boolean getNoAuth() {
        return noAuth;
    }

    public void setNoAuth(boolean noAuth) {
        this.noAuth = noAuth;
    }

    public String getQualifiedUser() {
        return currentUserIdentity == null ? null : currentUserIdentity.getQualifiedUser();
    }

    public boolean getIsTempUser() {
        return isTempUser;
    }

    public void setIsTempUser(boolean isTempUser) {
        this.isTempUser = isTempUser;
    }

    public UserIdentity getCurrentUserIdentity() {
        return currentUserIdentity;
    }

    public Principal getAuthenticatedPrincipal() {
        return authenticatedPrincipal;
    }

    public void setAuthenticatedPrincipal(Principal authenticatedPrincipal) {
        this.authenticatedPrincipal = authenticatedPrincipal;
    }

    public Set<String> getAuthenticatedRoles() {
        return authenticatedRoles;
    }

    public void setAuthenticatedRoles(Set<String> authenticatedRoles) {
        if (authenticatedRoles.isEmpty()) {
            this.authenticatedRoles = Collections.emptySet();
            return;
        }
        this.authenticatedRoles = Collections.unmodifiableSet(new HashSet<>(authenticatedRoles));
    }

    // used for select user(), select session_user();
    // return string similar with user@127.0.0.1
    public String getUserWithLoginRemoteIpString() {
        return UserIdentity.createAnalyzedUserIdentWithIp(getQualifiedUser(), remoteIP).toString();
    }

    public void setCurrentUserIdentity(UserIdentity currentUserIdentity) {
        synchronized (lifecycleLock) {
            // A registered connection's identity is also its quota owner. Only changeUser may
            // move that ownership; private contexts and same-user refreshes need no migration.
            Preconditions.checkState(registeredPool == null
                            || getQualifiedUser().equals(currentUserIdentity.getQualifiedUser()),
                    "Use changeUser to change a registered connection's user");
            this.currentUserIdentity = currentUserIdentity;
        }
    }

    /**
     * Commits COM_CHANGE_USER after unconditional session reset and authentication. The command
     * owner validates fresh user settings and the requested namespace using a private context, so
     * failure preserves the old identity and quota until the protocol sends an error and closes.
     * Catalog/Auth calls run outside the lifecycle lock. The final quota transfer and identity
     * update share the registration/close lock order; a close that wins prevents the transfer.
     */
    public Optional<Pair<ErrorCode, String>> changeUser(UserIdentity identity, String database) {
        synchronized (lifecycleLock) {
            Preconditions.checkState(owner == Thread.currentThread(), "Change user requires command ownership");
            if (sessionState != SessionState.OPEN) {
                throw new SessionClosedException();
            }
            Preconditions.checkState(registeredPool != null, "Change user requires a registered connection");
        }

        ConnectContext candidate = new ConnectContext();
        try {
            candidate.setEnv(getEnv());
            candidate.setCurrentUserIdentity(identity);
            candidate.setRemoteIP(remoteIP);
            candidate.setConnectAttributes(connectAttributes);
            // User properties and cloud database privileges consult ConnectContext.get().
            ConnectContext previous = ConnectContext.get();
            long userLimit;
            try {
                candidate.setThreadLocalInfo();
                candidate.resetSessionVariable();
                candidate.setComputeGroup(getEnv().getAuth().getComputeGroup(identity.getQualifiedUser()));
                Optional<Pair<ErrorCode, String>> catalogError = ConnectContextUtil.initDefaultCatalog(candidate);
                if (catalogError.isPresent()) {
                    return catalogError;
                }
                if (!Strings.isNullOrEmpty(database)) {
                    Optional<Pair<ErrorCode, String>> error = ConnectContextUtil.initCatalogAndDb(candidate, database);
                    if (error.isPresent()) {
                        return error;
                    }
                }
                // External-user defaults also consult the authenticated thread-local context.
                userLimit = getEnv().getAuth().getMaxConn(identity.getQualifiedUser());
            } finally {
                restoreThreadContext(previous);
            }

            synchronized (lifecycleLock) {
                if (sessionState != SessionState.OPEN) {
                    throw new SessionClosedException();
                }
                if (!registeredPool.changeUser(this, identity.getQualifiedUser(), userLimit)) {
                    return Optional.of(Pair.of(ErrorCode.ERR_TOO_MANY_USER_CONNECTIONS,
                            "Reach limit of connections for user '" + identity.getQualifiedUser()
                                    + "'. User limit: " + userLimit));
                }
                currentUserIdentity = identity;
                // These assignments cannot perform catalog IO after the quota has moved.
                sessionVariable = candidate.sessionVariable;
                sessionContext = candidate.sessionContext;
                authenticatedPrincipal = candidate.authenticatedPrincipal;
                authenticatedRoles = candidate.authenticatedRoles;
                isTempUser = candidate.isTempUser;
                computeGroup = candidate.computeGroup;
                cloudCluster = candidate.cloudCluster;
                effectiveCloudCluster = candidate.effectiveCloudCluster;
                restoreNameSpaceContext(candidate.getNameSpaceContext());
                lastDBOfCatalog.clear();
                lastDBOfCatalog.putAll(candidate.lastDBOfCatalog);
            }
            return Optional.empty();
        } finally {
            candidate.cleanup();
        }
    }

    public SessionVariable getSessionVariable() {
        return sessionVariable;
    }

    public void setSessionVariable(SessionVariable sessionVariable) {
        this.sessionVariable = sessionVariable;
    }

    public ConnectScheduler getConnectScheduler() {
        return connectScheduler;
    }

    public void setConnectScheduler(ConnectScheduler connectScheduler) {
        this.connectScheduler = connectScheduler;
    }

    public MysqlCommand getCommand() {
        return command;
    }

    public void setCommand(MysqlCommand command) {
        this.command = command;
    }

    public long getStartTime() {
        return startTime;
    }

    public Instant getStartTimeInstant() {
        return startTimeInstant;
    }

    public void setStartTime() {
        startTimeInstant = Instant.now();
        startTime = startTimeInstant.toEpochMilli();
        returnRows = 0;
        queryBackendSelectionDecision = null;
        loadBackendSelectionDecision = null;
        backendSelectionProfile.reset();
    }

    /**
     * The client was active just now: wait_timeout counts from here. Unlike {@link #setStartTime},
     * which starts a statement, this leaves what the last statement recorded (its rows, its backend
     * selection) as it is, for a command that runs no statement of its own -- an Arrow Flight SQL
     * session option or metadata request.
     */
    public void refreshStartTime() {
        startTimeInstant = Instant.now();
        startTime = startTimeInstant.toEpochMilli();
    }

    public BackendSelection.SelectionHint getQueryBackendSelectionDecision() {
        if (queryBackendSelectionDecision == null) {
            queryBackendSelectionDecision = BackendSelectionManager.getQuerySelectionHint(this);
        }
        return queryBackendSelectionDecision;
    }

    // Audit runs for every statement type, so it must not create a query selection hint.
    public BackendSelection.SelectionHint getQueryBackendSelectionDecisionForAudit() {
        return queryBackendSelectionDecision == null
                ? BackendSelection.SelectionHint.noSelection()
                : queryBackendSelectionDecision;
    }

    // Load hints are resolved at several scheduling sites (sink, coordinator, group commit);
    // each records the statement-level hint here so the audit reflects the load decision
    // instead of the scan-side query decision.
    public void recordLoadBackendSelectionDecision(BackendSelection.SelectionHint hint) {
        loadBackendSelectionDecision = hint;
    }

    public void recordLoadBackendSelectionHint(BackendSelection.SelectionHint hint) {
        loadBackendSelectionHint = hint;
    }

    public BackendSelection.SelectionHint getLoadBackendSelectionHint() {
        return loadBackendSelectionHint;
    }

    public BackendSelection.SelectionHint getLoadBackendSelectionDecision() {
        return loadBackendSelectionDecision;
    }

    public BackendSelection.SelectionHint getLoadBackendSelectionDecisionForAudit() {
        return loadBackendSelectionDecision != null
                ? loadBackendSelectionDecision : loadBackendSelectionHint;
    }

    public BackendSelectionProfile getBackendSelectionProfile() {
        return backendSelectionProfile;
    }

    public void updateReturnRows(int returnRows) {
        this.returnRows += returnRows;
    }

    public long getReturnRows() {
        return returnRows;
    }

    public void resetReturnRows() {
        returnRows = 0;
    }

    public int getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(int connectionId) {
        this.connectionId = connectionId;
    }

    public void resetLoginTime() {
        this.loginTime = System.currentTimeMillis();
    }

    // Arrow Flight SQL only.
    public void addPreparedQuery(String preparedStatementId, PreparedQuery preparedQuery) {
        FlightProtocolAdapter.of(this).addPreparedQuery(preparedStatementId, preparedQuery);
    }

    public PreparedQuery getPreparedQuery(String preparedStatementId) {
        return FlightProtocolAdapter.of(this).getPreparedQuery(preparedStatementId);
    }

    public void removePreparedQuery(String preparedStatementId) {
        FlightProtocolAdapter.of(this).removePreparedQuery(preparedStatementId);
    }

    public void setRunningQuery(String runningQuery) {
        FlightProtocolAdapter.of(this).setRunningQuery(runningQuery);
    }

    public String getRunningQuery() {
        return FlightProtocolAdapter.of(this).getRunningQuery();
    }

    public void addFlightSqlEndpointsLocation(FlightSqlEndpointsLocation flightSqlEndpointsLocation) {
        FlightProtocolAdapter.of(this).addEndpointsLocation(flightSqlEndpointsLocation);
    }

    public List<FlightSqlEndpointsLocation> getFlightSqlEndpointsLocations() {
        return FlightProtocolAdapter.of(this).getEndpointsLocations();
    }

    /**
     * Whether the result of the statement being executed comes from this frontend, or is left
     * on the backends for the client to pull; see {@link ProtocolAdapter#returnsResultFromLocal}.
     */
    public boolean isReturnResultFromLocal() {
        return protocolAdapter.returnsResultFromLocal(this);
    }

    // The bearer token of an Arrow Flight SQL session, null for any other connection.
    public String getPeerIdentity() {
        return protocolAdapter instanceof FlightProtocolAdapter
                ? ((FlightProtocolAdapter) protocolAdapter).getPeerIdentity() : null;
    }

    public FlightSqlChannel getFlightSqlChannel() {
        return FlightProtocolAdapter.of(this).getChannel();
    }

    public MysqlChannel getMysqlChannel() {
        return MysqlProtocolAdapter.of(this).getChannel();
    }

    public String getClientIP() {
        return getRemoteHostPortString();
    }

    public QueryState getState() {
        return state;
    }

    public void setState(QueryState state) {
        this.state = state;
    }

    public MysqlCapability getCapability() {
        return MysqlProtocolAdapter.of(this).getCapability();
    }

    public void setCapability(MysqlCapability capability) {
        MysqlProtocolAdapter.of(this).setCapability(capability);
    }

    public MysqlCapability getServerCapability() {
        return MysqlProtocolAdapter.of(this).getServerCapability();
    }

    public String getDefaultCatalog() {
        return defaultCatalog;
    }

    public CatalogIf getCurrentCatalog() {
        // defaultCatalog is switched by SwitchStmt, so we don't need to check to exist of catalog.
        return getCatalog(defaultCatalog);
    }

    public String getSessionId() {
        return sessionId;
    }

    /**
     * Maybe return when catalogName is not exist. So need to check nullable.
     */
    public CatalogIf getCatalog(String catalogName) {
        String realCatalogName = catalogName == null ? defaultCatalog : catalogName;
        if (env == null) {
            return Env.getCurrentEnv().getCatalogMgr().getCatalog(realCatalogName);
        }
        return env.getCatalogMgr().getCatalog(realCatalogName);
    }

    public FunctionRegistry getFunctionRegistry() {
        if (env == null) {
            return Env.getCurrentEnv().getFunctionRegistry();
        }
        return env.getFunctionRegistry();
    }

    public void changeDefaultCatalog(String catalogName) {
        defaultCatalog = catalogName;
        clearDatabase();
    }

    /**
     * No current database: the state a session starts in and switching catalogs puts it back into.
     * No statement leads back to it (there is no USE of nothing), so a client that means to be in
     * none again, e.g. through the Arrow Flight SQL {@code schema} session option, comes here.
     */
    public void clearDatabase() {
        currentDb = "";
        currentDbId = -1;
    }

    public String getDatabase() {
        return currentDb;
    }

    public NameSpaceContext getNameSpaceContext() {
        return new NameSpaceContext(defaultCatalog, currentDb, currentDbId);
    }

    public void restoreNameSpaceContext(NameSpaceContext namespace) {
        // Restoring a captured namespace must not contact a catalog that may now be unavailable.
        defaultCatalog = namespace.getDefaultCatalog();
        currentDb = namespace.getCurrentDb();
        currentDbId = namespace.getCurrentDbId();
    }

    public void setDatabase(String db) {
        Optional<DatabaseIf> dbInstance = getCurrentCatalog().getDb(db);
        long dbId = dbInstance.map(DatabaseIf::getId).orElse(-1L);
        // A failed lookup must leave the previously selected database name and ID together.
        currentDb = db;
        currentDbId = dbId;
    }

    void restoreDatabaseName(String db) {
        // A forwarded session can retain its namespace even when the metastore cannot resolve the database ID.
        currentDb = db;
        currentDbId = -1;
    }

    public void setExecutor(StmtExecutor executor) {
        boolean cancelOnPublication;
        synchronized (lifecycleLock) {
            Preconditions.checkState(sessionState != SessionState.STOPPING || owner == Thread.currentThread(),
                    "Context is closed");
            this.executor = executor;
            cancelOnPublication = sessionState == SessionState.STOPPING;
        }
        // An immediate close may win before the command publishes its executor. StmtExecutor
        // remembers cancellation before coordinator publication. A deferred close lets the
        // admitted command finish its response without cancelling its remaining work.
        if (cancelOnPublication && executor != null) {
            executor.cancel(new Status(TStatusCode.CANCELLED, "session closed"), false);
        }
    }

    public StmtExecutor getExecutor() {
        return executor;
    }

    public void clear() {
        executor = null;
        statementContext = null;
        loadBackendSelectionDecision = null;
        loadBackendSelectionHint = null;
        protocolAdapter.afterStatement(this);
    }

    // Arrow Flight SQL only: the executors of queries whose results the client pulls from the
    // backend later, see FlightProtocolAdapter. A connection of any other protocol has none.
    public void addFlightSqlDeferredExecutor(StmtExecutor executor) {
        FlightProtocolAdapter.of(this).addDeferredExecutor(executor);
    }

    public void closeFlightSqlDeferredExecutors() {
        if (protocolAdapter instanceof FlightProtocolAdapter) {
            ((FlightProtocolAdapter) protocolAdapter).closeDeferredExecutors();
        }
    }

    // A request that failed after deferring its query: the query is cancelled on the backends, then
    // its executor finalized (see FlightProtocolAdapter.cancelDeferredExecutors).
    public void cancelFlightSqlDeferredExecutors(Status cancelReason) {
        if (protocolAdapter instanceof FlightProtocolAdapter) {
            ((FlightProtocolAdapter) protocolAdapter).cancelDeferredExecutors(cancelReason);
        }
    }

    /**
     * Releases what the protocol still holds for this session, after its operations exit:
     * for Arrow Flight SQL the channel-cached results and the deferred executors, after which
     * the session keeps none and runs no command
     * (see {@code FlightProtocolAdapter.releaseSession}); nothing for a MySQL connection.
     */
    public void releaseProtocolSession() {
        protocolAdapter.releaseSession(this);
    }

    /** Registers a configured client session; returns -1 on success, or the refused quota count. */
    public int registerClientSession(ConnectPoolMgr pool) {
        // Auth properties can wait on metadata locks or an external directory. Closure must remain
        // able to interrupt initialization while this lookup is outstanding.
        long userLimit = getEnv().getAuth().getMaxConn(getQualifiedUser());
        synchronized (lifecycleLock) {
            if (sessionState != SessionState.OPEN) {
                throw new SessionClosedException();
            }
            Preconditions.checkState(registeredPool == null, "Session is already registered");
            int result = pool.registerConnection(this, userLimit);
            if (result == -1) {
                registeredPool = pool;
                refreshStartTime();
            }
            return result;
        }
    }

    @FunctionalInterface
    public interface ContextAction<E extends Exception> {
        void run() throws E;
    }

    @FunctionalInterface
    public interface ContextCommand<T, E extends Exception> {
        T call() throws E;
    }

    public static class SessionClosedException extends IllegalStateException {
        private SessionClosedException() {
            super("Session is closing");
        }
    }

    public static class SessionBusyException extends IllegalStateException {
        private SessionBusyException() {
            super("Timed out waiting for the session's current command");
        }
    }

    public static class SessionWaitInterruptedException extends IllegalStateException {
        private SessionWaitInterruptedException(InterruptedException cause) {
            super("Interrupted while waiting for the session's current command", cause);
        }
    }

    /** Runs a non-waiting protocol entry; false means closure rejected it before it began. */
    public <E extends Exception> boolean runCommand(ContextAction<E> action) throws E {
        return runCommand(action, null);
    }

    /** The idle action may only schedule MySQL reads; it must not block or execute request code. */
    public <E extends Exception> boolean runCommand(ContextAction<E> action, Runnable onIdle) throws E {
        try {
            acquireExecution(0, false);
        } catch (SessionClosedException closed) {
            return false;
        }
        executeOwned(() -> {
            action.run();
            return null;
        }, onIdle);
        return true;
    }

    /** Runs a protocol request with bounded admission waiting, preserving the body's own exceptions. */
    public <T, E extends Exception> T executeCommand(long waitMillis, ContextCommand<T, E> command) throws E {
        acquireExecution(waitMillis, false);
        return executeOwned(command, null);
    }

    /**
     * Continues delivery of an existing result, including while DRAINING. The protocol must
     * validate the ticket under this ownership before consuming it. Ordinary commands use executeCommand.
     */
    public <T, E extends Exception> T executeResult(long waitMillis, ContextCommand<T, E> command) throws E {
        acquireExecution(waitMillis, true);
        return executeOwned(command, null);
    }

    private boolean acceptsExecution(boolean resultContinuation) {
        return sessionState == SessionState.OPEN
                || (resultContinuation && sessionState == SessionState.DRAINING);
    }

    private void acquireExecution(long waitMillis, boolean resultContinuation) {
        synchronized (lifecycleLock) {
            Preconditions.checkState(owner != Thread.currentThread(), "Nested session command");
            long remaining = TimeUnit.MILLISECONDS.toNanos(waitMillis);
            long started = System.nanoTime();
            while (acceptsExecution(resultContinuation) && owner != null) {
                if (remaining <= 0) {
                    throw new SessionBusyException();
                }
                try {
                    TimeUnit.NANOSECONDS.timedWait(lifecycleLock, remaining);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new SessionWaitInterruptedException(interrupted);
                }
                remaining = TimeUnit.MILLISECONDS.toNanos(waitMillis) - (System.nanoTime() - started);
            }
            if (!acceptsExecution(resultContinuation)) {
                throw new SessionClosedException();
            }
            owner = Thread.currentThread();
            // Draining tickets cannot extend the session's existing expiry bound.
            if (sessionState == SessionState.OPEN) {
                refreshStartTime();
            }
        }
    }

    private <T, E extends Exception> T executeOwned(ContextCommand<T, E> command, Runnable onIdle) throws E {
        ConnectContext previous = ConnectContext.get();
        try {
            setThreadLocalInfo();
            return command.call();
        } finally {
            try {
                restoreThreadContext(previous);
            } finally {
                yieldExecution(onIdle);
            }
        }
    }

    private void restoreThreadContext(ConnectContext previous) {
        if (previous == null) {
            ConnectContext.remove();
        } else {
            previous.setThreadLocalInfo();
        }
    }

    // The exiting owner either returns execution or keeps it to clean up. An immediate close
    // cannot hand off cleanup until its synchronous interruption has finished.
    private void yieldExecution(Runnable onIdle) {
        boolean cleanup;
        boolean closeTransport;
        Throwable rearmFailure = null;
        StmtExecutor toCancel = null;
        // Still holding execution: no request or expiry maintenance can change the response here.
        boolean hasPendingResponse = protocolAdapter.hasPendingResponse();
        synchronized (lifecycleLock) {
            Preconditions.checkState(owner == Thread.currentThread(), "Execution belongs to another thread");
            closeTransport = sessionState == SessionState.DRAINING && !hasPendingResponse;
            if (closeTransport) {
                sessionState = SessionState.STOPPING;
                unregisterClientSessionLocked();
            }
            cleanup = closeTransport || cleanupPending;
            cleanupPending = false;
            if (!cleanup) {
                owner = null;
                if (sessionState == SessionState.OPEN && onIdle != null) {
                    try {
                        onIdle.run();
                    } catch (RuntimeException | Error failure) {
                        rearmFailure = failure;
                        requestCloseLocked(CloseMode.IMMEDIATE);
                        toCancel = executor;
                    }
                }
            }
            lifecycleLock.notifyAll();
        }
        if (rearmFailure != null) {
            interruptAndHandOffCleanup(toCancel);
            if (rearmFailure instanceof Error) {
                throw (Error) rearmFailure;
            }
            throw (RuntimeException) rearmFailure;
        }
        if (cleanup) {
            finishCleanup(closeTransport);
        }
    }

    // Expiry and an idle graceful close borrow the same owner without renewing the session. They
    // never wait for or recurse into a request; its exit will reconsider graceful finalization.
    private void runIdleMaintenance(Runnable action) {
        synchronized (lifecycleLock) {
            if (owner != null || sessionState == SessionState.STOPPING) {
                return;
            }
            owner = Thread.currentThread();
        }
        executeOwned(() -> {
            action.run();
            return null;
        }, null);
    }

    // A snapshot; empty for a connection of any other protocol.
    @VisibleForTesting
    public List<StmtExecutor> getFlightSqlDeferredExecutors() {
        return protocolAdapter instanceof FlightProtocolAdapter
                ? ((FlightProtocolAdapter) protocolAdapter).getDeferredExecutors() : Collections.emptyList();
    }

    // Called under execution ownership by timeout maintenance, before session expiry:
    // finalizes each deferred query whose own bound has passed since it started (see
    // FlightProtocolAdapter.takeExpiredDeferredExecutors), not the ones whose bound has not, and
    // not the connection. The bound counts from the query's start, not from startTime: the
    // session's later commands (a session option, a metadata request) move startTime on without
    // finishing that query, and must not keep its coordinator alive either.
    private void reapIdleFlightSqlDeferredExecutors(long now) {
        if (!(protocolAdapter instanceof FlightProtocolAdapter)) {
            return;
        }
        List<StmtExecutor> expired = ((FlightProtocolAdapter) protocolAdapter).takeExpiredDeferredExecutors(now);
        for (StmtExecutor deferredExecutor : expired) {
            LOG.warn("release deferred arrow flight query {} of idle connection, connectionId: {}, remote: {}, "
                            + "deferred for: {}ms, bound: {}ms",
                    DebugUtil.printId(deferredExecutor.getDeferredQueryId()),
                    connectionId, getRemoteHostPortString(), now - deferredExecutor.getDeferredStartTimeMs(),
                    FlightProtocolAdapter.deferredBoundMs(deferredExecutor,
                            Config.arrow_flight_deferred_query_idle_timeout_second));
        }
        FlightProtocolAdapter.finalizeDeferredExecutors(expired);
    }

    /**
     * This method is idempotent.
     */
    protected void closeChannel() {
        protocolAdapter.closeConnection(this);
    }

    /**
     * kill connection by other thread
     */
    protected void killConnection() {
        cleanup();
    }

    /** Requests immediate closure. A running request retains execution until its entire exit completes. */
    public void cleanup() {
        requestClose(CloseMode.IMMEDIATE);
    }

    /** Rejects new commands now; AFTER_RESPONSE still admits continuations of existing results. */
    public void requestClose(CloseMode mode) {
        Preconditions.checkNotNull(mode, "close mode");
        boolean interrupt;
        StmtExecutor toCancel;
        synchronized (lifecycleLock) {
            interrupt = requestCloseLocked(mode);
            toCancel = executor;
        }
        if (interrupt) {
            interruptAndHandOffCleanup(toCancel);
        } else {
            // With no command to yield execution, make the same response-readiness decision now.
            runIdleMaintenance(() -> { });
        }
    }

    // The return value identifies the unique immediate interruption owner. Graceful cleanup becomes
    // ready when the execution owner has no response resources left; immediate cleanup becomes ready
    // only after all synchronous interruption returns.
    private boolean requestCloseLocked(CloseMode mode) {
        if (sessionState == SessionState.STOPPING
                || (sessionState == SessionState.DRAINING && mode == CloseMode.AFTER_RESPONSE)) {
            return false;
        }
        boolean interrupt = mode == CloseMode.IMMEDIATE;
        sessionState = interrupt ? SessionState.STOPPING : SessionState.DRAINING;
        if (interrupt) {
            unregisterClientSessionLocked();
        }
        lifecycleLock.notifyAll();
        return interrupt;
    }

    private void unregisterClientSessionLocked() {
        ConnectPoolMgr pool = registeredPool;
        registeredPool = null;
        if (pool != null) {
            cleanupStep("unregister connection", () -> pool.unregisterConnection(this));
        }
    }

    // Called only by the unique interruption winner, after cancellation and TLS restoration.
    private void handOffCleanup() {
        synchronized (lifecycleLock) {
            Preconditions.checkState(!cleanupPending, "Cleanup was already handed off");
            if (owner != null) {
                cleanupPending = true;
                return;
            }
            owner = Thread.currentThread();
        }
        finishCleanup(false);
    }

    private void finishCleanup(boolean closeTransport) {
        try {
            if (closeTransport) {
                cleanupStep("close transport", this::closeChannel);
            }
            releaseResources();
        } finally {
            synchronized (lifecycleLock) {
                Preconditions.checkState(owner == Thread.currentThread(), "Cleanup belongs to another thread");
                owner = null;
                lifecycleLock.notifyAll();
            }
        }
    }

    // Reached only by the unique cleanup owner, after the current response and
    // immediate interruption have both stopped using session resources.
    private void releaseResources() {
        // Transaction rollback/forwarding uses ConnectContext.get(). Bind the closing context,
        // even when the caller is the timeout checker or another connection's KILL command.
        ConnectContext previous = ConnectContext.get();
        setThreadLocalInfo();
        try {
            cleanupStep("release protocol session", this::releaseProtocolSession);
            if (statementContext != null) {
                cleanupStep("close statement", statementContext::close);
            }
            cleanupStep("close transaction", this::closeTxn);
            cleanupStep("drop temporary tables", this::deleteTempTable);
            // init can fail before it allocates a session id.
            if (sessionId != null) {
                cleanupStep("unregister session info", () -> Env.getCurrentEnv().unregisterSessionInfo(sessionId));
            }
            cleanupStep("clear statement", this::clear);
            preparedStatementContextMap.clear();
            returnRows = 0;
        } finally {
            // The closing operation has exited; detach it or restore the unrelated caller.
            if (previous == null || previous == this) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }

    private void interruptAndHandOffCleanup(StmtExecutor toCancel) {
        ConnectContext previous = ConnectContext.get();
        try {
            setThreadLocalInfo();
            // Wake packet readers even if query cancellation subsequently waits on a remote FE.
            cleanupStep("close transport", this::closeChannel);
            cleanupStep("cancel query", () -> {
                if (toCancel != null) {
                    toCancel.cancel(new Status(TStatusCode.CANCELLED, "session closed"), false);
                }
            });
        } finally {
            try {
                restoreThreadContext(previous);
            } finally {
                handOffCleanup();
            }
        }
    }

    private void cleanupStep(String action, Runnable step) {
        try {
            step.run();
        } catch (Throwable failure) {
            LOG.warn("Failed to {} for connection {}", action, connectionId, failure);
        }
    }

    protected void deleteTempTable() {
        try {
            cleanupTemporaryTables(false);
        } catch (DdlException e) {
            LOG.error("drop temporary table error", e);
        }
    }

    private void cleanupTemporaryTables(boolean reportFailure) throws DdlException {
        // only delete temporary table in its creating session, not proxy session in master fe
        if (isProxy) {
            return;
        }

        Map<String, Set<String>> tempTables = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : dbToTempTableNamesMap.entrySet()) {
            tempTables.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }

        // if current fe is master, delete temporary table directly
        if (Env.getCurrentEnv().isMaster()) {
            for (String dbName : tempTables.keySet()) {
                for (String tableName : tempTables.get(dbName)) {
                    LOG.info("try to drop temporary table: {}.{}", dbName, tableName);
                    try {
                        Database db = Env.getCurrentEnv().getInternalCatalog().getDb(dbName).get();
                        Env.getCurrentEnv().getInternalCatalog()
                            .dropTableWithoutCheck(db, db.getTable(tableName).get(), false, true);
                    } catch (Exception e) {
                        if (reportFailure) {
                            if (e instanceof DdlException) {
                                throw (DdlException) e;
                            }
                            throw new DdlException(String.format(
                                    "drop temporary table error: db: %s, table: %s", dbName, tableName), e);
                        }
                        LOG.error("drop temporary table error: {}.{}", dbName, tableName, e);
                    }
                }
            }
        } else {
            // forward to master fe to drop table
            RedirectStatus redirectStatus = new RedirectStatus(true, false);
            for (String dbName : tempTables.keySet()) {
                for (String tableName : tempTables.get(dbName)) {
                    LOG.info("request to delete temporary table: {}.{}", dbName, tableName);
                    String dropTableSql = String.format("drop table `%s`", tableName);
                    OriginStatement originStmt = new OriginStatement(dropTableSql, 0);
                    MasterOpExecutor masterOpExecutor = new MasterOpExecutor(originStmt, this, redirectStatus, false);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("need to transfer to Master. stmt: {}", this.getStmtId());
                    }
                    try {
                        masterOpExecutor.execute();
                    } catch (Exception e) {
                        if (reportFailure) {
                            throw new DdlException(String.format(
                                    "master FE drop temporary table error: db: %s, table: %s",
                                    dbName, tableName), e);
                        }
                        LOG.error("master FE drop temporary table error: db: {}, table: {}", dbName, tableName, e);
                    }
                }
            }
        }
    }

    /** True only while the current response is allowed to finish before transport closure. */
    public boolean isClosingAfterResponse() {
        return sessionState == SessionState.DRAINING;
    }

    public boolean isClosingOrClosed() {
        return sessionState != SessionState.OPEN;
    }

    public void setQueryId(TUniqueId queryId) {
        if (this.queryId != null) {
            this.lastQueryId = this.queryId.deepCopy();
        }
        this.queryId = queryId;
        synchronized (lifecycleLock) {
            // An admitted command may assign its query id after immediate closure unregistered
            // the connection. Keep the id for auditing, but never republish its removed trace index.
            if (sessionState != SessionState.STOPPING
                    && connectScheduler != null && !Strings.isNullOrEmpty(traceId)) {
                connectScheduler.getConnectPoolMgr().putTraceId2QueryId(traceId, queryId);
            }
        }
    }

    public void resetQueryId() {
        if (this.queryId != null) {
            this.lastQueryId = this.queryId.deepCopy();
        }
        this.queryId = null;
    }

    public void setNeedRegenerateInstanceId(TUniqueId needRegenerateInstanceId) {
        this.needRegenerateInstanceId = needRegenerateInstanceId;
    }

    public void setTraceId(String traceId) {
        // When traceId is set, we need to remove the old traceId from connectScheduler.
        if (connectScheduler != null) {
            connectScheduler.removeOldTraceId(this.traceId);
        }
        this.traceId = traceId;
    }

    public String traceId() {
        return traceId;
    }

    public TUniqueId queryId() {
        return queryId;
    }

    public TUniqueId getLastQueryId() {
        return lastQueryId;
    }

    public TUniqueId nextInstanceId() {
        if (loadId != null) {
            return new TUniqueId(loadId.hi, loadId.lo + instanceIdGenerator.incrementAndGet());
        } else {
            return new TUniqueId(queryId.hi, queryId.lo + instanceIdGenerator.incrementAndGet());
        }
    }

    public boolean consumeNeedRegenerateQueryId() {
        if (needRegenerateInstanceId == queryId) {
            needRegenerateInstanceId = null; // consume it
            return true;
        }
        return false;
    }

    public String getSqlHash() {
        return sqlHash;
    }

    public void setSqlHash(String sqlHash) {
        this.sqlHash = sqlHash;
    }

    public StatementContext getStatementContext() {
        return statementContext;
    }

    public void setStatementContext(StatementContext statementContext) {
        this.statementContext = statementContext;
    }

    /** Returns true if any table is targeted for synthetic write-column injection. */
    public boolean needsSyntheticWriteCol() {
        return syntheticWriteColTargetTableId >= 0;
    }

    /** Check if a specific table should inject its hidden synthetic write column. */
    public boolean needsSyntheticWriteColForTable(long tableId) {
        return syntheticWriteColTargetTableId >= 0 && syntheticWriteColTargetTableId == tableId;
    }

    /** Set the target table ID for synthetic write-column injection. Use -1 to clear. */
    public void setSyntheticWriteColTargetTableId(long tableId) {
        this.syntheticWriteColTargetTableId = tableId;
    }

    /** Get the previously saved target table ID (for the save/restore pattern). */
    public long getSyntheticWriteColTargetTableId() {
        return syntheticWriteColTargetTableId;
    }

    public String getRemoteHostPortString() {
        return protocolAdapter.remoteHostPortString(this);
    }

    // kill operation with no protect.
    public void kill(boolean killConnection) {
        LOG.warn("kill query from {}, kill {} connection: {}", getRemoteHostPortString(), getConnectType(),
                killConnection);

        if (killConnection) {
            killConnection();
            return;
        }
        // Now, cancel running query.
        cancelQuery(new Status(TStatusCode.CANCELLED, "cancel query by user from " + getRemoteHostPortString()));
    }

    public void cancelQuery(Status cancelReason) {
        StmtExecutor executorRef = executor;
        if (executorRef != null) {
            executorRef.cancel(cancelReason);
        }
    }

    public void checkTimeout(long now) {
        boolean idleClose = false;
        StmtExecutor timedOut = null;
        long queryStartTime = 0;
        synchronized (lifecycleLock) {
            if (sessionState == SessionState.STOPPING || startTime <= 0) {
                return;
            }
            long delta = now - startTime;
            if (command == MysqlCommand.COM_SLEEP) {
                // Flight metadata/option commands can show COM_SLEEP while using session resources.
                // MySQL, however, can block reading an incomplete packet before dispatch sets its
                // command. Keep wait_timeout able to close that transport and unblock the reader.
                // A draining Flight session has a fixed bound even while DoGet is streaming.
                // Interruption may start now, but resource release still waits for that owner.
                boolean idle = owner == null || getConnectType() == ConnectType.MYSQL
                        || sessionState == SessionState.DRAINING;
                if (idle && delta > sessionVariable.getWaitTimeoutS() * 1000L) {
                    idleClose = requestCloseLocked(CloseMode.IMMEDIATE);
                    timedOut = executor;
                }
            } else {
                timedOut = executor;
                queryStartTime = startTime;
            }
        }
        if (idleClose) {
            LOG.warn("kill wait timeout connection, connection type: {}, connectionId: {}, wait timeout: {}",
                    getConnectType(), connectionId, sessionVariable.getWaitTimeoutS());
            interruptAndHandOffCleanup(timedOut);
        } else if (timedOut != null) {
            cancelIfQueryTimedOut(now, timedOut, queryStartTime);
        } else if (protocolAdapter instanceof FlightProtocolAdapter) {
            runIdleMaintenance(() -> {
                ((FlightProtocolAdapter) protocolAdapter).expirePendingResults();
                reapIdleFlightSqlDeferredExecutors(now);
            });
        }
    }

    private void cancelIfQueryTimedOut(long now, StmtExecutor observed, long queryStartTime) {
        // This can acquire Auth locks or access LDAP. Do not prevent closure while waiting for it.
        int timeoutS = getExecTimeoutS();
        synchronized (lifecycleLock) {
            // A different statement can start during the lookup. Its kind, session overrides and
            // deadline may differ, so leave it to the next tick instead of applying this timeout.
            if (sessionState == SessionState.STOPPING || executor != observed || startTime != queryStartTime
                    || command == MysqlCommand.COM_SLEEP || now - queryStartTime <= timeoutS * 1000L) {
                return;
            }
        }
        LOG.warn("cancel timeout query for connection {}, connection type: {}", connectionId, getConnectType());
        observed.cancel(new Status(TStatusCode.TIMEOUT, "query is timeout, killed by timeout checker"), false);
    }

    // Helper to dump connection information.
    public ThreadInfo toThreadInfo(boolean isFull) {
        if (threadInfo == null) {
            threadInfo = new ThreadInfo();
        }
        threadInfo.isFull = isFull;
        return threadInfo;
    }

    public boolean isSetComputeGroup() {
        return computeGroup != null;
    }

    public void setComputeGroup(ComputeGroup computeGroup) {
        this.computeGroup = computeGroup;
    }

    public void setCurrentConnectedFEIp(String ip) {
        this.currentConnectedFEIp = ip;
    }

    public String getCurrentConnectedFEIp() {
        return currentConnectedFEIp;
    }

    public void setConnectingFeLocalResourceGroup(String connectingFeLocalResourceGroup) {
        this.connectingFeLocalResourceGroup = Strings.nullToEmpty(connectingFeLocalResourceGroup);
    }

    public String getConnectingFeLocalResourceGroup() {
        return connectingFeLocalResourceGroup;
    }

    /**
     * We calculate and get the exact execution timeout here, rather than setting
     * execution timeout in many other places.
     *
     * @return exact execution timeout
     */
    public int getExecTimeoutS() {
        // Timeout scanning and Flight command admission can overlap the preceding command's clear().
        StmtExecutor running = executor;
        if (running != null && running.isSyncLoadKindStmt()) {
            // particular for insert stmt, we can expand other type of timeout in the same way
            return Math.max(getInsertTimeoutS(), getQueryTimeoutS());
        } else {
            // normal query stmt
            return getQueryTimeoutS();
        }
    }

    /**
     * First, retrieve from the user's attributes. If not, retrieve from the session variable
     *
     * @return insertTimeoutS
     */
    public int getInsertTimeoutS() {
        int userInsertTimeout = getInsertTimeoutSFromProperty();
        if (userInsertTimeout > 0) {
            return userInsertTimeout;
        }
        return sessionVariable.getInsertTimeoutS();
    }

    private int getInsertTimeoutSFromProperty() {
        if (env == null || env.getAuth() == null || StringUtils.isEmpty(getQualifiedUser())) {
            return 0;
        }
        return env.getAuth().getInsertTimeout(getQualifiedUser());
    }

    /**
     * First, retrieve from the user's attributes. If not, retrieve from the session variable
     *
     * @return queryTimeoutS
     */
    public int getQueryTimeoutS() {
        int userQueryTimeout = getQueryTimeoutSFromProperty();
        if (userQueryTimeout > 0) {
            return userQueryTimeout;
        }
        return sessionVariable.getQueryTimeoutS();
    }

    private int getQueryTimeoutSFromProperty() {
        if (env == null || env.getAuth() == null || StringUtils.isEmpty(getQualifiedUser())) {
            return 0;
        }
        return env.getAuth().getQueryTimeout(getQualifiedUser());
    }

    /**
     * First, retrieve from the user's attributes. If not, retrieve from the session variable
     *
     * @return maxExecMemByte
     */
    public long getMaxExecMemByte() {
        long userLimit = getMaxExecMemByteFromProperty();
        if (userLimit > 0) {
            return userLimit;
        }
        return sessionVariable.getMaxExecMemByte();
    }

    private long getMaxExecMemByteFromProperty() {
        if (env == null || env.getAuth() == null || StringUtils.isEmpty(getQualifiedUser())) {
            return 0L;
        }
        return env.getAuth().getExecMemLimit(getQualifiedUser());
    }

    public void addResultAttachedInfo(Map<String, String> resultAttachedInfo) {
        this.resultAttachedInfo.add(resultAttachedInfo);
    }

    public List<Map<String, String>> getResultAttachedInfo() {
        return resultAttachedInfo;
    }

    public class ThreadInfo {
        public boolean isFull;

        public List<String> toRow(int connId, long nowMs, Optional<String> timeZone) {
            List<String> row = Lists.newArrayList();
            if (connId == connectionId) {
                row.add("Yes");
            } else {
                row.add("No");
            }
            row.add("" + connectionId);
            row.add(getQualifiedUser());
            row.add(getRemoteHostPortString());
            if (timeZone.isPresent()) {
                row.add(TimeUtils.longToTimeStringWithTimeZone(loginTime, timeZone.get()));
            } else {
                row.add(TimeUtils.longToTimeString(loginTime));
            }
            row.add(defaultCatalog);
            row.add(currentDb);
            row.add(command.toString());
            row.add("" + (nowMs - startTime) / 1000);
            row.add(state.toString());
            row.add(DebugUtil.printId(queryId));
            row.add(Strings.nullToEmpty(traceId));
            if (state.getStateType() == QueryState.MysqlStateType.ERR) {
                row.add(state.getErrorMessage());
            } else if (executor != null) {
                String sql = executor.getOriginStmtInString();
                if (!isFull) {
                    sql = sql.substring(0, Math.min(sql.length(), 100));
                }
                row.add(sql);
            } else {
                row.add("");
            }

            row.add(Env.getCurrentEnv().getSelfNode().getHost());
            String currentCloudCluster = sessionVariable.getCloudCluster();
            if (Strings.isNullOrEmpty(currentCloudCluster)) {
                currentCloudCluster = cloudCluster;
            }
            if (currentCloudCluster == null) {
                row.add("NULL");
            } else {
                row.add(currentCloudCluster);
            }
            // Protocol is the last column, after CloudCluster; a row of another frontend that lacks or
            // exceeds the local columns is fitted to them by whoever merges it (ShowProcessListCommand,
            // the BE processlist scanner).
            row.add(getConnectType().protocolName());
            return row;
        }
    }

    public String getQueryIdentifier() {
        return "stmt[" + stmtId + ", " + DebugUtil.printId(queryId) + "]";
    }

    public boolean supportHandleByFe() {
        return protocolAdapter.supportsFeSideResult() && getCommand() != MysqlCommand.COM_STMT_EXECUTE;
    }

    public void setCloudCluster(String cluster) {
        // A compute group only exists in cloud mode. Swallowing the call here instead of making
        // every caller wrap it in `if (Config.isCloudMode())` keeps that check in one place.
        if (!Config.isCloudMode()) {
            return;
        }
        this.getSessionVariable().setCloudCluster(cluster);
    }

    public String getEffectiveCloudCluster() {
        return effectiveCloudCluster;
    }

    public void setEffectiveCloudCluster(String cluster) {
        this.effectiveCloudCluster = cluster;
    }

    public String getCloudCluster() throws ComputeGroupException {
        return getCloudCluster(true);
    }

    public static class CloudClusterResult {
        public enum Comment {
            FOUND_BY_DEFAULT_CLUSTER,
            DEFAULT_CLUSTER_SET_BUT_NOT_EXIST,
            FOUND_BY_FIRST_CLUSTER_WITH_ALIVE_BE,
            FOUND_BY_FRIST_CLUSTER_HAS_AUTH,
        }

        public String clusterName;
        public Comment comment;

        public CloudClusterResult(final String name, Comment c) {
            this.clusterName = name;
            this.comment = c;
        }

        @Override
        public String toString() {
            return "CloudClusterResult{"
                + "clusterName='" + clusterName + '\''
                + ", comment=" + comment
                + '}';
        }
    }

    // can't get cluster from context, use the following strategy to obtain the cluster name
    // 当用户有多个集群的权限时，会按照如下策略进行拉取：
    // 如果当前mysql用户没有指定cluster(没有default 或者 use), 选择有权限的cluster。
    // 如果有多个cluster满足权限条件,优先选活的，按字母序选
    // 如果没有活的，则拉起一个，按字母序选
    public CloudClusterResult getCloudClusterByPolicy() {
        List<String> cloudClusterNames = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterNames();
        // try set default cluster
        String defaultCloudCluster = Env.getCurrentEnv().getAuth().getDefaultCloudCluster(getQualifiedUser());
        if (!Strings.isNullOrEmpty(defaultCloudCluster)) {
            // check cluster validity
            CloudClusterResult r;
            if (cloudClusterNames.contains(defaultCloudCluster)) {
                // valid
                r = new CloudClusterResult(defaultCloudCluster,
                    CloudClusterResult.Comment.FOUND_BY_DEFAULT_CLUSTER);
                LOG.info("use default compute group {}", defaultCloudCluster);
            } else {
                // invalid
                r = new CloudClusterResult(defaultCloudCluster,
                    CloudClusterResult.Comment.DEFAULT_CLUSTER_SET_BUT_NOT_EXIST);
                LOG.warn("default compute group {} current invalid, please change it", r);
            }
            return r;
        }

        List<String> hasAuthCluster = new ArrayList<>();
        // get all available cluster of the user
        for (String cloudClusterName : cloudClusterNames) {
            if (Env.getCurrentEnv().getAccessManager().checkCloudPriv(getCurrentUserIdentity(),
                    cloudClusterName, PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER)) {
                if (((CloudSystemInfoService) Env.getCurrentSystemInfo()).isStandByComputeGroup(cloudClusterName)) {
                    continue;
                }

                hasAuthCluster.add(cloudClusterName);
                // find a cluster has more than one alive be
                List<Backend> bes = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                        .getBackendsByClusterName(cloudClusterName);
                AtomicBoolean hasAliveBe = new AtomicBoolean(false);
                bes.stream().filter(Backend::isAlive).findAny().ifPresent(backend -> {
                    LOG.debug("get a compute group {}, it's has more than one alive be {}", cloudCluster, backend);
                    hasAliveBe.set(true);
                });
                if (hasAliveBe.get()) {
                    // set a cluster to context cloudCluster
                    CloudClusterResult r = new CloudClusterResult(cloudClusterName,
                            CloudClusterResult.Comment.FOUND_BY_FIRST_CLUSTER_WITH_ALIVE_BE);
                    LOG.debug("set context {}", r);
                    return r;
                }
            }
        }
        return hasAuthCluster.isEmpty() ? null
            : new CloudClusterResult(hasAuthCluster.get(0), CloudClusterResult.Comment.FOUND_BY_FRIST_CLUSTER_HAS_AUTH);
    }

    public ComputeGroup getComputeGroupSafely() {
        try {
            return getComputeGroup();
        } catch (UserException e) {
            throw new RuntimeException(e);
        }
    }

    public ComputeGroup getComputeGroup() throws UserException {
        ComputeGroupMgr cgMgr = Env.getCurrentEnv().getComputeGroupMgr();
        if (Config.isCloudMode()) {
            return cgMgr.getComputeGroupByName(getCloudCluster());
        } else {
            // In order to be compatible with resource tag's old logic,
            // when a user login in FE by mysql client, then its tags are set in ConnectContext which
            // means isSetComputeGroup = true
            if (this.isSetComputeGroup()) {
                return computeGroup;
            } else {
                String currentUser = getQualifiedUser();
                if (!StringUtils.isEmpty(currentUser)) {
                    return Env.getCurrentEnv().getAuth().getComputeGroup(currentUser);
                } else {
                    return Env.getCurrentEnv().getComputeGroupMgr().getComputeGroupByName(Tag.VALUE_DEFAULT_TAG);
                }
            }
        }
    }

    /**
     * Tries to choose an available cluster in the following order:
     * 1. Get cluster from session variable (set by `use @` command or setCloudCluster())
     * 2. Get cluster from cached variable (this.cloudCluster) if available (from previous policy selection)
     * 3. Get cluster from user's default cluster property if set
     * 4. Choose an authorized cluster by policy if all preceding conditions failed
     *
     * @param updateErr whether set the connect state to error if the returned cluster is null or empty
     * @return non-empty cluster name if a cluster has been chosen otherwise null or empty string
     * @throws ComputeGroupException, outer get reason by exception
     */
    public String getCloudCluster(boolean updateErr) throws ComputeGroupException {
        if (!Config.isCloudMode()) {
            throw new ComputeGroupException("not cloud mode", ComputeGroupException.FailedTypeEnum.NOT_CLOUD_MODE);
        }

        String choseWay = null;
        // 1 get cluster from session
        String sessionCluster = getSessionVariable().getCloudCluster();
        if (!Strings.isNullOrEmpty(sessionCluster)) {
            choseWay = "use session";
            if (LOG.isDebugEnabled()) {
                LOG.debug("finally set context compute group name {} for user {} with chose way '{}'",
                        sessionCluster, getCurrentUserIdentity(), choseWay);
            }
            return sessionCluster;
        }

        // 2 get cluster from a cached variable in connect context
        // this value comes from a cluster selection policy
        if (!Strings.isNullOrEmpty(this.cloudCluster)) {
            choseWay = "user selection policy";
            if (LOG.isDebugEnabled()) {
                LOG.debug("finally set context compute group name {} for user {} with chose way '{}'",
                        cloudCluster, getCurrentUserIdentity(), choseWay);
            }
            return cloudCluster;
        }

        // 3 get cluster from user
        String userPropCluster = getDefaultCloudClusterFromUser(true);
        if (!StringUtils.isEmpty(userPropCluster)) {
            choseWay = "user property";
            if (LOG.isDebugEnabled()) {
                LOG.debug("finally set context compute group name {} for user {} with chose way '{}'", userPropCluster,
                        getCurrentUserIdentity(), choseWay);
            }
            this.cloudCluster = userPropCluster;
            return userPropCluster;
        }

        String policyCluster = "";
        CloudClusterResult cloudClusterTypeAndName = getCloudClusterByPolicy();
        if (cloudClusterTypeAndName != null && !Strings.isNullOrEmpty(cloudClusterTypeAndName.clusterName)) {
            policyCluster = cloudClusterTypeAndName.clusterName;
            choseWay = "by policy";
        }

        if (Strings.isNullOrEmpty(policyCluster)) {
            List<String> cloudClusterNames
                    = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterNames();
            LOG.warn("Can not get a valid compute group for user {} {} to use, all cluster: {}",
                    getCurrentUserIdentity(),
                    getQualifiedUser(), cloudClusterNames);
            ComputeGroupException exception = new ComputeGroupException(
                    "the user is not granted permission to the compute group",
                    ComputeGroupException.FailedTypeEnum.CURRENT_USER_NO_AUTH_TO_USE_ANY_COMPUTE_GROUP);
            if (updateErr) {
                getState().setError(ErrorCode.ERR_CLOUD_CLUSTER_ERROR, exception.getMessage());
            }
            throw exception;
        }
        this.cloudCluster = policyCluster;

        if (LOG.isDebugEnabled()) {
            LOG.debug("finally set context compute group name {} for user {} with chose way '{}'", this.cloudCluster,
                    getCurrentUserIdentity(), choseWay);
        }

        return this.cloudCluster;
    }

    private String getDefaultCloudClusterFromUser(boolean checkExist) {
        String defaultCluster = Env.getCurrentEnv().getAuth().getDefaultCloudCluster(getQualifiedUser());
        if (Strings.isNullOrEmpty(defaultCluster)) {
            return null;
        }
        if (!checkExist) {
            // default cluster may be dropped.
            return defaultCluster;
        }

        // Validate cluster existence
        List<String> cloudClusterNames = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterNames();
        if (cloudClusterNames.contains(defaultCluster)) {
            return defaultCluster;
        }
        LOG.warn("default compute group {} of user {} is invalid, all cluster: {}", defaultCluster,
                getQualifiedUser(), cloudClusterNames);
        return null;
    }

    // for log use, compute group name and the way to get it
    // the way may be context policy, session, default compute group from user
    public static Pair<String, String> computeGroupFromHintMsg() {
        String clusterName = "";
        try {
            if (ConnectContext.get() != null) {
                clusterName = ConnectContext.get().getCloudCluster();
            }
        } catch (Exception e) {
            clusterName = "ctx empty cant get clusterName";

        }
        String fromSession = ConnectContext.get().getSessionVariable().getCloudCluster();
        String fromDefaultComputeGroup = ConnectContext.get().getDefaultCloudClusterFromUser(false);
        String clusterFrom = "context policy";
        if (clusterName.equalsIgnoreCase(fromSession)) {
            clusterFrom = "session variable";
        } else if (clusterName.equalsIgnoreCase(fromDefaultComputeGroup)) {
            clusterFrom = "default compute group from user";
        }
        return Pair.of(clusterName, clusterFrom);
    }

    public StatsErrorEstimator getStatsErrorEstimator() {
        return statsErrorEstimator;
    }

    public void setStatsErrorEstimator(StatsErrorEstimator statsErrorEstimator) {
        this.statsErrorEstimator = statsErrorEstimator;
    }

    public void setWorkloadGroupName(String workloadGroupName) {
        this.workloadGroupName = workloadGroupName;
    }

    public String getWorkloadGroupName() {
        return this.workloadGroupName;
    }

    public boolean isSkipAuth() {
        return skipAuth;
    }

    public void setSkipAuth(boolean skipAuth) {
        this.skipAuth = skipAuth;
    }

    public int getNetReadTimeout() {
        return this.sessionVariable.getNetReadTimeout();
    }

    public int getNetWriteTimeout() {
        return this.sessionVariable.getNetWriteTimeout();
    }

    public boolean isGroupCommit() {
        return isGroupCommit;
    }

    public void setGroupCommit(boolean groupCommit) {
        isGroupCommit = groupCommit;
    }

    public Map<String, LiteralExpr> getUserVars() {
        return userVars;
    }

    public void setUserVars(Map<String, LiteralExpr> userVars) {
        this.userVars = userVars;
    }

    public boolean isProxy() {
        return isProxy;
    }

    public void setMysqlHandshakePacket(MysqlHandshakePacket mysqlHandshakePacket) {
        MysqlProtocolAdapter.of(this).setHandshakePacket(mysqlHandshakePacket);
    }

    public byte[] getAuthPluginData() {
        return MysqlProtocolAdapter.of(this).getAuthPluginData();
    }

    @Override
    public String toString() {
        return getClass().getName() + "@" + Integer.toHexString(hashCode()) + ":" + getQualifiedUser();
    }

    public Map<String, Set<String>> getDbToTempTableNamesMap() {
        return dbToTempTableNamesMap;
    }

    public void addTempTableToDB(String database, String tableName) {
        Set<String> tableNameSet = dbToTempTableNamesMap.get(database);
        if (tableNameSet == null) {
            tableNameSet = new HashSet<>();
            dbToTempTableNamesMap.put(database, tableNameSet);
        }

        tableNameSet.add(tableName);
    }

    public void removeTempTableFromDB(String database, String tableName) {
        Set<String> tableNameSet = dbToTempTableNamesMap.get(database);
        if (tableNameSet != null) {
            tableNameSet.remove(tableName);
        }
    }

    public int getAliveBeNumber() {
        return Math.max(1, this.getEnv().getClusterInfo().getBackendsNumber(true));
    }

    public int getParallelInstanceNum() {
        String clusterName = this.getSessionVariable().resolveCloudClusterName(this);
        return Math.max(1, this.getSessionVariable().getParallelExecInstanceNum(clusterName));
    }

    public int getTotalInstanceNum() {
        return getAliveBeNumber() * getParallelInstanceNum();
    }
}
