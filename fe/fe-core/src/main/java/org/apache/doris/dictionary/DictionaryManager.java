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

package org.apache.doris.dictionary;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.CustomThreadFactory;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Status;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.dictionary.Dictionary.DictionaryStatus;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.job.manager.TaskDisruptorGroupManager;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.info.CreateDictionaryInfo;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoDictionaryCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.persist.CreateDictionaryPersistInfo;
import org.apache.doris.persist.DictionaryDecreaseVersionInfo;
import org.apache.doris.persist.DictionaryIncreaseVersionInfo;
import org.apache.doris.persist.DropDictionaryPersistInfo;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.proto.InternalService;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TDictionaryStatus;
import org.apache.doris.thrift.TDictionaryStatusList;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manager for dictionary operations, including creation, deletion, and data loading.
 */
public class DictionaryManager extends MasterDaemon implements Writable {
    private static final Logger LOG = LogManager.getLogger(DictionaryManager.class);

    private static final long DICTIONARY_JOB_ID = -493209151411825L; // "DICTIONARY" to INT

    // Lock for protecting dictionaryIds map
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    /// ATTN: we MUST have only one container holds the dictionary object because of GSON deserialization.
    /// make it `idToDictionary`. so all others MUST be secondary index.\

    // Map of database name -> dictionary name -> dictionary id
    @SerializedName(value = "ids")
    private Map<String, Map<String, Long>> dictionaryIds = Maps.newConcurrentMap();
    // dbname -> tablename -> dict id
    @SerializedName(value = "t")
    private Map<String, ListMultimap<String, Long>> dbTableToDicIds = Maps.newConcurrentMap();
    @SerializedName(value = "idmap")
    private Map<Long, Dictionary> idToDictionary = Maps.newConcurrentMap();

    @SerializedName(value = "i")
    private long uniqueId = 0;

    private static final int DISPATCH_DICTIONARY_THREAD_NUM = Config.job_dictionary_task_consumer_thread_num > 0
            ? Config.job_dictionary_task_consumer_thread_num
            : TaskDisruptorGroupManager.DEFAULT_CONSUMER_THREAD_NUM;

    private static final int DISPATCH_DICTIONARY_TASK_QUEUE_SIZE = TaskDisruptorGroupManager
            .normalizeRingbufferSize(Config.dictionary_task_queue_size);

    // thread pool for dictionary data load and unload
    private ExecutorService executor;

    public DictionaryManager() {
        super("Dictionary Manager", Config.dictionary_auto_refresh_interval_seconds * 1000);
        this.executor = new ThreadPoolExecutor(
                DISPATCH_DICTIONARY_THREAD_NUM, // default thread num
                DISPATCH_DICTIONARY_THREAD_NUM, // max = default
                0L, // max = default so not useful
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(DISPATCH_DICTIONARY_TASK_QUEUE_SIZE),
                new CustomThreadFactory("dictionary-task-execute"),
                new ThreadPoolExecutor.AbortPolicy() // throw when queue is full
        );
    }

    @Override
    protected void runAfterCatalogReady() {
        // interval unit is ms
        setInterval(Config.dictionary_auto_refresh_interval_seconds * 1000);
        // Check and update dictionary data in each cycle
        try {
            checkAndUpdateDictionaries();
            LOG.info("Collect dictionaries status succeed");
        } catch (Exception e) {
            LOG.warn("Failed to check and update dictionaries", e);
        }
    }

    // if lock manager and dictionary together, manager should be locked first!
    public void lockRead() {
        lock.readLock().lock();
    }

    public void unlockRead() {
        lock.readLock().unlock();
    }

    public void lockWrite() {
        lock.writeLock().lock();
    }

    public void unlockWrite() {
        lock.writeLock().unlock();
    }

    /**
     * Create a new dictionary based on the provided info.
     *
     * @throws Exception
     */
    public Dictionary createDictionary(ConnectContext ctx, CreateDictionaryInfo info) throws Exception {
        lockWrite();
        try {
            // 1. Check if dictionary already exists
            if (hasDictionaryWithoutLock(info.getDbName(), info.getDictName())) {
                if (info.isIfNotExists()) {
                    return getDictionary(info.getDbName(), info.getDictName());
                } else {
                    throw new DdlException(
                            "Dictionary " + info.getDictName() + " already exists in database " + info.getDbName());
                }
            }
            // 2. Create dictionary object
            Dictionary dictionary = new Dictionary(info, ++uniqueId);
            // Add to dictionaryIds map. no throw here. so schedule below is safe.
            idToDictionary.put(dictionary.getId(), dictionary);
            Map<String, Long> dbDictIds = dictionaryIds.computeIfAbsent(info.getDbName(),
                    k -> Maps.newConcurrentMap());
            dbDictIds.put(info.getDictName(), dictionary.getId());
            ListMultimap<String, Long> tableToDicIds = dbTableToDicIds.computeIfAbsent(info.getDbName(),
                            k -> ArrayListMultimap.create());
            tableToDicIds.put(info.getSourceTableName(), dictionary.getId());

            // 3. Log the creation operation
            Env.getCurrentEnv().getEditLog().logCreateDictionary(dictionary);

            submitDataLoad(dictionary);
            return dictionary;
        } finally {
            unlockWrite();
        }
    }

    /// for all drop operations, we don't care about data drop on BE. drop metadata and when BEs report them,
    /// they are unknown dicts at that time. daemon will schedule to drop them on BEs.
    /**
     * Delete a dictionary.
     *
     * @throws DdlException if the dictionary does not exist or unload failed
     */
    public void dropDictionary(ConnectContext ctx, String dbName, String dictName, boolean ifExists)
            throws DdlException {
        lockWrite();
        Dictionary dictionary = null;
        try {
            Map<String, Long> dbDictIds = dictionaryIds.get(dbName);
            if (dbDictIds == null || !dbDictIds.containsKey(dictName)) {
                if (!ifExists) {
                    throw new DdlException("Dictionary " + dictName + " does not exist in database " + dbName);
                }
                return;
            }
            Long id = dbDictIds.remove(dictName);
            dictionary = idToDictionary.remove(id);

            // remove mapping from table to dict
            dbTableToDicIds.get(dbName).remove(dictionary.getSourceTableName(), id);

            // Log the drop operation
            Env.getCurrentEnv().getEditLog().logDropDictionary(dbName, dictName);
        } finally {
            unlockWrite();
        }
    }

    /**
     * Drop all dictionaries in a table. Used when dropping a table. So maybe no db or table records.
     */
    public void dropTableDictionaries(String dbName, String tableName) {
        lockWrite();
        try {
            ListMultimap<String, Long> tableToDictIds = dbTableToDicIds.get(dbName);
            if (tableToDictIds == null) { // this db has no table with dictionary records.
                return;
            }
            // get all dictionary names of this table
            List<Long> dictIds = tableToDictIds.removeAll(tableName);
            if (dictIds == null) { // this table has no dictionaries.
                return;
            }
            // all this db's dictionaries. tableToDictIds is not null so nameToDics must not be null.
            Map<String, Long> nameToIds = dictionaryIds.get(dbName);
            for (Long id : dictIds) {
                Dictionary dict = idToDictionary.remove(id);
                if (id == null) {
                    LOG.warn("Dictionary {} does not exist in dictionaryIds", id);
                    continue;
                }
                nameToIds.remove(dict.getName());
                // Log the drop operation
                Env.getCurrentEnv().getEditLog().logDropDictionary(dbName, dict.getName());
            }
        } finally {
            unlockWrite();
        }
    }

    /**
     * Drop all dictionaries in a database. Used when dropping a database.
     */
    public void dropDbDictionaries(String dbName) {
        lockWrite();
        try {
            // pop and save item from dictionaries
            Map<String, Long> dbDictIds = dictionaryIds.remove(dbName);
            // Log the drop operation
            if (dbDictIds != null) {
                for (Map.Entry<String, Long> entry : dbDictIds.entrySet()) {
                    Env.getCurrentEnv().getEditLog().logDropDictionary(dbName, entry.getKey());
                }
                // also drop all name mapping records.
                dbTableToDicIds.remove(dbName);
            }
        } finally {
            unlockWrite();
        }
    }

    private boolean hasDictionaryWithoutLock(String dbName, String dictName) {
        Map<String, Long> dbDictIds = dictionaryIds.get(dbName);
        return dbDictIds != null && dbDictIds.containsKey(dictName);
    }

    public Map<String, Dictionary> getDictionaries(String dbName) {
        lockRead();
        try {
            Map<String, Long> ids = dictionaryIds.computeIfAbsent(dbName, k -> Maps.newConcurrentMap());
            return Maps.transformValues(ids, id -> idToDictionary.get(id));
        } finally {
            unlockRead();
        }
    }

    /**
     * Get a dictionary.
     *
     * @throws DdlException if the dictionary does not exist
     */
    public Dictionary getDictionary(String dbName, String dictName) throws DdlException {
        lockRead();
        try {
            Map<String, Long> dbDictIds = dictionaryIds.get(dbName);
            if (dbDictIds == null || !dbDictIds.containsKey(dictName)) {
                throw new DdlException("Dictionary " + dictName + " does not exist in database " + dbName);
            }
            return idToDictionary.get(dbDictIds.get(dictName));
        } finally {
            unlockRead();
        }
    }

    public Dictionary getDictionary(long dictId) {
        lockRead();
        try {
            return idToDictionary.get(dictId);
        } finally {
            unlockRead();
        }
    }

    /**
     * Get all BE's dictionaries' status. Then load for lack of dictionary and
     * unload for unknown dictionary.
     */
    private void checkAndUpdateDictionaries() throws Exception {
        long now = System.currentTimeMillis();
        // get all BE dictionaries' status
        Map<Long, List<Long>> unknownDictsIdtoBes = collectDictionaryStatus(null);

        // DROP unknown dictionaries
        for (Map.Entry<Long, List<Long>> entry : unknownDictsIdtoBes.entrySet()) {
            Long dictId = entry.getKey();
            List<Long> beIds = entry.getValue();
            submitDataUnload(dictId, beIds);
        }

        // check all dictionaries and REFRESH if needed
        for (Map<String, Long> dbDictIds : dictionaryIds.values()) {
            for (Long id : dbDictIds.values()) {
                Dictionary dictionary = idToDictionary.get(id);
                // when data duration is older than its lifetime AND TODO:, refresh it.
                if (dictionary.getLastUpdateTime() + dictionary.getDataLifetimeSecs() * 1000 < now) {
                    // should schedule refresh. ONLY trigger when it's NORMAL because if not,
                    // it's already going to refresh or drop.
                    if (dictionary.trySetStatusIf(DictionaryStatus.NORMAL, DictionaryStatus.OUT_OF_DATE)) {
                        submitDataLoad(dictionary);
                    }
                }
            }
        }
    }

    private void submitDataLoad(Dictionary dictionary) {
        LOG.info("Submit dictionary {} refresh task", dictionary.getName());
        executor.execute(() -> {
            try {
                dataLoad(null, dictionary);
            } catch (Exception e) {
                LOG.warn("Failed to load dictionary " + dictionary.getName(), e);
            }
        });
    }

    public void dataLoad(ConnectContext ctx, Dictionary dictionary) throws Exception {
        // use atomic status as a lock.
        if (!dictionary.trySetStatus(Dictionary.DictionaryStatus.LOADING)) {
            throw new AnalysisException("Dictionary " + dictionary.getName() + " cannot load now, status is "
                    + dictionary.getStatus().name());
        }
        LOG.info("Start loading data into dictionary " + dictionary.getName());
        if (ctx == null) { // for run with scheduler, not by command.
            // priv check is done in relative(caller) command. so use ADMIN here is ok.
            ctx = InsertTask.makeConnectContext(UserIdentity.ADMIN, dictionary.getDbName());
        }

        // not use rerfresh command's executor to avoid potential problems.
        StmtExecutor executor = InsertTask.makeStmtExecutor(ctx);
        NereidsParser parser = new NereidsParser();
        InsertIntoTableCommand baseCommand = (InsertIntoTableCommand) parser
                .parseSingle("insert into " + dictionary.getDbName() + "." + dictionary.getName() + " select * from "
                        + dictionary.getDbName() + "." + dictionary.getSourceTableName());
        TUniqueId queryId = InsertTask.generateQueryId();
        if (!baseCommand.getLabelName().isPresent()) {
            baseCommand.setLabelName(Optional.of(DICTIONARY_JOB_ID + "_" + queryId.toString()));
        }
        if (baseCommand.getJobId() == 0) {
            baseCommand.setJobId(DICTIONARY_JOB_ID);
        }

        InsertIntoDictionaryCommand command = new InsertIntoDictionaryCommand(baseCommand, dictionary);

        // run with sync by status.
        try {
            dictionary.increaseVersion();
            command.run(ctx, executor);
        } catch (Exception e) {
            // wait next shedule.
            LOG.warn("Dictionary {} refresh failed", dictionary.getName());
            dictionary.decreaseVersion();
            // wont fail cuz status is LOADING owned by me.
            dictionary.trySetStatus(Dictionary.DictionaryStatus.OUT_OF_DATE);
            dictionary.setLastUpdateResult(e.getMessage());
            throw e;
        }
        // only when succeed we can do this. because of deleting does NOT conflict with loading,
        // we should check existance again!
        lockRead();
        try {
            if (dictionaryIds.get(dictionary.getDbName()).containsKey(dictionary.getName())) {
                Env.getCurrentEnv().getEditLog().logDictionaryIncVersion(dictionary);
                // wont fail cuz status is LOADING owned by me.
                dictionary.trySetStatus(Dictionary.DictionaryStatus.NORMAL);
                dictionary.updateLastUpdateTime();
                dictionary.setLastUpdateResult("succeed");
                LOG.info("Dictionary {} refresh succeed", dictionary.getName());
            } else {
                LOG.warn("Dictionary {} has been dropped during loading", dictionary.getName());
                // the dictionary will be GC soon.
            }
        } finally {
            unlockRead();
        }
    }

    private void submitDataUnload(long dictId, List<Long> beIds) {
        LOG.info("Submit dictionary {} unload data task", dictId);
        executor.execute(() -> {
            try {
                dataUnload(dictId, beIds);
                LOG.info("Unload data of dictionary {} succeed", dictId);
            } catch (Exception e) {
                // already logged in dataUnload
            }
        });
    }

    /**
     * Unload dictionary data from all alive backends. Only for drop unknown dictionary we could directly call this.
     *
     * @param dictId dictionary id
     * @param beIds backend ids to unload. if null, unload all alive backends.
     * @return true if all succeed, false if some failed.
     */
    private boolean dataUnload(long dictId, List<Long> beIds) {
        // some of them not alive will lead to fail. acceptable(try next time collect infos of them).
        List<Backend> aliveBes = Env.getCurrentSystemInfo().getBackends(beIds);
        // get all alive BEs and send rpc.
        List<Future<InternalService.PDeleteDictionaryResponse>> futureList = new ArrayList<>();
        boolean allSucceed = true;
        try {
            for (Backend be : aliveBes) {
                if (!be.isAlive()) {
                    continue;
                }
                final InternalService.PDeleteDictionaryRequest request = InternalService.PDeleteDictionaryRequest
                        .newBuilder().setDictionaryId(dictId).build();
                Future<InternalService.PDeleteDictionaryResponse> response = BackendServiceProxy.getInstance()
                        .deleteDictionaryAsync(be.getBrpcAddress(), Config.dictionary_delete_rpc_timeout_ms, request);
                futureList.add(response);
            }
            // wait all responses. if succeed, delete dictionary.
            for (int i = 0; i < futureList.size(); i++) {
                Future<InternalService.PDeleteDictionaryResponse> future = futureList.get(i);
                Backend be = aliveBes.get(i);
                if (future == null) {
                    continue;
                }
                InternalService.PDeleteDictionaryResponse response = future.get(Config.dictionary_delete_rpc_timeout_ms,
                        TimeUnit.SECONDS);
                if (response.hasStatus()) {
                    Status status = new Status(response.getStatus());
                    if (status.getErrorCode() != TStatusCode.OK) {
                        LOG.warn("Failed to unload dictionary " + dictId + " on be "
                                + be.getAddress() + " because " + status.getErrorMsg());
                        allSucceed = false;
                    }
                } else {
                    LOG.warn("Failed to unload dictionary " + dictId + " on be " + be.getAddress());
                    allSucceed = false;
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to unload dictionary " + dictId, e);
            allSucceed = false;
        }
        return allSucceed;
    }

    /**
     * Get dictionary status from all alive backends.
     *
     * @param queryDicts query dictionaries. if null, query all dictionaries.
     * @return Map of unknown dictionary <id, List<beId>>
     */
    public Map<Long, List<Long>> collectDictionaryStatus(List<Long> queryDicts) throws RuntimeException {
        Map<Long, List<Long>> unknownDictionaries = Maps.newHashMap();
        Set<Long> updatedDictIds = Sets.newHashSet();
        if (queryDicts == null) {
            queryDicts = ImmutableList.of(); // query all dictionaries
        }
        LOG.info("Collecting all dictionaries status for " + queryDicts.size() + " dictionaries");
        // traverse all backends
        for (Backend backend : Env.getCurrentSystemInfo().getAllClusterBackends(true)) {
            BackendService.Client client = null;
            TNetworkAddress address = null;
            TDictionaryStatusList allStatusList = null;
            try {
                address = new TNetworkAddress(backend.getHost(), backend.getBePort());
                client = ClientPool.backendPool.borrowObject(address);
                // rpc. query for dictionaries status
                allStatusList = client.getDictionaryStatus(queryDicts);
                ClientPool.backendPool.returnObject(address, client);
            } catch (Exception e) {
                LOG.warn("failed to get dictionary status from backend[{}]", backend.getId(), e);
                ClientPool.backendPool.invalidateObject(address, client);
            }

            if (allStatusList == null || !allStatusList.isSetDictionaryStatusList()) {
                throw new RuntimeException("failed to get dictionary status from backend[" + backend.getId() + "]");
            }

            // traverse all dictionary status in this BE
            for (TDictionaryStatus status : allStatusList.getDictionaryStatusList()) {
                if (!status.isSetDictionaryId() || !status.isSetVersionId() || !status.isSetDictionaryMemorySize()) {
                    throw new RuntimeException("invalid dictionary status from backend[" + backend.getId() + "]");
                }
                long dictionaryId = status.getDictionaryId();
                Dictionary dictionary = idToDictionary.get(dictionaryId);
                if (dictionary == null) {
                    // Found an unknown dictionary, record it
                    unknownDictionaries.computeIfAbsent(dictionaryId, k -> Lists.newArrayList()).add(backend.getId());
                    continue;
                }

                // add one record of this dictionary in this BE
                DictionaryDistribution newDistribution = new DictionaryDistribution(backend, status.getVersionId(),
                        status.getDictionaryMemorySize());

                // Update the distribution list
                List<DictionaryDistribution> distributions = dictionary.getDataDistributions();
                // if it's new here, set it with a new list(invalidating the old list)
                if (updatedDictIds.add(dictionaryId)) {
                    dictionary.resetDataDistributions();
                    distributions = dictionary.getDataDistributions();
                }
                // add new distribution
                distributions.add(newDistribution);
            }
        }
        LOG.info("Collect all dictionaries status succeed");
        return unknownDictionaries;
    }

    public void replayCreateDictionary(CreateDictionaryPersistInfo info) {
        Dictionary dictionary = info.getDictionary();
        lockWrite();
        try {
            // Add to dictionaries map
            Map<String, Long> dbDictIds = dictionaryIds.computeIfAbsent(dictionary.getDbName(),
                    k -> Maps.newConcurrentMap());
            if (dbDictIds.containsKey(dictionary.getName())) {
                LOG.warn("Dictionary {} already exists when replaying create dictionary", dictionary.getName());
                return;
            }
            dbDictIds.put(dictionary.getName(), dictionary.getId());
            dbTableToDicIds.computeIfAbsent(dictionary.getDbName(), k -> ArrayListMultimap.create())
                    .put(dictionary.getSourceTableName(), dictionary.getId());
            idToDictionary.put(dictionary.getId(), dictionary);
            uniqueId = Math.max(uniqueId, dictionary.getId());
        } finally {
            unlockWrite();
        }
    }

    public void replayDropDictionary(DropDictionaryPersistInfo info) {
        lockWrite();
        try {
            Map<String, Long> dbDictIds = dictionaryIds.get(info.getDbName());
            if (dbDictIds != null) {
                Long id = dbDictIds.remove(info.getDictionaryName());
                Dictionary dict = idToDictionary.remove(id);
                if (dbDictIds.isEmpty()) {
                    dictionaryIds.remove(info.getDbName());
                }
                dbTableToDicIds.get(info.getDbName()).remove(dict.getSourceTableName(), id);
            } else {
                LOG.warn("Database {} does not exist when replaying drop dictionary", info.getDbName());
            }
        } finally {
            unlockWrite();
        }
    }

    public void replayIncreaseVersion(DictionaryIncreaseVersionInfo info) throws DdlException {
        String dbName = info.getDictionary().getDbName();
        String dictName = info.getDictionary().getName();
        Dictionary dictionary = getDictionary(dbName, dictName);
        dictionary.writeLock();
        dictionary.increaseVersion();
        dictionary.writeUnlock();
    }

    public void replayDecreaseVersion(DictionaryDecreaseVersionInfo info) throws DdlException {
        String dbName = info.getDictionary().getDbName();
        String dictName = info.getDictionary().getName();
        Dictionary dictionary = getDictionary(dbName, dictName);
        dictionary.writeLock();
        dictionary.decreaseVersion();
        dictionary.writeUnlock();
    }

    // Metadata serialization
    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static DictionaryManager read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, DictionaryManager.class);
    }
}
