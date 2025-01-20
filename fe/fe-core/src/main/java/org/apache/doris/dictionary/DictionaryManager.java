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
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Status;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.job.extensions.insert.InsertTask;
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
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Manager for dictionary operations, including creation, deletion, and data loading.
 */
public class DictionaryManager extends MasterDaemon implements Writable {
    private static final Logger LOG = LogManager.getLogger(DictionaryManager.class);

    private static final long jobId = -493209151411825L;

    // Lock for protecting dictionaries map
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    // Map of database name -> dictionary name -> dictionary
    @SerializedName(value = "d")
    private Map<String, Map<String, Dictionary>> dictionaries = Maps.newConcurrentMap();

    // dbname -> tablename -> dictname
    @SerializedName(value = "t")
    private Map<String, ListMultimap<String, String>> dbTableToDicNames = Maps.newConcurrentMap();

    @SerializedName(value = "i")
    private long uniqueId = 0;

    public DictionaryManager() {
        super("Dictionary Manager", 10 * 60 * 1000); // run every 10 minutes
    }

    @Override
    protected void runAfterCatalogReady() {
        // Check and update dictionary data in each cycle
        try {
            checkAndUpdateDictionaries();
        } catch (Exception e) {
            LOG.warn("Failed to check and update dictionaries", e);
        }
    }

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
            if (hasDictionaryUnlock(info.getDbName(), info.getDictName())) {
                if (info.isIfNotExists()) {
                    return getDictionary(info.getDbName(), info.getDictName());
                } else {
                    throw new DdlException(
                            "Dictionary " + info.getDictName() + " already exists in database " + info.getDbName());
                }
            }
            // 2. Create dictionary object
            Dictionary dictionary = new Dictionary(info, ++uniqueId);
            // Add to dictionaries map. no throw here. so schedule below is safe.
            Map<String, Dictionary> dbDictionaries = dictionaries.computeIfAbsent(info.getDbName(),
                    k -> Maps.newConcurrentMap());
            dbDictionaries.put(info.getDictName(), dictionary);
            ListMultimap<String, String> tableToDicNames = dbTableToDicNames.computeIfAbsent(info.getDbName(),
                    k -> ArrayListMultimap.create());
            tableToDicNames.put(info.getSourceTableName(), info.getDictName());

            // 3. Log the creation operation
            Env.getCurrentEnv().getEditLog().logCreateDictionary(dictionary);

            return dictionary;
        } finally {
            unlockWrite();
        }
    }

    /**
     * Delete a dictionary.
     *
     * @throws DdlException if the dictionary does not exist
     * @throws AnalysisException
     */
    public void dropDictionary(ConnectContext ctx, String dbName, String dictName, boolean ifExists)
            throws DdlException, AnalysisException {
        lockWrite();
        Dictionary dic = null;
        try {
            Map<String, Dictionary> dbDictionaries = dictionaries.get(dbName);
            if (dbDictionaries == null || !dbDictionaries.containsKey(dictName)) {
                if (!ifExists) {
                    throw new DdlException("Dictionary " + dictName + " does not exist in database " + dbName);
                }
                return;
            }
            dic = dbDictionaries.remove(dictName);

            // remove mapping from table to dict
            dbTableToDicNames.get(dbName).remove(dic.getSourceTableName(), dictName);

            // Log the drop operation
            Env.getCurrentEnv().getEditLog().logDropDictionary(dbName, dictName);
        } finally {
            unlockWrite();
        }
        // The data in BE doesn't always have the same situation with FE(think FE crash). But we have periodic report
        // so that we can drop unknown dictionary at that time.
        scheduleDataUnload(ctx, dic);
    }

    /**
     * Drop all dictionaries in a table. Used when dropping a table. So maybe no db or table records.
     *
     * @throws AnalysisException
     */
    public void dropTableDictionaries(String dbName, String tableName)
            throws AnalysisException {
        lockWrite();
        List<Dictionary> droppedDictionaries = Lists.newArrayList();
        try {
            ListMultimap<String, String> tableToDicNames = dbTableToDicNames.get(dbName);
            if (tableToDicNames == null) { // this db has no table with dictionary records.
                return;
            }
            // get all dictionary names of this table
            List<String> dictNames = tableToDicNames.removeAll(tableName);
            if (dictNames == null) { // this table has no dictionaries.
                return;
            }
            // all this db's dictionaries. tableToDicNames is not null so nameToDics must not be null.
            Map<String, Dictionary> nameToDics = dictionaries.get(dbName);
            for (String dictName : dictNames) {
                Dictionary dictionary = nameToDics.remove(dictName);
                droppedDictionaries.add(dictionary);
                // Log the drop operation
                Env.getCurrentEnv().getEditLog().logDropDictionary(dbName, dictName);
            }
        } finally {
            unlockWrite();
        }
        for (Dictionary dictionary : droppedDictionaries) {
            scheduleDataUnload(null, dictionary);
        }
    }

    /**
     * Drop all dictionaries in a database. Used when dropping a database.
     *
     * @throws AnalysisException
     */
    public void dropDbDictionaries(String dbName) throws AnalysisException {
        lockWrite();
        List<Dictionary> droppedDictionaries = Lists.newArrayList();
        try {
            // pop and save item from dictionaries
            Map<String, Dictionary> dbDictionaries = dictionaries.remove(dbName);
            // Log the drop operation
            if (dbDictionaries != null) {
                for (Map.Entry<String, Dictionary> entry : dbDictionaries.entrySet()) {
                    droppedDictionaries.add(entry.getValue());
                    Env.getCurrentEnv().getEditLog().logDropDictionary(dbName, entry.getKey());
                }
                // also drop all name mapping records.
                dbTableToDicNames.remove(dbName);
            }
        } finally {
            unlockWrite();
        }
        for (Dictionary dictionary : droppedDictionaries) {
            scheduleDataUnload(null, dictionary);
        }
    }

    private boolean hasDictionaryUnlock(String dbName, String dictName) {
        Map<String, Dictionary> dbDictionaries = dictionaries.get(dbName);
        return dbDictionaries != null && dbDictionaries.containsKey(dictName);
    }

    public Map<String, Dictionary> getDictionaries(String dbName) {
        lockRead();
        try {
            return dictionaries.computeIfAbsent(dbName, k -> Maps.newConcurrentMap());
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
            Map<String, Dictionary> dbDictionaries = dictionaries.get(dbName);
            if (dbDictionaries == null || !dbDictionaries.containsKey(dictName)) {
                throw new DdlException("Dictionary " + dictName + " does not exist in database " + dbName);
            }
            return dbDictionaries.get(dictName);
        } finally {
            unlockRead();
        }
    }

    private void checkAndUpdateDictionaries() {
        // TODO: Implement dictionary data check and update logic
    }

    /// data load and unload are also used for dictionary data check and update to keep data consistency between BE
    /// and FE. So they are not private.

    public void scheduleDataLoad(ConnectContext ctx, Dictionary dictionary) throws Exception {
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
        baseCommand.setLabelName(Optional.of(jobId + "_" + queryId.toString()));
        baseCommand.setJobId(jobId);

        dictionary.setStatus(Dictionary.DictionaryStatus.LOADING);
        InsertIntoDictionaryCommand command = new InsertIntoDictionaryCommand(baseCommand, dictionary);

        // run with sync
        try {
            dictionary.writeLock();
            dictionary.increaseVersion();
            command.run(ctx, executor);

            Env.getCurrentEnv().getEditLog().logDictionaryIncVersion(dictionary);
            dictionary.setStatus(Dictionary.DictionaryStatus.NORMAL);
            dictionary.updateLastUpdateTime();
            LOG.info("Dictionary {} refresh succeed", dictionary.getName());
        } catch (Exception e) {
            // wait next shedule.
            LOG.warn("Dictionary {} refresh failed", dictionary.getName());
            dictionary.decreaseVersion();
            dictionary.setStatus(Dictionary.DictionaryStatus.OUT_OF_DATE);
            throw e;
        } finally {
            dictionary.writeUnlock();
        }
    }

    public void scheduleDataUnload(ConnectContext ctx, Dictionary dictionary)
            throws AnalysisException {
        List<Backend> aliveBes = new ArrayList<>();
        try {
            aliveBes = Env.getCurrentSystemInfo().getAllBackendsByAllCluster().values().stream()
                    .filter(be -> be.isAlive()).collect(Collectors.toList());
        } catch (AnalysisException e) {
            // actually it wont throw.
        }
        // get all alive BEs and send rpc.
        List<Future<InternalService.PDeleteDictionaryResponse>> futureList = new ArrayList<>();
        for (Backend be : aliveBes) {
            if (!be.isAlive()) {
                continue;
            }
            final InternalService.PDeleteDictionaryRequest request = InternalService.PDeleteDictionaryRequest
                    .newBuilder().setDictionaryId(dictionary.getId()).build();
            Future<InternalService.PDeleteDictionaryResponse> response = BackendServiceProxy.getInstance()
                    .deleteDictionaryAsync(be.getBrpcAddress(), Config.dictionary_delete_rpc_timeout_ms, request);
            futureList.add(response);
        }
        // wait all responses. if succeed, delete dictionary.
        for (Future<InternalService.PDeleteDictionaryResponse> future : futureList) {
            if (future == null) {
                continue;
            }
            try {
                InternalService.PDeleteDictionaryResponse response = future.get(Config.dictionary_delete_rpc_timeout_ms,
                        TimeUnit.SECONDS);
                if (response.hasStatus()) {
                    Status status = new Status(response.getStatus());
                    if (status.getErrorCode() != TStatusCode.OK) {
                        LOG.warn("Failed to delete dictionary " + dictionary.getName() + " on be "
                                + status.getErrorMsg());
                        dictionary.setStatus(Dictionary.DictionaryStatus.REMOVING);
                    }
                }
            } catch (Throwable t) {
                LOG.warn("Failed to delete dictionary " + dictionary.getName(), t);
                throw new AnalysisException("Failed to delete dictionary " + dictionary.getName());
            }
        }
    }

    public void replayCreateDictionary(CreateDictionaryPersistInfo info) {
        Dictionary dictionary = info.getDictionary();
        lockWrite();
        try {
            // Add to dictionaries map
            Map<String, Dictionary> dbDictionaries = dictionaries.computeIfAbsent(dictionary.getDbName(),
                    k -> Maps.newConcurrentMap());
            if (dbDictionaries.containsKey(dictionary.getName())) {
                LOG.warn("Dictionary {} already exists when replaying create dictionary", dictionary.getName());
                return;
            }
            dbDictionaries.put(dictionary.getName(), dictionary);
            uniqueId = Math.max(uniqueId, dictionary.getId());
        } finally {
            unlockWrite();
        }
    }

    public void replayDropDictionary(DropDictionaryPersistInfo info) {
        lockWrite();
        try {
            Map<String, Dictionary> dbDictionaries = dictionaries.get(info.getDbName());
            if (dbDictionaries != null) {
                dbDictionaries.remove(info.getDictionaryName());
                if (dbDictionaries.isEmpty()) {
                    dictionaries.remove(info.getDbName());
                }
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
