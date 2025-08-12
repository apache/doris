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

package org.apache.doris.indexpolicy;

import org.apache.doris.analysis.DropIndexPolicyStmt;
import org.apache.doris.analysis.ShowIndexPolicyStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ShowResultSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class IndexPolicyMgr implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(IndexPolicyMgr.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @SerializedName(value = "idToIndexPolicy")
    private final Map<Long, IndexPolicy> idToIndexPolicy = Maps.newHashMap();
    private final Map<String, IndexPolicy> nameToIndexPolicy = Maps.newHashMap();

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    public List<IndexPolicy> getCopiedIndexPolicies() {
        List<IndexPolicy> copiedPolicies = Lists.newArrayList();
        readLock();
        try {
            copiedPolicies.addAll(idToIndexPolicy.values());
        } finally {
            readUnlock();
        }
        return copiedPolicies;
    }

    public void validateAnalyzerExists(String analyzerName) throws DdlException {
        readLock();
        try {
            IndexPolicy policy = nameToIndexPolicy.get(analyzerName);
            if (policy == null) {
                throw new DdlException("Analyzer '" + analyzerName + "' does not exist");
            }
            if (policy.getType() != IndexPolicyTypeEnum.ANALYZER) {
                throw new DdlException("Policy '" + analyzerName + "' is not an analyzer");
            }
            if (policy.isInvalid()) {
                throw new DdlException("Analyzer '" + analyzerName + "' is invalid");
            }
        } finally {
            readUnlock();
        }
    }

    public void createIndexPolicy(boolean ifNotExists, String policyName,
            IndexPolicyTypeEnum type, Map<String, String> properties) throws UserException {
        if (policyName == null || policyName.trim().isEmpty()) {
            throw new DdlException("Policy name cannot be empty or null");
        }
        if (IndexPolicy.BUILTIN_TOKENIZERS.contains(policyName)) {
            throw new DdlException("Policy name '" + policyName + "' conflicts with built-in tokenizer name");
        }
        if (IndexPolicy.BUILTIN_TOKEN_FILTERS.contains(policyName)) {
            throw new DdlException("Policy name '" + policyName + "' conflicts with built-in token filter name");
        }

        IndexPolicy indexPolicy = IndexPolicy.create(policyName, type, properties);

        writeLock();
        try {
            validatePolicyProperties(type, properties);

            if (nameToIndexPolicy.containsKey(policyName)) {
                if (ifNotExists) {
                    return;
                }
                throw new DdlException("Index policy " + policyName + " already exists");
            }

            if (idToIndexPolicy.size() >= 100) {
                throw new DdlException("Index policy number cannot exceed 100");
            }

            nameToIndexPolicy.put(policyName, indexPolicy);
            idToIndexPolicy.put(indexPolicy.getId(), indexPolicy);
            Env.getCurrentEnv().getEditLog().logCreateIndexPolicy(indexPolicy);
        } finally {
            writeUnlock();
        }
        LOG.info("Created index policy successfully: {}", indexPolicy);
    }

    public IndexPolicy getPolicyByName(String name) {
        readLock();
        try {
            return nameToIndexPolicy.get(name);
        } finally {
            readUnlock();
        }
    }

    private void validatePolicyProperties(IndexPolicyTypeEnum type, Map<String, String> properties)
            throws DdlException {
        if (properties == null) {
            throw new DdlException("Properties cannot be null");
        }

        switch (type) {
            case ANALYZER:
                validateAnalyzerProperties(properties);
                break;
            case TOKENIZER:
                validateTokenizerProperties(properties);
                break;
            case TOKEN_FILTER:
                validateTokenFilterProperties(properties);
                break;
            default:
                throw new DdlException("Unknown index policy type: " + type);
        }
    }

    private void validateAnalyzerProperties(Map<String, String> properties) throws DdlException {
        for (String key : properties.keySet()) {
            if (!key.equals(IndexPolicy.PROP_TOKENIZER)
                    && !key.equals(IndexPolicy.PROP_TOKEN_FILTER)) {
                throw new DdlException("Invalid analyzer property: '" + key + "'. Only '"
                    + IndexPolicy.PROP_TOKENIZER + "' and '" + IndexPolicy.PROP_TOKEN_FILTER
                        + "' are allowed.");
            }
        }

        String tokenizer = properties.get(IndexPolicy.PROP_TOKENIZER);
        if (tokenizer == null || tokenizer.isEmpty()) {
            throw new DdlException("ANALYZER must specify a 'tokenizer' property");
        }
        validatePolicyReference(tokenizer, IndexPolicyTypeEnum.TOKENIZER);

        String tokenFilters = properties.get(IndexPolicy.PROP_TOKEN_FILTER);
        if (tokenFilters != null && !tokenFilters.isEmpty()) {
            for (String filter : tokenFilters.split(",\\s*")) {
                validatePolicyReference(filter, IndexPolicyTypeEnum.TOKEN_FILTER);
            }
        }
    }

    private void validatePolicyReference(String name, IndexPolicyTypeEnum expectedType)
            throws DdlException {
        if (expectedType == IndexPolicyTypeEnum.TOKENIZER
                && IndexPolicy.BUILTIN_TOKENIZERS.contains(name)) {
            return;
        }
        if (expectedType == IndexPolicyTypeEnum.TOKEN_FILTER
                && IndexPolicy.BUILTIN_TOKEN_FILTERS.contains(name)) {
            return;
        }

        IndexPolicy policy = getPolicyByName(name);
        if (policy == null) {
            throw new DdlException("Referenced " + expectedType + " policy '" + name + "' does not exist");
        }
        if (policy.getType() != expectedType) {
            throw new DdlException("Referenced policy '" + name + "' is of type "
                    + policy.getType() + " but expected " + expectedType);
        }
    }

    private void validateTokenizerProperties(Map<String, String> properties) throws DdlException {
        String type = properties.get(IndexPolicy.PROP_TYPE);
        if (type == null || type.isEmpty()) {
            throw new DdlException("TOKENIZER must specify a 'type' property");
        }
        PolicyPropertyValidator validator;
        switch (type) {
            case "ngram":
                validator = new NGramTokenizerValidator();
                break;
            case "edge_ngram":
                validator = new EdgeNGramTokenizerValidator();
                break;
            case "standard":
                validator = new StandardTokenizerValidator();
                break;
            case "keyword":
                validator = new KeywordTokenizerValidator();
                break;
            case "char_group":
                validator = new CharGroupTokenizerValidator();
                break;
            default:
                throw new DdlException("Unsupported tokenizer type: " + type
                        + ". Supported types: " + IndexPolicy.BUILTIN_TOKENIZERS);
        }
        validator.validate(properties);
    }

    private void validateTokenFilterProperties(Map<String, String> properties) throws DdlException {
        String type = properties.get(IndexPolicy.PROP_TYPE);
        if (type == null || type.isEmpty()) {
            throw new DdlException("TOKEN_FILTER must specify a 'type' property");
        }
        PolicyPropertyValidator validator;
        switch (type) {
            case "asciifolding":
                validator = new AsciiFoldingTokenFilterValidator();
                break;
            case "word_delimiter":
                validator = new WordDelimiterTokenFilterValidator();
                break;
            case "lowercase":
                validator = new LowerCaseTokenFilterValidator();
                break;
            default:
                throw new DdlException("Unsupported token filter type: " + type
                        + ". Supported types: " + IndexPolicy.BUILTIN_TOKEN_FILTERS);
        }
        validator.validate(properties);
    }

    public void dropIndexPolicy(boolean isIfExists, String indexPolicyName,
            IndexPolicyTypeEnum type) throws DdlException, AnalysisException {
        writeLock();
        try {
            IndexPolicy policyToDrop = nameToIndexPolicy.get(indexPolicyName);
            if (policyToDrop == null) {
                if (isIfExists) {
                    return;
                }
                throw new DdlException("index policy " + indexPolicyName + " does not exist");
            }
            if (policyToDrop.getType() == IndexPolicyTypeEnum.ANALYZER) {
                checkAnalyzerNotUsedByIndex(policyToDrop.getName());
            }
            if (policyToDrop.getType() == IndexPolicyTypeEnum.TOKENIZER
                    || policyToDrop.getType() == IndexPolicyTypeEnum.TOKEN_FILTER) {
                checkPolicyNotReferenced(policyToDrop);
            }
            long id = policyToDrop.getId();
            idToIndexPolicy.remove(id);
            nameToIndexPolicy.remove(indexPolicyName);
            Env.getCurrentEnv().getEditLog().logDropIndexPolicy(new DropIndexPolicyLog(id));
        } finally {
            writeUnlock();
        }
        LOG.info("Drop index policy success: {}", indexPolicyName);
    }

    public void dropIndexPolicy(DropIndexPolicyStmt stmt) throws DdlException, AnalysisException {
        boolean isIfExists = stmt.isIfExists();
        String indexPolicyName = stmt.getName();
        IndexPolicyTypeEnum type = stmt.getType();

        dropIndexPolicy(isIfExists, indexPolicyName, type);
    }

    private void checkAnalyzerNotUsedByIndex(String analyzerName) throws DdlException {
        List<Database> databases = Env.getCurrentEnv().getInternalCatalog().getDbs();
        for (Database db : databases) {
            List<Table> tables = db.getTables();
            for (Table table : tables) {
                if (table instanceof OlapTable) {
                    OlapTable olapTable = (OlapTable) table;
                    for (Index index : olapTable.getIndexes()) {
                        Map<String, String> properties = index.getProperties();
                        if (properties != null
                                && analyzerName.equals(properties.get(IndexPolicy.PROP_ANALYZER))) {
                            throw new DdlException("the analyzer " + analyzerName + " is used by index: "
                                    + index.getIndexName() + " in table: " + table.getName());
                        }
                    }
                }
            }
        }
    }

    private void checkPolicyNotReferenced(IndexPolicy policy) throws DdlException {
        String policyName = policy.getName();
        IndexPolicyTypeEnum policyType = policy.getType();
        for (IndexPolicy analyzerPolicy : idToIndexPolicy.values()) {
            if (analyzerPolicy.getType() == IndexPolicyTypeEnum.ANALYZER) {
                Map<String, String> properties = analyzerPolicy.getProperties();
                if (policyType == IndexPolicyTypeEnum.TOKENIZER) {
                    String tokenizer = properties.get(IndexPolicy.PROP_TOKENIZER);
                    if (policyName.equals(tokenizer)) {
                        throw new DdlException("Cannot drop " + policyType + " policy '" + policyName
                                + "' as it is referenced by ANALYZER policy '"
                                        + analyzerPolicy.getName() + "'");
                    }
                } else if (policyType == IndexPolicyTypeEnum.TOKEN_FILTER) {
                    String tokenFilters = properties.get(IndexPolicy.PROP_TOKEN_FILTER);
                    if (tokenFilters != null && !tokenFilters.isEmpty()) {
                        for (String filter : tokenFilters.split(",\\s*")) {
                            if (policyName.equals(filter)) {
                                throw new DdlException("Cannot drop " + policyType + " policy '"
                                         + policyName + "' as it is referenced by ANALYZER policy '"
                                                + analyzerPolicy.getName() + "'");
                            }
                        }
                    }
                }
            }
        }
    }

    public ShowResultSet showIndexPolicy(IndexPolicyTypeEnum type) throws AnalysisException {
        List<List<String>> rows = Lists.newArrayList();
        readLock();
        try {
            for (IndexPolicy policy : idToIndexPolicy.values()) {
                if (policy.isInvalid()) {
                    continue;
                }
                if (type != null && policy.getType() != type) {
                    continue;
                }
                rows.add(policy.getShowInfo());
            }
            return new ShowResultSet(IndexPolicy.INDEX_POLICY_META_DATA, rows);
        } finally {
            readUnlock();
        }
    }

    public ShowResultSet showIndexPolicy(ShowIndexPolicyStmt showStmt) throws AnalysisException {
        IndexPolicyTypeEnum type = showStmt.getType();

        return showIndexPolicy(type);
    }

    public void replayCreateIndexPolicy(IndexPolicy indexPolicy) {
        writeLock();
        try {
            idToIndexPolicy.put(indexPolicy.getId(), indexPolicy);
            nameToIndexPolicy.put(indexPolicy.getName(), indexPolicy);
            LOG.debug("Replayed index policy: id={}, name={}",
                    indexPolicy.getId(), indexPolicy.getName());
        } finally {
            writeUnlock();
        }
    }

    public void replayDropIndexPolicy(DropIndexPolicyLog dropLog) {
        long id = dropLog.getId();
        writeLock();
        try {
            if (!idToIndexPolicy.containsKey(id)) {
                return;
            }
            IndexPolicy indexPolicy = idToIndexPolicy.get(id);
            idToIndexPolicy.remove(id);
            nameToIndexPolicy.remove(indexPolicy.getName());
            LOG.debug("Replayed drop index policy: {}", indexPolicy.getName());
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static IndexPolicyMgr read(DataInput in) throws IOException {
        String json = Text.readString(in);
        IndexPolicyMgr mgr = GsonUtils.GSON.fromJson(json, IndexPolicyMgr.class);
        return mgr;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        idToIndexPolicy.forEach(
                (id, indexPolicy) -> nameToIndexPolicy.put(indexPolicy.getName(), indexPolicy));
    }
}
