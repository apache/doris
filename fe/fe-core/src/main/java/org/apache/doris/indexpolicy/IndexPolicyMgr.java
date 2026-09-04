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

import com.google.common.collect.ImmutableSet;
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
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class IndexPolicyMgr implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(IndexPolicyMgr.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @SerializedName(value = "idToIndexPolicy")
    private final Map<Long, IndexPolicy> idToIndexPolicy = Maps.newHashMap();
    // Keys are normalized to lowercase for case-insensitive lookup
    private final Map<String, IndexPolicy> nameToIndexPolicy = Maps.newHashMap();

    /**
     * Normalize policy name to lowercase for case-insensitive lookup.
     * Policy names are case-insensitive in Doris.
     */
    private static String normalizeKey(String name) {
        return name == null ? null : name.trim().toLowerCase();
    }

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
        String normalizedName = normalizeKey(analyzerName);
        // Built-in analyzers are stored in lowercase, so use normalized name for comparison
        if (IndexPolicy.BUILTIN_ANALYZERS.contains(normalizedName)) {
            return;
        }

        readLock();
        try {
            IndexPolicy policy = nameToIndexPolicy.get(normalizedName);
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

    /**
     * Validate that {@code analyzerName} resolves to a usable analyzer graph and report whether
     * that graph ends in a common_grams token filter. Callers need the flag because CommonGrams
     * indexes store gram terms, which only SNII can read back.
     */
    public boolean validateAnalyzerUsesCommonGrams(String analyzerName) throws DdlException {
        String normalizedName = normalizeKey(analyzerName);
        if (IndexPolicy.BUILTIN_ANALYZERS.contains(normalizedName)) {
            return false;
        }

        readLock();
        try {
            IndexPolicy analyzer = requireAnalyzerLocked(analyzerName);
            return validateAnalyzerGraphLocked(analyzer.getName(), analyzer.getProperties());
        } finally {
            readUnlock();
        }
    }

    public void validateNormalizerExists(String normalizerName) throws DdlException {
        String normalizedName = normalizeKey(normalizerName);
        // Built-in normalizers are stored in lowercase, so use normalized name for comparison
        if (IndexPolicy.BUILTIN_NORMALIZERS.contains(normalizedName)) {
            return;
        }

        readLock();
        try {
            IndexPolicy policy = nameToIndexPolicy.get(normalizedName);
            if (policy == null) {
                throw new DdlException("Normalizer '" + normalizerName + "' does not exist");
            }
            if (policy.getType() != IndexPolicyTypeEnum.NORMALIZER) {
                throw new DdlException("Policy '" + normalizerName + "' is not a normalizer");
            }
            if (policy.isInvalid()) {
                throw new DdlException("Normalizer '" + normalizerName + "' is invalid");
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
        // Normalize policy name for case-insensitive comparison with built-in names
        String normalizedName = normalizeKey(policyName);
        if (IndexPolicy.BUILTIN_TOKENIZERS.contains(normalizedName)) {
            throw new DdlException("Policy name '" + policyName + "' conflicts with built-in tokenizer name");
        }
        if (IndexPolicy.BUILTIN_TOKEN_FILTERS.contains(normalizedName)) {
            throw new DdlException("Policy name '" + policyName + "' conflicts with built-in token filter name");
        }
        if (IndexPolicy.BUILTIN_CHAR_FILTERS.contains(normalizedName)) {
            throw new DdlException("Policy name '" + policyName + "' conflicts with built-in char filter name");
        }
        if (IndexPolicy.BUILTIN_ANALYZERS.contains(normalizedName)) {
            throw new DdlException("Policy name '" + policyName + "' conflicts with built-in analyzer name");
        }

        writeLock();
        try {
            validatePolicyProperties(type, properties);
            if (type == IndexPolicyTypeEnum.ANALYZER) {
                validateAnalyzerGraphLocked(policyName, properties);
            }
            IndexPolicy indexPolicy = IndexPolicy.create(policyName, type, properties);

            if (nameToIndexPolicy.containsKey(normalizedName)) {
                if (ifNotExists) {
                    return;
                }
                throw new DdlException("Index policy " + policyName + " already exists");
            }

            if (idToIndexPolicy.size() >= 100) {
                throw new DdlException("Index policy number cannot exceed 100");
            }

            // Store with normalized key for case-insensitive lookup
            nameToIndexPolicy.put(normalizedName, indexPolicy);
            idToIndexPolicy.put(indexPolicy.getId(), indexPolicy);
            Env.getCurrentEnv().getEditLog().logCreateIndexPolicy(indexPolicy);
        } finally {
            writeUnlock();
        }
        LOG.info("Created index policy successfully: {}", policyName);
    }

    public IndexPolicy getPolicyByName(String name) {
        readLock();
        try {
            return nameToIndexPolicy.get(normalizeKey(name));
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
            case CHAR_FILTER:
                validateCharFilterProperties(properties);
                break;
            case NORMALIZER:
                validateNormalizerProperties(properties);
                break;
            default:
                throw new DdlException("Unknown index policy type: " + type);
        }
    }

    private void validateAnalyzerProperties(Map<String, String> properties) throws DdlException {
        for (String key : properties.keySet()) {
            if (!key.equals(IndexPolicy.PROP_TOKENIZER)
                    && !key.equals(IndexPolicy.PROP_TOKEN_FILTER)
                    && !key.equals(IndexPolicy.PROP_CHAR_FILTER)) {
                throw new DdlException("Invalid analyzer property: '" + key + "'. Only '"
                    + IndexPolicy.PROP_TOKENIZER + "' and '" + IndexPolicy.PROP_TOKEN_FILTER
                        + "' and '" + IndexPolicy.PROP_CHAR_FILTER + "' are allowed.");
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

        String charFilters = properties.get(IndexPolicy.PROP_CHAR_FILTER);
        if (charFilters != null && !charFilters.isEmpty()) {
            for (String filter : charFilters.split(",\\s*")) {
                validatePolicyReference(filter, IndexPolicyTypeEnum.CHAR_FILTER);
            }
        }
    }

    // CommonGrams pairs each token with its neighbour, so it is only correct when every token
    // upstream of it advances the position by exactly one. These are the tokenizers and token
    // filters that guarantee that; anything else (a synonym expander, a word splitter, an
    // asciifolding that preserves the original) can stack tokens at one position and would make
    // the resulting grams span the wrong terms.
    private static final Set<String> UNIT_POSITION_TOKENIZERS =
            ImmutableSet.of("empty", "char_group", "ngram", "edge_ngram");
    private static final Set<String> UNIT_POSITION_TOKEN_FILTERS =
            ImmutableSet.of("empty", "lowercase", "icu_normalizer", "asciifolding");

    private boolean validateAnalyzerGraphLocked(String analyzerName,
            Map<String, String> properties) throws DdlException {
        ResolvedAnalyzerComponent tokenizer = resolveAnalyzerComponentLocked(
                properties.get(IndexPolicy.PROP_TOKENIZER), IndexPolicyTypeEnum.TOKENIZER);
        List<ResolvedAnalyzerComponent> tokenFilters = new ArrayList<>();
        String tokenFilterNames = properties.get(IndexPolicy.PROP_TOKEN_FILTER);
        if (tokenFilterNames != null && !tokenFilterNames.isEmpty()) {
            for (String tokenFilterName : tokenFilterNames.split(",\\s*")) {
                tokenFilters.add(resolveAnalyzerComponentLocked(
                        tokenFilterName, IndexPolicyTypeEnum.TOKEN_FILTER));
            }
        }

        int commonGramsIndex = -1;
        int commonGramsCount = 0;
        for (int i = 0; i < tokenFilters.size(); i++) {
            if (IndexPolicy.COMMON_GRAMS_TYPE.equals(tokenFilters.get(i).componentType)) {
                commonGramsIndex = i;
                commonGramsCount++;
            }
        }
        if (commonGramsCount == 0) {
            return false;
        }
        if (commonGramsCount != 1 || commonGramsIndex != tokenFilters.size() - 1) {
            throw new DdlException("CommonGrams analyzer '" + analyzerName
                    + "' requires common_grams exactly once as the terminal token filter");
        }

        if (!UNIT_POSITION_TOKENIZERS.contains(tokenizer.componentType)) {
            throw new DdlException("CommonGrams analyzer '" + analyzerName
                    + "' tokenizer '" + tokenizer.referenceName
                    + "' does not guarantee unit position increments");
        }
        for (int i = 0; i < commonGramsIndex; i++) {
            ResolvedAnalyzerComponent tokenFilter = tokenFilters.get(i);
            boolean unitPositionFilter =
                    UNIT_POSITION_TOKEN_FILTERS.contains(tokenFilter.componentType);
            if (unitPositionFilter && "asciifolding".equals(tokenFilter.componentType)) {
                unitPositionFilter = "false".equalsIgnoreCase(
                        tokenFilter.properties.getOrDefault("preserve_original", "false"));
            }
            if (!unitPositionFilter) {
                throw new DdlException("CommonGrams analyzer '" + analyzerName
                        + "' token filter '" + tokenFilter.referenceName
                        + "' does not guarantee unit position increments");
            }
        }
        return true;
    }

    private IndexPolicy requireAnalyzerLocked(String analyzerName) throws DdlException {
        IndexPolicy analyzer = nameToIndexPolicy.get(normalizeKey(analyzerName));
        if (analyzer == null) {
            throw new DdlException("Analyzer '" + analyzerName + "' does not exist");
        }
        if (analyzer.getType() != IndexPolicyTypeEnum.ANALYZER) {
            throw new DdlException("Policy '" + analyzerName + "' is not an analyzer");
        }
        if (analyzer.isInvalid()) {
            throw new DdlException("Analyzer '" + analyzerName + "' is invalid");
        }
        return analyzer;
    }

    private ResolvedAnalyzerComponent resolveAnalyzerComponentLocked(String referenceName,
            IndexPolicyTypeEnum expectedType) throws DdlException {
        String normalizedName = normalizeKey(referenceName);
        boolean builtin = expectedType == IndexPolicyTypeEnum.TOKENIZER
                ? IndexPolicy.BUILTIN_TOKENIZERS.contains(normalizedName)
                : IndexPolicy.BUILTIN_TOKEN_FILTERS.contains(normalizedName);
        if (builtin) {
            return new ResolvedAnalyzerComponent(referenceName, normalizedName,
                    Map.of(IndexPolicy.PROP_TYPE, normalizedName));
        }

        IndexPolicy policy = nameToIndexPolicy.get(normalizedName);
        if (policy == null) {
            throw new DdlException("Referenced " + expectedType + " policy '"
                    + referenceName + "' does not exist");
        }
        if (policy.getType() != expectedType) {
            throw new DdlException("Referenced policy '" + referenceName + "' is of type "
                    + policy.getType() + " but expected " + expectedType);
        }
        return new ResolvedAnalyzerComponent(referenceName,
                policy.getProperties().get(IndexPolicy.PROP_TYPE),
                policy.getProperties());
    }

    private static final class ResolvedAnalyzerComponent {
        private final String referenceName;
        private final String componentType;
        private final Map<String, String> properties;

        private ResolvedAnalyzerComponent(String referenceName, String componentType,
                Map<String, String> properties) {
            this.referenceName = referenceName;
            this.componentType = componentType;
            this.properties = properties;
        }
    }

    private void validateNormalizerProperties(Map<String, String> properties) throws DdlException {
        if (properties.containsKey(IndexPolicy.PROP_TOKENIZER)) {
            throw new DdlException("Normalizer cannot contain 'tokenizer' field");
        }

        String charFilters = properties.get(IndexPolicy.PROP_CHAR_FILTER);
        String tokenFilters = properties.get(IndexPolicy.PROP_TOKEN_FILTER);

        if ((charFilters == null || charFilters.isEmpty())
                && (tokenFilters == null || tokenFilters.isEmpty())) {
            throw new DdlException("Normalizer must contain at least one 'char_filter' or 'token_filter'");
        }

        if (charFilters != null && !charFilters.isEmpty()) {
            for (String filter : charFilters.split(",\\s*")) {
                validatePolicyReference(filter, IndexPolicyTypeEnum.CHAR_FILTER);
            }
        }

        if (tokenFilters != null && !tokenFilters.isEmpty()) {
            for (String filter : tokenFilters.split(",\\s*")) {
                validatePolicyReference(filter, IndexPolicyTypeEnum.TOKEN_FILTER);
            }
        }

        for (String key : properties.keySet()) {
            if (!key.equals(IndexPolicy.PROP_CHAR_FILTER)
                    && !key.equals(IndexPolicy.PROP_TOKEN_FILTER)) {
                throw new DdlException("Invalid normalizer property: '" + key + "'. Only '"
                        + IndexPolicy.PROP_CHAR_FILTER + "' and '" + IndexPolicy.PROP_TOKEN_FILTER
                        + "' are allowed.");
            }
        }
    }

    private void validatePolicyReference(String name, IndexPolicyTypeEnum expectedType)
            throws DdlException {
        String normalizedName = normalizeKey(name);
        if (expectedType == IndexPolicyTypeEnum.TOKENIZER
                && IndexPolicy.BUILTIN_TOKENIZERS.contains(normalizedName)) {
            return;
        }
        if (expectedType == IndexPolicyTypeEnum.TOKEN_FILTER
                && IndexPolicy.BUILTIN_TOKEN_FILTERS.contains(normalizedName)) {
            return;
        }
        if (expectedType == IndexPolicyTypeEnum.CHAR_FILTER
                && IndexPolicy.BUILTIN_CHAR_FILTERS.contains(normalizedName)) {
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
            case "empty":
                validator = new NoOperationValidator("empty tokenizer");
                break;
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
                validator = new NoOperationValidator("keyword tokenizer");
                break;
            case "char_group":
                validator = new CharGroupTokenizerValidator();
                break;
            case "pinyin":
                validator = new PinyinTokenizerValidator();
                break;
            case "icu":
                validator = new ICUTokenizerValidator();
                break;
            case "basic":
                validator = new BasicTokenizerValidator();
                break;
            default:
                Set<String> userFacingTypes = IndexPolicy.BUILTIN_TOKENIZERS.stream()
                        .filter(t -> !t.equals("empty"))
                        .collect(Collectors.toSet());
                throw new DdlException("Unsupported tokenizer type: " + type
                        + ". Supported types: " + userFacingTypes);
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
            case "empty":
                validator = new NoOperationValidator("empty token filter");
                break;
            case "asciifolding":
                validator = new AsciiFoldingTokenFilterValidator();
                break;
            case "word_delimiter":
                validator = new WordDelimiterTokenFilterValidator();
                break;
            case "lowercase":
                validator = new NoOperationValidator("lowercase token filter");
                break;
            case "pinyin":
                validator = new PinyinTokenFilterValidator();
                break;
            case "icu_normalizer":
                validator = new ICUNormalizerTokenFilterValidator();
                break;
            case "common_grams":
                // No properties beyond the type: the word list it grams against is a BE-local file
                // named by be.conf's common_grams_wordset_path, deliberately not selectable per
                // policy, since every replica of a tablet must gram the same terms.
                validator = new NoOperationValidator("common_grams token filter");
                break;
            default:
                Set<String> userFacingTypes = IndexPolicy.BUILTIN_TOKEN_FILTERS.stream()
                        .filter(t -> !t.equals("empty"))
                        .collect(Collectors.toSet());
                throw new DdlException("Unsupported token filter type: " + type
                        + ". Supported types: " + userFacingTypes);
        }
        validator.validate(properties);
    }

    private void validateCharFilterProperties(Map<String, String> properties) throws DdlException {
        String type = properties.get(IndexPolicy.PROP_TYPE);
        if (type == null || type.isEmpty()) {
            throw new DdlException("CHAR_FILTER must specify a 'type' property");
        }
        PolicyPropertyValidator validator;
        switch (type) {
            case "empty":
                validator = new NoOperationValidator("empty char filter");
                break;
            case "char_replace":
                validator = new CharReplaceCharFilterValidator();
                break;
            case "icu_normalizer":
                validator = new ICUNormalizerCharFilterValidator();
                break;
            default:
                Set<String> userFacingTypes = IndexPolicy.BUILTIN_CHAR_FILTERS.stream()
                        .filter(t -> !t.equals("empty"))
                        .collect(Collectors.toSet());
                throw new DdlException("Unsupported char filter type: " + type
                        + ". Supported types: " + userFacingTypes);
        }
        validator.validate(properties);
    }

    public void dropIndexPolicy(boolean isIfExists, String indexPolicyName,
            IndexPolicyTypeEnum type) throws DdlException, AnalysisException {
        String normalizedName = normalizeKey(indexPolicyName);
        writeLock();
        try {
            IndexPolicy policyToDrop = nameToIndexPolicy.get(normalizedName);
            if (policyToDrop == null) {
                if (isIfExists) {
                    return;
                }
                throw new DdlException("index policy " + indexPolicyName + " does not exist");
            }
            if (policyToDrop.getType() != type) {
                throw new DdlException("Cannot drop " + policyToDrop.getType() + " policy '"
                        + indexPolicyName + "' by DROP " + type + " statement.");
            }
            if (policyToDrop.getType() == IndexPolicyTypeEnum.ANALYZER) {
                checkAnalyzerNotUsedByIndex(policyToDrop.getName());
            } else if (policyToDrop.getType() == IndexPolicyTypeEnum.NORMALIZER) {
                checkNormalizerNotUsedByIndex(policyToDrop.getName());
            }
            if (policyToDrop.getType() == IndexPolicyTypeEnum.TOKENIZER
                    || policyToDrop.getType() == IndexPolicyTypeEnum.TOKEN_FILTER
                    || policyToDrop.getType() == IndexPolicyTypeEnum.CHAR_FILTER) {
                checkPolicyNotReferenced(policyToDrop);
            }
            long id = policyToDrop.getId();
            idToIndexPolicy.remove(id);
            nameToIndexPolicy.remove(normalizedName);
            Env.getCurrentEnv().getEditLog().logDropIndexPolicy(new DropIndexPolicyLog(id));
        } finally {
            writeUnlock();
        }
        LOG.info("Drop index policy success: {}", indexPolicyName);
    }

    /**
     * Check if an analyzer is used by any inverted index.
     *
     * <p><b>PERFORMANCE WARNING:</b> This method performs a full scan of all databases,
     * tables, and indexes. In large-scale clusters with many tables, this can be slow.
     * Consider maintaining a reverse index (analyzer -> tables) if this becomes a bottleneck.
     *
     * @param analyzerName the analyzer name to check
     * @throws DdlException if the analyzer is in use by any index
     */
    private void checkAnalyzerNotUsedByIndex(String analyzerName) throws DdlException {
        String normalizedName = normalizeKey(analyzerName);
        List<Database> databases = Env.getCurrentEnv().getInternalCatalog().getDbs();
        for (Database db : databases) {
            List<Table> tables = db.getTables();
            for (Table table : tables) {
                if (table instanceof OlapTable) {
                    OlapTable olapTable = (OlapTable) table;
                    for (Index index : olapTable.getIndexes()) {
                        Map<String, String> properties = index.getProperties();
                        String indexAnalyzer = properties == null ? null
                                : properties.get(IndexPolicy.PROP_ANALYZER);
                        if (indexAnalyzer != null
                                && normalizedName.equals(normalizeKey(indexAnalyzer))) {
                            throw new DdlException("the analyzer " + analyzerName + " is used by index: "
                                    + index.getIndexName() + " in table: "
                                    + db.getFullName() + "." + table.getName());
                        }
                    }
                }
            }
        }
    }

    /**
     * Check if a normalizer is used by any inverted index.
     *
     * <p><b>PERFORMANCE WARNING:</b> This method performs a full scan of all databases,
     * tables, and indexes. In large-scale clusters with many tables, this can be slow.
     * Consider maintaining a reverse index (normalizer -> tables) if this becomes a bottleneck.
     *
     * @param normalizerName the normalizer name to check
     * @throws DdlException if the normalizer is in use by any index
     */
    private void checkNormalizerNotUsedByIndex(String normalizerName) throws DdlException {
        String normalizedName = normalizeKey(normalizerName);
        List<Database> databases = Env.getCurrentEnv().getInternalCatalog().getDbs();
        for (Database db : databases) {
            List<Table> tables = db.getTables();
            for (Table table : tables) {
                if (table instanceof OlapTable) {
                    OlapTable olapTable = (OlapTable) table;
                    for (Index index : olapTable.getIndexes()) {
                        Map<String, String> properties = index.getProperties();
                        String indexNormalizer = properties == null ? null
                                : properties.get(IndexPolicy.PROP_NORMALIZER);
                        if (indexNormalizer != null
                                && normalizedName.equals(normalizeKey(indexNormalizer))) {
                            throw new DdlException("the normalizer " + normalizerName + " is used by index: "
                                    + index.getIndexName() + " in table: "
                                    + db.getFullName() + "." + table.getName());
                        }
                    }
                }
            }
        }
    }

    private void checkPolicyNotReferenced(IndexPolicy policy) throws DdlException {
        String policyName = policy.getName();
        IndexPolicyTypeEnum policyType = policy.getType();

        for (IndexPolicy otherPolicy : idToIndexPolicy.values()) {
            IndexPolicyTypeEnum otherType = otherPolicy.getType();

            if (otherType != IndexPolicyTypeEnum.ANALYZER
                    && otherType != IndexPolicyTypeEnum.NORMALIZER) {
                continue;
            }

            Map<String, String> properties = otherPolicy.getProperties();
            if (policyType == IndexPolicyTypeEnum.TOKENIZER
                    && otherType == IndexPolicyTypeEnum.ANALYZER) {
                String tokenizer = properties.get(IndexPolicy.PROP_TOKENIZER);
                if (normalizeKey(policyName).equals(normalizeKey(tokenizer))) {
                    throw new DdlException("Cannot drop " + policyType + " policy '" + policyName
                            + "' as it is referenced by " + otherType + " policy '"
                            + otherPolicy.getName() + "'");
                }
            } else if (policyType == IndexPolicyTypeEnum.TOKEN_FILTER) {
                checkFilterReference(policyName, policyType, otherType, otherPolicy,
                        properties.get(IndexPolicy.PROP_TOKEN_FILTER));
            } else if (policyType == IndexPolicyTypeEnum.CHAR_FILTER) {
                checkFilterReference(policyName, policyType, otherType, otherPolicy,
                        properties.get(IndexPolicy.PROP_CHAR_FILTER));
            }
        }
    }

    private void checkFilterReference(String policyName, IndexPolicyTypeEnum policyType,
            IndexPolicyTypeEnum referencingType, IndexPolicy referencingPolicy,
            String filterList) throws DdlException {
        if (filterList != null && !filterList.isEmpty()) {
            for (String filter : filterList.split(",\\s*")) {
                if (normalizeKey(policyName).equals(normalizeKey(filter))) {
                    throw new DdlException("Cannot drop " + policyType + " policy '" + policyName
                            + "' as it is referenced by " + referencingType + " policy '"
                            + referencingPolicy.getName() + "'");
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

    public void replayCreateIndexPolicy(IndexPolicy indexPolicy) {
        writeLock();
        try {
            idToIndexPolicy.put(indexPolicy.getId(), indexPolicy);
            // Store with normalized key for case-insensitive lookup
            nameToIndexPolicy.put(normalizeKey(indexPolicy.getName()), indexPolicy);
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
            nameToIndexPolicy.remove(normalizeKey(indexPolicy.getName()));
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
        // Store with normalized key for case-insensitive lookup
        nameToIndexPolicy.clear();
        idToIndexPolicy.forEach(
                (id, indexPolicy) ->
                        nameToIndexPolicy.put(normalizeKey(indexPolicy.getName()), indexPolicy));
    }

}
