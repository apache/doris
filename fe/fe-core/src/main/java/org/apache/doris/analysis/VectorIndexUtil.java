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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class VectorIndexUtil {

    public static final int INDEX_FAMILY_FAISS = 0;
    public static final int INDEX_FAMILY_SPARSE = 1;

    public static String VECTOR_INDEX_INDEX_TYPE_KEY = "index_type";
    public static String VECTOR_INDEX_METRIC_TYPE_KEY = "metric_type";
    public static String VECTOR_INDEX_DIM_KEY = "dim";
    public static String VECTOR_INDEX_IS_VECTOR_NORMED_KEY = "is_vector_normed";
    public static String VECTOR_INDEX_M_KEY = "M";
    public static String VECTOR_INDEX_EFCONSTRUCTION_KEY = "efConstruction";
    public static String VECTOR_INDEX_EFSEARCH_KEY = "efSearch";
    public static String VECTOR_NLIST_KEY = "nlist";
    public static String VECTOR_NBITS_KEY = "nbits";
    public static String VECTOR_DROP_RATIO_SEARCH_KEY = "drop_ratio_search";
    public static String VECTOR_INDEX_PQM_KEY = "pqM";
    public static String VECTOR_INDEX_NPROBE_KEY = "nprobe";

    public static String DEFAULT_NBITS_VALUE = "8";

    public static String VECTOR_INDEX_METRIC_TYPE_COSINE_SIMILARITY = "cosine_similarity";
    public static String VECTOR_INDEX_METRIC_TYPE_INNER_PRODUCT = "inner_product";
    public static String VECTOR_INDEX_METRIC_TYPE_L2_DISTANCE = "l2_distance";

    public static void checkIndexProperties(Map<String, String> properties, int indexFamily) throws AnalysisException {
        Set<String> allowedKeys = new HashSet<>(Arrays.asList(
                VECTOR_INDEX_INDEX_TYPE_KEY,
                VECTOR_INDEX_METRIC_TYPE_KEY,
                VECTOR_INDEX_DIM_KEY,
                VECTOR_INDEX_IS_VECTOR_NORMED_KEY,
                VECTOR_INDEX_M_KEY,
                VECTOR_INDEX_EFCONSTRUCTION_KEY,
                VECTOR_INDEX_EFSEARCH_KEY,
                VECTOR_NLIST_KEY,
                VECTOR_NBITS_KEY,
                VECTOR_DROP_RATIO_SEARCH_KEY,
                VECTOR_INDEX_PQM_KEY,
                VECTOR_INDEX_NPROBE_KEY
        ));

        for (String key : properties.keySet()) {
            if (!allowedKeys.contains(key)) {
                throw new AnalysisException("Invalid vector index property key: " + key);
            }
        }

        String indexType = properties.get(VECTOR_INDEX_INDEX_TYPE_KEY);
        String metricType = properties.get(VECTOR_INDEX_METRIC_TYPE_KEY);
        String dim = properties.get(VECTOR_INDEX_DIM_KEY);
        String isVectorNormed = properties.get(VECTOR_INDEX_IS_VECTOR_NORMED_KEY);
        String m = properties.get(VECTOR_INDEX_M_KEY);
        // hnsw
        String efConstruction = properties.get(VECTOR_INDEX_EFCONSTRUCTION_KEY);
        String efSearch = properties.get(VECTOR_INDEX_EFSEARCH_KEY);
        String pqM = properties.get(VECTOR_INDEX_PQM_KEY);
        // ivfpq
        String nlist = properties.get(VECTOR_NLIST_KEY);
        String nbits = properties.get(VECTOR_NBITS_KEY);
        String nprobe = properties.get(VECTOR_INDEX_NPROBE_KEY);
        // sparse_wand
        String dropRatioSearch = properties.get(VECTOR_DROP_RATIO_SEARCH_KEY);

        if (indexFamily == INDEX_FAMILY_FAISS && indexType != null && !indexType.matches("hnsw|ivfpq|hnswpq")) {
            throw new AnalysisException("Invalid vector index 'index_type' value: " + indexType
                + ", index_type must be hnsw, ivfpq or hnswpq for array column.");
        } else if (indexFamily == INDEX_FAMILY_SPARSE && indexType != null && !indexType.matches("sparse_wand")) {
            throw new AnalysisException("Invalid vector index 'index_type' value: " + indexType
                + ", index_type must be sparse_wand for map column.");
        }

        if (indexType.equals("hnsw")) {
            if (m == null || efConstruction == null || efSearch == null) {
                throw new AnalysisException("Invalid vector index 'index_type' value: " + indexType
                    + ", index_type must have m, efConstruction and efSearch properties");
            }
        } else if (indexType.equals("ivfpq")) {
            if (m == null || nlist == null || nprobe == null || dim == null) {
                throw new AnalysisException("Invalid vector index 'index_type' value: " + indexType
                    + ", index_type must have m, nlist, nprobe and dim properties");
            }
            if (nbits == null) {
                properties.put(VECTOR_NBITS_KEY, DEFAULT_NBITS_VALUE);
            }
        } else if (indexType.equals("sparse_wand")) {
            if (dropRatioSearch == null) {
                throw new AnalysisException("Invalid vector index 'index_type' value: " + indexType
                    + ", index_type must have drop_ratio_search properties");
            }
        } else if (indexType.equals("hnswpq")) {
            if (m == null || efConstruction == null || efSearch == null || dim == null || pqM == null) {
                throw new AnalysisException("Invalid vector index 'index_type' value: " + indexType
                    + ", index_type must have m, efConstruction, efSearch, dim and pqM properties");
            }
        }

        if (metricType != null) {
            if (indexType.equals("hnsw") && !metricType.matches("l2_distance|cosine_similarity|inner_product")) {
                throw new AnalysisException("Invalid vector index 'metric_type' value: " + metricType
                    + ", metric_type must be l2_distance, cosine_similarity or inner_product with index_type hnsw");
            }

            if (indexType.equals("ivfpq") && !metricType.matches("l2_distance|cosine_similarity|inner_product")) {
                throw new AnalysisException("Invalid vector index 'metric_type' value: " + metricType
                    + ", metric_type must be l2_distance, cosine_distance or inner_product with index_type ivfpq");
            }

            if (indexType.equals("sparse_wand") && !metricType.matches("inner_product")) {
                throw new AnalysisException("Invalid vector index 'metric_type' value: " + metricType
                    + ", metric_type must be inner_product with index_type sparse_wand");
            }
        }

        int dimValue = 0;
        if (dim != null) {
            try {
                dimValue = Integer.parseInt(dim);
                if (dimValue <= 0) {
                    throw new AnalysisException("Invalid vector index 'dim' value: " + dimValue
                        + ", dim must be positive");
                }
            } catch (NumberFormatException e) {
                throw new AnalysisException(
                    "Invalid vector index 'dim' value, dim must be integer");
            }
        }

        if (isVectorNormed != null && !isVectorNormed.matches("true|false")) {
            throw new AnalysisException(
                "Invalid vector index 'is_vector_normed' value: " + isVectorNormed
                    + ", is_vector_normed must be true or false");
        }

        if (m != null) {
            try {
                int mValue = Integer.parseInt(m);
                if (mValue <= 0) {
                    throw new AnalysisException("Invalid vector index 'M' value: " + mValue
                        + ", M must be positive");
                }
                if (indexType.equals("ivfpq")) {
                    if (dimValue % mValue != 0) {
                        throw new AnalysisException("Invalid vector index param: The dim parameter needs to be "
                            + "divisible by the M parameter. M: " + m + ", dim: " + dim);
                    }
                }
            } catch (NumberFormatException e) {
                throw new AnalysisException(
                    "Invalid vector index 'M' value, M must be integer");
            }
        }

        if (efConstruction != null) {
            try {
                int efConstructionValue = Integer.parseInt(efConstruction);
                if (efConstructionValue <= 0) {
                    throw new AnalysisException("Invalid vector index 'efConstruction' value: " + efConstructionValue
                        + ", efConstruction must be positive");
                }
            } catch (NumberFormatException e) {
                throw new AnalysisException(
                    "Invalid vector index 'efConstruction' value, efConstruction must be integer");
            }
        }

        if (efSearch != null) {
            try {
                int efSearchValue = Integer.parseInt(efSearch);
                if (efSearchValue <= 0) {
                    throw new AnalysisException("Invalid vector index 'efSearch' value: " + efSearchValue
                        + ", efSearch must be positive");
                }
            } catch (NumberFormatException e) {
                throw new AnalysisException(
                    "Invalid vector index 'efSearch' value, efSearch must be integer");
            }
        }

        if (nlist != null) {
            try {
                int nlistValue = Integer.parseInt(nlist);
                if (nlistValue <= 0) {
                    throw new AnalysisException("Invalid vector index 'nlist' value: " + nlistValue
                        + ", nlist must be positive");
                }
            } catch (NumberFormatException e) {
                throw new AnalysisException(
                    "Invalid vector index 'nlist' value, nlist must be integer");
            }
        }

        if (nbits != null) {
            try {
                int nbitsValue = Integer.parseInt(nbits);
                if (nbitsValue <= 0) {
                    throw new AnalysisException("Invalid vector index 'nbits' value: " + nbits
                        + ", nbits must be positive");
                }
            } catch (NumberFormatException e) {
                throw new AnalysisException(
                    "Invalid vector index 'nbits' value, nbits must be integer");
            }
        }

        if (dropRatioSearch != null) {
            try {
                float dropRatioSearchValue = Float.parseFloat(dropRatioSearch);
                if (dropRatioSearchValue < 0 || dropRatioSearchValue > 1) {
                    throw new AnalysisException("Invalid vector index 'drop_ratio_search' value: "
                        + dropRatioSearchValue + ", drop_ratio_build must be within the range of 0 to 1");
                }
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid vector index 'drop_ratio_search' value, drop_ratio_search must be"
                    + "within the range of 0 to 1");
            }
        }

        if (pqM != null) {
            try {
                int pqMValue = Integer.parseInt(pqM);
                if (pqMValue <= 0) {
                    throw new AnalysisException("Invalid vector index 'pqM' value: " + pqMValue
                        + ", pqM must be positive");
                }
                if (indexType.equals("hnswpq")) {
                    if (dimValue % pqMValue != 0) {
                        throw new AnalysisException("Invalid vector index param: The dim parameter needs to be "
                            + "divisible by the pqM parameter. pqM: " + pqM + ", dim: " + dim);
                    }
                }
            } catch (NumberFormatException e) {
                throw new AnalysisException(
                    "Invalid vector index 'pqM' value, pqM must be integer");
            }
        }

        if (nprobe != null) {
            try {
                int nprobeValue = Integer.parseInt(nprobe);
                if (nprobeValue <= 0) {
                    throw new AnalysisException("Invalid vector index 'nprobe' value: " + nprobeValue
                        + ", nprobe must be positive");
                }
            } catch (NumberFormatException e) {
                throw new AnalysisException(
                    "Invalid vector index 'nprobe' value, nprobe must be integer");
            }
        }
    }
}
