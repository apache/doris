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

import org.apache.doris.nereids.exceptions.AnalysisException;

import java.util.Map;

public class AnnIndexPropertiesChecker {
    public static void checkProperties(Map<String, String> properties) {
        String type = null;
        String metric = null;
        String dim = null;
        for (String key : properties.keySet()) {
            switch (key) {
                case "index_type":
                    type = properties.get(key);
                    if (!type.equals("hnsw")) {
                        throw new AnalysisException("only support ann index with type hnsw, got: " + type);
                    }
                    break;
                case "metric_type":
                    metric = properties.get(key);
                    if (!metric.equals("l2_distance") && !metric.equals("inner_product")) {
                        throw new AnalysisException(
                                "only support ann index with metric l2_distance or inner_product, got: " + metric);
                    }
                    break;
                case "dim":
                    dim = properties.get(key);
                    try {
                        int dimension = Integer.parseInt(dim);
                        if (dimension <= 0) {
                            throw new AnalysisException("dim of ann index must be a positive integer, got: " + dim);
                        }
                    } catch (NumberFormatException e) {
                        throw new AnalysisException("dim of ann index must be a positive integer, got: " + dim);
                    }
                    break;
                case "max_degree":
                    String maxDegree = properties.get(key);
                    try {
                        int degree = Integer.parseInt(maxDegree);
                        if (degree <= 0) {
                            throw new AnalysisException(
                                    "max_degree of ann index must be a positive integer, got: " + maxDegree);
                        }
                    } catch (NumberFormatException e) {
                        throw new AnalysisException(
                                "max_degree of ann index must be a positive integer, got: " + maxDegree);
                    }
                    break;
                case "ef_construction":
                    String efConstruction = properties.get(key);
                    try {
                        int ef = Integer.parseInt(efConstruction);
                        if (ef <= 0) {
                            throw new AnalysisException(
                                    "ef_construction of ann index must be a positive integer, got: " + efConstruction);
                        }
                    } catch (NumberFormatException e) {
                        throw new AnalysisException(
                                "ef_construction of ann index must be a positive integer, got: " + efConstruction);
                    }
                    break;
                case "quantizer":
                    String quantizer = properties.get(key);
                    if (!quantizer.equals("flat") && !quantizer.equals("sq4") && !quantizer.equals("sq8")) {
                        throw new AnalysisException(
                                "only support ann index with quantizer flat, sq4 or sq8, got: " + quantizer);
                    }
                    break;
                default:
                    throw new AnalysisException("unknown ann index property: " + key);
            }
        }

        if (type == null) {
            throw new AnalysisException("index_type of ann index be specified.");
        }
        if (metric == null) {
            throw new AnalysisException("metric_type of ann index must be specified.");
        }
        if (dim == null) {
            throw new AnalysisException("dim of ann index must be specified");
        }
    }
}
