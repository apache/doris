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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.analysis.AccessPathInfo;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.rewrite.AccessPathExpressionCollector.CollectAccessPathResult;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TAccessPathType;
import org.apache.doris.thrift.TColumnAccessPaths;
import org.apache.doris.thrift.TColumnNameAccessPath;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * <li> 1. prune the data type of struct/map
 *
 * <p> for example, column s is a struct&lt;id: int, value: double&gt;,
 *     and `select struct(s, 'id') from tbl` will return the data type: `struct&lt;id: int&gt;`
 * </p>
 * </li>
 *
 * <li> 2. collect the access paths
 * <p> for example, select struct(s, 'id'), struct(s, 'data') from tbl` will collect the access path:
 *     [s.id, s.data]
 * </p>
 * </li>
 */
public class NestedColumnPruning implements CustomRewriter {
    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        StatementContext statementContext = jobContext.getCascadesContext().getStatementContext();
        SessionVariable sessionVariable = statementContext.getConnectContext().getSessionVariable();
        if (!sessionVariable.enablePruneNestedColumns || !statementContext.hasNestedColumns()) {
            return plan;
        }

        AccessPathPlanCollector collector = new AccessPathPlanCollector();
        Map<Slot, List<CollectAccessPathResult>> slotToAccessPaths = collector.collect(plan, statementContext);
        Map<Integer, AccessPathInfo> slotToResult = pruneDataType(slotToAccessPaths);

        if (!slotToResult.isEmpty()) {
            Map<Integer, AccessPathInfo> slotIdToPruneType = Maps.newLinkedHashMap();
            for (Entry<Integer, AccessPathInfo> kv : slotToResult.entrySet()) {
                Integer slotId = kv.getKey();
                AccessPathInfo accessPathInfo = kv.getValue();
                slotIdToPruneType.put(slotId, accessPathInfo);
            }
            SlotTypeReplacer typeReplacer = new SlotTypeReplacer(slotIdToPruneType);
            return plan.accept(typeReplacer, null);
        }
        return plan;
    }

    private static Map<Integer, AccessPathInfo> pruneDataType(
            Map<Slot, List<CollectAccessPathResult>> slotToAccessPaths) {
        Map<Integer, AccessPathInfo> result = new LinkedHashMap<>();
        Map<Slot, DataTypeAccessTree> slotIdToAllAccessTree = new LinkedHashMap<>();
        Map<Slot, DataTypeAccessTree> slotIdToPredicateAccessTree = new LinkedHashMap<>();

        Comparator<List<String>> pathComparator
                = Comparator.comparing(a -> StringUtils.join(a, "."));

        Multimap<Integer, List<String>> allAccessPaths = TreeMultimap.create(
                Comparator.naturalOrder(), pathComparator);
        Multimap<Integer, List<String>> predicateAccessPaths = TreeMultimap.create(
                Comparator.naturalOrder(), pathComparator);

        // first: build access data type tree
        for (Entry<Slot, List<CollectAccessPathResult>> kv : slotToAccessPaths.entrySet()) {
            Slot slot = kv.getKey();
            List<CollectAccessPathResult> collectAccessPathResults = kv.getValue();
            for (CollectAccessPathResult collectAccessPathResult : collectAccessPathResults) {
                List<String> path = collectAccessPathResult.getPath();
                DataTypeAccessTree allAccessTree = slotIdToAllAccessTree.computeIfAbsent(
                        slot, i -> DataTypeAccessTree.ofRoot(slot)
                );
                allAccessTree.setAccessByPath(path, 0);
                allAccessPaths.put(slot.getExprId().asInt(), path);

                if (collectAccessPathResult.isPredicate()) {
                    DataTypeAccessTree predicateAccessTree = slotIdToPredicateAccessTree.computeIfAbsent(
                            slot, i -> DataTypeAccessTree.ofRoot(slot)
                    );
                    predicateAccessTree.setAccessByPath(path, 0);
                    predicateAccessPaths.put(slot.getExprId().asInt(), path);
                }
            }
        }

        // second: build non-predicate access paths
        for (Entry<Slot, DataTypeAccessTree> kv : slotIdToAllAccessTree.entrySet()) {
            Slot slot = kv.getKey();
            DataTypeAccessTree accessTree = kv.getValue();
            DataType prunedDataType = accessTree.pruneDataType().orElse(slot.getDataType());

            List<TColumnNameAccessPath> allPaths = new ArrayList<>();
            for (List<String> path : allAccessPaths.get(slot.getExprId().asInt())) {
                if (path == null) {
                    throw new AnalysisException("This is a bug, please report this");
                }
                TColumnNameAccessPath tPath = new TColumnNameAccessPath();
                tPath.setPath(path);
                // only retain access the whole root
                if (path.size() == 1) {
                    allPaths = ImmutableList.of(tPath);
                    break;
                }
                allPaths.add(tPath);
            }
            TColumnAccessPaths allPath = new TColumnAccessPaths();
            allPath.type = TAccessPathType.NAME;
            allPath.setNameAccessPaths(allPaths);

            TColumnAccessPaths predicatePath = new TColumnAccessPaths();
            predicatePath.type = TAccessPathType.NAME;
            predicatePath.setNameAccessPaths(new ArrayList<>());
            result.put(slot.getExprId().asInt(), new AccessPathInfo(prunedDataType, allPath, predicatePath));
        }

        // third: build predicate access path
        for (Entry<Slot, DataTypeAccessTree> kv : slotIdToPredicateAccessTree.entrySet()) {
            Slot slot = kv.getKey();

            List<TColumnNameAccessPath> predicatePaths = new ArrayList<>();
            for (List<String> path : predicateAccessPaths.get(slot.getExprId().asInt())) {
                if (path == null) {
                    throw new AnalysisException("This is a bug, please report this");
                }
                TColumnNameAccessPath tPath = new TColumnNameAccessPath();
                tPath.setPath(path);
                // only retain access the whole root
                if (path.size() == 1) {
                    predicatePaths = ImmutableList.of(tPath);
                    break;
                }
                predicatePaths.add(tPath);
            }

            AccessPathInfo accessPathInfo = result.get(slot.getExprId().asInt());
            accessPathInfo.getPredicateAccessPaths().name_access_paths.addAll(predicatePaths);
        }

        return result;
    }

    private static class DataTypeAccessTree {
        private DataType type;
        private boolean isRoot;
        private boolean accessPartialChild;
        private boolean accessAll;
        private Map<String, DataTypeAccessTree> children = new LinkedHashMap<>();

        public DataTypeAccessTree(DataType type) {
            this(false, type);
        }

        public DataTypeAccessTree(boolean isRoot, DataType type) {
            this.isRoot = isRoot;
            this.type = type;
        }

        public void setAccessByPath(List<String> path, int accessIndex) {
            if (accessIndex >= path.size()) {
                accessAll = true;
                return;
            } else {
                accessPartialChild = true;
            }

            if (type.isStructType()) {
                String fieldName = path.get(accessIndex);
                DataTypeAccessTree child = children.get(fieldName);
                child.setAccessByPath(path, accessIndex + 1);
                return;
            } else if (type.isArrayType()) {
                DataTypeAccessTree child = children.get("*");
                if (path.get(accessIndex).equals("*")) {
                    // enter this array and skip next *
                    child.setAccessByPath(path, accessIndex + 1);
                }
                return;
            } else if (type.isMapType()) {
                String fieldName = path.get(accessIndex);
                if (fieldName.equals("*")) {
                    // access value by the key, so we should access key and access value, then prune the value's type.
                    // e.g. map_column['id'] should access the keys, and access the values
                    DataTypeAccessTree keysChild = children.get("KEYS");
                    DataTypeAccessTree valuesChild = children.get("VALUES");
                    keysChild.accessAll = true;
                    valuesChild.setAccessByPath(path, accessIndex + 1);
                    return;
                } else if (fieldName.equals("KEYS")) {
                    // only access the keys and not need enter keys, because it must be primitive type.
                    // e.g. map_keys(map_column)
                    DataTypeAccessTree keysChild = children.get("KEYS");
                    keysChild.accessAll = true;
                    return;
                } else if (fieldName.equals("VALUES")) {
                    // only access the values without keys, and maybe prune the value's data type.
                    // e.g. map_values(map_columns)[0] will access the array of values first,
                    //      and then access the array, so the access path is ['VALUES', '*']
                    DataTypeAccessTree valuesChild = children.get("VALUES");
                    valuesChild.setAccessByPath(path, accessIndex + 1);
                    return;
                }
            } else if (isRoot) {
                children.get(path.get(accessIndex)).setAccessByPath(path, accessIndex + 1);
                return;
            }
            throw new AnalysisException("unsupported data type: " + type);
        }

        public static DataTypeAccessTree ofRoot(Slot slot) {
            DataTypeAccessTree child = of(slot.getDataType());
            DataTypeAccessTree root = new DataTypeAccessTree(true, NullType.INSTANCE);
            root.children.put(slot.getName(), child);
            return root;
        }

        public static DataTypeAccessTree of(DataType type) {
            DataTypeAccessTree root = new DataTypeAccessTree(type);
            if (type instanceof StructType) {
                StructType structType = (StructType) type;
                for (Entry<String, StructField> kv : structType.getNameToFields().entrySet()) {
                    root.children.put(kv.getKey(), of(kv.getValue().getDataType()));
                }
            } else if (type instanceof ArrayType) {
                root.children.put("*", of(((ArrayType) type).getItemType()));
            } else if (type instanceof MapType) {
                root.children.put("KEYS", of(((MapType) type).getKeyType()));
                root.children.put("VALUES", of(((MapType) type).getValueType()));
            }
            return root;
        }

        public Optional<DataType> pruneDataType() {
            if (isRoot) {
                return children.values().iterator().next().pruneDataType();
            } else if (accessAll) {
                return Optional.of(type);
            } else if (!accessPartialChild) {
                return Optional.empty();
            }

            List<Pair<String, DataType>> accessedChildren = new ArrayList<>();

            if (type instanceof StructType) {
                for (Entry<String, DataTypeAccessTree> kv : children.entrySet()) {
                    DataTypeAccessTree childTypeTree = kv.getValue();
                    Optional<DataType> childDataType = childTypeTree.pruneDataType();
                    if (childDataType.isPresent()) {
                        accessedChildren.add(Pair.of(kv.getKey(), childDataType.get()));
                    }
                }
            } else if (type instanceof ArrayType) {
                Optional<DataType> childDataType = children.get("*").pruneDataType();
                if (childDataType.isPresent()) {
                    accessedChildren.add(Pair.of("*", childDataType.get()));
                }
            } else if (type instanceof MapType) {
                DataType prunedValueType = children.get("VALUES")
                        .pruneDataType()
                        .orElse(((MapType) type).getValueType());
                // can not prune keys but can prune values
                accessedChildren.add(Pair.of("KEYS", ((MapType) type).getKeyType()));
                accessedChildren.add(Pair.of("VALUES", prunedValueType));
            }
            if (accessedChildren.isEmpty()) {
                return Optional.of(type);
            }

            return Optional.of(pruneDataType(type, accessedChildren));
        }

        private DataType pruneDataType(DataType dataType, List<Pair<String, DataType>> newChildrenTypes) {
            if (dataType instanceof StructType) {
                // prune struct fields
                StructType structType = (StructType) dataType;
                Map<String, StructField> nameToFields = structType.getNameToFields();
                List<StructField> newFields = new ArrayList<>();
                for (Pair<String, DataType> kv : newChildrenTypes) {
                    String fieldName = kv.first;
                    StructField originField = nameToFields.get(fieldName);
                    DataType prunedType = kv.second;
                    newFields.add(new StructField(
                            originField.getName(), prunedType, originField.isNullable(), originField.getComment()
                    ));
                }
                return new StructType(newFields);
            } else if (dataType instanceof ArrayType) {
                return ArrayType.of(newChildrenTypes.get(0).second, ((ArrayType) dataType).containsNull());
            } else if (dataType instanceof MapType) {
                return MapType.of(newChildrenTypes.get(0).second, newChildrenTypes.get(1).second);
            } else {
                throw new AnalysisException("unsupported data type: " + dataType);
            }
        }
    }
}
