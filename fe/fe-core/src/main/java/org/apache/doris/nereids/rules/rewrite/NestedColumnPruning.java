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
import org.apache.doris.nereids.trees.expressions.SlotReference;
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
import org.apache.doris.thrift.TColumnAccessPath;
import org.apache.doris.thrift.TDataAccessPath;
import org.apache.doris.thrift.TMetaAccessPath;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    public static final Logger LOG = LogManager.getLogger(NestedColumnPruning.class);

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        try {
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
                return new SlotTypeReplacer(slotIdToPruneType, plan).replace();
            }
            return plan;
        } catch (Throwable t) {
            LOG.warn("NestedColumnPruning failed.", t);
            return plan;
        }
    }

    private static Map<Integer, AccessPathInfo> pruneDataType(
            Map<Slot, List<CollectAccessPathResult>> slotToAccessPaths) {
        Map<Integer, AccessPathInfo> result = new LinkedHashMap<>();
        Map<Slot, DataTypeAccessTree> slotIdToAllAccessTree = new LinkedHashMap<>();
        Map<Slot, DataTypeAccessTree> slotIdToPredicateAccessTree = new LinkedHashMap<>();

        Comparator<Pair<TAccessPathType, List<String>>> pathComparator = Comparator.comparing(
                l -> StringUtils.join(l.second, "."));

        Multimap<Integer, Pair<TAccessPathType, List<String>>> allAccessPaths = TreeMultimap.create(
                Comparator.naturalOrder(), pathComparator);
        Multimap<Integer, Pair<TAccessPathType, List<String>>> predicateAccessPaths = TreeMultimap.create(
                Comparator.naturalOrder(), pathComparator);

        // first: build access data type tree
        for (Entry<Slot, List<CollectAccessPathResult>> kv : slotToAccessPaths.entrySet()) {
            Slot slot = kv.getKey();
            List<CollectAccessPathResult> collectAccessPathResults = kv.getValue();
            for (CollectAccessPathResult collectAccessPathResult : collectAccessPathResults) {
                List<String> path = collectAccessPathResult.getPath();
                TAccessPathType pathType = collectAccessPathResult.getType();
                DataTypeAccessTree allAccessTree = slotIdToAllAccessTree.computeIfAbsent(
                        slot, i -> DataTypeAccessTree.ofRoot(slot, pathType)
                );
                allAccessTree.setAccessByPath(path, 0, pathType);
                allAccessPaths.put(slot.getExprId().asInt(), Pair.of(pathType, path));

                if (collectAccessPathResult.isPredicate()) {
                    DataTypeAccessTree predicateAccessTree = slotIdToPredicateAccessTree.computeIfAbsent(
                            slot, i -> DataTypeAccessTree.ofRoot(slot, pathType)
                    );
                    predicateAccessTree.setAccessByPath(path, 0, pathType);
                    predicateAccessPaths.put(
                            slot.getExprId().asInt(), Pair.of(pathType, path)
                    );
                }
            }
        }

        // second: build non-predicate access paths
        for (Entry<Slot, DataTypeAccessTree> kv : slotIdToAllAccessTree.entrySet()) {
            Slot slot = kv.getKey();
            DataTypeAccessTree accessTree = kv.getValue();
            DataType prunedDataType = accessTree.pruneDataType().orElse(slot.getDataType());

            List<TColumnAccessPath> allPaths = new ArrayList<>();
            boolean accessWholeColumn = false;
            TAccessPathType accessWholeColumnType = TAccessPathType.META;
            for (Pair<TAccessPathType, List<String>> pathInfo : allAccessPaths.get(slot.getExprId().asInt())) {
                if (pathInfo.first == TAccessPathType.DATA) {
                    TDataAccessPath dataAccessPath = new TDataAccessPath();
                    dataAccessPath.setPath(new ArrayList<>(pathInfo.second));
                    TColumnAccessPath accessPath = new TColumnAccessPath(TAccessPathType.DATA);
                    accessPath.setDataAccessPath(dataAccessPath);
                    allPaths.add(accessPath);
                } else {
                    TMetaAccessPath dataAccessPath = new TMetaAccessPath();
                    dataAccessPath.setPath(new ArrayList<>(pathInfo.second));
                    TColumnAccessPath accessPath = new TColumnAccessPath(TAccessPathType.META);
                    accessPath.setMetaAccessPath(dataAccessPath);
                    allPaths.add(accessPath);
                }
                // only retain access the whole root
                if (pathInfo.second.size() == 1) {
                    accessWholeColumn = true;
                    if (pathInfo.first == TAccessPathType.DATA) {
                        accessWholeColumnType = TAccessPathType.DATA;
                    }
                } else {
                    accessWholeColumnType = TAccessPathType.DATA;
                }
            }
            if (accessWholeColumn) {
                TColumnAccessPath accessPath = new TColumnAccessPath(accessWholeColumnType);
                SlotReference slotReference = (SlotReference) kv.getKey();
                String wholeColumnName = slotReference.getOriginalColumn().get().getName();
                if (accessWholeColumnType == TAccessPathType.DATA) {
                    accessPath.setDataAccessPath(new TDataAccessPath(ImmutableList.of(wholeColumnName)));
                } else {
                    accessPath.setMetaAccessPath(new TMetaAccessPath(ImmutableList.of(wholeColumnName)));
                }
                allPaths = ImmutableList.of(accessPath);
            }
            result.put(slot.getExprId().asInt(), new AccessPathInfo(prunedDataType, allPaths, new ArrayList<>()));
        }

        // third: build predicate access path
        for (Entry<Slot, DataTypeAccessTree> kv : slotIdToPredicateAccessTree.entrySet()) {
            Slot slot = kv.getKey();

            List<TColumnAccessPath> predicatePaths = new ArrayList<>();
            boolean accessWholeColumn = false;
            TAccessPathType accessWholeColumnType = TAccessPathType.META;
            for (Pair<TAccessPathType, List<String>> pathInfo : predicateAccessPaths.get(slot.getExprId().asInt())) {
                if (pathInfo == null) {
                    throw new AnalysisException("This is a bug, please report this");
                }

                if (pathInfo.first == TAccessPathType.DATA) {
                    TDataAccessPath dataAccessPath = new TDataAccessPath();
                    dataAccessPath.setPath(new ArrayList<>(pathInfo.second));
                    TColumnAccessPath accessPath = new TColumnAccessPath(TAccessPathType.DATA);
                    accessPath.setDataAccessPath(dataAccessPath);
                    predicatePaths.add(accessPath);
                } else {
                    TMetaAccessPath dataAccessPath = new TMetaAccessPath();
                    dataAccessPath.setPath(new ArrayList<>(pathInfo.second));
                    TColumnAccessPath accessPath = new TColumnAccessPath(TAccessPathType.META);
                    accessPath.setMetaAccessPath(dataAccessPath);
                    predicatePaths.add(accessPath);
                }
                // only retain access the whole root
                if (pathInfo.second.size() == 1) {
                    accessWholeColumn = true;
                    if (pathInfo.first == TAccessPathType.DATA) {
                        accessWholeColumnType = TAccessPathType.DATA;
                    }
                } else {
                    accessWholeColumnType = TAccessPathType.DATA;
                }
            }

            if (accessWholeColumn) {
                TColumnAccessPath accessPath = new TColumnAccessPath(accessWholeColumnType);
                SlotReference slotReference = (SlotReference) kv.getKey();
                String wholeColumnName = slotReference.getOriginalColumn().get().getName();
                if (accessWholeColumnType == TAccessPathType.DATA) {
                    accessPath.setDataAccessPath(new TDataAccessPath(ImmutableList.of(wholeColumnName)));
                } else {
                    accessPath.setMetaAccessPath(new TMetaAccessPath(ImmutableList.of(wholeColumnName)));
                }
                predicatePaths = ImmutableList.of(accessPath);
            }

            AccessPathInfo accessPathInfo = result.get(slot.getExprId().asInt());
            accessPathInfo.getPredicateAccessPaths().addAll(predicatePaths);
        }

        return result;
    }

    /** DataTypeAccessTree */
    public static class DataTypeAccessTree {
        // type of this level
        private DataType type;
        // is the root column?
        private boolean isRoot;
        // if access 's.a.b' the node 's' and 'a' has accessPartialChild, and node 'b' has accessAll
        private boolean accessPartialChild;
        private boolean accessAll;
        // for the future, only access the meta of the column,
        // e.g. `is not null` can only access the column's offset, not need to read the data
        private TAccessPathType pathType;
        // the children of the column, for example, column s is `struct<a:int, b:int>`,
        // then node 's' has two children: 'a' and 'b', and the key is the column name
        private Map<String, DataTypeAccessTree> children = new LinkedHashMap<>();

        public DataTypeAccessTree(DataType type, TAccessPathType pathType) {
            this(false, type, pathType);
        }

        public DataTypeAccessTree(boolean isRoot, DataType type, TAccessPathType pathType) {
            this.isRoot = isRoot;
            this.type = type;
            this.pathType = pathType;
        }

        public DataType getType() {
            return type;
        }

        public boolean isRoot() {
            return isRoot;
        }

        public boolean isAccessPartialChild() {
            return accessPartialChild;
        }

        public boolean isAccessAll() {
            return accessAll;
        }

        public TAccessPathType getPathType() {
            return pathType;
        }

        public Map<String, DataTypeAccessTree> getChildren() {
            return children;
        }

        /** pruneCastType */
        public DataType pruneCastType(DataTypeAccessTree origin, DataTypeAccessTree cast) {
            if (type instanceof StructType) {
                Map<String, String> nameMapping = new LinkedHashMap<>();
                List<String> castNames = new ArrayList<>(cast.children.keySet());
                int i = 0;
                for (String s : origin.children.keySet()) {
                    nameMapping.put(s, castNames.get(i++));
                }
                List<StructField> mappingFields = new ArrayList<>();
                StructType originCastStructType = (StructType) cast.type;
                for (Entry<String, DataTypeAccessTree> kv : children.entrySet()) {
                    String originName = kv.getKey();
                    String mappingName = nameMapping.getOrDefault(originName, originName);
                    DataTypeAccessTree originPrunedTree = kv.getValue();
                    DataType mappingType = originPrunedTree.pruneCastType(
                            origin.children.get(originName),
                            cast.children.get(mappingName)
                    );
                    StructField originCastField = originCastStructType.getField(mappingName);
                    mappingFields.add(
                            new StructField(mappingName, mappingType,
                                    originCastField.isNullable(), originCastField.getComment())
                    );
                }
                return new StructType(mappingFields);
            } else if (type instanceof ArrayType) {
                return ArrayType.of(
                        children.values().iterator().next().pruneCastType(
                                origin.children.values().iterator().next(),
                                cast.children.values().iterator().next()
                        ),
                        ((ArrayType) cast.type).containsNull()
                );
            } else if (type instanceof MapType) {
                return MapType.of(
                        children.get(AccessPathInfo.ACCESS_MAP_KEYS)
                                .pruneCastType(
                                        origin.children.get(AccessPathInfo.ACCESS_MAP_KEYS),
                                        cast.children.get(AccessPathInfo.ACCESS_MAP_KEYS)
                                ),
                        children.get(AccessPathInfo.ACCESS_MAP_VALUES)
                                .pruneCastType(
                                        origin.children.get(AccessPathInfo.ACCESS_MAP_VALUES),
                                        cast.children.get(AccessPathInfo.ACCESS_MAP_VALUES)
                                )
                );
            } else {
                return cast.type;
            }
        }

        /** replacePathByAnotherTree */
        public boolean replacePathByAnotherTree(DataTypeAccessTree cast, List<String> path, int index) {
            if (index >= path.size()) {
                return true;
            }
            if (cast.type instanceof StructType) {
                List<StructField> fields = ((StructType) cast.type).getFields();
                for (int i = 0; i < fields.size(); i++) {
                    String castFieldName = path.get(index);
                    if (fields.get(i).getName().equalsIgnoreCase(castFieldName)) {
                        String originFieldName = ((StructType) type).getFields().get(i).getName();
                        path.set(index, originFieldName);
                        return children.get(originFieldName).replacePathByAnotherTree(
                                cast.children.get(castFieldName), path, index + 1
                        );
                    }
                }
            } else if (cast.type instanceof ArrayType) {
                return children.values().iterator().next().replacePathByAnotherTree(
                        cast.children.values().iterator().next(), path, index + 1);
            } else if (cast.type instanceof MapType) {
                String fieldName = path.get(index);
                return children.get(AccessPathInfo.ACCESS_MAP_VALUES).replacePathByAnotherTree(
                        cast.children.get(fieldName), path, index + 1
                );
            }
            return false;
        }

        /** setAccessByPath */
        public void setAccessByPath(List<String> path, int accessIndex, TAccessPathType pathType) {
            if (accessIndex >= path.size()) {
                accessAll = true;
                return;
            } else {
                accessPartialChild = true;
            }

            if (pathType == TAccessPathType.DATA) {
                this.pathType = TAccessPathType.DATA;
            }

            if (this.type.isStructType()) {
                String fieldName = path.get(accessIndex).toLowerCase();
                DataTypeAccessTree child = children.get(fieldName);
                if (child != null) {
                    child.setAccessByPath(path, accessIndex + 1, pathType);
                } else {
                    // can not find the field
                    accessAll = true;
                }
                return;
            } else if (this.type.isArrayType()) {
                DataTypeAccessTree child = children.get(AccessPathInfo.ACCESS_ALL);
                if (path.get(accessIndex).equals(AccessPathInfo.ACCESS_ALL)) {
                    // enter this array and skip next *
                    child.setAccessByPath(path, accessIndex + 1, pathType);
                }
                return;
            } else if (this.type.isMapType()) {
                String fieldName = path.get(accessIndex);
                if (fieldName.equals(AccessPathInfo.ACCESS_ALL)) {
                    // access value by the key, so we should access key and access value, then prune the value's type.
                    // e.g. map_column['id'] should access the keys, and access the values
                    DataTypeAccessTree keysChild = children.get(AccessPathInfo.ACCESS_MAP_KEYS);
                    DataTypeAccessTree valuesChild = children.get(AccessPathInfo.ACCESS_MAP_VALUES);
                    keysChild.accessAll = true;
                    valuesChild.setAccessByPath(path, accessIndex + 1, pathType);
                    return;
                } else if (fieldName.equals(AccessPathInfo.ACCESS_MAP_KEYS)) {
                    // only access the keys and not need enter keys, because it must be primitive type.
                    // e.g. map_keys(map_column)
                    DataTypeAccessTree keysChild = children.get(AccessPathInfo.ACCESS_MAP_KEYS);
                    keysChild.accessAll = true;
                    return;
                } else if (fieldName.equals(AccessPathInfo.ACCESS_MAP_VALUES)) {
                    // only access the values without keys, and maybe prune the value's data type.
                    // e.g. map_values(map_columns)[0] will access the array of values first,
                    //      and then access the array, so the access path is ['VALUES', '*']
                    DataTypeAccessTree valuesChild = children.get(AccessPathInfo.ACCESS_MAP_VALUES);
                    valuesChild.setAccessByPath(path, accessIndex + 1, pathType);
                    return;
                }
            } else if (isRoot) {
                children.get(path.get(accessIndex).toLowerCase()).setAccessByPath(path, accessIndex + 1, pathType);
                return;
            }
            throw new AnalysisException("unsupported data type: " + this.type);
        }

        public static DataTypeAccessTree ofRoot(Slot slot, TAccessPathType pathType) {
            DataTypeAccessTree child = of(slot.getDataType(), pathType);
            DataTypeAccessTree root = new DataTypeAccessTree(true, NullType.INSTANCE, pathType);
            root.children.put(slot.getName().toLowerCase(), child);
            return root;
        }

        /** of */
        public static DataTypeAccessTree of(DataType type, TAccessPathType pathType) {
            DataTypeAccessTree root = new DataTypeAccessTree(type, pathType);
            if (type instanceof StructType) {
                StructType structType = (StructType) type;
                for (Entry<String, StructField> kv : structType.getNameToFields().entrySet()) {
                    root.children.put(kv.getKey().toLowerCase(), of(kv.getValue().getDataType(), pathType));
                }
            } else if (type instanceof ArrayType) {
                root.children.put(AccessPathInfo.ACCESS_ALL, of(((ArrayType) type).getItemType(), pathType));
            } else if (type instanceof MapType) {
                root.children.put(AccessPathInfo.ACCESS_MAP_KEYS, of(((MapType) type).getKeyType(), pathType));
                root.children.put(AccessPathInfo.ACCESS_MAP_VALUES, of(((MapType) type).getValueType(), pathType));
            }
            return root;
        }

        /** pruneDataType */
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
                Optional<DataType> childDataType = children.get(AccessPathInfo.ACCESS_ALL).pruneDataType();
                if (childDataType.isPresent()) {
                    accessedChildren.add(Pair.of(AccessPathInfo.ACCESS_ALL, childDataType.get()));
                }
            } else if (type instanceof MapType) {
                DataType prunedValueType = children.get(AccessPathInfo.ACCESS_MAP_VALUES)
                        .pruneDataType()
                        .orElse(((MapType) type).getValueType());
                // can not prune keys but can prune values
                accessedChildren.add(Pair.of(AccessPathInfo.ACCESS_MAP_KEYS, ((MapType) type).getKeyType()));
                accessedChildren.add(Pair.of(AccessPathInfo.ACCESS_MAP_VALUES, prunedValueType));
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
