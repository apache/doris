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

package org.apache.doris.backup;

import org.apache.doris.backup.RestoreFileMapping.IdChain;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * This is a memory structure mapping the job info file in repository.
 * It contains all content of a job info file.
 * It also be used to save the info of a restore job, such as alias of table and meta info file path
 */
public class BackupJobInfo implements Writable {
    private static final Logger LOG = LogManager.getLogger(BackupJobInfo.class);

    public String name;
    public String dbName;
    public long dbId;
    public long backupTime;
    public Map<String, BackupTableInfo> tables = Maps.newHashMap();
    public boolean success;

    public int metaVersion;

    // This map is used to save the table alias mapping info when processing a restore job.
    // origin -> alias
    public Map<String, String> tblAlias = Maps.newHashMap();

    public boolean containsTbl(String tblName) {
        return tables.containsKey(tblName);
    }

    public BackupTableInfo getTableInfo(String tblName) {
        return tables.get(tblName);
    }

    public void retainTables(Set<String> tblNames) {
        Iterator<Map.Entry<String, BackupTableInfo>> iter = tables.entrySet().iterator();
        while (iter.hasNext()) {
            if (!tblNames.contains(iter.next().getKey())) {
                iter.remove();
            }
        }
    }

    public void setAlias(String orig, String alias) {
        tblAlias.put(orig, alias);
    }

    public String getAliasByOriginNameIfSet(String orig) {
        return tblAlias.containsKey(orig) ? tblAlias.get(orig) : orig;
    }

    public String getOrginNameByAlias(String alias) {
        for (Map.Entry<String, String> entry : tblAlias.entrySet()) {
            if (entry.getValue().equals(alias)) {
                return entry.getKey();
            }
        }
        return alias;
    }

    public static class BackupTableInfo {
        public String name;
        public long id;
        public Map<String, BackupPartitionInfo> partitions = Maps.newHashMap();

        public boolean containsPart(String partName) {
            return partitions.containsKey(partName);
        }

        public BackupPartitionInfo getPartInfo(String partName) {
            return partitions.get(partName);
        }

        public void retainPartitions(Collection<String> partNames) {
            if (partNames == null || partNames.isEmpty()) {
                // retain all
                return;
            }
            Iterator<Map.Entry<String, BackupPartitionInfo>> iter = partitions.entrySet().iterator();
            while (iter.hasNext()) {
                if (!partNames.contains(iter.next().getKey())) {
                    iter.remove();
                }
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("name: ").append(name).append(", id: ").append(id);
            sb.append(", partitions: [").append(Joiner.on(", ").join(partitions.keySet())).append("]");
            return sb.toString();
        }
    }

    public static class BackupPartitionInfo {
        public String name;
        public long id;
        public long version;
        public long versionHash;
        public Map<String, BackupIndexInfo> indexes = Maps.newHashMap();

        public BackupIndexInfo getIdx(String idxName) {
            return indexes.get(idxName);
        }
    }

    public static class BackupIndexInfo {
        public String name;
        public long id;
        public int schemaHash;
        public List<BackupTabletInfo> tablets = Lists.newArrayList();

        public BackupTabletInfo getTablet(long tabletId) {
            for (BackupTabletInfo backupTabletInfo : tablets) {
                if (backupTabletInfo.id == tabletId) {
                    return backupTabletInfo;
                }
            }
            return null;
        }
    }

    public static class BackupTabletInfo {
        public long id;
        public List<String> files = Lists.newArrayList();
    }

    // eg: __db_10001/__tbl_10002/__part_10003/__idx_10002/__10004
    public String getFilePath(String db, String tbl, String part, String idx, long tabletId) {
        if (!db.equalsIgnoreCase(dbName)) {
            LOG.debug("db name does not equal: {}-{}", dbName, db);
            return null;
        }

        BackupTableInfo tblInfo = tables.get(tbl);
        if (tblInfo == null) {
            LOG.debug("tbl {} does not exist", tbl);
            return null;
        }

        BackupPartitionInfo partInfo = tblInfo.getPartInfo(part);
        if (partInfo == null) {
            LOG.debug("part {} does not exist", part);
            return null;
        }

        BackupIndexInfo idxInfo = partInfo.getIdx(idx);
        if (idxInfo == null) {
            LOG.debug("idx {} does not exist", idx);
            return null;
        }

        List<String> pathSeg = Lists.newArrayList();
        pathSeg.add(Repository.PREFIX_DB + dbId);
        pathSeg.add(Repository.PREFIX_TBL + tblInfo.id);
        pathSeg.add(Repository.PREFIX_PART + partInfo.id);
        pathSeg.add(Repository.PREFIX_IDX + idxInfo.id);
        pathSeg.add(Repository.PREFIX_COMMON + tabletId);

        return Joiner.on("/").join(pathSeg);
    }

    // eg: __db_10001/__tbl_10002/__part_10003/__idx_10002/__10004
    public String getFilePath(IdChain ids) {
        List<String> pathSeg = Lists.newArrayList();
        pathSeg.add(Repository.PREFIX_DB + dbId);
        pathSeg.add(Repository.PREFIX_TBL + ids.getTblId());
        pathSeg.add(Repository.PREFIX_PART + ids.getPartId());
        pathSeg.add(Repository.PREFIX_IDX + ids.getIdxId());
        pathSeg.add(Repository.PREFIX_COMMON + ids.getTabletId());

        return Joiner.on("/").join(pathSeg);
    }

    public static BackupJobInfo fromCatalog(long backupTime, String label, String dbName, long dbId,
            Collection<Table> tbls, Map<Long, SnapshotInfo> snapshotInfos) {

        BackupJobInfo jobInfo = new BackupJobInfo();
        jobInfo.backupTime = backupTime;
        jobInfo.name = label;
        jobInfo.dbName = dbName;
        jobInfo.dbId = dbId;
        jobInfo.success = true;
        jobInfo.metaVersion = FeConstants.meta_version;

        // tbls
        for (Table tbl : tbls) {
            OlapTable olapTbl = (OlapTable) tbl;
            BackupTableInfo tableInfo = new BackupTableInfo();
            tableInfo.id = tbl.getId();
            tableInfo.name = tbl.getName();
            jobInfo.tables.put(tableInfo.name, tableInfo);
            // partitions
            for (Partition partition : olapTbl.getPartitions()) {
                BackupPartitionInfo partitionInfo = new BackupPartitionInfo();
                partitionInfo.id = partition.getId();
                partitionInfo.name = partition.getName();
                partitionInfo.version = partition.getVisibleVersion();
                partitionInfo.versionHash = partition.getVisibleVersionHash();
                tableInfo.partitions.put(partitionInfo.name, partitionInfo);
                // indexes
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    BackupIndexInfo idxInfo = new BackupIndexInfo();
                    idxInfo.id = index.getId();
                    idxInfo.name = olapTbl.getIndexNameById(index.getId());
                    idxInfo.schemaHash = olapTbl.getSchemaHashByIndexId(index.getId());
                    partitionInfo.indexes.put(idxInfo.name, idxInfo);
                    // tablets
                    for (Tablet tablet : index.getTablets()) {
                        BackupTabletInfo tabletInfo = new BackupTabletInfo();
                        tabletInfo.id = tablet.getId();
                        tabletInfo.files.addAll(snapshotInfos.get(tablet.getId()).getFiles());
                        idxInfo.tablets.add(tabletInfo);
                    }
                }
            }
        }

        return jobInfo;
    }

    public static BackupJobInfo fromFile(String path) throws IOException {
        byte[] bytes = Files.readAllBytes(Paths.get(path));
        String json = new String(bytes, StandardCharsets.UTF_8);
        BackupJobInfo jobInfo = new BackupJobInfo();
        genFromJson(json, jobInfo);
        return jobInfo;
    }

    private static void genFromJson(String json, BackupJobInfo jobInfo) {
        /* parse the json string: 
         * {
         *   "backup_time": 1522231864000,
         *   "name": "snapshot1",
         *   "database": "db1"
         *   "id": 10000
         *   "backup_result": "succeed",
         *   "meta_version" : 40 // this is optional
         *   "backup_objects": {
         *       "table1": {
         *           "partitions": {
         *               "partition2": {
         *                   "indexes": {
         *                       "rollup1": {
         *                           "id": 10009,
         *                           "schema_hash": 3473401,
         *                           "tablets": {
         *                               "10008": ["__10029_seg1.dat", "__10029_seg2.dat"],
         *                               "10007": ["__10029_seg1.dat", "__10029_seg2.dat"]
         *                           },
         *                           "tablets_order": ["10029", "10030"]
         *                       },
         *                       "table1": {
         *                           "id": 10008,
         *                           "schema_hash": 9845021,
         *                           "tablets": {
         *                               "10004": ["__10027_seg1.dat", "__10027_seg2.dat"],
         *                               "10005": ["__10028_seg1.dat", "__10028_seg2.dat"]
         *                           },
         *                           "tablets_order": ["10027, "10028"]
         *                       }
         *                   },
         *                   "id": 10007
         *                   "version": 10
         *                   "version_hash": 1273047329538
         *               },
         *           },
         *           "id": 10001
         *       }
         *   }
         * }
         */
        JSONObject root = new JSONObject(json);
        jobInfo.name = (String) root.get("name");
        jobInfo.dbName = (String) root.get("database");
        jobInfo.dbId = root.getLong("id");
        jobInfo.backupTime = root.getLong("backup_time");
        
        try {
            jobInfo.metaVersion = root.getInt("meta_version");
        } catch (JSONException e) {
            // meta_version does not exist
            jobInfo.metaVersion = FeConstants.meta_version;
        }
        
        JSONObject backupObjs = root.getJSONObject("backup_objects");
        String[] tblNames = JSONObject.getNames(backupObjs);
        for (String tblName : tblNames) {
            BackupTableInfo tblInfo = new BackupTableInfo();
            tblInfo.name = tblName;
            JSONObject tbl = backupObjs.getJSONObject(tblName);
            tblInfo.id = tbl.getLong("id");
            JSONObject parts = tbl.getJSONObject("partitions");
            String[] partsNames = JSONObject.getNames(parts);
            for (String partName : partsNames) {
                BackupPartitionInfo partInfo = new BackupPartitionInfo();
                partInfo.name = partName;
                JSONObject part = parts.getJSONObject(partName);
                partInfo.id = part.getLong("id");
                partInfo.version = part.getLong("version");
                partInfo.versionHash = part.getLong("version_hash");
                JSONObject indexes = part.getJSONObject("indexes");
                String[] indexNames = JSONObject.getNames(indexes);
                for (String idxName : indexNames) {
                    BackupIndexInfo indexInfo = new BackupIndexInfo();
                    indexInfo.name = idxName;
                    JSONObject idx = indexes.getJSONObject(idxName);
                    indexInfo.id = idx.getLong("id");
                    indexInfo.schemaHash = idx.getInt("schema_hash");
                    JSONObject tablets = idx.getJSONObject("tablets");
                    String[] tabletIds = JSONObject.getNames(tablets);

                    JSONArray tabletsOrder = null;
                    if (idx.has("tablets_order")) {
                        tabletsOrder = idx.getJSONArray("tablets_order");
                    }
                    String[] orderedTabletIds = sortTabletIds(tabletIds, tabletsOrder);
                    Preconditions.checkState(tabletIds.length == orderedTabletIds.length);

                    for (String tabletId : orderedTabletIds) {
                        BackupTabletInfo tabletInfo = new BackupTabletInfo();
                        tabletInfo.id = Long.valueOf(tabletId);
                        JSONArray files = tablets.getJSONArray(tabletId);
                        for (Object object : files) {
                            tabletInfo.files.add((String) object);
                        }
                        indexInfo.tablets.add(tabletInfo);
                    }
                    partInfo.indexes.put(indexInfo.name, indexInfo);
                }
                tblInfo.partitions.put(partName, partInfo);
            }
            jobInfo.tables.put(tblName, tblInfo);
        }
        
        String result = root.getString("backup_result");
        if (result.equals("succeed")) {
            jobInfo.success = true;
        } else {
            jobInfo.success = false;
        }
    }

    private static String[] sortTabletIds(String[] tabletIds, JSONArray tabletsOrder) {
        if (tabletsOrder == null || tabletsOrder.toList().isEmpty()) {
            // in previous version, we are not saving tablets order(which was a BUG),
            // so we have to sort the tabletIds to restore the original order of tablets.
            List<String> tmpList = Lists.newArrayList(tabletIds);
            tmpList.sort((o1, o2) -> Long.valueOf(o1).compareTo(Long.valueOf(o2)));
            return tmpList.toArray(new String[0]);
        } else {
            return (String[]) tabletsOrder.toList().toArray(new String[0]);
        }
    }

    public void writeToFile(File jobInfoFile) throws FileNotFoundException {
        PrintWriter printWriter = new PrintWriter(jobInfoFile);
        try {
            printWriter.print(toJson(true).toString());
            printWriter.flush();
        } finally {
            printWriter.close();
        }
    }

    // Only return basic info, table and partitions
    public String getBrief() {
        return toJson(false).toString(1);
    }

    public JSONObject toJson(boolean verbose) {
        JSONObject root = new JSONObject();
        root.put("name", name);
        root.put("database", dbName);
        if (verbose) {
            root.put("id", dbId);
        }
        root.put("backup_time", backupTime);
        JSONObject backupObj = new JSONObject();
        root.put("backup_objects", backupObj);
        root.put("meta_version", FeConstants.meta_version);
        
        for (BackupTableInfo tblInfo : tables.values()) {
            JSONObject tbl = new JSONObject();
            if (verbose) {
                tbl.put("id", tblInfo.id);
            }
            JSONObject parts = new JSONObject();
            tbl.put("partitions", parts);
            for (BackupPartitionInfo partInfo : tblInfo.partitions.values()) {
                JSONObject part = new JSONObject();
                if (verbose) {
                    part.put("id", partInfo.id);
                    part.put("version", partInfo.version);
                    part.put("version_hash", partInfo.versionHash);
                    JSONObject indexes = new JSONObject();
                    part.put("indexes", indexes);
                    for (BackupIndexInfo idxInfo : partInfo.indexes.values()) {
                        JSONObject idx = new JSONObject();
                        idx.put("id", idxInfo.id);
                        idx.put("schema_hash", idxInfo.schemaHash);
                        JSONObject tablets = new JSONObject();
                        JSONArray tabletsOrder = new JSONArray();
                        idx.put("tablets", tablets);
                        for (BackupTabletInfo tabletInfo : idxInfo.tablets) {
                            JSONArray files = new JSONArray();
                            tablets.put(String.valueOf(tabletInfo.id), files);
                            for (String fileName : tabletInfo.files) {
                                files.put(fileName);
                            }
                            // to save the order of tablets
                            tabletsOrder.put(String.valueOf(tabletInfo.id));
                        }
                        indexes.put(idxInfo.name, idx);
                    }
                }
                parts.put(partInfo.name, part);
            }
            backupObj.put(tblInfo.name, tbl);
        }

        root.put("backup_result", "succeed");
        return root;
    }

    public String toString(int indentFactor) {
        return toJson(true).toString(indentFactor);
    }

    public String getInfo() {
        List<String> objs = Lists.newArrayList();
        for (BackupTableInfo tblInfo : tables.values()) {
            StringBuilder sb = new StringBuilder();
            sb.append(tblInfo.name);
            List<String> partNames = tblInfo.partitions.values().stream()
                    .filter(n -> !n.name.equals(tblInfo.name)).map(n -> n.name).collect(Collectors.toList());
            if (!partNames.isEmpty()) {
                sb.append(" PARTITIONS [").append(Joiner.on(", ").join(partNames)).append("]");
            }
            objs.add(sb.toString());
        }
        return Joiner.on(", ").join(objs);
    }

    public static BackupJobInfo read(DataInput in) throws IOException {
        BackupJobInfo jobInfo = new BackupJobInfo();
        jobInfo.readFields(in);
        return jobInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, toJson(true).toString());
        out.writeInt(tblAlias.size());
        for (Map.Entry<String, String> entry : tblAlias.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        String json = Text.readString(in);
        genFromJson(json, this);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String tbl = Text.readString(in);
            String alias = Text.readString(in);
            tblAlias.put(tbl, alias);
        }
    }

    @Override
    public String toString() {
        return toJson(true).toString();
    }
}

