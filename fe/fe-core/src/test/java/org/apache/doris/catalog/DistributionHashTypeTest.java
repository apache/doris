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

package org.apache.doris.catalog;

import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.HashDistributionInfo.HashType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.List;
import java.util.Map;

// Tests for the pluggable bucketing hash function carried by the `distribution_hash_type` table
// property. Today HashType has CRC32 (default/legacy) and IDENTITY; more types will be added later,
// so the framework-level cases (gson round-trip, equals, property parse) iterate over
// HashType.values() and stay correct as new constants appear. Identity-specific cases verify that
// canonical bytes from every valid distribution-column type and multiple columns are accepted.
public class DistributionHashTypeTest {

    private Column intCol(String name) {
        return new Column(name, PrimitiveType.BIGINT, true);
    }

    // ------------------------------------------------------------------
    // Metadata / backward compatibility
    // ------------------------------------------------------------------

    @Test
    public void testLegacyConstructorsDefaultToCrc32() {
        Assert.assertEquals(HashType.CRC32, new HashDistributionInfo().getHashType());
        Assert.assertEquals(HashType.CRC32,
                new HashDistributionInfo(8, Lists.newArrayList(intCol("id"))).getHashType());
        Assert.assertEquals(HashType.CRC32,
                new HashDistributionInfo(8, false, Lists.newArrayList(intCol("id"))).getHashType());
    }

    @Test
    public void testLegacyMetadataWithoutHashTypeDeserializesToCrc32() {
        // Metadata written before hashType existed has no "hashType" key; gson leaves it null and
        // getHashType() must fall back to CRC32 so old tables keep their historical bucket layout.
        HashDistributionInfo original
                = new HashDistributionInfo(8, false, Lists.newArrayList(intCol("id")), HashType.CRC32);
        String json = GsonUtils.GSON.toJson(original);
        String legacyJson = json.replaceAll(",?\\s*\"hashType\"\\s*:\\s*\"[A-Z0-9_]+\"", "");
        Assert.assertFalse(legacyJson.contains("hashType"));
        HashDistributionInfo restored = GsonUtils.GSON.fromJson(legacyJson, HashDistributionInfo.class);
        Assert.assertEquals(HashType.CRC32, restored.getHashType());
    }

    @Test
    public void testHashTypeSurvivesGsonRoundTrip() {
        // Framework-level: every hash type must round-trip. Adding a new HashType automatically
        // extends this coverage.
        for (HashType type : HashType.values()) {
            HashDistributionInfo original = new HashDistributionInfo(8, false, Lists.newArrayList(intCol("id")), type);
            HashDistributionInfo restored
                    = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(original), HashDistributionInfo.class);
            Assert.assertEquals("hashType lost in gson round trip: " + type, type, restored.getHashType());
        }
    }

    @Test
    public void testEqualityAndHashCodeConsiderHashType() {
        // Any two distinct hash types must make otherwise-identical infos unequal.
        HashType[] types = HashType.values();
        for (int i = 0; i < types.length; i++) {
            HashDistributionInfo a = new HashDistributionInfo(8, false, Lists.newArrayList(intCol("id")), types[i]);
            HashDistributionInfo aSame = new HashDistributionInfo(8, false, Lists.newArrayList(intCol("id")), types[i]);
            Assert.assertEquals(a, aSame);
            Assert.assertEquals(a.hashCode(), aSame.hashCode());
            for (int j = i + 1; j < types.length; j++) {
                HashDistributionInfo b = new HashDistributionInfo(8, false, Lists.newArrayList(intCol("id")), types[j]);
                Assert.assertNotEquals(a, b);
            }
        }
    }

    @Test
    public void testToDistributionDescCarriesHashType() throws DdlException {
        // toDistributionDesc() is used when a partition deep-copies the table distribution
        // (dynamic partition / addMultiPartitions); the hashType must ride along. Verify by
        // round-tripping desc back to info (HashDistributionDesc has no getter).
        for (HashType type : HashType.values()) {
            List<Column> columns = Lists.newArrayList(intCol("id"));
            HashDistributionInfo info = new HashDistributionInfo(8, false, columns, type);
            DistributionDesc desc = info.toDistributionDesc();
            Assert.assertTrue(desc instanceof HashDistributionDesc);
            HashDistributionInfo rebuilt = (HashDistributionInfo) desc.toDistributionInfo(columns);
            Assert.assertEquals(type, rebuilt.getHashType());
        }
    }

    @Test
    public void testSetHashTypeInheritedByAddPartition() {
        // ADD PARTITION with an explicit DISTRIBUTED BY builds a CRC32 info, then
        // InternalCatalog.addPartition overwrites hashType with the table's. Verify the setter path.
        HashDistributionInfo partition
                = new HashDistributionInfo(8, false, Lists.newArrayList(intCol("id")), HashType.CRC32);
        Assert.assertEquals(HashType.CRC32, partition.getHashType());
        partition.setHashType(HashType.IDENTITY);
        Assert.assertEquals(HashType.IDENTITY, partition.getHashType());
    }

    // ------------------------------------------------------------------
    // Property parsing
    // ------------------------------------------------------------------

    @Test
    public void testAnalyzeDistributionHashType() throws AnalysisException {
        // missing property -> CRC32
        Assert.assertEquals(HashType.CRC32, PropertyAnalyzer.analyzeDistributionHashType(null));
        Assert.assertEquals(HashType.CRC32, PropertyAnalyzer.analyzeDistributionHashType(Maps.newHashMap()));

        // every hash type parses case-insensitively and the property is consumed (removed) so it is
        // not later flagged as an unknown property.
        for (HashType type : HashType.values()) {
            Map<String, String> props = Maps.newHashMap();
            props.put(PropertyAnalyzer.PROPERTIES_DISTRIBUTION_HASH_TYPE, mixCase(type.name()));
            Assert.assertEquals(type, PropertyAnalyzer.analyzeDistributionHashType(props));
            Assert.assertFalse(props.containsKey(PropertyAnalyzer.PROPERTIES_DISTRIBUTION_HASH_TYPE));
        }
    }

    @Test
    public void testAnalyzeDistributionHashTypeInvalidValueThrows() {
        Map<String, String> bad = Maps.newHashMap();
        bad.put(PropertyAnalyzer.PROPERTIES_DISTRIBUTION_HASH_TYPE, "murmur3");
        AnalysisException e
                = Assert.assertThrows(AnalysisException.class, () -> PropertyAnalyzer.analyzeDistributionHashType(bad));
        Assert.assertTrue(e.getMessage().contains(PropertyAnalyzer.PROPERTIES_DISTRIBUTION_HASH_TYPE));
    }

    // ------------------------------------------------------------------
    // identity accepts canonical bytes from all valid distribution columns
    // ------------------------------------------------------------------

    @Test
    public void testToDistributionInfoIdentitySingleIntegerColumn() throws DdlException {
        List<Column> schema = Lists.newArrayList(intCol("shard_num"), new Column("v", PrimitiveType.INT, false));
        HashDistributionDesc desc
                = new HashDistributionDesc(8, false, Lists.newArrayList("shard_num"), HashType.IDENTITY);
        HashDistributionInfo info = (HashDistributionInfo) desc.toDistributionInfo(schema);
        Assert.assertEquals(HashType.IDENTITY, info.getHashType());
        Assert.assertEquals(1, info.getDistributionColumns().size());
    }

    @Test
    public void testToDistributionInfoIdentityAllowsLargeInt() throws DdlException {
        List<Column> schema = Lists.newArrayList(new Column("big_id", PrimitiveType.LARGEINT, true));
        HashDistributionDesc desc = new HashDistributionDesc(8, false, Lists.newArrayList("big_id"), HashType.IDENTITY);
        HashDistributionInfo info = (HashDistributionInfo) desc.toDistributionInfo(schema);
        Assert.assertEquals(HashType.IDENTITY, info.getHashType());
    }

    @Test
    public void testToDistributionInfoIdentityAllowsNonIntegerColumn() throws DdlException {
        List<Column> schema = Lists.newArrayList(new Column("s", PrimitiveType.VARCHAR, true));
        HashDistributionDesc desc = new HashDistributionDesc(8, false, Lists.newArrayList("s"), HashType.IDENTITY);
        HashDistributionInfo info = (HashDistributionInfo) desc.toDistributionInfo(schema);
        Assert.assertEquals(HashType.IDENTITY, info.getHashType());
        Assert.assertEquals(PrimitiveType.VARCHAR,
                info.getDistributionColumns().get(0).getType().getPrimitiveType());
    }

    @Test
    public void testToDistributionInfoIdentityAllowsMultipleColumns() throws DdlException {
        List<Column> schema = Lists.newArrayList(intCol("a"), new Column("b", PrimitiveType.VARCHAR, true));
        HashDistributionDesc desc = new HashDistributionDesc(8, false, Lists.newArrayList("a", "b"),
                HashType.IDENTITY);
        HashDistributionInfo info = (HashDistributionInfo) desc.toDistributionInfo(schema);
        Assert.assertEquals(HashType.IDENTITY, info.getHashType());
        Assert.assertEquals(2, info.getDistributionColumns().size());
    }

    @Test
    public void testToDistributionInfoCrc32AllowsNonIntegerAndMultiColumn() throws DdlException {
        // crc32 (default) keeps its historical freedom: multi-column and non-integer are fine.
        List<Column> schema = Lists.newArrayList(new Column("a", PrimitiveType.VARCHAR, true), intCol("b"));
        HashDistributionDesc desc = new HashDistributionDesc(8, false, Lists.newArrayList("a", "b"), HashType.CRC32);
        HashDistributionInfo info = (HashDistributionInfo) desc.toDistributionInfo(schema);
        Assert.assertEquals(HashType.CRC32, info.getHashType());
        Assert.assertEquals(2, info.getDistributionColumns().size());
    }

    // ------------------------------------------------------------------
    // ColocateGroupSchema: hashType participates in colocate compatibility and metadata
    // ------------------------------------------------------------------

    private ColocateGroupSchema schemaWith(HashType type) {
        return new ColocateGroupSchema(new GroupId(1L, 2L), Lists.newArrayList(intCol("id")), 8,
                new ReplicaAllocation((short) 1), type);
    }

    @Test
    public void testCheckDistributionAllowsSameHashType() throws DdlException {
        // A table whose distribution hashType matches the group's must pass checkDistribution.
        for (HashType type : HashType.values()) {
            ColocateGroupSchema schema = schemaWith(type);
            HashDistributionInfo info = new HashDistributionInfo(8, false, Lists.newArrayList(intCol("id")), type);
            schema.checkDistribution(info); // should not throw
        }
    }

    @Test
    public void testCheckDistributionRejectsDifferentHashType() {
        // Mixing hash types inside one colocate group would break co-location, so it must be
        // rejected before the buckets-num / column checks even when those are identical.
        HashType[] types = HashType.values();
        for (int i = 0; i < types.length; i++) {
            for (int j = 0; j < types.length; j++) {
                if (i == j) {
                    continue;
                }
                ColocateGroupSchema schema = schemaWith(types[i]);
                HashDistributionInfo info
                        = new HashDistributionInfo(8, false, Lists.newArrayList(intCol("id")), types[j]);
                Assert.assertThrows(DdlException.class, () -> schema.checkDistribution(info));
            }
        }
    }

    @Test
    public void testWritableRoundTripPreservesHashType() throws Exception {
        // With a current-version journal, write() appends the hashType name and readFields() must
        // restore it verbatim for every hash type.
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_141);
        metaContext.setThreadLocalInfo();
        try {
            for (HashType type : HashType.values()) {
                ColocateGroupSchema original = schemaWith(type);
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                original.write(new DataOutputStream(bos));
                ColocateGroupSchema restored
                        = ColocateGroupSchema.read(new DataInputStream(new ByteArrayInputStream(bos.toByteArray())));
                Assert.assertEquals("hashType lost in Writable round trip: " + type, type, restored.getHashType());
                Assert.assertEquals(8, restored.getBucketsNum());
            }
        } finally {
            MetaContext.remove();
        }
    }

    @Test
    public void testReadFieldsBeforeVersion141FallsBackToCrc32() throws Exception {
        // Metadata streams written before VERSION_141 have no trailing hashType token. Simulate an
        // old reader (journal version < 141) so readFields must skip that read and fall back to
        // CRC32 to keep legacy colocate groups on their historical bucket layout.
        MetaContext writeContext = new MetaContext();
        writeContext.setMetaVersion(FeMetaVersion.VERSION_141);
        writeContext.setThreadLocalInfo();
        byte[] bytes;
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            schemaWith(HashType.CRC32).write(new DataOutputStream(bos));
            bytes = bos.toByteArray();
        } finally {
            MetaContext.remove();
        }
        // Now read with an old journal version: readFields must NOT consume any hashType token and
        // returns CRC32 regardless of what trailing bytes exist.
        MetaContext readContext = new MetaContext();
        readContext.setMetaVersion(FeMetaVersion.VERSION_140);
        readContext.setThreadLocalInfo();
        try {
            ColocateGroupSchema restored
                    = ColocateGroupSchema.read(new DataInputStream(new ByteArrayInputStream(bytes)));
            Assert.assertEquals(HashType.CRC32, restored.getHashType());
        } finally {
            MetaContext.remove();
        }
    }

    @Test
    public void testGetHashTypeNullFallsBackToCrc32() {
        // Legacy gson metadata has no "hashType" field; getHashType() must not NPE and defaults to
        // CRC32, matching HashDistributionInfo's fallback.
        ColocateGroupSchema schema = schemaWith(HashType.IDENTITY);
        String json = GsonUtils.GSON.toJson(schema);
        String legacyJson = json.replaceAll(",?\\s*\"hashType\"\\s*:\\s*\"[A-Z0-9_]+\"", "");
        Assert.assertFalse(legacyJson.contains("hashType"));
        ColocateGroupSchema restored = GsonUtils.GSON.fromJson(legacyJson, ColocateGroupSchema.class);
        Assert.assertEquals(HashType.CRC32, restored.getHashType());
    }

    // Alternate the case of each character so the parse path is exercised case-insensitively
    // regardless of which hash type name it is.
    private String mixCase(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            sb.append((i & 1) == 0
                    ? Character.toUpperCase(c)
                    : Character.toLowerCase(c));
        }
        return sb.toString();
    }
}
