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

import java.net.InetAddress
import java.net.NetworkInterface
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import org.apache.doris.regression.action.ProfileAction

suite("test_local_tvf_iceberg_variant", "p0,external") {
    List<List<Object>> backends = sql """ show backends """
    assertTrue(backends.size() > 0)

    def dataFilePath = context.config.dataPath + "/external_table_p0/tvf/"
    def beId = backends[0][0]
    def outFilePath = "/"
    def unshreddedData = "${dataFilePath}/iceberg_variant_unshredded.parquet"
    def shreddedData = "${dataFilePath}/iceberg_variant_shredded.parquet"
    def typedOnlyData = "${dataFilePath}/iceberg_variant_typed_only.parquet"
    def temporalUnshreddedData = "${dataFilePath}/iceberg_variant_temporal_unshredded.parquet"
    def temporalTypedData = "${dataFilePath}/iceberg_variant_temporal_typed.parquet"
    def binaryUnshreddedData = "${dataFilePath}/iceberg_variant_binary_unshredded.parquet"
    def binaryTypedData = "${dataFilePath}/iceberg_variant_binary_typed.parquet"

    def localHosts = ["localhost", "127.0.0.1", InetAddress.localHost.hostAddress, InetAddress.localHost.hostName] as Set
    NetworkInterface.networkInterfaces.each { networkInterface ->
        networkInterface.inetAddresses.each { inetAddress ->
            localHosts.add(inetAddress.hostAddress)
            localHosts.add(inetAddress.hostName)
        }
    }

    def dorisHome = new File(context.config.dataPath).parentFile.parentFile
    def localBeHome = new File(dorisHome, "output/be")
    def localJdbc = context.config.jdbcUrl.contains("127.0.0.1") || context.config.jdbcUrl.contains("localhost")
    if (localJdbc && backends.size() == 1 && localHosts.contains(backends[0][1]) && localBeHome.exists()) {
        outFilePath = ""
        Files.copy(new File(unshreddedData).toPath(), new File(localBeHome, "iceberg_variant_unshredded.parquet").toPath(),
                StandardCopyOption.REPLACE_EXISTING)
        Files.copy(new File(shreddedData).toPath(), new File(localBeHome, "iceberg_variant_shredded.parquet").toPath(),
                StandardCopyOption.REPLACE_EXISTING)
        Files.copy(new File(typedOnlyData).toPath(), new File(localBeHome, "iceberg_variant_typed_only.parquet").toPath(),
                StandardCopyOption.REPLACE_EXISTING)
        Files.copy(new File(temporalUnshreddedData).toPath(), new File(localBeHome, "iceberg_variant_temporal_unshredded.parquet").toPath(),
                StandardCopyOption.REPLACE_EXISTING)
        Files.copy(new File(temporalTypedData).toPath(), new File(localBeHome, "iceberg_variant_temporal_typed.parquet").toPath(),
                StandardCopyOption.REPLACE_EXISTING)
        Files.copy(new File(binaryUnshreddedData).toPath(), new File(localBeHome, "iceberg_variant_binary_unshredded.parquet").toPath(),
                StandardCopyOption.REPLACE_EXISTING)
        Files.copy(new File(binaryTypedData).toPath(), new File(localBeHome, "iceberg_variant_binary_typed.parquet").toPath(),
                StandardCopyOption.REPLACE_EXISTING)
    } else {
        for (List<Object> backend : backends) {
            def beHost = backend[1]
            scpFiles("root", beHost, unshreddedData, outFilePath, false)
            scpFiles("root", beHost, shreddedData, outFilePath, false)
            scpFiles("root", beHost, typedOnlyData, outFilePath, false)
            scpFiles("root", beHost, temporalUnshreddedData, outFilePath, false)
            scpFiles("root", beHost, temporalTypedData, outFilePath, false)
            scpFiles("root", beHost, binaryUnshreddedData, outFilePath, false)
            scpFiles("root", beHost, binaryTypedData, outFilePath, false)
        }
    }

    def unshredded = outFilePath + "iceberg_variant_unshredded.parquet"
    def shredded = outFilePath + "iceberg_variant_shredded.parquet"
    def typedOnly = outFilePath + "iceberg_variant_typed_only.parquet"
    def temporalUnshredded = outFilePath + "iceberg_variant_temporal_unshredded.parquet"
    def temporalTyped = outFilePath + "iceberg_variant_temporal_typed.parquet"
    def binaryUnshredded = outFilePath + "iceberg_variant_binary_unshredded.parquet"
    def binaryTyped = outFilePath + "iceberg_variant_binary_typed.parquet"
    def profileAction = new ProfileAction(context)
    def getProfileByToken = { token ->
        for (int i = 0; i < 60; ++i) {
            List profileData = profileAction.getProfileList()
            for (final def profileItem in profileData) {
                if (profileItem["Sql Statement"].toString().contains(token)) {
                    def profileText = profileAction.getProfile(profileItem["Profile ID"].toString()).toString()
                    if (profileText.contains("ParquetReadColumnPaths")) {
                        return profileText
                    }
                }
            }
            Thread.sleep(1000)
        }
        assertTrue(false)
    }
    def getParquetReadColumnPathSet = { profileText ->
        def parquetReadColumnPaths = profileText.readLines().find { it.contains("ParquetReadColumnPaths") }
        assertTrue(parquetReadColumnPaths != null)
        logger.info("Iceberg variant shredding ${parquetReadColumnPaths}")
        def separatorIndex = parquetReadColumnPaths.indexOf(":")
        assertTrue(separatorIndex >= 0)
        return parquetReadColumnPaths.substring(separatorIndex + 1)
                .split(",")
                .collect { it.trim() }
                .findAll { !it.isEmpty() } as Set
    }
    def getProfileCounter = { profileText, counterName ->
        def counterLine = profileText.readLines().find { it.contains(counterName) }
        assertTrue(counterLine != null)
        def matcher = counterLine =~ /${counterName}:\s*([0-9,]+)/
        assertTrue(matcher.find())
        return matcher.group(1).replace(",", "").toLong()
    }

    qt_desc_unshredded """
        desc function local(
            "file_path" = "${unshredded}",
            "backend_id" = "${beId}",
            "format" = "parquet")
    """

    qt_desc_typed_only """
        desc function local(
            "file_path" = "${typedOnly}",
            "backend_id" = "${beId}",
            "format" = "parquet")
    """

    order_qt_unshredded_complex """
        select id,
               cast(v['id'] as int) as variant_id,
               cast(v['name'] as string) as name,
               cast(v['metric'] as bigint) as metric,
               cast(v['nested']['score'] as int) as score,
               cast(v['nested']['flag'] as boolean) as flag,
               cast(v['arr'] as array<int>)[1] as first_arr,
               cast(v['arr'] as array<text>)[2] as second_arr
        from local(
            "file_path" = "${unshredded}",
            "backend_id" = "${beId}",
            "format" = "parquet")
        order by id
    """

    order_qt_shredded_fields """
        select id,
               cast(v['metric'] as bigint) as metric,
               cast(v['name'] as string) as name
        from local(
            "file_path" = "${shredded}",
            "backend_id" = "${beId}",
            "format" = "parquet")
        order by id
    """

    order_qt_shredded_full_variant_with_scalar """
        select id,
               cast(v as string) like '%"name":"name-%' as has_name,
               cast(v as string) like '%"metric":%' as has_metric
        from local(
            "file_path" = "${shredded}",
            "backend_id" = "${beId}",
            "format" = "parquet")
        order by id
    """

    order_qt_typed_only_fields """
        select id,
               cast(v['metric'] as bigint) as metric,
               cast(v['nested']['x'] as string) as nested_x,
               cast(v['f'] as string) is null as non_finite_float_is_null,
               cast(v['items'] as string) as items
        from local(
            "file_path" = "${typedOnly}",
            "backend_id" = "${beId}",
            "format" = "parquet")
        order by id
    """

    order_qt_typed_only_missing_field """
        select id,
               cast(v['missing'] as string) is null as missing_is_null
        from local(
            "file_path" = "${typedOnly}",
            "backend_id" = "${beId}",
            "format" = "parquet")
        order by id
    """

    order_qt_typed_only_nested_missing_field """
        select id,
               cast(v['nested']['missing'] as string) is null as missing_is_null
        from local(
            "file_path" = "${typedOnly}",
            "backend_id" = "${beId}",
            "format" = "parquet")
        order by id
    """

    order_qt_temporal_parity """
        select u.id,
               cast(u.v['d'] as bigint) as unshredded_date,
               cast(t.v['d'] as bigint) as typed_date,
               cast(u.v['t'] as bigint) as unshredded_time,
               cast(t.v['t'] as bigint) as typed_time,
               cast(u.v['ts'] as bigint) as unshredded_ts,
               cast(t.v['ts'] as bigint) as typed_ts,
               cast(u.v['d'] as bigint) = cast(t.v['d'] as bigint) as same_date,
               cast(u.v['t'] as bigint) = cast(t.v['t'] as bigint) as same_time,
               cast(u.v['ts'] as bigint) = cast(t.v['ts'] as bigint) as same_ts
        from local(
            "file_path" = "${temporalUnshredded}",
            "backend_id" = "${beId}",
            "format" = "parquet") u
        join local(
            "file_path" = "${temporalTyped}",
            "backend_id" = "${beId}",
            "format" = "parquet") t
          on u.id = t.id
        order by u.id
    """

    def binaryUnshreddedRows = sql """
        select id, hex(cast(v['b'] as varbinary))
        from local(
            "file_path" = "${binaryUnshredded}",
            "backend_id" = "${beId}",
            "format" = "parquet",
            "enable_mapping_varbinary" = "true")
        order by id
    """
    assertEquals(2, binaryUnshreddedRows.size())
    assertEquals("1", binaryUnshreddedRows[0][0].toString())
    assertEquals("FF0041", binaryUnshreddedRows[0][1].toString())
    assertEquals("2", binaryUnshreddedRows[1][0].toString())
    assertEquals("C328", binaryUnshreddedRows[1][1].toString())

    def binaryTypedRows = sql """
        select id, hex(cast(v['b'] as varbinary))
        from local(
            "file_path" = "${binaryTyped}",
            "backend_id" = "${beId}",
            "format" = "parquet",
            "enable_mapping_varbinary" = "true")
        order by id
    """
    assertEquals(2, binaryTypedRows.size())
    assertEquals("1", binaryTypedRows[0][0].toString())
    assertEquals("FF0041", binaryTypedRows[0][1].toString())
    assertEquals("2", binaryTypedRows[1][0].toString())
    assertEquals("C328", binaryTypedRows[1][1].toString())

    try {
    sql """ set enable_profile = true """
    sql """ set profile_level = 2 """
    def profileToken = UUID.randomUUID().toString()
    sql """
        select "${profileToken}", sum(cast(v['metric'] as bigint))
        from local(
            "file_path" = "${shredded}",
            "backend_id" = "${beId}",
            "format" = "parquet")
    """
    def profile = getProfileByToken(profileToken)
    def metricColumnPaths = getParquetReadColumnPathSet(profile)
    assertTrue(metricColumnPaths.contains("v.metadata"))
    // typed_value.metric.value is the field-level residual fallback for mixed-type metric rows.
    // The top-level v.value stores non-shredded object fields and is not needed for this projection.
    assertTrue(metricColumnPaths.contains("v.typed_value.metric.value"))
    assertTrue(metricColumnPaths.contains("v.typed_value.metric.typed_value"))
    assertFalse(metricColumnPaths.contains("v.value"))
    assertFalse(metricColumnPaths.contains("v.typed_value.name"))

    def nestedProfileToken = UUID.randomUUID().toString()
    sql """
        select "${nestedProfileToken}", count(cast(v['metric']['x'] as string))
        from local(
            "file_path" = "${shredded}",
            "backend_id" = "${beId}",
            "format" = "parquet")
    """
    def nestedProfile = getProfileByToken(nestedProfileToken)
    def nestedColumnPaths = getParquetReadColumnPathSet(nestedProfile)
    assertTrue(nestedColumnPaths.contains("v.typed_value.metric.typed_value.x"))
    // metric.value is required for rows that store metric as field-level residual instead of metric.typed_value.
    assertTrue(nestedColumnPaths.contains("v.metadata"))
    assertTrue(nestedColumnPaths.contains("v.typed_value.metric.value"))
    assertFalse(nestedColumnPaths.contains("v.value"))
    assertFalse(nestedColumnPaths.contains("v.typed_value.name"))
    assertEquals(0, getProfileCounter(nestedProfile, "VariantDirectTypedValueReadRows"))
    assertTrue(getProfileCounter(nestedProfile, "VariantRowWiseReadRows") > 0)

    def typedOnlyProfileToken = UUID.randomUUID().toString()
    sql """
        select "${typedOnlyProfileToken}", sum(cast(v['metric'] as bigint))
        from local(
            "file_path" = "${typedOnly}",
            "backend_id" = "${beId}",
            "format" = "parquet")
    """
    def typedOnlyProfile = getProfileByToken(typedOnlyProfileToken)
    def typedOnlyColumnPaths = getParquetReadColumnPathSet(typedOnlyProfile)
    assertTrue(typedOnlyColumnPaths.contains("v.typed_value.metric"))
    assertFalse(typedOnlyColumnPaths.contains("v.metadata"))
    assertFalse(typedOnlyColumnPaths.contains("v.value"))
    assertTrue(getProfileCounter(typedOnlyProfile, "VariantDirectTypedValueReadRows") > 0)
    assertEquals(0, getProfileCounter(typedOnlyProfile, "VariantRowWiseReadRows"))

    def typedOnlyNestedProfileToken = UUID.randomUUID().toString()
    sql """
        select "${typedOnlyNestedProfileToken}", count(cast(v['nested']['x'] as string))
        from local(
            "file_path" = "${typedOnly}",
            "backend_id" = "${beId}",
            "format" = "parquet")
    """
    def typedOnlyNestedProfile = getProfileByToken(typedOnlyNestedProfileToken)
    def typedOnlyNestedColumnPaths = getParquetReadColumnPathSet(typedOnlyNestedProfile)
    assertTrue(typedOnlyNestedColumnPaths.contains("v.typed_value.nested.typed_value.x"))
    assertFalse(typedOnlyNestedColumnPaths.contains("v.metadata"))
    assertFalse(typedOnlyNestedColumnPaths.contains("v.typed_value.nested.value"))
    assertFalse(typedOnlyNestedColumnPaths.contains("v.value"))
    assertTrue(getProfileCounter(typedOnlyNestedProfile, "VariantDirectTypedValueReadRows") > 0)
    assertEquals(0, getProfileCounter(typedOnlyNestedProfile, "VariantRowWiseReadRows"))

    def binaryTypedProfileToken = UUID.randomUUID().toString()
    sql """
        select "${binaryTypedProfileToken}", max(hex(cast(v['b'] as varbinary)))
        from local(
            "file_path" = "${binaryTyped}",
            "backend_id" = "${beId}",
            "format" = "parquet",
            "enable_mapping_varbinary" = "true")
    """
    def binaryTypedProfile = getProfileByToken(binaryTypedProfileToken)
    def binaryTypedColumnPaths = getParquetReadColumnPathSet(binaryTypedProfile)
    assertTrue(binaryTypedColumnPaths.contains("v.typed_value.b"))
    assertFalse(binaryTypedColumnPaths.contains("v.metadata"))
    assertFalse(binaryTypedColumnPaths.contains("v.value"))
    assertTrue(getProfileCounter(binaryTypedProfile, "VariantDirectTypedValueReadRows") > 0)
    assertEquals(0, getProfileCounter(binaryTypedProfile, "VariantRowWiseReadRows"))

    def typedOnlyMissingProfileToken = UUID.randomUUID().toString()
    sql """
        select "${typedOnlyMissingProfileToken}", count(cast(v['missing'] as string))
        from local(
            "file_path" = "${typedOnly}",
            "backend_id" = "${beId}",
            "format" = "parquet")
    """
    def typedOnlyMissingProfile = getProfileByToken(typedOnlyMissingProfileToken)
    def typedOnlyMissingColumnPaths = getParquetReadColumnPathSet(typedOnlyMissingProfile)
    assertTrue(typedOnlyMissingColumnPaths.contains("v.metadata"))
    assertFalse(typedOnlyMissingColumnPaths.any { it.startsWith("v.typed_value") })
    assertFalse(typedOnlyMissingColumnPaths.contains("v.value"))

    def typedOnlyNestedMissingProfileToken = UUID.randomUUID().toString()
    sql """
        select "${typedOnlyNestedMissingProfileToken}", count(cast(v['nested']['missing'] as string))
        from local(
            "file_path" = "${typedOnly}",
            "backend_id" = "${beId}",
            "format" = "parquet")
    """
    def typedOnlyNestedMissingProfile = getProfileByToken(typedOnlyNestedMissingProfileToken)
    def typedOnlyNestedMissingColumnPaths = getParquetReadColumnPathSet(typedOnlyNestedMissingProfile)
    assertTrue(typedOnlyNestedMissingColumnPaths.contains("v.metadata"))
    assertFalse(typedOnlyNestedMissingColumnPaths.any { it.startsWith("v.typed_value.nested.typed_value") })
    assertFalse(typedOnlyNestedMissingColumnPaths.contains("v.typed_value.nested.value"))
    assertFalse(typedOnlyNestedMissingColumnPaths.contains("v.value"))

    def temporalProfileToken = UUID.randomUUID().toString()
    sql """
        select "${temporalProfileToken}",
               sum(cast(v['d'] as bigint) + cast(v['t'] as bigint) + cast(v['ts'] as bigint))
        from local(
            "file_path" = "${temporalTyped}",
            "backend_id" = "${beId}",
            "format" = "parquet")
    """
    def temporalProfile = getProfileByToken(temporalProfileToken)
    def temporalColumnPaths = getParquetReadColumnPathSet(temporalProfile)
    assertTrue(temporalColumnPaths.contains("v.typed_value.d"))
    assertTrue(temporalColumnPaths.contains("v.typed_value.t"))
    assertTrue(temporalColumnPaths.contains("v.typed_value.ts"))
    assertFalse(temporalColumnPaths.contains("v.metadata"))
    assertFalse(temporalColumnPaths.contains("v.value"))
    assertTrue(getProfileCounter(temporalProfile, "VariantDirectTypedValueReadRows") > 0)
    assertEquals(0, getProfileCounter(temporalProfile, "VariantRowWiseReadRows"))

    def caseProfileToken = UUID.randomUUID().toString()
    sql """
        select "${caseProfileToken}", count(cast(v['Name'] as string))
        from local(
            "file_path" = "${shredded}",
            "backend_id" = "${beId}",
            "format" = "parquet")
    """
    def caseProfile = getProfileByToken(caseProfileToken)
    def caseColumnPaths = getParquetReadColumnPathSet(caseProfile)
    assertTrue(caseColumnPaths.contains("v.metadata"))
    assertTrue(caseColumnPaths.contains("v.value"))
    assertFalse(caseColumnPaths.contains("v.typed_value.name"))

    def fullVariantWithPredicateProfileToken = UUID.randomUUID().toString()
    sql """
        select "${fullVariantWithPredicateProfileToken}", cast(v as string)
        from local(
            "file_path" = "${shredded}",
            "backend_id" = "${beId}",
            "format" = "parquet")
        where cast(v['metric'] as bigint) >= 20
    """
    def fullVariantWithPredicateProfile = getProfileByToken(fullVariantWithPredicateProfileToken)
    def fullVariantWithPredicateColumnPaths = getParquetReadColumnPathSet(fullVariantWithPredicateProfile)
    assertTrue(fullVariantWithPredicateColumnPaths.contains("v.metadata"))
    assertTrue(fullVariantWithPredicateColumnPaths.contains("v.value"))
    assertTrue(fullVariantWithPredicateColumnPaths.contains("v.typed_value.metric.value"))
    assertTrue(fullVariantWithPredicateColumnPaths.contains("v.typed_value.metric.typed_value"))
    assertTrue(fullVariantWithPredicateColumnPaths.contains("v.typed_value.name"))

    order_qt_complex_join """
        select u.id,
               cast(u.v['name'] as string) as name,
               cast(s.v['metric'] as bigint) as metric
        from local(
            "file_path" = "${unshredded}",
            "backend_id" = "${beId}",
            "format" = "parquet") u
        join local(
            "file_path" = "${shredded}",
            "backend_id" = "${beId}",
            "format" = "parquet") s
          on u.id = s.id
        where cast(u.v['nested']['score'] as int) >= 20
        order by u.id
    """
    } finally {
        sql """ set enable_profile = false """
        sql """ set profile_level = 0 """
    }
}
