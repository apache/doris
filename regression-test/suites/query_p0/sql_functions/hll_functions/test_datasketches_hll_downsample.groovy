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

suite("test_datasketches_hll_downsample") {
    // Fixtures produced by datasketches-cpp 46025e9: update(int32_t i), i in [0,n), HLL_8.
    // lgK=21/n=5 stays LIST; lgK=12/n=5000 is dense; lgK=12/n=0 is empty.
    def sparse = 'AgEHFQMIBQjL18IEK/L7BoYv+Q11gWYHgbxdBg=='
    def dense = 'CgEHDAAIAAp0NJgpTVCzQAAAAFDYUJ9AAAAAAAAAAADFBAAAAAAAAAQEAwEAAQABAAIDBQABAgADAAMDAAICAQAAAQMBAwMDAQIAAAABAgIAAgQCAgMFAAQAAgEDAAEAAAUAAgAEAwICAgYDCAMEAQIBAAACAggAAwMAAgIAAQIDAAIAAgEABAEBAwIAAwEABgQDAQICAAEBBQEDAQEDAgIABAADAAACBAIDAAMEAgADAgMAAwUAAQEEAwABAQIBAQAHAQECAAADAQECAwIDAgMBAAECAQAAAQYFAAEDBgIBAQABAQMBAQIAAwEBAwECAQEBBgAABwEAAAICAAMDBgACAgAABAMBAAACAAEDAAIDAQAAAAIBAQYCAgQAAwEFBAMCAQMBAQABBQMBAgACAwMDAgMCAAECAAECAQEBAgICAgACBAEBAQIBAwIBAAQAAQEAAgABAgIACQIDBAMGBgAABgEBBAIGAQMAAAECAwMBAwABBAACAgACCgEBAAEDAwMAAQEAAQEBAgEBAAECAAABAAEDAwEAAgUBAQQCAgAAAAYDAQAAAAICAAECAgYAAgEEAgYBAAQCBAQFAgMCAQIAAwEAAQQGAAMCAAAAAQAGBwAAAQECAwABAQICAQEBBAIBAQQDBAADBAICAQQDAQUDAAACAAMBAQECAwEAAQEAAAIABAABAQQBAQQEBQMBAAIEBgMBAgACAQACAwQGAAABBQMAAAEEAQICAQAAAgIEAwQBAAADAAAFAwABAwMEBAQAAgIABQAAAQAAAwEDAAEBAAICBAMEAgABAAMCBQAAAwADAAECAwECBAUCAQACBQAAAgYBAwAAAwQBAAMDAAEAAwAAAQMABAICAgAAAAIAAQACAwIBAQEEAwIABgECAAEAAgUBBAAFAAADAgYBAgMABQABAgABAAIFAQAAAwACAgAEAQEAAgAAAQICAgADBAMDAQACBQABAQAAAgIAAAUFBAUCAQABAgACAQAEAQEAAQICAQEBAQEAAQAABAMBAAICAQQAAAEDAgAAAwEAAgADBQQAAwIAAAABAAADAgEHAAAABQEFAAACAgAFAwICAQEAAAEBAgIAAAIAAAIABAADAgEIAAMFAAMAAAACBAMAAAEAAAABBAMCBAEAAQACAAEBAgQCAAAAAgIAAAECAQMAAQIAAAACAQECAQAEAAMCAQIAAQIDAAMAAQIBAAABAgEBAgUCAgQBAgABAAIGAQEAAwABAwQDAgAAAwMBBAABAAACAQICAQAAAgADAwIGAQADAgEDAwECAAACAAMAAQAABQABBgEDAQMBBQMDAQYBAAEBBwEABwIBAwECBwAAAQIEAwADAAEAAAABAAQAAgMDAAMBBAEBAgACAQAAAAIAAAYBAgEBAAUAAQMCBAMEAAYFBAICAQEDBAACAQAFAgIFAgECAQABAAIIAgIBAgQCAwYAAAACAAEAAwIJBAABAQICAQQFAwAHAAQFAAMAAAEIBwQFAwMAAgICAgABAwAEBwEABAAIAgAAAAMAAQAAAwABAQEBAQEAAAABAQEABAQAAwECAQEEAQEBAQMCAAECAAICAQEABAABAgACAQEAAgEBAQECAQQEAQEBAAUCBAAGAAEAAAYAAQcDAAABAQQBAQIBAQABAAMBAgIAAQEAAAAFAgIABgAAAgAAAAEBAAUCAAEAAAEBAwADAgACAgEHAgQGAwIAAwIBAgMBBgEBAwQBAgECAAAAAAUDAgQGAAEBAAACAAIAAgEEAAADAgAEAgAAAAMAAgIDAgAABAIEAAEIAgAAAQEBBwIBAAEAAAECBgECAQECAgcFBgABAgAIAQABAAMEAAQEBAEBAgABAAIFBAIAAgEBBQIBAQACBwAAAAcFBgAEBQEEBQMFAgcABAABBgEBAAEBAQQBAAIEAgMCAgQAAAEBAQcBAAECBAAIAAABBAAEAQEBAgADAQEGAAIBBgEAAgACAAEBAQUBBQAABAIEBQQAAAAEBAAFAAUBAgECAAUCAAECAAECBgAACgAAAgIBAAECAgABAAQAAQEBAQIDAAAAAwEBBAIDBgEFAgABAgACAQMAAgQAAAUCAAEAAwACAwMAAQMAAAABAAIBAgEAAwABAQQBAQkCAgEAAAABAwIDBAIBAwEAAAEBAgEDAQAAAgAAAAMFAwICAAAABAEAAgIBAwABAAEBAwEDAAgBAgIBAgUDAQcCBAAAAAUDAgABAwEAAwEDAwADAAACAwICAgEIAAIAAQQAAQYDAgIAAAMCAwIAAQAGAAMDAQIEAQEDAgECAAAAAQEEAQICAgABAgIDAQAEBQECAAAIAQMAAAAAAAABBQEFAwIDAQABBQABAAAAAgIABAAAAgUAAQEBAAUCAgEBBAEEAQUAAQEBAQAEAQIAAAIAAgACAgEBBAABBAIABgIAAgADAAEAAQIAAAEDAwABBAICAQABAwICAAAABAUAAAMAAQACAgEEAAMAAAECCQABAAECAwQBCAABAAACAwABBAEDAAICAgMBBAEAAQMCBgICAAIAAAECCgMAAgACAQADBAABAgEAAAECAgMCAAEDAAEEAQQCAQACBAECAQIAAQABAQABAwICAQMCAQECBAACAwIAAQUDAQIBAAAFBAIBAQABAAcCAAAAAQEBAgABBAMCAgAAAAEABQIBAgAABQMAAQECAgABAAACAgACBAEDAQECAQQBAAMCAAQDAAEAAwMBAwEBAAECAAICBAMAAAIBAAEAAQABBgIAAAEAAQECAQEBAQEAAQEAAQABAQABAQEDAQQCAwIABQIEAgACAAEBAAkDBAIDAAICAAICAgMCAAMBAwECAQABAQIAAgMCAQEEBQMBBAAAAAQAAgACAAIAAAICAAMAAAIBAQEBAAAAAAIEAwIAAgQCAAgAAAAAAQACAwABAgIFAAADAgEAAAQEAwICAwACAAAAAwEDAgEEAgAAAQIAAAIABAAAAQAAAQAEAQIDAQEAAAQEAQQBAQIIAAAAAQcCAQACAgMBAQEBAQIFAwADBgMHAwIEAAQBBAIGAQICAAICAAEAAQABAAQHBQEBAgMBBAUCAQEABQACAAADAAAAAAADAQMEAQADBQEAAAADAQMDAgACAggBAggAAwIAAwIDAQECAAEAAwMCAQQBAgIBBAEBAAIGAwIAAAAAAgABAAQBAgAAAQcBAgAAAwACAgAAAgQFAgMCBAQDAAAAAgAAAAIDAgIAAQECAQICAQIBAgEEAAACAwABAgUABQAAAgEDAgEAAAUEAQEFAgEBAAECAAIEAQABAgAHAAIFAAEEAgIAAAEEAQIAAgEGAwABAQMAAQIBAwEDBAACAAMBBAEDAAMCBAMEAAAAAAUCAAMCBAQBAQACAgMDAAEBAAIAAAMAAgMBAgIAAQEDAAAAAgAAAgMAAAEBAwUDBQIBAgAABAEBAwMBAAUEAwIAAwIAAQUCAwAFAAAAAQABBAMBAgIBAAQBAQAAAgQDAwgCAAMAAgIAAwECAgEAAQADAAECAwQCAQABAwEBAAECAQIBAQEAAAQAAwMCAAABAAIDAQIAAAAFAQIAAAEBAAAAAwICAQMAAAAAAwEAAAIBAQQEAAEAAAECAgIBAQEBAwMEAQAAAAACBQECAAAAAgEAAwEBCgIEBAEBAQECAQAFAQUAAgMBBAAABgEEBAICAQQBAAQHAQADAgICAgEAAQABAAEBAgACAgAAAAACAAACAgABAQECAAABAwMCAAIFAAAJAAACAgQAAwEEAAMFAwABAwAAAAABAQQEAgEDBAAAAQIBAAADAgMEAQAAAgIBAwABBAgCAAIBAQACAQMBAQIAAAICAAECAAMAAQAEAAEBAgMBAQEAAAECBAQBAAEDAAEAAAUBAgcCAwIAAAMBAAECAAACAAIBAwEDAQAAAAICBAAFAAIBAAACAQUCAAAAAwADAQAFAgEAAQABBAQAAgIBAAMAAAACAQEABAAAAQABAQMBAAAAAwADBAYABQUDAQECAAYCAAEAAwIDAwAABQIAAAkAAgEAAQECAgEEAAICAAADBQYAAgAAAgMBAAAAAAIAAgYAAgEBAQABAwAABQEBAAYEAgIAAgADAwIBAAECAAACAAQAAgABAAIBAQADAQIBAgACAwADAwABAAgAAQECAAACAgEAAgAGAAABBQACAgEHAwIHBAABAwAGAgQFAQACAAACAQADAgQAAAIDBQADBQIBBgMBBwMAAQICAQACAwAAAQECAgMBAQEBAgAEAAAAAAQCAwABAQAABQMABQIEAAIBAQUAAgADAgMCAgIGAAEAAwACAQABAAEBAAEBAAACAwECAAIBAQABAQAABAIAAAAAAAEAAQIHAwADAQABAwABBQEAAQABAQQGAgACBAIDAAQDCQMAAQAEAQMMAQcFAQADAQAFAQIAAQMCAQQAAQAEAwABAQAGAAACAwICAwMGAgQEAAQBAQEAAgECAgADAAAHAAACAAICBQECAwEGAAAEAAICBQMAAwEGAQYCCAICAwQAAwABBQYEBQACAQECAAACBgMAAAQCAwEAAAcFAAACAgEBAQUAAQIBAAQBAwAAAAUHAQQABQMBAAICAQgAAAICAAUAAAIBAgABAgEDAQEAAAAAAQABAQIBAAIBAQAABgAFAwUAAwAIAAoBAQMBAAEAAQMCAQcBAAABAAAAAAAEAQIIAAIAAAEBAwECAwEAAAAEAwECAAQAAQAEAAADAQMAAAEAAQIAAwAAAAABAQYCAAABAQEEAQUEAwEAAQACAwADAwMCAAQAAgEBAAEEAAMAAQACAQEAAwQDAAIAAwIABAUCAgIEAgADAAACAgAAAgACAQAEAgEBAAIDBQIBAgIEAggGAgABBAQAAAIABQICAQECAQACAAEAAQEDAQICAAADAQUBAAUAAQADBAAAAAACAAECAQEBAQgJAAADAAEAAwEBAwECAwAAAAQCCQIDAAABAgEAAAIAAgADAgUCBQIBAwMBAgACAQEAAQAAAQ4CAgMEAgABAAsHBQAEAAUEAAIFAAAAAQECAQYBBgACAQUGAQUCAwAAAAICAgEBAAEDAQMCAAEDAAAFAQAAAAAIAAIABAAAAgEBAQECAQECAgAAAAABAwQDAQQCAgACAgIBAgQAAQIBAQABAAECAgEFCQEAAwACAAIBAwECAQACAQAAAAMBAAECAAEABAQBAQACAAECAAUCAAEDAAEDAwMAAAAAAAACBAAAAgIAAgQAAwABAwEBAAAAAQACAAEFAwABAwIBAQIBAQQBAwABAAEDAQEDAwMBAAMAAgAGBAICBAMEBwIBAQIBBQADAAEBAQIBAAELBQIAAAQCAwICAAEBAwYCAgMDBAIBAAAAAwIAAwEDAwMEAgIBAgMAAAAAAQEBAwIBBQQCAQAAAAIAAgECAQEBAQMCAAYBAQAEAwABAAAAAQQAAAIAAgQABAIBAwIBAgEAAQABAgADAwMEAAIBBAUBAQABAgECAAMBAAIABAICAAAAAQEDAAA='
    def empty = 'AgEHDAMMAAg='
    sql "DROP TABLE IF EXISTS test_ds_hll_downsample"
    sql """CREATE TABLE test_ds_hll_downsample (
          dt DATE, id INT, sk STRING, payload INT)
        DUPLICATE KEY(dt, id)
        PARTITION BY RANGE(dt) (
          PARTITION p_old VALUES [('2024-01-01'), ('2024-02-01')),
          PARTITION p_new VALUES [('2024-02-01'), ('2024-03-01')))
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')"""
    sql """INSERT INTO test_ds_hll_downsample VALUES
        ('2024-01-01',1,from_base64('${dense}'),11),
        ('2024-01-01',2,from_base64('${sparse}'),22),
        ('2024-01-01',3,from_base64('${empty}'),33),
        ('2024-01-01',4,NULL,44),
        ('2024-02-01',5,from_base64('${dense}'),55)"""

    for (int k : [4, 7, 10, 15, 21]) {
        for (String t : ['HLL_4', 'hll_6', 'HlL_8']) {
            // Header lgK and type bits are inspected through HEX, not through UTF-8 SUBSTR(sk).
            order_qt_matrix """SELECT id, length(out), substr(hex(out),7,2), substr(hex(out),15,2),
                datasketches_hll_downsample(out,${k},'${t}') = out AS idempotent
                FROM (SELECT id,datasketches_hll_downsample(sk,${k},'${t}') AS out
                      FROM test_ds_hll_downsample PARTITION(p_old)) d ORDER BY id"""
        }
    }
    order_qt_default """SELECT id,
        datasketches_hll_downsample(sk,10) = datasketches_hll_downsample(sk,10,'HLL_4'),
        datasketches_hll_downsample(sk,21,'HLL_8') = sk
        FROM test_ds_hll_downsample ORDER BY id"""
    order_qt_varchar """SELECT id,
        datasketches_hll_downsample(CAST(sk AS VARCHAR(65533)),10) = datasketches_hll_downsample(sk,10)
        FROM test_ds_hll_downsample ORDER BY id"""
    order_qt_binary """SELECT id,
        hex(datasketches_hll_downsample(CAST(sk AS VARBINARY),10)) = hex(datasketches_hll_downsample(sk,10))
        FROM test_ds_hll_downsample ORDER BY id"""
    order_qt_union """SELECT round(datasketches_hll_union_agg(datasketches_hll_downsample(sk,10))),
        round(datasketches_hll_union_agg(sk,10))
        FROM test_ds_hll_downsample WHERE id=1"""
    order_qt_constant """SELECT length(datasketches_hll_downsample(from_base64('${dense}'),10)),
        length(datasketches_hll_downsample(from_base64_binary('${dense}'),10)),
        datasketches_hll_downsample(NULL,10) IS NULL"""
    order_qt_constant_expr """SELECT id,
        datasketches_hll_downsample(sk,8+2,concat('HLL_','4')) = datasketches_hll_downsample(sk,10),
        datasketches_hll_downsample(sk,10 + CAST(crc32('') AS INT),
            IF(crc32('')=0,'HLL_4','HLL_6')) = datasketches_hll_downsample(sk,10)
        FROM test_ds_hll_downsample ORDER BY id"""
    sql 'SET debug_skip_fold_constant=true'
    order_qt_unfolded """SELECT id,
        datasketches_hll_downsample(sk,8+2,concat('HLL_','4')) = datasketches_hll_downsample(sk,10)
        FROM test_ds_hll_downsample ORDER BY id"""
    sql 'SET debug_skip_fold_constant=false'
    order_qt_probe """SELECT id FROM test_ds_hll_downsample PARTITION(p_old)
        WHERE datasketches_hll_downsample(sk,10) <> sk ORDER BY id LIMIT 1"""
    sql """INSERT OVERWRITE TABLE test_ds_hll_downsample PARTITION(p_old)
        SELECT dt,id,datasketches_hll_downsample(sk,10),payload
        FROM test_ds_hll_downsample PARTITION(p_old)"""
    order_qt_overwrite """SELECT dt,id,payload,length(sk),substr(hex(sk),7,2),substr(hex(sk),15,2)
        FROM test_ds_hll_downsample ORDER BY dt,id"""
    order_qt_no_match """SELECT 1 FROM test_ds_hll_downsample PARTITION(p_old)
        WHERE datasketches_hll_downsample(sk,10) <> sk LIMIT 1"""
    order_qt_no_pending """SELECT count(*),count(sk),
        coalesce(sum(datasketches_hll_downsample(sk,10) <> sk),0)
        FROM test_ds_hll_downsample PARTITION(p_old)"""
    for (String arg : ['3','22','4294967308','18446744073709551628']) {
        test {
            sql "SELECT datasketches_hll_downsample(sk,${arg}) FROM test_ds_hll_downsample"
            exception 'requires lg_k to be between 4 and 21'
        }
    }
    for (String arg : ['NULL','10.5',"'10'",'id']) {
        test {
            sql "SELECT datasketches_hll_downsample(sk,${arg}) FROM test_ds_hll_downsample"
            exception 'requires lg_k to be a constant integer'
        }
    }
    for (String arg : ['NULL','4','sk']) {
        test {
            sql "SELECT datasketches_hll_downsample(sk,10,${arg}) FROM test_ds_hll_downsample"
            exception 'requires tgt_type to be a constant string'
        }
    }
    test {
        sql "SELECT datasketches_hll_downsample(NULL,30)"
        exception 'requires lg_k to be between 4 and 21'
    }
    test {
        sql "SELECT datasketches_hll_downsample(sk,10,'HLL_5') FROM test_ds_hll_downsample"
        exception 'requires tgt_type to be one of HLL_4, HLL_6, HLL_8'
    }
    test {
        sql "SELECT datasketches_hll_downsample(id,10) FROM test_ds_hll_downsample"
        exception 'first argument should be of STRING/VARCHAR/VARBINARY type'
    }
    test {
        sql "SELECT datasketches_hll_downsample(NULL,22 + CAST(crc32('') AS INT))"
        exception "lg_k must be between 4 and 21"
    }
    test {
        // The compact HLL_4 below has auxCount=0. Replace the first register byte by AUX_TOKEN.
        sql """SELECT datasketches_hll_downsample(
            unhex(concat(substr(h,1,80),'0F',substr(h,83))),21)
            FROM (SELECT hex(datasketches_hll_downsample(from_base64('${dense}'),12)) AS h) t"""
        exception "AUX_TOKEN without an auxiliary map"
    }
    // datasketches-cpp 46025e9 fixtures with complete malformed auxiliary maps, or lgArr=32.
    // Cover the shared deserialization entry through both the scalar and the existing aggregate.
    def malformedAux = [
        ['0a01070402280006000000000000f03f0000000000002f4000000000000000000f000000010000000000000000000000040000000003000050',
         'cannot contain an auxiliary map'],
        ['0a0107040228000a000000000000f03f0000000000002f4000000000000000000f000000010000000000000000000000000000010000000003000050',
         'cannot contain an auxiliary map'],
        ['03010708200000010a000000', 'SET array exponent'],
        ['0a01070420200002565d6e7f888800400000003000002c4000000000000000000e00000002000000f0f000000000000000000000010000500000000003000054',
         'auxiliary array exponent'],
        ['0a01070402200002565d6e7f888800400000003000002c4000000000000000000e00000002000000f0f000000000000003000050130000540000000000000000',
         'duplicate slots'],
        ['0a01070402280002565d6e7f888800400000003000002c4000000000000000000e00000002000000f0f00000000000000300005013000054',
         'duplicate slots']
    ]
    for (def bad : malformedAux) {
        test {
            sql "SELECT datasketches_hll_downsample(unhex('${bad[0]}'),21)"
            exception bad[1]
        }
        test {
            sql "SELECT datasketches_hll_union_agg(unhex('${bad[0]}'))"
            exception bad[1]
        }
    }
    test {
        // Keep only auxiliary slot 3, but leave the AUX_TOKEN at register 1 unchanged.
        sql """SELECT datasketches_hll_downsample(unhex(
            '0a01070402280002565d6e7f888800400000003000002c4000000000000000000e00000001000000f0f000000000000003000054'),21)"""
        exception 'no matching auxiliary slot'
    }
    test {
        sql """SELECT datasketches_hll_union_agg(unhex(
            '0a01070402280002565d6e7f888800400000003000002c4000000000000000000e00000001000000f0f000000000000003000054'))"""
        exception 'no matching auxiliary slot'
    }
    for (String bad : ["''", "from_base64('AA==')", "'not a sketch'"]) {
        test {
            sql "SELECT datasketches_hll_downsample(${bad},21)"
            exception 'CORRUPTION'
        }
    }
}
