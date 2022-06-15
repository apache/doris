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

package org.apache.doris.common.parquet;

import org.apache.doris.analysis.BrokerDesc;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

public class ParquetReaderTest {
    private static final Logger LOG = LogManager.getLogger(ParquetReaderTest.class);

    // you need to set
    //  localfile, remote file
    //  ak, sk, broker desc
    // before running this test
    @Ignore
    @Test
    public void testWrongFormat() {
        try {
            // local
            String file = "/path/to/local/all_type_none.parquet";
            ParquetReader reader = ParquetReader.create(file);
            LOG.info(reader.getSchema(false));

            // remote
            String file2 = "bos://cmy-repe/all_type_none.parquet";
            Map<String, String> properties = Maps.newHashMap();
            properties.put("bos_endpoint", "http://bj.bcebos.com");
            properties.put("bos_accesskey", "1");
            properties.put("bos_secret_accesskey", "2");
            BrokerDesc brokerDesc = new BrokerDesc("dummy", properties);

            ParquetReader reader2 = ParquetReader.create(file2, brokerDesc, "127.0.0.1", 8118);
            LOG.info(reader2.getSchema(false));
        } catch (Exception e) {
            LOG.info("error: ", e);
        }
    }
}
