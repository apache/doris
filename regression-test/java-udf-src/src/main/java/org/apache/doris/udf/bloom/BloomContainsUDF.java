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
// This file is copied from
// https://github.com/klout/brickhouse/blob/master/src/main/java/brickhouse/udf/bloom/BloomContainsUDF.java
// and modified by Doris

package org.apache.doris.udf.bloom;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.util.bloom.Filter;
import org.apache.hadoop.util.bloom.Key;

/**
 * Returns true if the bloom (probably) contains the string
 */
@Description(
    name = "bloom_contains",
    value = " Returns true if the referenced bloom filter contains the key.. \n " +
        "_FUNC_(string key, string bloomfilter) "
)
public class BloomContainsUDF extends UDF {


    public Boolean evaluate(String key, String bloomFilter) throws HiveException {
        Filter bloom = BloomFactory.GetBloomFilter(bloomFilter);
        if (bloom != null) {
            return bloom.membershipTest(new Key(key.getBytes()));
        } else {
            return false;
        }
    }

}

