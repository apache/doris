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
// https://github.com/klout/brickhouse/blob/master/src/main/java/brickhouse/udf/bloom/BloomFactory.java
// and modified by Doris

package org.apache.doris.udf.bloom;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Filter;
import org.apache.hadoop.util.hash.Hash;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for construction and serialization of BloomFilters ...
 */
public class BloomFactory {
    private static final Logger LOG = Logger.getLogger(BloomFactory.class);
    private static Map<String, Filter> localBloomMap = new HashMap<String, Filter>();

    public static final int DEFAULT_NUM_ELEMENTS = 10000000;
    public static final double DEFAULT_FALSE_POS_PROB = 0.005;
    public static final int DEFAULT_HASH_TYPE = Hash.JENKINS_HASH;
    public static final int NUMBER_OF_BLOOMS = 5;


    public static Filter NewBloomInstance() {
        return NewBloomInstance(DEFAULT_NUM_ELEMENTS, DEFAULT_FALSE_POS_PROB);
    }

    static Filter NewVesselBloom() {
        return new BloomFilter();
    }

    public static Filter NewBloomInstance(int expectedNumberOfElements, double falsePositiveProbability) {
        return NewBloomInstance(Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))) / Math.log(2), // c = k / ln(2)
            expectedNumberOfElements,
            (int) Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2)))); // k = ceil(-log_2(false prob.))

    }

    public static Filter NewBloomInstance(double c, int n, int k) {
        LOG.info("Creating new Bloom filter C = " + c + " N =  " + n + " K = " + k);
        BloomFilter dbf = new BloomFilter((int) Math.ceil(c * n),
            k, DEFAULT_HASH_TYPE);
        return dbf;
    }

    /**
     * Generic method for getting BloomFilter from a string.
     * First, the local map is checked for a bloom loaded from
     * the distributed cache. Next the bloom is attempted to be
     * parsed from UUencoded format.
     *
     * @param name
     * @return
     */
    public static Filter GetBloomFilter(String str) {
        Filter bloom = GetNamedBloomFilter(str);
        if (bloom == null) {
            try {
                bloom = ReadBloomFromString(str);
                return bloom;
            } catch (IOException e) {
                LOG.error(" Unable to get bloom for string " + str);
                return null;
            }
        } else {
            return bloom;
        }
    }

    public static Filter GetNamedBloomFilter(String name) {
        return localBloomMap.get(name);
    }

    public static void PutNamedBloomFilter(String name, Filter bloom) {
        localBloomMap.put(name, bloom);
    }


    public static Filter ReadBloomFromStream(InputStream stream) throws IOException {
        /// Need to UUDecode first,
        /// TODO - read bytes directly when hive handles byte arrays better
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] bufferArr = new byte[4096];
        int len = 0;
        while ((len = stream.read(bufferArr, 0, 4096)) > 0) {
            buffer.write(bufferArr, 0, len);
        }
        if (buffer.size() == 0) {
            return BloomFactory.NewBloomInstance();
        }
        return ReadBloomFromString(new String(buffer.toByteArray()));
    }

    public static void WriteBloomToStream(OutputStream stream, Filter bloom) throws IOException {
        stream.write(WriteBloomToString(bloom).getBytes());
        stream.flush();
    }

    public static Filter ReadBloomFromString(String str) throws IOException {
        if (str != null) {
            Filter filter = NewVesselBloom();
            byte[] decoded = Base64.decodeBase64(str.getBytes());
            DataInputStream dataInput = new DataInputStream(new ByteArrayInputStream(decoded));

            filter.readFields(dataInput);
            return filter;
        } else {
            return NewBloomInstance();
        }
    }

    public static String WriteBloomToString(Filter bloom) throws IOException {
        if (bloom != null) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            bloom.write(new DataOutputStream(buffer));
            byte[] encodedBloom = Base64.encodeBase64(buffer.toByteArray());
            return new String(encodedBloom);
        } else {
            return null;
        }
    }


}

