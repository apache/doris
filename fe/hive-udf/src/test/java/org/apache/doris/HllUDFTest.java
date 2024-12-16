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

package org.apache.doris;

import org.apache.doris.common.HllUtil;
import org.apache.doris.common.io.Hll;
import org.apache.doris.udf.HllCardinalityUDF;
import org.apache.doris.udf.HllUnionUDAF;
import org.apache.doris.udf.ToHllUDAF;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;

// hive hll udf test
public class HllUDFTest {

    private BinaryObjectInspector inputOI0 = new JavaConstantBinaryObjectInspector(new byte[0]);

    @Test
    public void hllCardinalityTest() throws Exception {
        // Theoretically, the HLL algorithm can count very large cardinality with little relative error rate,
        // which is less than 2% in Doris. We cost about 5 minutes to test with one billion input numbers here
        // successfully, but in order to shorten the test time, we chose one million numbers.
        HllCardinalityUDF hllCardinalityUDF = new HllCardinalityUDF();
        hllCardinalityUDF.initialize(new ObjectInspector[] { inputOI0 });
        Hll hll = new Hll();
        long largeInputSize = 1000000L;
        for (long i = 1; i <= largeInputSize; i++) {
            hll.updateWithHash(i);
        }
        byte[] hllLargeBytes = HllUtil.serializeToBytes(hll);
        hllCardinalityUDF.initialize(new ObjectInspector[] { inputOI0 });
        Object evaluateLarge = hllCardinalityUDF
                .evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(hllLargeBytes) });
        long actualCardinality = (long) evaluateLarge;

        double relativeError = Math.abs(actualCardinality - largeInputSize) / (double) largeInputSize;
        Assert.assertTrue("Relative error rate should be less than 2%", relativeError <= 0.02);
    }

    @Test
    public void hllUnionUDAFTest() throws Exception {
        Hll hll0 = new Hll();
        Hll hll1 = new Hll();

        hll0.updateWithHash(1);
        hll0.updateWithHash(2);

        hll1.updateWithHash(2);
        hll1.updateWithHash(3);
        hll1.updateWithHash(4);

        byte[] hll0Bytes = HllUtil.serializeToBytes(hll0);
        byte[] hll1Bytes = HllUtil.serializeToBytes(hll1);

        HllUnionUDAF hllUnionUDAF = new HllUnionUDAF();
        HllUnionUDAF.GenericEvaluate evaluator = (HllUnionUDAF.GenericEvaluate) hllUnionUDAF
                .getEvaluator(new TypeInfo[] { TypeInfoFactory.binaryTypeInfo });

        GenericUDAFEvaluator.AggregationBuffer aggBuffer = evaluator.getNewAggregationBuffer();

        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, new ObjectInspector[] { inputOI0 });
        evaluator.iterate(aggBuffer, new Object[] { hll0Bytes });
        evaluator.iterate(aggBuffer, new Object[] { hll1Bytes });
        byte[] mergedHllBytes = (byte[]) evaluator.terminate(aggBuffer);

        Hll mergedHll = HllUtil.deserializeToHll(mergedHllBytes);
        Assert.assertEquals(4L, mergedHll.estimateCardinality());
    }

    @Test
    public void toHllUDAFTest() throws Exception {
        ToHllUDAF toHllUDAF = new ToHllUDAF();
        ToHllUDAF.GenericEvaluate evaluator = (ToHllUDAF.GenericEvaluate) toHllUDAF
                .getEvaluator(new TypeInfo[] { TypeInfoFactory.longTypeInfo });

        GenericUDAFEvaluator.AggregationBuffer aggBuffer = evaluator.getNewAggregationBuffer();

        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1,
                new ObjectInspector[] { PrimitiveObjectInspectorFactory.javaLongObjectInspector });
        evaluator.iterate(aggBuffer, new Object[] { 1L });
        evaluator.iterate(aggBuffer, new Object[] { 2L });
        byte[] hllBytes = (byte[]) evaluator.terminate(aggBuffer);

        Hll hll = HllUtil.deserializeToHll(hllBytes);
        Assert.assertEquals(2L, hll.estimateCardinality());
    }
}
