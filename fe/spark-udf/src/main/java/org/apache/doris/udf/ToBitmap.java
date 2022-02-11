package org.apache.doris.udf;

import java.io.*;

import org.apache.doris.common.io.BitmapValue;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.util.StringUtils;


/**
 * GenericUDAFToBitmap.
 *
 */
@Description(name = "to_bitmap", value = "_FUNC_(expr) - Returns an doris bitmap representation of a column.")
public class ToBitmap extends AbstractGenericUDAFResolver {

    static final Logger LOG = LoggerFactory.getLogger(org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEWAHBitmap.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }
        ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
        if (!ObjectInspectorUtils.compareSupported(oi)) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Cannot support comparison of map<> type or complex type containing map<>.");
        }
        return new org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEWAHBitmap.GenericUDAFEWAHBitmapEvaluator();
    }

    //The UDAF evaluator assumes that all rows it's evaluating have
    //the same (desired) value.
    public static class GenericEvaluate extends GenericUDAFEvaluator {

        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private PrimitiveObjectInspector inputOI;

        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations
        // (doris bitmaps)

        private transient BinaryObjectInspector internalMergeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            // init output object inspectors
            // The output of a partial aggregation is a binary
            if (m == Mode.PARTIAL1) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                this.internalMergeOI = (BinaryObjectInspector) parameters[0];
            }
            return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
        }

        /** class for storing the current partial result aggregation */
        @AggregationType(estimable = true)
        static class BitmapAgg extends AbstractAggregationBuffer {
            BitmapValue bitmap;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((BitmapAgg) agg).bitmap = new BitmapValue();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            BitmapAgg result = new BitmapAgg();
            reset(result);
            return result;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            assert (parameters.length == 1);
            Object p = parameters[0];
            if (p != null) {
                BitmapAgg myagg = (BitmapAgg) agg;
                try {
                    int row = PrimitiveObjectInspectorUtils.getInt(p, inputOI);
                    addBitmap(row, myagg);
                } catch (NumberFormatException e) {
                    LOG.warn(getClass().getSimpleName() + " " +
                            StringUtils.stringifyException(e));
                }
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg){
            BitmapAgg myagg = (BitmapAgg) agg;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            try {
                myagg.bitmap.serialize(dos);
                bos.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return bos.toByteArray();
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) {
            BitmapAgg myagg = (BitmapAgg) agg;
            byte[] partialResult = this.internalMergeOI.getPrimitiveJavaObject(partial);
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(partialResult));
            BitmapValue partialBitmap = new BitmapValue();
            try {
                partialBitmap.deserialize(in);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            myagg.bitmap.or(partialBitmap);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg){
            return terminate(agg);
        }

        private void addBitmap(int newRow, BitmapAgg myagg) {
            myagg.bitmap.add(newRow);
        }
    }
}
