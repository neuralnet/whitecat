package siwicki.whitecat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by rsiwicki on 10/12/2014.
 */
public class CollectPrim extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(CollectPrim.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        // Type-checking goes here!
        TypeInfo[] parameters = info.getParameters();
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Please specify exactly one argument.");
        }

        // validate the first parameter, which is the expression to compute over
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + parameters[0].getTypeName() + " was passed as parameter 1.");
        }

        return new CollectStructEvaluator();
    }


        public static class CollectStructEvaluator extends GenericUDAFEvaluator {

            // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
            private PrimitiveObjectInspector inputOI;

            private StandardListObjectInspector internalMergeOI;

            // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list of doubles)
            private StandardListObjectInspector loi;


            @Override
            public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
                super.init(m, parameters);
                // return type goes here

                if(m == Mode.PARTIAL1) {
                    inputOI = (PrimitiveObjectInspector) parameters[0];
                    return ObjectInspectorFactory.getStandardListObjectInspector(
                            (PrimitiveObjectInspector) ObjectInspectorUtils
                                    .getStandardObjectInspector(inputOI));
                } else {
                    if (!(parameters[0] instanceof StandardListObjectInspector)) {
                        inputOI = (PrimitiveObjectInspector) ObjectInspectorUtils
                                .getStandardObjectInspector(parameters[0]);
                        return (StandardListObjectInspector) ObjectInspectorFactory
                                .getStandardListObjectInspector(inputOI);

                    } else {
                        internalMergeOI = (StandardListObjectInspector) parameters[0];
                        inputOI = (PrimitiveObjectInspector)
                                internalMergeOI.getListElementObjectInspector();
                        loi = (StandardListObjectInspector) ObjectInspectorUtils
                                .getStandardObjectInspector(internalMergeOI);
                        return loi;
                    }


                }

            }

            @Override
            public AggregationBuffer getNewAggregationBuffer() throws HiveException {
                MkAggregationBuffer ret = new MkAggregationBuffer();
                reset(ret);
                return ret;
            }

            @Override
            public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
                ((MkAggregationBuffer) aggregationBuffer).container = new ArrayList<Object>();
            }

            static class MkAggregationBuffer implements  AggregationBuffer {
                List<Object> container;
            }

            @Override
            public Object terminatePartial(AggregationBuffer agg) throws HiveException {
                // return value goes here
                MkAggregationBuffer myagg = (MkAggregationBuffer) agg;
                ArrayList<Object> ret = new ArrayList<Object>(myagg.container.size());

                ret.addAll(myagg.container);

                return ret;
            }

            private void putIntoList(Object p, MkAggregationBuffer myagg) {
                Object pCopy = ObjectInspectorUtils.copyToStandardObject(p, this.inputOI);

                myagg.container.add(pCopy);
            }

            @Override
            public Object terminate(AggregationBuffer agg) throws HiveException {
                // final return value goes here
                MkAggregationBuffer myagg = (MkAggregationBuffer) agg;
                ArrayList<Object> ret = new ArrayList<Object>(myagg.container.size());
                ret.addAll(myagg.container);
                return ret;
            }

            @Override
            public void merge(AggregationBuffer agg, Object partial) throws HiveException {
                MkAggregationBuffer myagg = (MkAggregationBuffer) agg;
                ArrayList<Object> partialResult = (ArrayList<Object>) internalMergeOI.getList(partial);

                for(Object i: partialResult) {
                    putIntoList(i, myagg);
                }

            }

            @Override
            public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
                Object p = parameters[0];

                if(p!=null) {
                    MkAggregationBuffer myagg = (MkAggregationBuffer) agg;
                    putIntoList(p, myagg);
                }
            }


        }
    }

