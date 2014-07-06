package com.amirtuval.pighll.udf;

import com.amirtuval.pighll.Constants;
import com.amirtuval.pighll.HyperLogLog;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.Arrays;

public abstract class HyperLogLogUdfBase<TReturnType>
        extends EvalFunc<TReturnType> implements Algebraic {

    protected static final String SCALAR_VALUE = "ScalarValue";
    protected static final String HLL_VALUE = "HLL";

    static public class InitialScalar extends EvalFunc<Tuple> {
        public Tuple exec(Tuple input) throws IOException {
            return tupleFromSingleValue(input, SCALAR_VALUE);
        }
    }

    static public class InitialHLL extends EvalFunc<Tuple> {
        public Tuple exec(Tuple input) throws IOException {
            return tupleFromSingleValue(input, HLL_VALUE);
        }
    }

    public String getIntermed() {return Intermed.class.getName();}

    static public class Intermed extends EvalFunc<Tuple> {
        public Tuple exec(Tuple input) throws IOException {
            return TupleFactory.getInstance().newTuple(Arrays.asList(HLL_VALUE, hllFromTuples(input).asString()));
        }
    }

    static public class FinalEstimate extends EvalFunc<Long> {
        public Long exec(Tuple input) throws IOException {
            return (long) hllFromTuples(input).estimate();
        }
    }

    static public class FinalHll extends EvalFunc<String> {
        public String exec(Tuple input) throws IOException {
            return hllFromTuples(input).asString();
        }
    }

    private static interface InputAction {
        void call(String item);
    }

    private static void iterateInput(Tuple input, InputAction action) throws ExecException {
        Object values = input.get(0);

        if (values instanceof String) {
            action.call(values.toString());
        }

        if (values instanceof DataBag) {
            DataBag data = (DataBag) values;

            for (Tuple value : data) {
                action.call(value.get(0).toString());
            }
        }
    }

    protected static HyperLogLog hllFromTuples(Tuple input) throws ExecException {
        HyperLogLog hll = new HyperLogLog(Constants.HLL_BIT_WIDTH);

        Object values = input.get(0);

        if (values instanceof DataBag) {
            DataBag data = (DataBag)values;

            for (Tuple value : data) {
                if (value.size() == 0) {
                    continue;
                }

                String valueType = value.get(0).toString();
                String valueStr = value.get(1).toString();
                if (valueType.equals(SCALAR_VALUE)) {
                    hll.add(valueStr);
                } else {
                    hll.merge(new HyperLogLog(valueStr));
                }
            }
        }

        return hll;
    }

    protected static HyperLogLog hllFromHlls(Tuple input) throws ExecException {
        final HyperLogLog hll = new HyperLogLog(Constants.HLL_BIT_WIDTH);

        iterateInput(input, new InputAction() {
            public void call(String item) {
                hll.merge(new HyperLogLog(item));
            }
        });

        return hll;
    }

    protected static HyperLogLog hllFromValues(Tuple input) throws ExecException {
        final HyperLogLog hll = new HyperLogLog(Constants.HLL_BIT_WIDTH);

        iterateInput(input, new InputAction() {
            public void call(String item) {
                hll.add(item);
            }
        });

        return hll;
    }

    protected static Tuple tupleFromSingleValue(Tuple input, String valueType) throws ExecException {
        DataBag data = (DataBag) input.get(0);

        if (data.size() == 0) {
            return TupleFactory.getInstance().newTuple();
        }

        Tuple value = data.iterator().next();
        Object fieldValue = value.get(0);
        return TupleFactory.getInstance().newTuple(Arrays.asList(valueType, fieldValue == null ? "" : fieldValue.toString()));
    }
}
