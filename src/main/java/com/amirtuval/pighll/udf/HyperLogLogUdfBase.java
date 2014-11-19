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
            return TupleFactory.getInstance().newTuple(Arrays.asList(HLL_VALUE, hllFromTuples(input, hllCreator()).asString()));
        }

        protected CreateHll hllCreator() {
            return normalHll();
        }
    }

    static public class LegacyIntermed extends Intermed {
        @Override
        protected CreateHll hllCreator() {
            return legacyHll();
        }
    }

    static public class FinalEstimate extends EvalFunc<Long> {
        public Long exec(Tuple input) throws IOException {
            return (long) hllFromTuples(input, normalHll()).estimate();
        }
    }

    static public class FinalHll extends EvalFunc<String> {
        public String exec(Tuple input) throws IOException {
            return hllFromTuples(input, normalHll()).asString();
        }
    }

    static public class LegacyFinalHll extends EvalFunc<String> {
        public String exec(Tuple input) throws IOException {
            return hllFromTuples(input, legacyHll()).asString();
        }
    }

    private static interface InputAction {
        HyperLogLog call(HyperLogLog current, String item);
    }

    private static HyperLogLog iterateInput(Tuple input, InputAction action) throws ExecException {
        Object values = input.get(0);

        if (values instanceof String) {
            return action.call(null, values.toString());
        }

        if (values instanceof DataBag) {
            DataBag data = (DataBag) values;

            HyperLogLog current = null;
            for (Tuple value : data) {
                current = action.call(current, value.get(0).toString());
            }
            return current;
        }

        return null;
    }

    protected static HyperLogLog hllFromTuples(Tuple input, CreateHll creator) throws ExecException {
        HyperLogLog hll = null;

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
                    if (hll == null)
                        hll = creator.create();
                    hll.add(valueStr);
                } else {
                    HyperLogLog currentHll = new HyperLogLog(valueStr);
                    if (hll == null) {
                        hll = currentHll;
                    } else {
                        hll.merge(currentHll);
                    }
                }
            }
        }

        return hll;
    }

    protected static HyperLogLog hllFromHlls(Tuple input) throws ExecException {
        return iterateInput(input, new InputAction() {
            public HyperLogLog call(HyperLogLog current,String item) {
                HyperLogLog newHll = new HyperLogLog(item);
                if (current != null) {
                    current.merge(newHll);
                    return current;
                } else {
                    return newHll;
                }
            }
        });
    }

    protected interface CreateHll {
        HyperLogLog create();
    }

    protected static class CreateNormalHll implements CreateHll {

        public HyperLogLog create() {
            return new HyperLogLog(Constants.HLL_BIT_WIDTH, false);
        }
    }

    protected static CreateHll normalHll() {
        return new CreateNormalHll();
    }

    protected static class CreateLegacyHll implements CreateHll {

        public HyperLogLog create() {
            return new HyperLogLog(12, true);
        }
    }

    protected static CreateHll legacyHll() {
        return new CreateLegacyHll();
    }

    protected static HyperLogLog hllFromValues(Tuple input, final CreateHll creator) throws ExecException {
        return iterateInput(input, new InputAction() {
            public HyperLogLog call(HyperLogLog current, String item) {
                if (current == null)
                    current = creator.create();
                current.add(item);
                return current;
            }
        });
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
