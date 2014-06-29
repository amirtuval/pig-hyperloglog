package com.amirtuval.pighll.udf;

import com.amirtuval.pighll.HyperLogLog;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.Arrays;

public class HLL_COMPUTE extends EvalFunc<Long> implements Algebraic {

    public String getInitial() {return Initial.class.getName();}
    public String getIntermed() {return Intermed.class.getName();}
    public String getFinal() {return Final.class.getName();}

    static public class Initial extends EvalFunc<Tuple> {
        public Tuple exec(Tuple input) throws IOException {
            DataBag data = (DataBag) input.get(0);

            if (data.size() == 0) {
                return TupleFactory.getInstance().newTuple();
            }

            return TupleFactory.getInstance().newTuple(Arrays.asList("Value", data.iterator().next().toString()));
         }
    }

    static public class Intermed extends EvalFunc<Tuple> {
        public Tuple exec(Tuple input) throws IOException {

            return TupleFactory.getInstance().newTuple(Arrays.asList("HLL", hllFromHlls(input).asString()));
        }
    }

    static public class Final extends EvalFunc<Long> {
        public Long exec(Tuple input) throws IOException {
            return (long) hllFromHlls(input).estimate();
        }
    }

    @Override
    public Long exec(Tuple tuple) throws IOException {
        return (long) hllFromValues(tuple).estimate();
    }

    private static HyperLogLog hllFromValues(Tuple input) throws ExecException {
        Object values = input.get(0);

        HyperLogLog hll = new HyperLogLog(12);

        if (values instanceof DataBag) {
            DataBag data = (DataBag) values;

            for (Tuple value : data) {
                hll.add(value.get(0).toString());
            }
        }

        return hll;
    }

    private static HyperLogLog hllFromHlls(Tuple input) throws ExecException {
        HyperLogLog hll = new HyperLogLog(12);
        Object values = input.get(0);

        if (values instanceof DataBag) {
            DataBag data = (DataBag)values;

            for (Tuple value : data) {
                if (value.size() == 0) {
                    continue;
                }

                String valueType = value.get(0).toString();
                String valueStr = value.get(1).toString();
                if (valueType.equals("Value")) {
                    hll.add(valueStr);
                } else {
                    hll.merge(new HyperLogLog(valueStr));
                }
            }
        }

        return hll;
    }
}
