package com.amirtuval.pighll.udf;

import com.amirtuval.pighll.HyperLogLog;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;

public class HLL_COMPUTE extends EvalFunc<Long> implements Algebraic {

    public String getInitial() {return Initial.class.getName();}
    public String getIntermed() {return Intermed.class.getName();}
    public String getFinal() {return Final.class.getName();}

    static public class Initial extends EvalFunc<Tuple> {
        public Tuple exec(Tuple input) throws IOException {
            return TupleFactory.getInstance().newTuple(hllCount(input).asString());
         }
    }

    static public class Intermed extends EvalFunc<Tuple> {
        public Tuple exec(Tuple input) throws IOException {
            return TupleFactory.getInstance().newTuple(hllMerge(input).asString());
        }
    }

    static public class Final extends EvalFunc<Long> {
        public Long exec(Tuple input) throws IOException {
            return (long)hllMerge(input).estimate();
        }
    }

    @Override
    public Long exec(Tuple tuple) throws IOException {
        return (long)hllCount(tuple).estimate();
    }

    private static HyperLogLog hllCount(Tuple input) throws ExecException {
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

    private static HyperLogLog hllMerge(Tuple input) throws ExecException {
        HyperLogLog hll = new HyperLogLog(12);
        Object values = input.get(0);

        if (values instanceof DataBag) {
            DataBag data = (DataBag)values;

            for (Tuple value : data) {
                hll.merge(new HyperLogLog(value.get(0).toString()));
            }
        }

        return hll;
    }
}
