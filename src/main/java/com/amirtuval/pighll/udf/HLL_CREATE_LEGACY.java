package com.amirtuval.pighll.udf;

import org.apache.pig.data.Tuple;

import java.io.IOException;

public class HLL_CREATE_LEGACY extends HyperLogLogUdfBase<String> {

    public String getInitial() {return InitialScalar.class.getName();}
    public String getIntermed() {return LegacyIntermed.class.getName();}
    public String getFinal() {return LegacyFinalHll.class.getName();}

    @Override
    public String exec(Tuple tuple) throws IOException {
        return hllFromValues(tuple, legacyHll()).asString();
    }
}
