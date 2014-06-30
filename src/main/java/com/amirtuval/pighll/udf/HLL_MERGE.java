package com.amirtuval.pighll.udf;

import org.apache.pig.data.Tuple;

import java.io.IOException;

public class HLL_MERGE extends HyperLogLogUdfBase<String> {
    public String getInitial() {return InitialHLL.class.getName();}
    public String getFinal() {return FinalHll.class.getName();}

    @Override
    public String exec(Tuple tuple) throws IOException {
        return hllFromHlls(tuple, getLogger()).asString();
    }

}
