package com.amirtuval.pighll.udf;

import org.apache.pig.data.Tuple;

import java.io.IOException;

public class HLL_MERGE_COMPUTE extends HyperLogLogUdfBase<Long> {
    public String getInitial() {return InitialHLL.class.getName();}
    public String getFinal() {return FinalEstimate.class.getName();}

    @Override
    public Long exec(Tuple tuple) throws IOException {
        getLogger().error("!!! DEBUG !!! HLL_MERGE_COMPUTE.exec");
        return (long)hllFromHlls(tuple, getLogger()).estimate();
    }

}
