package com.amirtuval.pighll.udf;

import com.amirtuval.pighll.HyperLogLog;
import junit.framework.TestCase;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class UdfTest extends TestCase {

    public void testCompute() throws IOException {
        PigServer pig = new PigServer(ExecType.LOCAL);

        pig.registerQuery("A = LOAD 'jni/mysql-hyperloglog/sql/data.csv' using PigStorage(',') AS (url:chararray, user_id:int, visit_time: chararray, visit_time_in_minutes: int);");
        pig.registerQuery("B = GROUP A BY url;");
        pig.registerQuery("C = FOREACH B GENERATE group, com.amirtuval.pighll.udf.HLL_COMPUTE(A.user_id);");

        Iterator<Tuple> it = pig.openIterator("C");
        HashMap<String, Long> results = new HashMap<String, Long>();
        while(it.hasNext()) {
            Tuple t = it.next();
            results.put((String)t.get(0), (Long)t.get(1));
        }

        assertEquals(5, results.size());
        assertEquals(6330L, (long)results.get("http://www.abc.com"));
        assertEquals(6521L, (long)results.get("http://www.cnn.com"));
        assertEquals(6077L, (long)results.get("http://www.yahoo.com"));
        assertEquals(6483L, (long)results.get("http://www.google.com"));
        assertEquals(6752L, (long)results.get("http://www.wikipedia.org"));
    }

    public void testAllUDFs() throws IOException {
        PigServer pig = new PigServer(ExecType.LOCAL);

        pig.registerQuery("A = LOAD 'jni/mysql-hyperloglog/sql/data.csv' using PigStorage(',') AS (url:chararray, user_id:int, visit_time: chararray, visit_time_in_minutes: int);");
        pig.registerQuery("B = GROUP A BY url;");
        pig.registerQuery("C = FOREACH B GENERATE group as url, com.amirtuval.pighll.udf.HLL_CREATE(A.user_id) as hll;");
        pig.registerQuery("D = GROUP C ALL;");
        pig.registerQuery("E = FOREACH D GENERATE com.amirtuval.pighll.udf.HLL_MERGE_COMPUTE(C.hll);");
        pig.registerQuery("F = FOREACH D GENERATE com.amirtuval.pighll.udf.HLL_MERGE(C.hll);");

        Iterator<Tuple> it = pig.openIterator("E");

        assertTrue(it.hasNext());
        assertEquals(10140L, it.next().get(0));

        it = pig.openIterator("F");

        assertTrue(it.hasNext());
        long estimate = (long)new HyperLogLog(it.next().get(0).toString()).estimate();
        assertEquals(10140L, estimate);

    }
}
