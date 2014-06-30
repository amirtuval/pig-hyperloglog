package com.amirtuval.pighll.udf;

import junit.framework.TestCase;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;

public class UdfTest extends TestCase {

    public void testCompute() throws IOException {
        PigServer pig = new PigServer(ExecType.LOCAL);

        pig.registerQuery("A = LOAD '../jni/mysql-hyperloglog/sql/data.csv' using PigStorage(',') AS (url:chararray, user_id:int, visit_time: chararray, visit_time_in_minutes: int);");
        pig.registerQuery("B = GROUP A BY url;");
        pig.registerQuery("C = FOREACH B GENERATE group, com.amirtuval.pighll.udf.HLL_COMPUTE(A.user_id);");

        Iterator<Tuple> it = pig.openIterator("C");
        HashMap<String, Long> results = new HashMap<String, Long>();
        while(it.hasNext()) {
            Tuple t = it.next();
            results.put((String)t.get(0), (Long)t.get(1));
        }

        assertEquals(5, results.size());
        assertEquals(6407L, (long)results.get("http://www.abc.com"));
        assertEquals(6378L, (long)results.get("http://www.cnn.com"));
        assertEquals(6378L, (long)results.get("http://www.yahoo.com"));
        assertEquals(6593L, (long)results.get("http://www.google.com"));
        assertEquals(6470L, (long)results.get("http://www.wikipedia.org"));
    }
}
