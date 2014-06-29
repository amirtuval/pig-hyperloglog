package com.amirtuval.pighll;

import junit.framework.TestCase;

public class HyperLogLogTest extends TestCase{

    public void testConstructor() {
        HyperLogLog hll = new HyperLogLog(12);
        hll.close();
    }

    public void testEstimate() {
        HyperLogLog hll = new HyperLogLog(12);

        hll.add("a");
        hll.add("ab");
        hll.add("ba");
        hll.add("abc");
        hll.add("a");
        hll.add("a");
        hll.add("abc");
        assertEquals(4, (int) hll.estimate());
    }

    public void testEstimate2() {
        HyperLogLog hll = new HyperLogLog(12);

        for(int i = 0; i < 500000; ++i) {
            hll.add(new Integer(i % 2000).toString());
        }

        assertEquals(2017, (int)hll.estimate());
    }

    public void testMerge() {
        HyperLogLog hll = new HyperLogLog(12);

        for(int i = 0; i < 50000; ++i) {
            hll.add(new Integer(i).toString());
        }

        HyperLogLog hll2 = new HyperLogLog(12);

        for(int i = 25000; i < 75000; ++i) {
            hll2.add(new Integer(i).toString());
        }

        assertEquals(50254, (int)hll.estimate());
        assertEquals(50001, (int)hll2.estimate());

        hll.merge(hll2);

        assertEquals(75487, (int)hll.estimate());
    }

    public void testSerialization() {
        HyperLogLog hll = new HyperLogLog(12);

        for(int i = 0; i < 50000; ++i) {
            hll.add(new Integer(i).toString());
        }

        double estimate = hll.estimate();
        assertEquals(50254, (int) estimate);

        String serialized = hll.asString();

        HyperLogLog hll2 = new HyperLogLog(serialized);
        assertEquals(estimate, hll2.estimate());
    }
}
