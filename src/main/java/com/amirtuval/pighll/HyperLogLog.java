package com.amirtuval.pighll;

public class HyperLogLog {
    static {
        System.loadLibrary("pighll");
    }

    private long mHll;

    private native long createHll(int b, boolean legacyMode);
    private native long createHllFromString(String hllStr);
    private native void freeHll(long hll);
    private native void addElement(long hll, String element);
    private native double estimateHll(long hll);
    private native void mergeHll(long target, long source);
    private native String hllAsString(long hll);

    public HyperLogLog(int b) {
        this(b, true);
    }

    public HyperLogLog(int b, boolean legacyMode) {
        mHll = createHll(b, legacyMode);
    }

    public HyperLogLog(String hllStr) {
        mHll = createHllFromString(hllStr);
    }

    public void close() {
        if (mHll != 0) {
            freeHll(mHll);
            mHll = 0;
        }
    }

    public void add(String element) {
        addElement(mHll, element);
    }

    public double estimate() {
        return estimateHll(mHll);
    }

    public void merge(HyperLogLog other) {
        mergeHll(mHll, other.mHll);
    }

    public String asString() {
        return hllAsString(mHll);

    }

    protected void finalize() throws Throwable {
        close();
    }
}
