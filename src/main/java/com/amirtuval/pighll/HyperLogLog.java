package com.amirtuval.pighll;

public class HyperLogLog {
    static {
        System.load("pighll");
    }

    private long mHll;

    private native long createHll(int b);
    private native void freeHll(long hll);

    public HyperLogLog(int b) {
        mHll = createHll(b);
    }

    public void close() {
        if (mHll != 0) {
            freeHll(mHll);
            mHll = 0;
        }
    }

    protected void finalize() throws Throwable {
        close();
    }
}
