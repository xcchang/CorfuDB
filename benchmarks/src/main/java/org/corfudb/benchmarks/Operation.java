package org.corfudb.benchmarks;

import org.corfudb.runtime.CorfuRuntime;

public abstract class Operation {
    CorfuRuntime rt;
    String shortName;

    public Operation(CorfuRuntime rt) {
        this.rt = rt;
    }

    public abstract void execute();
}