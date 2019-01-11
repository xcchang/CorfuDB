package org.corfudb.runtime.exceptions;

public class CheckpointException extends RuntimeException {
    public CheckpointException(Throwable t) {
        super((t));
    }

    public CheckpointException(String msg) {
        super(msg);
    }
}
