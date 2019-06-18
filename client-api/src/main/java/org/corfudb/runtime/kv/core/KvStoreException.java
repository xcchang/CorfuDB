package org.corfudb.runtime.kv.core;

public class KvStoreException extends RuntimeException {

    public KvStoreException() {
        super();
    }

    public KvStoreException(String message) {
        super(message);
    }

    public KvStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public KvStoreException(Throwable cause) {
        super(cause);
    }

    public KvStoreException(
            String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
