package org.corfudb.runtime.kvstore;

import lombok.Builder;
import org.corfudb.runtime.KvStore.TableSchema;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

@Builder
public class TableRepository {

    private final ConcurrentMap<TableSchema, CrudService> cache = new ConcurrentHashMap<>();

    private static final Function<TableSchema, CrudService> CREATE_TABLE = tableSchema -> {
        throw new UnsupportedOperationException("create corfu table");
    };

    public CrudService get(TableSchema schema) {
        return cache.computeIfAbsent(schema, CREATE_TABLE);
    }
}
