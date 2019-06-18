package org.corfudb.runtime.kvstore;

import lombok.Builder;
import lombok.NonNull;
import org.corfudb.common.result.Result;
import org.corfudb.runtime.KvStore;
import org.corfudb.runtime.KvStore.ActionResult;
import org.corfudb.runtime.KvStore.Record;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.kv.core.DbKey;
import org.corfudb.runtime.kv.core.DbValue;
import org.corfudb.runtime.kv.service.Crud.CreateOperation;
import org.corfudb.runtime.kv.service.Crud.QueryOperation;
import org.corfudb.runtime.kv.core.KvStoreException;

@Builder
public class CrudService {

    @NonNull
    private final KvStore.TableSchema tableSchema;

    @NonNull
    private final CorfuTable<KvStore.Key, Record> table;

    public QueryOperation<Result<ActionResult, KvStoreException>> get(KvStore.GetQuery query) {
        return () -> Result.of(() -> {
            Record record = null; //table.get(new DbKey(query.getKey(), Object.class));
            return ActionResult.newBuilder().setResult(record).build();
        });
    }

    public CreateOperation<Result<ActionResult, KvStoreException>>
    create(KvStore.Command createCommand) {
        return () -> Result.of(() -> {
            DbKey key = null; //new DbKey(createCommand.getRecord().getKey());
            Record record = table.putIfAbsent(key, createCommand.getRecord());
            return ActionResult.newBuilder().setResult(record).build();
        });
    }
}
