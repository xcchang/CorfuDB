package org.corfudb.runtime.kvstore;

import org.corfudb.common.result.Result;
import org.corfudb.runtime.KvStore;
import org.corfudb.runtime.KvStore.ActionResult;
import org.corfudb.runtime.KvStore.Table;
import org.corfudb.runtime.KvStore.TableSchema;
import org.corfudb.runtime.kv.core.DbKey;
import org.corfudb.runtime.kv.core.KvBuilder;
import org.corfudb.runtime.kv.core.KvStoreException;
import org.junit.jupiter.api.Test;

class CrudServiceTest {

    @Test
    void get() {
        //Configuration
        Table table = Table.newBuilder().setName("my_table").build();

        TableSchema schema = TableSchema.newBuilder()
                .setTable(table)
                .build();

        TableRepository repository = TableRepository.builder().build();

        CrudService crud = repository.get(schema);


        //runtime
        KvStore.User user = KvStore.User.newBuilder().setName("Pavan").build();

        DbKey<KvStore.User> dbKey = KvBuilder.buildKey(user);

        KvStore.GetQuery getQuery = KvStore.GetQuery.newBuilder()
                .setKey(dbKey.getKey())
                .build();

        Result<ActionResult, KvStoreException> actionResult = crud.get(getQuery)
                .apply();

        actionResult.get().getResult().getValue().getPayload();

        System.out.println(actionResult);
    }
}