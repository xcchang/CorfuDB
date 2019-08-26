package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.Message;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.RecordMetadata;
import org.corfudb.runtime.object.transactions.TransactionType;

import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.util.serializer.ISerializer;
import lombok.Getter;

/**
 * Created by zlokhandwala on 2019-08-05.
 */
public class Table<K extends Message, V extends Message> {

    private final CorfuRuntime corfuRuntime;

    private final CorfuTable<K, CorfuRecord<V>> corfuTable;

    /**
     * Namespace this table belongs in.
     */
    @Getter
    private final String namespace;

    /**
     * Fully qualified table name: created by the namespace and the tablename.
     */
    @Getter
    private final String fullyQualifiedTableName;

    /**
     * Returns a Table instance backed by a CorfuTable.
     *
     * @param namespace               Namespace of the table.
     * @param fullyQualifiedTableName Fully qualified table name.
     * @param valueSchema             Value schema to identify secondary keys.
     * @param corfuRuntime            Connected instance of the Corfu Runtime.
     * @param serializer              Protobuf Serializer.
     */
    @Nonnull
    public Table(@Nonnull final String namespace,
                 @Nonnull final String fullyQualifiedTableName,
                 @Nonnull final Message valueSchema,
                 @Nonnull final CorfuRuntime corfuRuntime,
                 @Nonnull final ISerializer serializer) {

        this.corfuRuntime = corfuRuntime;
        this.namespace = namespace;
        this.fullyQualifiedTableName = fullyQualifiedTableName;
        this.corfuTable = corfuRuntime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<K, CorfuRecord<V>>>() {
                })
                .setStreamName(this.fullyQualifiedTableName)
                .setSerializer(serializer)
                .setArguments(new ProtobufIndexer(valueSchema))
                .open();
    }

    /**
     * Begins a write after write transaction.
     */
    private boolean TxBegin() {
        if (!TransactionalContext.isInTransaction()) {
            corfuRuntime.getObjectsView()
                    .TXBuild()
                    .type(TransactionType.OPTIMISTIC)
                    .build().begin();
            return true;
        }
        return false;
    }

    /**
     * Ends an ongoing transaction.
     */
    private void TxEnd() {
        corfuRuntime.getObjectsView().TXEnd();
    }

    /**
     * Create a new record.
     *
     * @param key      Key
     * @param value    Value
     * @param metadata Record metadata.
     * @return Previously stored record if any.
     */
    @Nullable
    public CorfuRecord<V> create(@Nonnull final K key,
                                 @Nullable final V value,
                                 @Nullable final RecordMetadata metadata) {
        boolean beganNewTxn = false;
        try {
            beganNewTxn = TxBegin();
            return corfuTable.put(key, new CorfuRecord<>(value, metadata));
        } finally {
            if (beganNewTxn) {
                TxEnd();
            }
        }
    }

    /**
     * Fetch the value for a key.
     *
     * @param key Key.
     * @return Corfu Record for key.
     */
    @Nullable
    public CorfuRecord<V> get(@Nonnull final K key) {
        return corfuTable.get(key);
    }

    /**
     * Update an existing key with the provided value.
     *
     * @param key      Key.
     * @param value    Value.
     * @param metadata Metadata.
     * @return Previously stored value for the provided key.
     */
    @Nullable
    public CorfuRecord<V> update(@Nonnull final K key,
                                 @Nonnull final V value,
                                 @Nullable final RecordMetadata metadata) {
        boolean beganNewTxn = false;
        try {
            beganNewTxn = TxBegin();
            return corfuTable.put(key, new CorfuRecord<>(value, metadata));
        } finally {
            if (beganNewTxn) {
                TxEnd();
            }
        }
    }

    /**
     * Delete a record mapped to the specified key.
     *
     * @param key Key.
     * @return Previously stored Corfu Record.
     */
    @Nullable
    public CorfuRecord<V> delete(@Nonnull final K key) {
        boolean beganNewTxn = false;
        try {
            beganNewTxn = TxBegin();
            return corfuTable.remove(key);
        } finally {
            if (beganNewTxn) {
                TxEnd();
            }
        }
    }

    /**
     * Count of records in the table.
     *
     * @return Count of records.
     */
    int count() {
        return corfuTable.size();
    }

    /**
     * Scan and filter.
     *
     * @param p Predicate to filter the values.
     * @return Collection of filtered values.
     */
    @Nonnull
    Collection<V> scanAndFilter(@Nonnull final Predicate<V> p) {
        return corfuTable.scanAndFilter(vCorfuRecord -> p.test(vCorfuRecord.getPayload())).stream()
                .map(CorfuRecord::getPayload)
                .collect(Collectors.toList());
    }

    /**
     * Get by secondary index.
     *
     * @param indexName Index name.
     * @param indexKey  Index key.
     * @param <I>       Type of index key.
     * @return Collection of entries filtered by the secondary index.
     */
    @Nonnull
    protected <I extends Comparable<I>>
    Collection<Entry<K, V>> getByIndex(@Nonnull final String indexName,
                                       @Nonnull final I indexKey) {
        return corfuTable.getByIndex(() -> indexName, indexKey).stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().getPayload()))
                .collect(Collectors.toList());
    }
}
