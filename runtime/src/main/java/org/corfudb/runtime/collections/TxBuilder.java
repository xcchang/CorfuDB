package org.corfudb.runtime.collections;

import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuStore.RecordMetadata;
import org.corfudb.runtime.CorfuStore.Timestamp;
import org.corfudb.runtime.object.transactions.Transaction;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.TableRegistry;

/**
 * Created by zlokhandwala on 2019-08-05.
 */
public class TxBuilder {

    private final ObjectsView objectsView;
    private final TableRegistry tableRegistry;
    private final String namespace;
    private Timestamp timestamp;
    private final List<Runnable> operations;

    /**
     * Creates a new TxBuilder.
     *
     * @param objectsView   ObjectsView from the Corfu client.
     * @param tableRegistry Table Registry.
     * @param namespace     Namespace boundary defined for the transaction.
     */
    @Nonnull
    TxBuilder(@Nonnull final ObjectsView objectsView,
              @Nonnull final TableRegistry tableRegistry,
              @Nonnull final String namespace) {
        this.objectsView = objectsView;
        this.tableRegistry = tableRegistry;
        this.namespace = namespace;
        this.operations = new ArrayList<>();
    }

    private static final RecordMetadata DEFAULT_RECORD_METADATA = RecordMetadata.newBuilder()
            .setVersion(0L)
            .build();

    private void validateNamespace(Table table) {
        if (table.getNamespace().equals(this.namespace)) {
            return;
        }
        throw new IllegalArgumentException(
                String.format("Transaction namespace: %s. Attempted transaction on table in namespace: %s",
                        this.namespace, table.getNamespace()));
    }

    private <K extends Message, V extends Message> Table<K, V> getTable(@Nonnull final String tableName) {
        return this.tableRegistry.getTable(this.namespace, tableName);
    }

    /**
     * Creates a record for the specified key.
     *
     * @param tableName Table name to perform the create on.
     * @param key       Key.
     * @param value     Value to create.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @return TxBuilder instance.
     */
    @Nonnull
    public <K extends Message, V extends Message> TxBuilder create(@Nonnull final String tableName,
                                                                   @Nonnull final K key,
                                                                   @Nullable final V value) {
        Table<K, V> table = getTable(tableName);
        validateNamespace(table);
        operations.add(() -> table.create(key, value, DEFAULT_RECORD_METADATA));
        return this;
    }

    /**
     * Updates the value on the specified key.
     *
     * @param tableName Table name to perform the update on.
     * @param key       Key.
     * @param value     Value to update.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @return TxBuilder instance.
     */
    @Nonnull
    public <K extends Message, V extends Message> TxBuilder update(@Nonnull final String tableName,
                                                                   @Nonnull final K key,
                                                                   @Nullable final V value) {
        Table<K, V> table = getTable(tableName);
        validateNamespace(table);
        operations.add(() -> {
            CorfuRecord<V> record = table.get(key);
            RecordMetadata metadata = DEFAULT_RECORD_METADATA;
            if (record != null) {
                metadata = record.getMetadata();
            }
            RecordMetadata newMetadata = RecordMetadata.newBuilder(metadata)
                    .setVersion(metadata.getVersion() + 1)
                    .build();
            table.update(key, value, newMetadata);
        });
        return this;
    }

    /**
     * Touches the specified key without mutating the version of the record.
     * This provides read after write conflict semantics.
     *
     * @param tableName Table name to perform the touch on.
     * @param key       Key to touch.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @return TxBuilder instance.
     */
    @Nonnull
    public <K extends Message, V extends Message> TxBuilder touch(@Nonnull final String tableName,
                                                                  @Nonnull final K key) {
        Table<K, V> table = getTable(tableName);
        validateNamespace(table);
        operations.add(() -> {
            CorfuRecord<V> record = table.get(key);
        });
        return this;
    }

    /**
     * Deletes the specified key.
     *
     * @param tableName Table name to perform the delete on.
     * @param key       Key.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @return TxBuilder instance.
     */
    @Nonnull
    public <K extends Message, V extends Message> TxBuilder delete(@Nonnull final String tableName,
                                                                   @Nonnull final K key) {
        Table<K, V> table = getTable(tableName);
        validateNamespace(table);
        operations.add(() -> table.delete(key));
        return this;
    }

    private void txBegin() {
        Transaction.TransactionBuilder transactionBuilder = this.objectsView
                .TXBuild()
                .type(TransactionType.OPTIMISTIC);
        if (timestamp != null) {
            transactionBuilder.snapshot(new Token(timestamp.getEpoch(), timestamp.getSequence()));
        }
        transactionBuilder.build().begin();
    }

    private void txEnd() {
        this.objectsView.TXEnd();
    }

    /**
     * Commit the transaction.
     * The commit call begins a Corfu transaction at the latest timestamp, applies all the updates and then
     * ends the Corfu transaction.
     * The commit returns successfully if the transaction was committed.
     * Otherwise this throws a TransactionAbortedException.
     */
    public void commit() {
        commit(null);
    }

    /**
     * Commit the transaction.
     * The commit call begins a Corfu transaction at the specified snapshot or at the latest snapshot
     * if no snapshot is specified, applies all the updates and then ends the Corfu transaction.
     * The commit returns successfully if the transaction was committed.
     * Otherwise this throws a TransactionAbortedException.
     *
     * @param timestamp Timestamp to commit the transaction on.
     */
    public void commit(final Timestamp timestamp) {
        this.timestamp = timestamp;
        try {
            txBegin();
            operations.forEach(Runnable::run);
        } finally {
            txEnd();
            operations.clear();
        }
    }
}
