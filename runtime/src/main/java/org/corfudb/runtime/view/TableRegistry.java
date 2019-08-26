package org.corfudb.runtime.view;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.RecordMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;

/**
 * Created by zlokhandwala on 2019-08-10.
 */
public class TableRegistry {

    private static final String CORFU_SYSTEM_NAMESPACE = "CorfuSystem";
    private static final String REGISTRY_TABLE_NAME = "RegistryTable";

    private final CorfuRuntime runtime;

    private final Map<String, Class<? extends Message>> classMap;

    private final Map<String, Table<Message, Message>> tableMap;

    private final ISerializer protobufSerializer;

    private final CorfuTable<TableName, CorfuRecord<TableDescriptors>> registryTable;

    public TableRegistry(CorfuRuntime runtime) {
        this.runtime = runtime;
        this.classMap = new ConcurrentHashMap<>();
        this.tableMap = new ConcurrentHashMap<>();
        this.protobufSerializer = new ProtobufSerializer((byte) 25, classMap);
        this.registryTable = this.runtime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<TableName, CorfuRecord<TableDescriptors>>>() {
                })
                .setStreamName(getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, REGISTRY_TABLE_NAME))
                .setSerializer(this.protobufSerializer)
                .open();

        registerTable(CORFU_SYSTEM_NAMESPACE,
                REGISTRY_TABLE_NAME,
                TableName.getDescriptor().toProto(),
                TableDescriptors.getDescriptor().toProto());
    }

    private void registerTable(String namespace,
                               String tableName,
                               DescriptorProto keyDescriptor,
                               DescriptorProto valueDescriptor) {

        TableName tableNameKey = TableName.newBuilder()
                .setNamespace(namespace)
                .setTableName(tableName)
                .build();
        TableDescriptors tableDescriptors = TableDescriptors.newBuilder()
                .setKeyDescriptor(keyDescriptor)
                .setValueDescriptor(valueDescriptor)
                .build();

        this.runtime.getObjectsView().TXBuild().type(TransactionType.OPTIMISTIC).build().begin();
        this.registryTable.putIfAbsent(tableNameKey,
                new CorfuRecord<>(tableDescriptors, RecordMetadata.newBuilder().build()));
        this.runtime.getObjectsView().TXEnd();
    }

    private String getTypeUrl(Descriptor descriptor) {
        return "type.googleapis.com/" + descriptor.getFullName();
    }

    private String getFullyQualifiedTableName(String namespace, String tableName) {
        return namespace + "$" + tableName;
    }

    public <K extends Message, V extends Message> Table<K, V> openTable(String namespace,
                                                                        String tableName,
                                                                        Class<K> kClass,
                                                                        Class<V> vClass,
                                                                        TableOptions tableOptions)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        K defaultKeyMessage = (K) kClass.getMethod("getDefaultInstance").invoke(null);
        V defaultValueMessage = (V) vClass.getMethod("getDefaultInstance").invoke(null);

        // Put FileDescriptor in registry for offline tool.

        String keyTypeUrl = getTypeUrl(defaultKeyMessage.getDescriptorForType());
        String valueTypeUrl = getTypeUrl(defaultValueMessage.getDescriptorForType());

        // Register the schemas to schema table.
        if (!classMap.containsKey(keyTypeUrl)) {
            classMap.put(keyTypeUrl, kClass);
        }
        if (!classMap.containsKey(valueTypeUrl)) {
            classMap.put(valueTypeUrl, vClass);
        }

        String fullyQualifiedTableName = getFullyQualifiedTableName(namespace, tableName);

        // Open and return table instance.
        Table<K, V> table = new Table<>(namespace, fullyQualifiedTableName,
                defaultValueMessage,
                this.runtime, this.protobufSerializer);
        tableMap.put(fullyQualifiedTableName, (Table<Message, Message>) table);
        registerTable(namespace,
                tableName,
                defaultKeyMessage.getDescriptorForType().toProto(),
                defaultValueMessage.getDescriptorForType().toProto());
        return table;
    }

    public <K extends Message, V extends Message> Table<K, V> getTable(String namespace, String tableName) {
        String fullyQualifiedTableName = getFullyQualifiedTableName(namespace, tableName);
        if (!tableMap.containsKey(fullyQualifiedTableName)) {
            throw new NoSuchElementException(
                    String.format("No such table found: namespace: %s, tableName: %s", namespace, tableName));
        }
        return (Table<K, V>) tableMap.get(fullyQualifiedTableName);
    }

    public void deleteTable(String namespace, String tableName) {
    }

    public Collection<TableName> listTables(final String namespace) {
        return registryTable.keySet()
                .stream()
                .filter(tableName -> namespace == null || tableName.getNamespace().equals(namespace))
                .collect(Collectors.toList());
    }
}
