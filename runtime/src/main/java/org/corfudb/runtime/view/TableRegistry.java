package org.corfudb.runtime.view;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.RocksDbStreamingMap;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.corfudb.util.serializer.Serializers;
import org.rocksdb.Options;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Created by zlokhandwala on 2019-08-10.
 */
public class TableRegistry {

    private static final String CORFU_SYSTEM_NAMESPACE = "CorfuSystem";
    private static final String REGISTRY_TABLE_NAME = "RegistryTable";

    private final CorfuRuntime runtime;

    private final Map<String, Class<? extends Message>> classMap;

    private final Map<String, Table<Message, Message, Message>> tableMap;

    private final ISerializer protobufSerializer;

    private final CorfuTable<TableName, CorfuRecord<TableDescriptors, Message>> registryTable;

    public TableRegistry(CorfuRuntime runtime) {
        this.runtime = runtime;
        this.classMap = new ConcurrentHashMap<>();
        this.tableMap = new ConcurrentHashMap<>();
        this.protobufSerializer = new ProtobufSerializer((byte) 25, classMap);
        Serializers.registerSerializer(this.protobufSerializer);
        this.registryTable = this.runtime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<TableName, CorfuRecord<TableDescriptors, Message>>>() {
                })
                .setStreamName(getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, REGISTRY_TABLE_NAME))
                .setSerializer(this.protobufSerializer)
                .open();

        // Register the table schemas to schema table.
        addTypeToClassMap(TableName.getDefaultInstance());
        addTypeToClassMap(TableDescriptors.getDefaultInstance());

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

        try {
            this.runtime.getObjectsView().TXBuild().type(TransactionType.OPTIMISTIC).build().begin();
            this.registryTable.putIfAbsent(tableNameKey,
                    new CorfuRecord<>(tableDescriptors, null));
        } finally {
            this.runtime.getObjectsView().TXEnd();
        }
    }

    private String getTypeUrl(Descriptor descriptor) {
        return "type.googleapis.com/" + descriptor.getFullName();
    }

    private String getFullyQualifiedTableName(String namespace, String tableName) {
        return namespace + "$" + tableName;
    }

    private <T extends Message> void addTypeToClassMap(T msg) {
        String typeUrl = getTypeUrl(msg.getDescriptorForType());
        // Register the schemas to schema table.
        if (!classMap.containsKey(typeUrl)) {
            classMap.put(typeUrl, msg.getClass());
        }
    }

    public <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> openTable(@Nonnull final String namespace,
                             @Nonnull final String tableName,
                             @Nonnull final Class<K> kClass,
                             @Nonnull final Class<V> vClass,
                             @Nullable final Class<M> mClass,
                             @Nonnull final TableOptions tableOptions)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        // Register the schemas to schema table.
        K defaultKeyMessage = (K) kClass.getMethod("getDefaultInstance").invoke(null);
        addTypeToClassMap(defaultKeyMessage);

        V defaultValueMessage = (V) vClass.getMethod("getDefaultInstance").invoke(null);
        addTypeToClassMap(defaultValueMessage);

        M defaultMetadataMessage = null;
        if (mClass != null) {
            defaultMetadataMessage = (M) mClass.getMethod("getDefaultInstance").invoke(null);
            addTypeToClassMap(defaultMetadataMessage);
        }

        String fullyQualifiedTableName = getFullyQualifiedTableName(namespace, tableName);

        Table<K, V, M> table;
        if (tableOptions.getDiskPath().isEmpty()) {
            table = new Table<>(
                    namespace,
                    fullyQualifiedTableName,
                    defaultValueMessage,
                    defaultMetadataMessage,
                    this.runtime,
                    this.protobufSerializer,
                    Optional.empty());
        } else {
            Options options = new Options();
            options.setCreateIfMissing(true);
            RocksDbStreamingMap map = new RocksDbStreamingMap<String, String>(
                    new File(tableOptions.getDiskPath()), options,
                    this.protobufSerializer, this.runtime);

            table = new Table<>(
                    namespace,
                    fullyQualifiedTableName,
                    defaultValueMessage,
                    defaultMetadataMessage,
                    this.runtime,
                    this.protobufSerializer,
                    Optional.of(map));
        }
        // Open and return table instance.
        tableMap.put(fullyQualifiedTableName, (Table<Message, Message, Message>) table);
        registerTable(namespace,
                tableName,
                defaultKeyMessage.getDescriptorForType().toProto(),
                defaultValueMessage.getDescriptorForType().toProto());
        return table;
    }

    public <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> getTable(String namespace, String tableName) {
        String fullyQualifiedTableName = getFullyQualifiedTableName(namespace, tableName);
        if (!tableMap.containsKey(fullyQualifiedTableName)) {
            throw new NoSuchElementException(
                    String.format("No such table found: namespace: %s, tableName: %s", namespace, tableName));
        }
        return (Table<K, V, M>) tableMap.get(fullyQualifiedTableName);
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
