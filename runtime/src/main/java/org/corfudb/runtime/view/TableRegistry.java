package org.corfudb.runtime.view;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.corfudb.util.serializer.Serializers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Table Registry manages the lifecycle of all the tables in the system.
 * This is a wrapper over the CorfuTable providing transactional operations and accepts only protobuf messages.
 * It accepts a primary key - which is a protobuf message.
 * The payload is a CorfuRecord which comprises of 2 fields - Payload and Metadata. These are protobuf messages as well.
 * The table creation registers the schema used in the table which is further used for serialization. This schema
 * is also used for offline (without access to client protobuf files) browsing and editing.
 * <p>
 * Created by zlokhandwala on 2019-08-10.
 */
public class TableRegistry {

    /**
     * System Table: To store the table schemas and other options.
     * This information is used to view and edit table using an offline tool without the dependency on the
     * application for the schemas.
     */
    private static final String CORFU_SYSTEM_NAMESPACE = "CorfuSystem";
    private static final String REGISTRY_TABLE_NAME = "RegistryTable";

    /**
     * Connected runtime instance.
     */
    private final CorfuRuntime runtime;

    /**
     * Stores the schemas of the Key, Value and Metadata.
     * A reference of this map is held by the {@link ProtobufSerializer} to serialize and deserialize the objects.
     */
    private final ConcurrentMap<String, Class<? extends Message>> classMap;

    /**
     * Cache of tables allowing the user to fetch a table by fullyQualified table name without the other options.
     */
    private final ConcurrentMap<String, Table<Message, Message, Message>> tableMap;

    /**
     * Serializer to be used for protobuf messages.
     */
    private final ISerializer protobufSerializer;

    /**
     * This {@link CorfuTable} holds the schemas of the key, payload and metadata for every table created.
     */
    private final CorfuTable<TableName, CorfuRecord<TableDescriptors, Message>> registryTable;

    /**
     * Byte code for the Protobuf Serializer.
     */
    private static final byte PROTOBUF_SERIALIZER_CODE = (byte) 25;

    public TableRegistry(CorfuRuntime runtime) {
        this.runtime = runtime;
        this.classMap = new ConcurrentHashMap<>();
        this.tableMap = new ConcurrentHashMap<>();
        this.protobufSerializer = new ProtobufSerializer(PROTOBUF_SERIALIZER_CODE, classMap);
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

        // Register the registry table itself.
        try {
            registerTable(CORFU_SYSTEM_NAMESPACE,
                    REGISTRY_TABLE_NAME,
                    TableName.class,
                    TableDescriptors.class,
                    null);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Register a table in the internal Table Registry.
     *
     * @param namespace     Namespace of the table to be registered.
     * @param tableName     Table name of the table to be registered.
     * @param keyClass      Key class.
     * @param payloadClass    Value class.
     * @param metadataClass Metadata class.
     * @param <K>           Type of Key.
     * @param <V>           Type of Value.
     * @param <M>           Type of Metadata.
     * @throws NoSuchMethodException     If this is not a protobuf message.
     * @throws InvocationTargetException If this is not a protobuf message.
     * @throws IllegalAccessException    If this is not a protobuf message.
     */
    private <K extends Message, V extends Message, M extends Message>
    void registerTable(@Nonnull String namespace,
                       @Nonnull String tableName,
                       @Nonnull Class<K> keyClass,
                       @Nonnull Class<V> payloadClass,
                       @Nullable Class<M> metadataClass)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        TableName tableNameKey = TableName.newBuilder()
                .setNamespace(namespace)
                .setTableName(tableName)
                .build();

        K defaultKeyMessage = (K) keyClass.getMethod("getDefaultInstance").invoke(null);
        V defaultValueMessage = (V) payloadClass.getMethod("getDefaultInstance").invoke(null);

        TableDescriptors.Builder tableDescriptorsBuilder = TableDescriptors.newBuilder()
                .setKeyDescriptor(defaultKeyMessage.getDescriptorForType().toProto())
                .setKeyClass(keyClass.getName())
                .setValueDescriptor(defaultValueMessage.getDescriptorForType().toProto())
                .setValueClass(payloadClass.getName());
        if (metadataClass != null) {
            M defaultMetadataMessage = (M) metadataClass.getMethod("getDefaultInstance").invoke(null);
            tableDescriptorsBuilder
                    .setMetadataDescriptor(defaultMetadataMessage.getDescriptorForType().toProto())
                    .setMetadataClass(metadataClass.getName());
        }
        TableDescriptors tableDescriptors = tableDescriptorsBuilder.build();

        try {
            this.runtime.getObjectsView().TXBuild().type(TransactionType.OPTIMISTIC).build().begin();
            this.registryTable.putIfAbsent(tableNameKey,
                    new CorfuRecord<>(tableDescriptors, null));
        } finally {
            this.runtime.getObjectsView().TXEnd();
        }
    }

    /**
     * Gets the type Url of the protobuf descriptor. Used to identify the message during serialization.
     * Note: This is same as used in Any.proto.
     *
     * @param descriptor Descriptor of the protobuf.
     * @return Type url string.
     */
    private final static String getTypeUrl(Descriptor descriptor) {
        return "type.googleapis.com/" + descriptor.getFullName();
    }

    /**
     * Fully qualified table name created to produce the stream uuid.
     *
     * @param namespace Namespace of the table.
     * @param tableName Table name of the table.
     * @return Fully qualified table name.
     */
    private String getFullyQualifiedTableName(String namespace, String tableName) {
        return namespace + "$" + tableName;
    }

    /**
     * Adds the schema to the class map to enable serialization of this table data.
     *
     * @param msg Default message of this protobuf message.
     * @param <T> Type of message.
     */
    private <T extends Message> void addTypeToClassMap(T msg) {
        String typeUrl = getTypeUrl(msg.getDescriptorForType());
        // Register the schemas to schema table.
        if (!classMap.containsKey(typeUrl)) {
            classMap.put(typeUrl, msg.getClass());
        }
    }

    /**
     * Opens a Corfu table with the specified options.
     *
     * @param namespace    Namespace of the table.
     * @param tableName    Name of the table.
     * @param kClass       Key class.
     * @param vClass       Value class.
     * @param mClass       Metadata class.
     * @param tableOptions Table options.
     * @param <K>          Key type.
     * @param <V>          Value type.
     * @param <M>          Metadata type.
     * @return Table instance.
     * @throws NoSuchMethodException     If this is not a protobuf message.
     * @throws InvocationTargetException If this is not a protobuf message.
     * @throws IllegalAccessException    If this is not a protobuf message.
     */
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

        // Open and return table instance.
        Table<K, V, M> table = new Table<>(
                namespace,
                fullyQualifiedTableName,
                defaultValueMessage,
                defaultMetadataMessage,
                this.runtime,
                this.protobufSerializer);
        tableMap.put(fullyQualifiedTableName, (Table<Message, Message, Message>) table);

        registerTable(namespace, tableName, kClass, vClass, mClass);
        return table;
    }

    /**
     * Get an already opened table. Fetches the table from the cache given only the namespace and table name.
     * Throws a NoSuchElementException if table is not previously opened and not present in cache.
     *
     * @param namespace Namespace of the table.
     * @param tableName Name of the table.
     * @param <K>       Key type.
     * @param <V>       Value type.
     * @param <M>       Metadata type.
     * @return Table instance.
     */
    public <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> getTable(String namespace, String tableName) {
        String fullyQualifiedTableName = getFullyQualifiedTableName(namespace, tableName);
        if (!tableMap.containsKey(fullyQualifiedTableName)) {
            throw new NoSuchElementException(
                    String.format("No such table found: namespace: %s, tableName: %s", namespace, tableName));
        }
        return (Table<K, V, M>) tableMap.get(fullyQualifiedTableName);
    }

    /**
     * Deletes a table.
     *
     * @param namespace Namespace of the table.
     * @param tableName Name of the table.
     */
    public void deleteTable(String namespace, String tableName) {
        throw new UnsupportedOperationException("deleteTable unsupported for now");
    }

    /**
     * Lists all the tables for a namespace.
     *
     * @param namespace Namespace for a table.
     * @return Collection of tables.
     */
    public Collection<TableName> listTables(@Nullable final String namespace) {
        return registryTable.keySet()
                .stream()
                .filter(tableName -> namespace == null || tableName.getNamespace().equals(namespace))
                .collect(Collectors.toList());
    }

    /**
     * Gets the table descriptors for a particular fully qualified table name.
     * This is used for reconstructing a message when the schema is not available.
     *
     * @param tableName Namespace and name of the table.
     * @return Table Descriptor.
     */
    @Nullable
    public TableDescriptors getTableDescriptor(@Nonnull TableName tableName) {
        return Optional.ofNullable(this.registryTable.get(tableName))
                .map(CorfuRecord::getPayload)
                .orElse(null);
    }
}