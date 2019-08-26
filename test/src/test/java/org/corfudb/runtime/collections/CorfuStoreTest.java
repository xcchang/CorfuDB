package org.corfudb.runtime.collections;

import com.google.protobuf.Message;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.RecordMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.SampleSchema.EventInfo;
import org.corfudb.test.SampleSchema.Uuid;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by zlokhandwala on 2019-08-12.
 */
public class CorfuStoreTest extends AbstractViewTest {

    @Test
    public void basicTest() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getDefaultRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);

        // Define a namespace for the table.
        final String nsxManager = "nsx-manager";
        // Define table name.
        final String tableName = "EventInfo";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, EventInfo> table = corfuStore.createTable(
                nsxManager,
                tableName,
                Uuid.class,
                EventInfo.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());


        final int count = 100;
        List<Uuid> uuids = new ArrayList<>();
        List<EventInfo> events = new ArrayList<>();

        // Simple CRUD using the table instance.
        // These are wrapped as transactional operations.
        table.create(Uuid.newBuilder().setLsb(0L).setMsb(0L).build(),
                EventInfo.newBuilder().setName("simpleCRUD").build(),
                RecordMetadata.newBuilder().setVersion(0).build());


        // Fetch timestamp to perform snapshot queries or transactions at a particular timestamp.
        Timestamp timestamp = corfuStore.getTimestamp();

        // Creating a transaction builder.
        TxBuilder tx = corfuStore.tx(nsxManager);
        for (int i = 0; i < count; i++) {
            UUID uuid = UUID.nameUUIDFromBytes(Integer.toString(i).getBytes());
            Uuid uuidMsg = Uuid.newBuilder()
                    .setMsb(uuid.getMostSignificantBits())
                    .setLsb(uuid.getLeastSignificantBits())
                    .build();
            uuids.add(uuidMsg);

            events.add(EventInfo.newBuilder()
                    .setId(i)
                    .setName("event_" + i)
                    .setEventTime(i)
                    .build());

            tx.update(tableName, uuids.get(i), events.get(i));
        }
        tx.commit();

        // Query interface.
        Query q = corfuStore.query(nsxManager);

        // Point lookup.
        final int fifty = 50;
        UUID uuid = UUID.nameUUIDFromBytes(Integer.toString(fifty).getBytes());
        Uuid lookupKey = Uuid.newBuilder()
                .setMsb(uuid.getMostSignificantBits())
                .setLsb(uuid.getLeastSignificantBits())
                .build();

        EventInfo expectedValue = EventInfo.newBuilder()
                .setId(fifty)
                .setName("event_" + fifty)
                .setEventTime(fifty)
                .build();

        assertThat((Message) q.get(tableName, timestamp, lookupKey)).isNull();
        assertThat((Message) q.get(tableName, lookupKey)).isEqualTo(expectedValue);

        // Get by secondary index.
        final long fiftyLong = 50L;
        Collection<Message> secondaryIndex = q.getByIndex(tableName, "event_time", fiftyLong).getResult()
                .stream()
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
        assertThat(secondaryIndex)
                .hasSize(1)
                .containsExactly(expectedValue);

        // Execute Query. (Scan and filter)
        final int sixty = 60;
        assertThat(q.exectuteQuery(tableName, event -> ((EventInfo) event).getEventTime() >= sixty)
                .getResult()
                .size())
                .isEqualTo(count - sixty);

        int count1 = corfuStore.query(nsxManager).count(tableName, timestamp);
        System.out.println("Size of table = " + q.count(tableName));
        System.err.println("Size of table = at timestamp :" + timestamp + " => " + count1);

        assertThat(corfuStore.listTables(nsxManager))
                .containsExactly(CorfuStoreMetadata.TableName.newBuilder()
                        .setNamespace(nsxManager)
                        .setTableName(tableName)
                        .build());

        table.update(lookupKey, EventInfo.getDefaultInstance(), null);
        assertThat(table.get(lookupKey))
                .isEqualTo(new CorfuRecord<>(EventInfo.getDefaultInstance(), null));
    }


//    @Test
//    public void prototest() throws Exception {
//        final int ruleId = 123;
//        FirewallRule rule = FirewallRule
//                .newBuilder()
//                .setRuleId(ruleId)
//                .setRuleName("TestRule")
//                .setInput(Appliance.newBuilder().setEndpoint("127.0.0.1").build())
//                .setOutput(Appliance.newBuilder().setEndpoint("196.168.0.1").build())
//                .build();
//
//        Message message = rule;
//
//        message.getAllFields().forEach((fieldDescriptor, field) -> {
//            if (fieldDescriptor.getOptions().getExtension(CorfuOptions.schema).getPrimaryKey()) {
//                System.out.println("Detected primary key " + fieldDescriptor.getName() + " = " + field);
//            }
//            if (fieldDescriptor.getOptions().getExtension(CorfuOptions.schema).getSecondaryKey()) {
//                System.out.println("Detected secondary key " + fieldDescriptor.getName() + " = " + field);
//            }
//        });
//
//        FileDescriptor applianceFileDescriptor = Appliance.getDescriptor().getFile();
//        FileDescriptor firewallFileDescriptor = FirewallRule.getDescriptor().getFile();
//        FileDescriptor schemaMetadataFileDescriptor = CorfuOptions.getDescriptor();
//        FileDescriptor googleDescriptor = DescriptorProto.getDescriptor().getFile();
//
//        byte[] data = message.toByteArray();
//        byte[] applianceSchemaBytes = applianceFileDescriptor.toProto().toByteArray();
//        byte[] firewallSchemaBytes = firewallFileDescriptor.toProto().toByteArray();
//        byte[] metadataSchemaBytes = schemaMetadataFileDescriptor.toProto().toByteArray();
//        byte[] googleSchemaBytes = googleDescriptor.toProto().toByteArray();
//
//        FileDescriptorProto applianceSchemaProto = FileDescriptorProto.parseFrom(applianceSchemaBytes);
//        FileDescriptorProto firewallSchemaProto = FileDescriptorProto.parseFrom(firewallSchemaBytes);
//        FileDescriptorProto metadataSchemaProto = FileDescriptorProto.parseFrom(metadataSchemaBytes);
//        FileDescriptorProto googleDescriptorProto = FileDescriptorProto.parseFrom(googleSchemaBytes);
//
//        FileDescriptorSet fileDescriptorSet = FileDescriptorSet.newBuilder()
//                .addFile(applianceSchemaProto)
//                .addFile(firewallSchemaProto)
//                .addFile(metadataSchemaProto)
//                .addFile(googleDescriptorProto)
//                .build();
//
//        Map<String, FileDescriptorProto> fileDescriptorProtoMap = new HashMap<>();
//        fileDescriptorSet.getFileList().forEach(fileDescriptorProto -> {
//            fileDescriptorProtoMap.put(fileDescriptorProto.getName(), fileDescriptorProto);
//        });
//
//        Any any = Any.pack(message);
//        byte[] anyBytes = any.toByteArray();
//        System.out.println(anyBytes);
//        any = Any.parseFrom(anyBytes);
//        System.out.println(any.getTypeUrl());
//        Message m = any.unpack(FirewallRule.class);
////        printMessage(data, fileDescriptorProtoMap);
//    }
//
//    void printMessage(byte[] data, Map<String, FileDescriptorProto> map) throws Exception {
//
//        FileDescriptor firewallDescriptor = getDescriptors("sample_schema.proto", map);
//        DynamicMessage msg = DynamicMessage.parseFrom(firewallDescriptor.findMessageTypeByName("FirewallRule"), data);
//        System.out.println(msg.toString());
//    }
//
//    FileDescriptor getDescriptors(String name, Map<String, FileDescriptorProto> map) throws DescriptorValidationException {
//
//        List<FileDescriptor> list = new ArrayList<>();
//        for (String s : map.get(name).getDependencyList()) {
//            FileDescriptor descriptors = getDescriptors(s, map);
//            list.add(descriptors);
//        }
//        FileDescriptor[] fileDescriptors = list.toArray(new FileDescriptor[list.size()]);
//        return FileDescriptor.buildFrom(map.get(name), fileDescriptors);
//    }
}
