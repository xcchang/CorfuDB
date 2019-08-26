package org.corfudb.runtime.collections;

import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import java.util.*;

import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.RecordMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.SampleAppliance.Appliance;
import org.corfudb.test.SampleSchema.EventInfo;
import org.corfudb.test.SampleSchema.FirewallRule;
import org.corfudb.test.SampleSchema.Uuid;
import org.junit.Test;

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
        table.create(Uuid.newBuilder().build(),
                EventInfo.newBuilder().build(),
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

            tx = tx.update(tableName, uuids.get(i), events.get(i));
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
        System.out.println("Table point lookup : " + q.get(tableName, timestamp, lookupKey));
        System.out.println("Table point lookup : " + q.get(tableName, lookupKey));


        // Get by secondary index.
        final long fiftyLong = 50L;
        System.out.println("All entries at event time 50 : " + q.getByIndex(tableName, "event_time", fiftyLong).toString());

        // Execute Query. (Scan and filter)
        final int sixty = 60;
        System.out.println("All tables with time greater than 60 : " + q
                .exectuteQuery(tableName, event -> ((EventInfo) event).getEventTime() >= sixty)
                .getResult()
                .size());

        System.out.println("Size of table at timestamp " + timestamp + " = " + q.count(tableName));
        System.out.println("Size of table = " + q.count(tableName));

        System.out.println("All tables in namespace : " + corfuStore.listTables(nsxManager));
    }


    @Test
    public void prototest() throws Exception {
        final int ruleId = 123;
        FirewallRule rule = FirewallRule
                .newBuilder()
                .setRuleId(ruleId)
                .setRuleName("TestRule")
                .setInput(Appliance.newBuilder().setEndpoint("127.0.0.1").build())
                .setOutput(Appliance.newBuilder().setEndpoint("196.168.0.1").build())
                .build();

        Message message = rule;

        message.getAllFields().forEach((fieldDescriptor, field) -> {
            if (fieldDescriptor.getOptions().getExtension(CorfuOptions.schema).getPrimaryKey()) {
                System.out.println("Detected primary key " + fieldDescriptor.getName() + " = " + field);
            }
            if (fieldDescriptor.getOptions().getExtension(CorfuOptions.schema).getSecondaryKey()) {
                System.out.println("Detected secondary key " + fieldDescriptor.getName() + " = " + field);
            }
        });

        FileDescriptor applianceFileDescriptor = Appliance.getDescriptor().getFile();
        FileDescriptor firewallFileDescriptor = FirewallRule.getDescriptor().getFile();
        FileDescriptor schemaMetadataFileDescriptor = CorfuOptions.getDescriptor();
        FileDescriptor googleDescriptor = DescriptorProto.getDescriptor().getFile();

        byte[] data = message.toByteArray();
        byte[] applianceSchemaBytes = applianceFileDescriptor.toProto().toByteArray();
        byte[] firewallSchemaBytes = firewallFileDescriptor.toProto().toByteArray();
        byte[] metadataSchemaBytes = schemaMetadataFileDescriptor.toProto().toByteArray();
        byte[] googleSchemaBytes = googleDescriptor.toProto().toByteArray();

        FileDescriptorProto applianceSchemaProto = FileDescriptorProto.parseFrom(applianceSchemaBytes);
        FileDescriptorProto firewallSchemaProto = FileDescriptorProto.parseFrom(firewallSchemaBytes);
        FileDescriptorProto metadataSchemaProto = FileDescriptorProto.parseFrom(metadataSchemaBytes);
        FileDescriptorProto googleDescriptorProto = FileDescriptorProto.parseFrom(googleSchemaBytes);

        FileDescriptorSet fileDescriptorSet = FileDescriptorSet.newBuilder()
                .addFile(applianceSchemaProto)
                .addFile(firewallSchemaProto)
                .addFile(metadataSchemaProto)
                .addFile(googleDescriptorProto)
                .build();

        Map<String, FileDescriptorProto> fileDescriptorProtoMap = new HashMap<>();
        fileDescriptorSet.getFileList().forEach(fileDescriptorProto -> {
            fileDescriptorProtoMap.put(fileDescriptorProto.getName(), fileDescriptorProto);
        });

        Any any = Any.pack(message);
        byte[] anyBytes = any.toByteArray();
        System.out.println(anyBytes);
        any = Any.parseFrom(anyBytes);
        System.out.println(any.getTypeUrl());
        Message m = any.unpack(FirewallRule.class);
//        printMessage(data, fileDescriptorProtoMap);
    }

    void printMessage(byte[] data, Map<String, FileDescriptorProto> map) throws Exception {

        FileDescriptor firewallDescriptor = getDescriptors("sample_schema.proto", map);
        DynamicMessage msg = DynamicMessage.parseFrom(firewallDescriptor.findMessageTypeByName("FirewallRule"), data);
        System.out.println(msg.toString());
    }

    FileDescriptor getDescriptors(String name, Map<String, FileDescriptorProto> map) throws DescriptorValidationException {

        List<FileDescriptor> list = new ArrayList<>();
        for (String s : map.get(name).getDependencyList()) {
            FileDescriptor descriptors = getDescriptors(s, map);
            list.add(descriptors);
        }
        FileDescriptor[] fileDescriptors = list.toArray(new FileDescriptor[list.size()]);
        return FileDescriptor.buildFrom(map.get(name), fileDescriptors);
    }
}
