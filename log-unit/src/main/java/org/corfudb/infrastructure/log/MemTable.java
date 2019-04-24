package org.corfudb.infrastructure.log;

import com.google.protobuf.MessageLite;
import lombok.Builder;

import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.PriorityBlockingQueue;

@Builder
public class MemTable<Data extends MessageLite> {
    //data
    private final PriorityBlockingQueue<Data> table;

    private final int limit = 1024 * 1024;

    public NavigableSet<Data> get() throws InterruptedException {
        NavigableSet<Data> buffer = new TreeSet<>(/*comparator*/);
        int bufferSize;

        Data entry = getSingleEntry();
        buffer.add(entry);
        bufferSize = entry.getSerializedSize();

        while (!table.isEmpty() || bufferSize < limit) {
            entry = getSingleEntry();
            buffer.add(entry);
            bufferSize = entry.getSerializedSize();
        }

        return buffer;
    }

    public Data getSingleEntry() throws InterruptedException {
        return table.take();
    }

    public int size(){
        return table.size();
    }
}
