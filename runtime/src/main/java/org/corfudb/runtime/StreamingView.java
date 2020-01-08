package org.corfudb.runtime;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.stream.StreamAddressSpace;

import java.util.*;
import java.util.stream.Stream;

//It provides a streaming view of a stream given a snapshot time.
//It is not thread safe.
public class StreamingView {
    Long snapshot;
    UUID streamId;
    Set keySet;
    StreamAddressSpace addressMap;

    StreamingView(Long snapshot, UUID streamId, StreamAddressSpace addressMap) {
        this.streamId = streamId;
        this.snapshot = snapshot;
        this.addressMap = addressMap;
        keySet = new TreeSet();
    }


    private List nextLogEntries(int numEntries) {
        ArrayList data = new ArrayList<ILogData>();
        return data;
    }

    //Given next numEntries of entries according to snapshot time.
    List next(int numEntries) {
        List<ILogData> input = nextLogEntries(numEntries);
        List output = new ArrayList(input.size ());
        int remainder = numEntries;
        //go over each entry if the key
        while(output.size() < numEntries) {
            input = nextLogEntries(remainder);
            if (input.size() == 0)
                break;

            for (ILogData entry: input) {
                String key = "abc";
                if (keySet.add(key) == true) {
                    output.add(entry);
                }
            }
            remainder = numEntries - output.size ();
        }
        return output;
    }

}
