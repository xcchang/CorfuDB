package org.corfudb.runtime.smr;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.io.Serializable;

/**
 * This is used by SMREngine to wrap commands before serializing them in the stream,
 * so that it can add its own header information.
 *
 */
public class SMRCommandWrapper implements Serializable
{
    public static boolean init = false;
    public static UUID uniquenodeid = new UUID(Long.MAX_VALUE, 0);
    public static AtomicLong ctr;
    public Pair<UUID, Long> uniqueid;
    public Serializable cmd;
    public Set<UUID> streams;
    public SMRCommandWrapper(Serializable tcmd, Set<UUID> tstreams)
    {
        if(!init) throw new RuntimeException("SMRCommandWrapper not initialized with unique node ID!");
        cmd = tcmd;
        uniqueid = new Pair(uniquenodeid, ctr.incrementAndGet());
        streams = tstreams;
    }
    public synchronized static void initialize(UUID tuniquenodeid)
    {
        if(init) return;
        uniquenodeid = tuniquenodeid;
        ctr = new AtomicLong();
        init = true;
    }
    public String toString()
    {
        return super.toString() + "::" + uniqueid + "::" + cmd;
    }
}

