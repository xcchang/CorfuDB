package org.corfudb.infrastructure.log;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.corfudb.infrastructure.utils.Persistence.syncDirectory;

/**
 * The global log is partition into segments, each segment contains a range of consecutive
 * addresses. Accessing the address space for a particular segment happens through this class.
 *
 * @author Maithem
 */
@Slf4j
class SegmentHandle {

    @Getter
    final long segment;

    @NonNull
    @Getter
    final FileChannel writeChannel;

    @NonNull
    @Getter
    final FileChannel readChannel;

    @NonNull
    @Getter
    final String fileName;

    @Getter
    final Map<Long, AddressMetaData> knownAddresses = new ConcurrentHashMap<>();

    @Getter
    private volatile int refCount = 0;

    public SegmentHandle(long segmentNumber, Path logDir) throws IOException {
        this.fileName = logDir + File.separator + segmentNumber + ".log";
        this.segment = segmentNumber;
        this.writeChannel = getChannel(fileName, false);
        this.readChannel = getChannel(fileName, true);
    }

    public boolean contains(long address) {
        return knownAddresses.containsKey(address);
    }

    public ByteBuffer get(long address) throws IOException {
        AddressMetaData metaData = knownAddresses.get(address);
        if (metaData == null) {
            return null;
        }
        ByteBuffer entryBuf = ByteBuffer.allocate(metaData.length);
        readChannel.read(entryBuf, metaData.offset);
        return entryBuf;
    }

    public void flush() throws IOException {
        writeChannel.force(true);
    }

    public synchronized void retain() {
        refCount++;
    }

    public synchronized void release() {
        if (refCount == 0) {
            throw new IllegalStateException("refCount cannot be less than 0, segment " + segment);
        }
        refCount--;
    }

    public void close() {
        Set<FileChannel> channels = new HashSet<>(
                Arrays.asList(writeChannel, readChannel)
        );

        for (FileChannel channel : channels) {
            try {
                channel.force(true);
            } catch (IOException e) {
                log.debug("Can't force updates in the channel", e.getMessage());
            } finally {
                IOUtils.closeQuietly(channel);
            }
        }
    }

    @Nullable
    private FileChannel getChannel(String filePath, boolean readOnly) throws IOException {
        if (readOnly) {
            if (!new File(filePath).exists()) {
                throw new FileNotFoundException(filePath);
            }

            return FileChannel.open(
                    FileSystems.getDefault().getPath(filePath),
                    EnumSet.of(StandardOpenOption.READ)
            );
        }

        try {
            EnumSet<StandardOpenOption> options = EnumSet.of(
                    StandardOpenOption.READ, StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.SPARSE
            );
            FileChannel channel = FileChannel.open(FileSystems.getDefault().getPath(filePath), options);

            // First time creating this segment file, need to sync the parent directory
            File segFile = new File(filePath);
            syncDirectory(segFile.getParent());
            return channel;
        } catch (FileAlreadyExistsException ex) {
            return FileChannel.open(
                    FileSystems.getDefault().getPath(filePath),
                    EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE)
            );
        }
    }
}