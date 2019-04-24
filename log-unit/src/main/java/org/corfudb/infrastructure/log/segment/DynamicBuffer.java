package org.corfudb.infrastructure.log.segment;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DynamicBuffer {
    private final int maxSize;
    private final int maxBatch;

    public static DynamicBuffer buildDefault() {
        return new DynamicBuffer(1024 * 1024, 256);
    }

    /**
     * 1 -> 1024k
     * 2 -> 512k
     * 4 -> 256k
     * 8 -> 128k
     * 16 -> 64k
     * 32 -> 32kb
     * 64 -> 16kb
     * 128 -> 8kb
     * 256 -> 4kb
     *
     * @param messageSize
     * @return
     */
    public int getBatch(int messageSize) {
        int currBatch = 1;
        int currMaxSize = maxSize;

        while (true) {
            if (currMaxSize <= messageSize) {
                return currBatch;
            }

            if (currBatch >= maxBatch) {
                return maxBatch;
            }

            currBatch *= 2;
            currMaxSize /= 2;
        }
    }

    public static void main(String[] args) {
        DynamicBuffer dynamicBuffer = DynamicBuffer.buildDefault();

        int size = 1;
        while (true){
            if (size > 1024 * 1024 * 10){
                break;
            }

            size *= 2;
            //System.out.println("Message size: " + size + ", batch: " + dynamicBuffer.getBatch(size));
        }
    }
}
