package org.corfudb.infrastructure.log.segment;

import lombok.AllArgsConstructor;

public interface Segment {

    @AllArgsConstructor
    class SegmentState {
        private final SegmentMetaData info;
        private final long latestAddress;
        private final int currentSize;

        public SegmentState(SegmentMetaData info){
            this.info = info;
            this.latestAddress = -1;
            this.currentSize = 0;
        }

        public SegmentState update(long newAddress) {
            if (!info.contains(newAddress)) {
                throw new IllegalArgumentException("invalid address");
            }

            return new SegmentState(info, Math.max(latestAddress, newAddress), currentSize + 1);
        }

        public boolean isSegmentFull() {
            return info.getLatestAddress() == latestAddress && info.getSize() == currentSize;
        }
    }
}
