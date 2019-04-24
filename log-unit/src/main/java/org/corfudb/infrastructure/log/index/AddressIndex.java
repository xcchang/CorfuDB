package org.corfudb.infrastructure.log.index;

import lombok.Getter;
import org.corfudb.infrastructure.log.Types.StreamLogAddress;

import java.util.StringJoiner;


public interface AddressIndex {
    AddressIndex getHead();

    StreamLogAddress getAddress();

    boolean isNonAddress();

    boolean isAddress();

    AddressIndex next(StreamLogAddress address);

    AddressIndex getLayer1();

    AddressIndex getLayer2();

    AddressIndex get(long globalAddress);

    class AddressIndexImpl implements AddressIndex {
        @Getter
        private final AddressIndex head;
        @Getter
        private final StreamLogAddress address;
        /**
         * Exponentially growing function: n^2
         */
        @Getter
        private final AddressIndex layer1;

        /**
         * layer two: n + (n/4)
         */
        @Getter
        private final AddressIndex layer2;
        //@Getter
        //private final AddressIndex layer3;

        private AddressIndexImpl(AddressIndex head, AddressIndex layer1, AddressIndex layer2,
                StreamLogAddress address) {
            this.head = head;
            this.address = address;
            this.layer1 = layer1;
            this.layer2 = layer2;
        }

        public AddressIndex next(StreamLogAddress nextAddress) {

            if (layer1.isNonAddress()) {
                return new AddressIndexImpl(this, this, this, nextAddress);
            }

            if (layer2.isNonAddress()) {
                return new AddressIndexImpl(this, layer1, this, nextAddress);
            }

            if (isPowerOfTwo(nextAddress)) {
                return new AddressIndexImpl(this, this, this, nextAddress);
            }

            long nextLevel2 = Math.max(1, layer2.getAddress().getAddress() / 4 + layer2.getAddress().getAddress());
            if (nextLevel2 == nextAddress.getAddress()) {
                return new AddressIndexImpl(this, layer1, this, nextAddress);
            }

            return new AddressIndexImpl(this, layer1, layer2, nextAddress);
        }

        public boolean isPowerOfTwo(StreamLogAddress addr) {
            long globalAddress = addr.getAddress();
            return globalAddress > 0 && (globalAddress & globalAddress - 1) == 0;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", AddressIndexImpl.class.getSimpleName() + "[", "]")
                    .add("globalAddress=" + address.getAddress())
                    .add("offset=" + address.getOffset())
                    .add("length=" + address.getLength())
                    .add("layer1=" + (layer1.isNonAddress() ? "NONE" : layer1.getAddress().getAddress()))
                    .add("layer2=" + (layer2.isNonAddress() ? "NONE" : layer2.getAddress().getAddress()))
                    .add("head=" + (head.isNonAddress() ? "NONE" : head.getAddress().getAddress()))
                    .toString();
        }

        public static void main(String[] args) {
            AddressIndex index = NonAddressIndex.NON_ADDRESS;
            for (int i = 0; i < 10_000; i++) {
                StreamLogAddress addr = StreamLogAddress.newBuilder()
                        .setAddress(i)
                        .setOffset(i)
                        .setLength(100)
                        .build();

                index = index.next(addr);
                System.out.println(index);
            }
            //AddressIndex x = addr.get(600);
            //System.out.println(x);
        }

        @Override
        public boolean isNonAddress() {
            return false;
        }

        @Override
        public boolean isAddress() {
            return true;
        }

        @Override
        public AddressIndex get(long globalAddress) {
            int iterations = 0;

            AddressIndex currIndex = this;
            while (true) {
                iterations++;

                if (currIndex.isNonAddress()) {
                    throw new IllegalStateException("Address not found: " + globalAddress);
                }

                if (globalAddress == currIndex.getAddress().getAddress()) {
                    //System.out.println("Found. Iterations: " + iterations);
                    return currIndex;
                }

                AddressIndex currLayer1 = currIndex.getLayer1();
                if (currLayer1.isNonAddress()) {
                    //System.out.println("Not fond in layer1. Iterations: " + iterations);
                    AddressIndex currLayer2 = currIndex.getLayer2();
                    if (currLayer2.getAddress().getAddress() == globalAddress) {
                        //System.out.println("Found on layer2. Iterations: " + iterations);
                        return currLayer2;
                    }

                    if (currIndex.getLayer2().getAddress().getAddress() > globalAddress) {
                        //System.out.println("layer two jump. Curr addr layer2: " + currIndex.getLayer2().getAddress());
                        currIndex = currIndex.getLayer2();
                    } else {
                        //System.out.println("Degrade to linead time, curr addr: " + currIndex.getAddress());
                        currIndex = currIndex.getHead();
                    }
                    continue;
                }

                if (currLayer1.getAddress().getAddress() > globalAddress) {
                    //System.out.println("Navigate layer1: " + currLayer1.getAddress());
                    currIndex = currLayer1;
                    continue;
                }

                if (currLayer1.getAddress().getAddress() < globalAddress) {
                    //System.out.println("Go to level2, curr level1: " + currLayer1.getAddress());
                    AddressIndex currLayer2 = currIndex.getLayer2();
                    if (currLayer2.getAddress().getAddress() == globalAddress) {
                        //System.out.println("Found on layer2");
                        return currLayer2;
                    }

                    if (currIndex.getLayer2().getAddress().getAddress() > globalAddress) {
                        //System.out.println("travel layer2: " + currIndex.getLayer2().getAddress());
                        currIndex = currIndex.getLayer2();
                    } else {
                        //System.out.println("linear: " + currIndex.getAddress());
                        currIndex = currIndex.getHead();
                    }

                    continue;
                }

                if (currLayer1.getAddress().getAddress() == globalAddress) {
                    return currLayer1;
                }
            }
        }

    }

    class NonAddressIndex implements AddressIndex {

        public static NonAddressIndex NON_ADDRESS = new NonAddressIndex();

        @Override
        public AddressIndex getHead() {
            throw new UnsupportedOperationException("NonAddressIndex");
        }

        @Override
        public StreamLogAddress getAddress() {
            throw new UnsupportedOperationException("NonAddressIndex");
        }

        @Override
        public boolean isNonAddress() {
            return true;
        }

        @Override
        public boolean isAddress() {
            return false;
        }

        @Override
        public AddressIndex next(StreamLogAddress address) {
            return new AddressIndexImpl(this, this, this, address);
        }

        @Override
        public AddressIndex getLayer1() {
            return this;
        }

        @Override
        public AddressIndex getLayer2() {
            return this;
        }

        @Override
        public AddressIndex get(long globalAddress) {
            throw new UnsupportedOperationException("NonAddressIndex");
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", NonAddressIndex.class.getSimpleName() + "[", "]")
                    .toString();
        }
    }
}