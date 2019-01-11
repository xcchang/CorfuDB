package org.corfudb.runtime.view;

/**
 * Created by maithem on 6/20/17.
 */
public class StreamOptions {
    public static StreamOptions DEFAULT = new StreamOptions(false);

    public final boolean ignoreTrimmed;

    public final boolean cacheReads;

    public StreamOptions(boolean ignoreTrimmed) {
        this.ignoreTrimmed = ignoreTrimmed;
        this.cacheReads = true;
    }

    public StreamOptions(boolean ignoreTrimmed, boolean cacheReads) {
        this.ignoreTrimmed = ignoreTrimmed;
        this.cacheReads = cacheReads;
    }

    public static StreamOptionsBuilder builder() {
        return new StreamOptionsBuilder();
    }

    public static class StreamOptionsBuilder {
        private boolean ignoreTrimmed;
        private boolean cacheReads = true;

        public StreamOptionsBuilder() {

        }

        public StreamOptionsBuilder ignoreTrimmed(boolean ignore) {
            this.ignoreTrimmed = ignore;
            return this;
        }

        public StreamOptionsBuilder cacheReads(boolean cache) {
            this.cacheReads = cache;
            return this;
        }

        public StreamOptions build() {
            return new StreamOptions(ignoreTrimmed, cacheReads);
        }
    }
}
