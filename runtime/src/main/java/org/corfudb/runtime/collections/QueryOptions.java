package org.corfudb.runtime.collections;

import com.google.protobuf.Message;

import java.util.Comparator;
import java.util.function.Function;

import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import lombok.Getter;

/**
 * Created by zlokhandwala on 2019-08-10.
 */
@Getter
public class QueryOptions<R> {

    public static final QueryOptions DEFAULT_OPTIONS = QueryOptionsBuilder.newBuilder().build();

    private final Timestamp timestamp;
    private final boolean distinct;
    private final Comparator<R> comparator;
    private final Function<Message, R> projection;

    private QueryOptions(Timestamp timestamp, boolean distinct, Comparator<R> comparator,
                         Function<Message, R> projection) {
        this.timestamp = timestamp;
        this.distinct = distinct;
        this.comparator = comparator;
        this.projection = projection;
    }

    public static class QueryOptionsBuilder<R> {

        private Timestamp timestamp = null;
        private boolean distinct = false;
        private Comparator<R> comparator = null;
        private Function<Message, R> projection = null;

        public static <S> QueryOptionsBuilder<S> newBuilder() {
            return new QueryOptionsBuilder<>();
        }

        public QueryOptionsBuilder<R> setTimestamp(Timestamp timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public QueryOptionsBuilder<R> setDistinct(boolean distinct) {
            this.distinct = distinct;
            return this;
        }

        public QueryOptionsBuilder<R> setComparator(Comparator<R> comparator) {
            this.comparator = comparator;
            return this;
        }

        public QueryOptionsBuilder<R> setProjection(Function<Message, R> projection) {
            this.projection = projection;
            return this;
        }

        public QueryOptions<R> build() {
            return new QueryOptions<>(
                    timestamp,
                    distinct,
                    comparator,
                    projection);
        }
    }
}
