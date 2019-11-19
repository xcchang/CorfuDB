package org.corfudb.infrastructure.log.statetransfer.metrics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.JsonUtils;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Information about the recent state transfer.
 * It can be saved in the current node's data store.
 */
@ToString
@Slf4j
@AllArgsConstructor
public class StateTransferStats {

    public enum TransferMethod {
        PROTOCOL
    }

    @ToString
    @Builder
    @EqualsAndHashCode
    public static class StateTransferAttemptStats {
        @NonNull
        private final String localEndpoint;
        @NonNull
        private final Layout layoutBeforeTransfer;
        @NonNull
        private final Duration durationOfTransfer;
        @Builder.Default
        private final boolean succeeded = false;
        @Builder.Default
        private final TransferMethod method = TransferMethod.PROTOCOL;
        @Builder.Default
        private final Optional<Duration> durationOfRestoration = Optional.empty();
        @Builder.Default
        private final Optional<Layout> layoutAfterTransfer = Optional.empty();
    }

    @Getter
    private final ImmutableList<StateTransferAttemptStats> attemptStats;


    public final StateTransferStats combine(StateTransferStats other) {
        if (other == null) {
            throw new IllegalStateException("Can't combine if other stats is null.");
        }

        if (!Sets.intersection(new HashSet<>(attemptStats), new HashSet<>(other.attemptStats)).isEmpty()) {
            throw new IllegalStateException("Some of the stat data is the same.");
        }

        ImmutableList<StateTransferAttemptStats> combinedAttempts =
                Stream.concat(attemptStats.stream(), other.getAttemptStats().stream())
                        .collect(ImmutableList.toImmutableList());

        return new StateTransferStats(combinedAttempts);
    }

    public String toJson() {
        return JsonUtils.toJson(this);
    }
}