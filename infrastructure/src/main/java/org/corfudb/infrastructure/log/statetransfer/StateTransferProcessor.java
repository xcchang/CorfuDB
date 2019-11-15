package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.StreamUtils;
import org.corfudb.common.util.StreamUtils.StreamHeadAndTail;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.policy.dynamicpolicy.DynamicPolicyData;
import org.corfudb.infrastructure.log.statetransfer.policy.dynamicpolicy.SlideWindowPolicy;
import org.corfudb.infrastructure.log.statetransfer.policy.errorpolicy.FailRestPolicy;
import org.corfudb.infrastructure.log.statetransfer.policy.errorpolicy.UpdateLayoutPolicy;
import org.corfudb.infrastructure.log.statetransfer.policy.staticpolicy.InitialBatchStream;
import org.corfudb.infrastructure.log.statetransfer.policy.staticpolicy.SimpleStaticPolicy;
import org.corfudb.infrastructure.log.statetransfer.policy.staticpolicy.StaticPolicyData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.WrongEpochException;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static lombok.Builder.Default;

/**
 * A class that handles an actual state transfer for an entire transfer address space.
 * It operates over a lazy stream of batches by running a sliding window algorithm.
 * A sliding window is used to aggregate a dynamic data over the transferred batches
 * and make the timely decisions with respect to how the future load will be handled in
 * the normal case as well as during cluster reconfigurations and state transfer failures.
 */
@Builder
@Getter
@Slf4j
public class StateTransferProcessor {

    /**
     * A sliding window of {@link StateTransferProcessor#windowSize} size.
     * It is used to hold a dynamic information needed to complete a state transfer.
     */
    @Getter
    @Builder(toBuilder = true)
    @ToString
    public static class SlidingWindow {
        /**
         * Total number of addresses transferred so far.
         */
        @Default
        private final CompletableFuture<Long> totalAddressesTransferred =
                CompletableFuture.completedFuture(0L);

        /**
         * A current window of the pending transfer batch responses.
         * Slides when the latest scheduled batch transfer completes successfully.
         */
        @Default
        private final ImmutableList<CompletableFuture<TransferBatchRequest>> pending = ImmutableList.of();

    }

    /**
     * The static size of a sliding window.
     */
    @NonNull
    private final int windowSize;

    /**
     * The static size of both processed and failed batch transfers after
     * which to invoke the dynamic protocols on the stream.
     */
    @NonNull
    private final int dynamicProtocolWindowSize;

    /**
     * Corfu runtime.
     */
    @NonNull
    private final CorfuRuntime runtime;

    /**
     * A state transfer batch processor.
     */
    @NonNull
    private final StateTransferBatchProcessor batchProcessor;

    /**
     * Update the current window without sliding it.
     *
     * @param newBatchResult A future of a batch transfer response.
     * @param currentWindow  A current sliding window.
     * @return A new updated window.
     */
    SlidingWindow updateWindow(CompletableFuture<TransferBatchRequest> newBatchResult,
                               SlidingWindow currentWindow) {
        return currentWindow.toBuilder()
                .pending(new ImmutableList.Builder<CompletableFuture<TransferBatchRequest>>()
                        .addAll(currentWindow.getPending())
                        .add(newBatchResult).build())
                .build();
    }

    /**
     * Slide a current window over a lazy stream of batches.
     * This entails creating a new instance of a window with a new updated data.
     *
     * @param newBatchResult A future of a batch response.
     * @return An instance of a new sliding window, with a new, updated data,
     * wrapped in a tail-recursive call.
     */
    CompletableFuture<DynamicPolicyData> slideWindow(TransferBatchRequest request,
                                                     DynamicPolicyData dynamicPolicyData) {

        SlidingWindow slidingWindow = dynamicPolicyData.getSlidingWindow();
        Stream<Optional<TransferBatchRequest>> stream = dynamicPolicyData.getTail();
        long remainingSize = dynamicPolicyData.getSize();
        // Create a policy data needed to handle errors.
        DynamicPolicyData policyData =
                new DynamicPolicyData(stream, slidingWindow, remainingSize, runtime);

        return slidingWindow
                .getPending()
                .stream()
                .findFirst()
                .map(lastScheduledTransfer -> lastScheduledTransfer.handle((data, exception) ->
                        Optional.ofNullable(exception).map(ex -> {
                            Throwable cause = ex.getCause();
                            if (cause instanceof WrongEpochException) {
                                return new UpdateLayoutPolicy().applyPolicy(policyData);
                            } else {
                                return new FailRestPolicy().applyPolicy(policyData);
                            }
                        }).orElse(new SlideWindowPolicy().applyPolicy(policyData))))
                .orElseGet(() -> CompletableFuture.completedFuture(new FailRestPolicy().applyPolicy(policyData)));

    }


    CompletableFuture<Long> doStateTransfer(CompletableFuture<DynamicPolicyData> dynamicPolicyData) {
        return dynamicPolicyData.thenApply(data -> {
            SlidingWindow slidingWindow = data.getSlidingWindow();
            Stream<Optional<TransferBatchRequest>> stream = data.getTail();
            long remainingSize = data.getSize();

            // Split the stream into a head and a tail.
            StreamHeadAndTail<TransferBatchRequest> headAndTail =
                    StreamUtils.splitTail(stream, remainingSize);
            Optional<TransferBatchRequest> head = headAndTail.getHead();
            Stream<Optional<TransferBatchRequest>> tail = headAndTail.getTail();

            int currentPendingSize = slidingWindow.getPending().size();

            if (head.isPresent() && currentPendingSize < windowSize) {
                TransferBatchRequest transferRequest = head.get();

                CompletableFuture<Void> transferResponse = batchProcessor.transfer(transferRequest);

                CompletableFuture<DynamicPolicyData> newData =
                        CompletableFuture.completedFuture(
                                new DynamicPolicyData(
                                        tail,
                                        updateWindow(transferResponse, slidingWindow),
                                        remainingSize - 1,
                                        runtime)
                        );

                return doStateTransfer(newData);
            } else if (head.isPresent() && currentPendingSize == windowSize) {
                TransferBatchRequest transferRequest = head.get();

                CompletableFuture<DynamicPolicyData> dataAfterWindowSlid = batchProcessor
                        .transfer(transferRequest)
                        .thenCompose(resp ->
                                slideWindow(resp, new DynamicPolicyData(
                                        tail,
                                        slidingWindow,
                                        remainingSize - 1,
                                        runtime))
                        );

                return doStateTransfer(dataAfterWindowSlid);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    public CompletableFuture<Long> stateTransfer(StaticPolicyData staticPolicyData) {
        SimpleStaticPolicy staticPolicy = new SimpleStaticPolicy();
        InitialBatchStream initialBatchStream = staticPolicy.applyPolicy(staticPolicyData);
        SlidingWindow slidingWindow = SlidingWindow.builder().build();
        DynamicPolicyData initialDynamicPolicyData = new DynamicPolicyData(
                initialBatchStream.getInitialStream().map(Optional::of),
                slidingWindow, initialBatchStream.getInitialBatchStreamSize(),
                runtime);
        return doStateTransfer(CompletableFuture.completedFuture(initialDynamicPolicyData));
    }


}
