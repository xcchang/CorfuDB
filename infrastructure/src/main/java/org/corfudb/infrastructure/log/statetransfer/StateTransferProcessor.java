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
 * A sliding window is used to aggregate pending transfer requests as well as make
 * the timely decisions with respect to how the future load will be handled in
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
        private final long totalAddressesTransferred = 0L;

        /**
         * A current window of the pending transfer batch responses.
         * Slides when the earliest scheduled batch transfer completes successfully.
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

    SlidingWindow doSlideWindow(CompletableFuture<TransferBatchRequest> newRequest,
                                TransferBatchRequest completedRequest,
                                SlidingWindow oldSlidingWindow) {
        ImmutableList<CompletableFuture<TransferBatchRequest>> newPending =
                Stream.concat(oldSlidingWindow.getPending().stream().skip(1), Stream.of(newRequest))
                        .collect(ImmutableList.toImmutableList());
        return oldSlidingWindow.toBuilder()
                .pending(newPending)
                .totalAddressesTransferred(oldSlidingWindow.getTotalAddressesTransferred() +
                        completedRequest.getAddresses().size())
                .build();
    }

    DynamicPolicyData handleErrors(Throwable batchProcessorError, DynamicPolicyData policyData) {
        Throwable cause = batchProcessorError.getCause();
        if (cause instanceof WrongEpochException) {
            return new UpdateLayoutPolicy().applyPolicy(policyData);
        } else {
            return new FailRestPolicy().applyPolicy(policyData);
        }
    }

    CompletableFuture<Long> finalizeTransfer(SlidingWindow slidingWindow) {


    }

    CompletableFuture<DynamicPolicyData> slideWindow(
            CompletableFuture<TransferBatchRequest> newRequest, DynamicPolicyData dynamicPolicyData) {

        SlidingWindow slidingWindow = dynamicPolicyData.getSlidingWindow();
        Stream<Optional<TransferBatchRequest>> stream = dynamicPolicyData.getTail();
        long remainingSize = dynamicPolicyData.getSize();
        DynamicPolicyData policyData =
                new DynamicPolicyData(stream, slidingWindow, remainingSize, runtime);

        return slidingWindow
                .getPending()
                .stream()
                .findFirst()
                .map(lastScheduledTransfer -> lastScheduledTransfer.handle(
                        (completedRequest, batchProcessorError) ->
                                Optional.ofNullable(batchProcessorError)
                                        .map(ex -> handleErrors(batchProcessorError, policyData))
                                        .orElse(policyData
                                                .toBuilder()
                                                .slidingWindow(
                                                        doSlideWindow(
                                                                newRequest,
                                                                completedRequest,
                                                                slidingWindow))
                                                .build())))
                .orElseGet(() ->
                        CompletableFuture.completedFuture(
                                new FailRestPolicy().applyPolicy(policyData))
                );
    }


    CompletableFuture<Long> doStateTransfer(CompletableFuture<DynamicPolicyData> dynamicPolicyData) {
        return dynamicPolicyData.thenCompose(data -> {
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

                CompletableFuture<TransferBatchRequest> request = batchProcessor.transfer(transferRequest);

                CompletableFuture<DynamicPolicyData> newData =
                        CompletableFuture.completedFuture(
                                new DynamicPolicyData(
                                        tail,
                                        updateWindow(request, slidingWindow),
                                        remainingSize - 1,
                                        runtime)
                        );
                return doStateTransfer(newData);
            } else if (head.isPresent() && currentPendingSize == windowSize) {
                TransferBatchRequest transferRequest = head.get();

                CompletableFuture<TransferBatchRequest> request = batchProcessor.transfer(transferRequest);
                CompletableFuture<DynamicPolicyData> newData =
                        slideWindow(request, new DynamicPolicyData(tail, slidingWindow, remainingSize - 1, runtime));

                return doStateTransfer(newData);
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
