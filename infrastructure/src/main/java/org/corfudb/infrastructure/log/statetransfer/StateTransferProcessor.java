package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.StreamUtils;
import org.corfudb.common.util.StreamUtils.StreamHeadAndTail;
import org.corfudb.common.util.TailCall;
import org.corfudb.common.util.TailCalls;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.policy.errorpolicy.ErrorPolicyData;
import org.corfudb.infrastructure.log.statetransfer.policy.errorpolicy.FailRestPolicy;
import org.corfudb.infrastructure.log.statetransfer.policy.errorpolicy.WrongEpochExceptionPolicy;
import org.corfudb.infrastructure.log.statetransfer.policy.staticpolicy.InitialBatchStream;
import org.corfudb.infrastructure.log.statetransfer.policy.staticpolicy.SimpleStaticPolicy;
import org.corfudb.infrastructure.log.statetransfer.policy.staticpolicy.StaticPolicyData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.WrongEpochException;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
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
        private final ImmutableList<CompletableFuture<TransferBatchResponse>> pending = ImmutableList.of();

        /**
         * Returns a future of true if the oldest scheduled batch transfer completes successfully.
         * If the window is empty or the latest scheduled transfer completes with a failure,
         * returns a future of false.
         *
         * @return True if the window can be slid, false otherwise.
         */
        public CompletableFuture<Boolean> canSlideWindow() {
            if (pending.isEmpty()) {
                return CompletableFuture.completedFuture(false);
            } else {
                return pending.get(0)
                        .thenApply(response -> response.getStatus() ==
                                TransferBatchResponse.TransferStatus.SUCCEEDED);
            }
        }
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
    SlidingWindow updateWindow(CompletableFuture<TransferBatchResponse> newBatchResult,
                               SlidingWindow currentWindow) {
        return currentWindow.toBuilder()
                .pending(new ImmutableList.Builder<CompletableFuture<TransferBatchResponse>>()
                        .addAll(currentWindow.getPending())
                        .add(newBatchResult).build())
                .build();
    }

    /**
     * Slide a current window over a lazy stream of batches.
     * This entails creating a new instance of a window with a new updated data.
     *
     * @param newBatchResult A future of a batch transferred by
     *                       {@link PolicyStreamProcessor#batchProcessor}.
     * @param slidingWindow  The most recent instance of a sliding window.
     * @return An instance of a new sliding window, with a new, updated data,
     * wrapped in a tail-recursive call.
     */
    CompletableFuture<SlidingWindow> slideWindow(CompletableFuture<TransferBatchResponse> newBatchResult,
                                                 SlidingWindow slidingWindow) {

        slidingWindow.canSlideWindow().thenCompose(canSlide -> {
            if (canSlide) {

            } else {
                // Window is empty -- can't slide.
                if (slidingWindow.getPending().isEmpty()) {
                    new FailRestPolicy().applyPolicy(new ErrorPolicyData<>());
                }
                // The first in-window transfer response failed.
                // Try handling it.
                slidingWindow.getPending().get(0).thenApply(resp -> {
                    resp.getCauseOfFailure().ifPresent(e -> {
                        try {
                            throw e.getCause();
                        } catch (Throwable cause) {
                            if (cause instanceof WrongEpochException) {
                                new WrongEpochExceptionPolicy()
                                        .applyPolicy(new ErrorPolicyData<>());
                            }
                            else{
                                new FailRestPolicy().applyPolicy(new ErrorPolicyData<>());
                            }
                        }
                    })
                })

            }
        })
        if (slidingWindow.canSlideWindow()) {
            CompletableFuture<BatchResult> completedBatch = slidingWindow.getWindow().get(0);

            ImmutableList<CompletableFuture<BatchResult>> newWindow = ImmutableList.copyOf(Stream
                    .concat(slidingWindow.getWindow().stream().skip(1),
                            Stream.of(newBatchResult)).collect(Collectors.toList()));

            return TailCalls.done(completedBatch.thenApply(batch -> {
                if (batch.getResult().isError()) {
                    return slidingWindow.toBuilder()
                            .window(newWindow)
                            .failed(
                                    new ImmutableList
                                            .Builder<CompletableFuture<BatchResult>>()
                                            .addAll(slidingWindow.getFailed())
                                            .add(CompletableFuture.completedFuture(batch))
                                            .build()
                            ).build();
                } else {
                    return slidingWindow.toBuilder()
                            .window(newWindow)
                            .totalAddressesTransferred(slidingWindow.getTotalAddressesTransferred()
                                    .thenApply(x -> x + batch.getResult().get()
                                            .getAddressesTransferred()))
                            .succeeded(
                                    new ImmutableList
                                            .Builder<CompletableFuture<BatchResult>>()
                                            .addAll(slidingWindow.getSucceeded())
                                            .add(CompletableFuture.completedFuture(batch))
                                            .build()
                            ).build();
                }
            }).join());
        } else {
            return TailCalls.call(() -> slideWindow(newBatchResult, slidingWindow));
        }
    }


    TailCall<CompletableFuture<Long>> doStateTransfer(
            Stream<Optional<TransferBatchRequest>> stream,
            SlidingWindow slidingWindow, long remainingStreamSize) {
        // Split the stream into a head and a tail.
        StreamHeadAndTail<TransferBatchRequest> headAndTail =
                StreamUtils.splitTail(stream, remainingStreamSize);
        Optional<TransferBatchRequest> head = headAndTail.getHead();
        Stream<Optional<TransferBatchRequest>> tail = headAndTail.getTail();
        final int currentPendingSize = slidingWindow.getPending().size();
        // Head is present and the window is not full yet.
        if (head.isPresent() && currentPendingSize < windowSize) {
            TransferBatchRequest transferRequest = head.get();
            CompletableFuture<TransferBatchResponse> transferResponse =
                    batchProcessor.transfer(transferRequest);
            // Update the window and call this function recursively
            // with a new window and the tail of a stream.
            return TailCalls.call(() ->
                    doStateTransfer(
                            tail,
                            updateWindow(transferResponse, slidingWindow),
                            remainingStreamSize - 1));
        }
        // Head is present, window is full.
        else if (head.isPresent() && currentPendingSize == windowSize) {
            TransferBatchRequest transferRequest = head.get();
            CompletableFuture<TransferBatchResponse> transferResponse =
                    batchProcessor.transfer(transferRequest);

        }
        // End of a stream, terminate, finalize transfers and return the total transferred addresses.
        else {

        }
    }

    public CompletableFuture<Long> stateTransfer(StaticPolicyData staticPolicyData) {
        SimpleStaticPolicy staticPolicy = new SimpleStaticPolicy();
        InitialBatchStream initialBatchStream = staticPolicy.applyPolicy(staticPolicyData);
        SlidingWindow slidingWindow = SlidingWindow.builder().build();
        return doStateTransfer(
                initialBatchStream.getInitialStream().map(Optional::of),
                slidingWindow,
                initialBatchStream.getInitialBatchStreamSize())
                .invoke();
    }


}
