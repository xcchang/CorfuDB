package org.corfudb.common.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * When a call to a method 2 is the last thing a calling method 1 does, there's nothing to
 * resume to after a method 2 returns. A method call in the last position,
 * a last thing to do before a calling function returns, is a tail call.
 * <p>
 * This interface represents a tail-recursive call.
 * This interface allows a conversion of a
 * regular recursive call into a tail call to make a recursion practical for the large inputs
 * (avoids a StackOverflowException). Avoiding pushing the environment on the stack to resume
 * a method after a tail call is called a Tail-Call Elimination, TCE.
 * <p>
 * This interface implements TCE by a way of 'trampolining' a method call via asynchronous tail call.
 * We delay the recursive calls by wrapping them into a completable future.
 * The terminal value will be wrapped in an already completed future.
 *
 * @author pzaytsev
 */
public class CompletableFutureTailCalls {
    private CompletableFutureTailCalls(){

    }

    public static <T> CompletableFuture<T> done(final T value) {
        return CompletableFuture.completedFuture(value);
    }

    public static <T> CompletableFuture<T> call(final Supplier<CompletableFuture<T>> nextCall) {
        return CompletableFuture
                .supplyAsync(nextCall)
                .thenCompose(Function.identity());
    }
}
