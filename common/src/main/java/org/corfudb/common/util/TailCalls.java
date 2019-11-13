package org.corfudb.common.util;

/**
 * A convenience class to easily operate with TailCall instances.
 */
public class TailCalls {
    private TailCalls() {
        // Singleton.
    }

    /**
     * Evaluate an intermediate case of a recursion.
     *
     * @param nextCall Next tail call to execute.
     * @param <T>      A type of object that returns when the tail call function executes.
     * @return A next instance of tail call.
     */
    public static <T> TailCall<T> call(final TailCall<T> nextCall) {
        return nextCall;
    }

    /**
     * Evaluate a base case of a recursion.
     *
     * @param value A base result of a recursive call.
     * @param <T>   A type of value.
     * @return An instance of a done TailCall.
     */
    public static <T> TailCall<T> done(final T value) {
        return new TailCall<T>() {
            /**
             * Returns true in the base recursive case.
             * @return True.
             */
            @Override
            public boolean isComplete() {
                return true;
            }

            /**
             * Returns a result in the base recursive case.
             * @return A result of an evaluation.
             */
            @Override
            public T result() {
                return value;
            }

            /**
             * Throw an exception if a base case returns a next call.
             * @throws IllegalStateException all the time.
             */
            @Override
            public TailCall<T> apply() {
                throw new IllegalStateException("Base recursive case can not return a new call.");
            }
        };
    }
}