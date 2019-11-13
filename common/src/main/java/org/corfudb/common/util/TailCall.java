package org.corfudb.common.util;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * When a call to a method 2 is the last thing a calling method 1 does, there's nothing to
 * resume to after a method 2 returns. A method call in the last position,
 * a last thing to do before a calling function returns, is a tail call.
 *
 * This interface represents a tail-recursive call.
 * It is an intermediate tail-recursive call by default. This interface allows a conversion of a
 * regular recursive call into a tail call to make a recursion practical for the large inputs
 * (avoids a StackOverflowException). Avoiding pushing the environment on the stack to resume
 * a method after a tail call is called a Tail-Call Elimination, TCE.
 *
 * This interface implements TCE by a way of 'trampolining' a method call.
 * Each unevaluated call is being suspended, returning a next unevaluated call
 * until the terminal condition is reached. That way we construct a lazily-evaluated linked list
 * that we later, upon the reach of a terminal case, evaluate in a reverse order. This linked list
 * is essentially a stack with an O(1) append and an O(1) access to the last element.
 *
 * @author pzaytsev
 */
@FunctionalInterface
public interface TailCall<T> {
    /**
     * Applies a function and returns immediately with a next instance of TailCall
     * ready for execution.
     * @return A result of a call wrapped into the TailCall.
     */
    TailCall<T> apply();

    /**
     * Returns false in the intermediate recursive case.
     * @return False.
     */
    default boolean isComplete() {
        return false;
    }

    /**
     * Throws an exception in the intermediate recursive case.
     * @return A result.
     */
    default T result() {
        throw new IllegalStateException("Intermediate recursive case has no value.");
    }

    /**
     * Lazily iterates over the pending tail call recursions until it
     * reaches the end.
     * @return An optional result of a recursive call.
     */
    default Optional<T> invoke() {
        return Stream.iterate(this, TailCall::apply)
                .filter(TailCall::isComplete)
                .findFirst()
                .map(TailCall::result);
    }
}