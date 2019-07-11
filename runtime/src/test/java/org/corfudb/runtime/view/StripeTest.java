package org.corfudb.runtime.view;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import org.corfudb.runtime.exceptions.LayoutModificationException;
import org.corfudb.runtime.view.Layout.LayoutStripe;
import org.junit.jupiter.api.Test;

public class StripeTest {

    @Test
    public void testNullListInConstructor() {
        assertThrows(NullPointerException.class, () -> new LayoutStripe(null));
    }

    @Test
    public void testEquals() {
        ImmutableList<String> stripe = ImmutableList.of("a", "b", "c");
        assertEquals(new LayoutStripe(stripe), new LayoutStripe(stripe));
    }

    @Test
    public void testCheckAndWithout() {
        ImmutableList<String> logServers = ImmutableList.of("a", "b", "c");
        LayoutStripe stripe = LayoutStripe.builder()
                .logServers(logServers)
                .build();

        assertEquals(logServers.size(), stripe.size());

        stripe.check(1);
        stripe = stripe.without("a");
        assertEquals(stripe.getLogServers(), ImmutableList.of("b", "c"));

        stripe = stripe.without("b");
        assertEquals(stripe.getLogServers(), ImmutableList.of("c"));

        LayoutStripe emptyStripe = stripe.without("c");
        assertThrows(LayoutModificationException.class, () -> emptyStripe.check(1));
    }

    @Test
    public void testMinReplicationFactor() {
        ImmutableList<String> logServers = ImmutableList.of("a", "b", "c");
        LayoutStripe stripe = LayoutStripe.builder()
                .logServers(logServers)
                .build();

        assertThrows(LayoutModificationException.class, () -> stripe.check(stripe.size() + 1));
    }
}
