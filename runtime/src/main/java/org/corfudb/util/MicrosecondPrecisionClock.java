package org.corfudb.util;

import io.jaegertracing.internal.clock.Clock;

public class MicrosecondPrecisionClock implements Clock {
    private static final long ONE_MINUTE_IN_NANOSECONDS = 60_000_000_000L;
    private final long systemTimeOffsetNs;
    private long lastWarningLoggedAt;

    public MicrosecondPrecisionClock() {
        systemTimeOffsetNs = System.currentTimeMillis() * 1_000_000 - System.nanoTime();
    }

    @Override
    public long currentTimeMicros() {
        long currentNano = System.nanoTime();
        logInaccuracies(currentNano);
        return (currentNano + systemTimeOffsetNs) / 1000;
    }

    private void logInaccuracies(long currentNano) {
        long systemTimeNs = System.currentTimeMillis() * 1_000_000;
        if (systemTimeNs > lastWarningLoggedAt + ONE_MINUTE_IN_NANOSECONDS) {
            if (Math.abs(currentNano + systemTimeOffsetNs - systemTimeNs) > 250_000_000) {
                lastWarningLoggedAt = systemTimeNs;
            }
        }
    }

    @Override
    public long currentNanoTicks() {
        return System.nanoTime();
    }

    @Override
    public boolean isMicrosAccurate() {
        return false;
    }
}