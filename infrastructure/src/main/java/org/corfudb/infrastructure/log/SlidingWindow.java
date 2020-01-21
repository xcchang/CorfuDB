package org.corfudb.infrastructure.log;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/*
    While there is a spike, we start the timer and and remember how many spikes and how long it happens
    continuously. While the spike period is longer than the timeWindow or cntWindow, it reports the problem.
    Any value is less than the thresVal, it will reset the silding window.
 */
@Slf4j
public class SlidingWindow {
    private long cntWindow;
    private long timeWindow;
    private long threshVal; //this is in seconds
    private long startTime = 0;

    @Getter
    ArrayList<Long> intervals;

    public SlidingWindow(long cntWindow, long timeWindow, long threshVal) {
        this.cntWindow = cntWindow;
        this.timeWindow = timeWindow;
        this.threshVal = threshVal;
        startTime = 0;
        intervals = new ArrayList<>();
    }

    public void update(long value) {
        if (value < threshVal && intervals.size()!= 0) {
            //This means the operation is normal, clear the sliding window
            startTime = 0;
            intervals.clear();
        } else if (value >= threshVal){
            // It is a spike

            if (intervals.size() == 0) {
                // Start recording the spike sliding window
                startTime = System.nanoTime();
            }

            if (intervals.size() == cntWindow) {
                // Make space for the new entry
                intervals.remove(0);
            }

            // Put the new value to the sliding window
            intervals.add(value);
        }

    }

    /*
     * It open report an error if the current slidingwindow of spikes has more than cntWindow entries or
     * last longer than timeWindow
     *
     */
    public boolean report() {
        long dur = 0;

        if (startTime != 0)
            dur = TimeUnit.SECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);

        if (intervals.size() == cntWindow || dur >= timeWindow) {
            log.warn("Report event interval size {} ==  {} or dur {} >= timeWindow.",
                    intervals.size(), cntWindow, dur, threshVal);
            return true;
        }
        else
            return false;
    }
}
