package com.flipkart.yak.sep.commons;

import static com.flipkart.yak.sep.commons.MessageQueueReplicationEndpoint.*;

public class CBConfig {

    private int cbSlidingWindowSize = CB_SLIDING_WINDOW_SIZE;

    private int cbSlowCallRateThreshold = CB_SLOW_CALL_RATE_THRESHOLD;

    private int cbFailureRateThreshold = CB_FAILURE_RATE_THRESHOLD;

    private double cbSlowCallDurationThresholdTimeoutFactor = CB_SLOW_CALL_DURATION_THRESHOLD_TIMEOUT_FACTOR;

    private long cbMaxWaitDurationInHalfOpenState = CB_MAX_WAIT_DURATION_IN_HALF_OPEN_STATE;

    public long getCbMaxWaitDurationInHalfOpenState() {
        return cbMaxWaitDurationInHalfOpenState;
    }

    public void setCbMaxWaitDurationInHalfOpenState(long cbMaxWaitDurationInHalfOpenState) {
        this.cbMaxWaitDurationInHalfOpenState = cbMaxWaitDurationInHalfOpenState;
    }

    public double getCbSlowCallDurationThresholdTimeoutFactor() {
        return cbSlowCallDurationThresholdTimeoutFactor;
    }

    public void setCbSlowCallDurationThresholdTimeoutFactor(double cbSlowCallDurationThresholdTimeoutFactor) {
        this.cbSlowCallDurationThresholdTimeoutFactor = cbSlowCallDurationThresholdTimeoutFactor;
    }

    public int getCbFailureRateThreshold() {
        return cbFailureRateThreshold;
    }

    public void setCbFailureRateThreshold(int cbFailureRateThreshold) {
        this.cbFailureRateThreshold = cbFailureRateThreshold;
    }

    public int getCbSlowCallRateThreshold() {
        return cbSlowCallRateThreshold;
    }

    public void setCbSlowCallRateThreshold(int cbSlowCallRateThreshold) {
        this.cbSlowCallRateThreshold = cbSlowCallRateThreshold;
    }

    public int getCbSlidingWindowSize() {
        return cbSlidingWindowSize;
    }

    public void setCbSlidingWindowSize(int cbSlidingWindowSize) {
        this.cbSlidingWindowSize = cbSlidingWindowSize;
    }

    @Override public String toString() {
        return "CBConfig{" + ", cbSlidingWindowSize=" + cbSlidingWindowSize
                + ", cbSlowCallRateThreshold=" + cbSlowCallRateThreshold + ", cbFailureRateThreshold='" + cbFailureRateThreshold + '\''
                + ", cbSlowCallDurationThresholdTimeoutFactor=" + cbSlowCallDurationThresholdTimeoutFactor + ", cbMaxWaitDurationInHalfOpenState=" + cbMaxWaitDurationInHalfOpenState + '}';
    }
}
