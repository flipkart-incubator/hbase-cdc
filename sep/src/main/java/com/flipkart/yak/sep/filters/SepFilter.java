package com.flipkart.yak.sep.filters;

import com.flipkart.yak.sep.commons.CFConfig;
import com.flipkart.yak.sep.metrics.SepMetricsPublisher;

import java.util.Optional;

public interface SepFilter<Model extends SepFilterBaseModel>{
    String name();
    void registerMetricPublisher(SepMetricsPublisher metricsPublisher);
    boolean filter(Model model, Optional<CFConfig> config);
}
