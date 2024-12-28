package com.flipkart.yak.sep.filters;

import com.flipkart.yak.sep.commons.CFConfig;
import com.flipkart.yak.sep.metrics.SepMetricsPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;


public class WALOriginBasedFilter implements SepFilter<SepFilterBaseModel> {

    private final Logger logger = LoggerFactory.getLogger(WALOriginBasedFilter.class);
    Optional<SepMetricsPublisher> metricsPublisher = Optional.empty();

    @Override
    public String name() {
        return "WALOriginBasedFilter";
    }

    @Override
    public void registerMetricPublisher(SepMetricsPublisher metricsPublisher) {
        this.metricsPublisher = Optional.of(metricsPublisher);
    }

    @Override
    public boolean filter(SepFilterBaseModel model, Optional<CFConfig> configArgs) {
        if (!configArgs.isPresent()) {
            return false;
        }
        CFConfig config = configArgs.get();
        WALEditOriginWhitelist originFilter = config.getWalEditOriginWhitelist();
        logger.debug("originFilter: {}, originClusterId: {}, whitelistedOrigins: {}", originFilter, model.getOriginClusterId(),
                config.getWhitelistedOrigins());
        if (originFilter.equals(WALEditOriginWhitelist.LOCAL_ORIGIN) && !model.getOriginClusterId().equals(model.getCurrentClusterId())) {
            metricsPublisher.ifPresent(publisher -> publisher.incrementMetric(SepMetricsPublisher.FILTER_NON_LOCAL));
            return true;
        } else if (originFilter.equals(WALEditOriginWhitelist.WHITELISTED_ORIGIN) && (
                (config.getWhitelistedOrigins() == null) || (
                        !config.getWhitelistedOrigins().contains(model.getOriginClusterId().toString())))) {
            metricsPublisher.ifPresent(publisher -> publisher.incrementMetric(SepMetricsPublisher.FILTER_NON_WHITELIST));
            return true;
        }
        return false;
    }
}
