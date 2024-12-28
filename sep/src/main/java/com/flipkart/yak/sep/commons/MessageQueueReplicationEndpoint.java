package com.flipkart.yak.sep.commons;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jmx.JmxReporter;
import com.flipkart.yak.sep.commons.exceptions.SepException;
import com.flipkart.yak.sep.filters.*;
import com.flipkart.yak.sep.metrics.SepMetricsPublisher;
import com.flipkart.yak.sep.utils.CFConfigUtils;
import com.flipkart.yak.sep.utils.SepMessageUtils;
import com.flipkart.yak.sep.utils.WalEntryUtils;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;
import io.micrometer.jmx.JmxMeterRegistry;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sep.shade.com.flipkart.yak.sep.proto.SepMessageProto;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Implements a framework to en-queue WAL edits. Enables circuit-breakers, {@link com.flipkart.yak.sep.metrics.MetricsPublisher}
 * around replication. ALong side, registers {@link SepFilter} to specifically attach to replication to selectively filter edits.
 *
 * @see com.flipkart.yak.sep.kafka.KafkaReplicationConfig
 *
 * @param <T> Specific Replication Config Model required by implementing classes.
 * @param <R> Response returned by en-queue method, depends on specific Queue implementation.
 */
public abstract class MessageQueueReplicationEndpoint<T extends CFConfig, R> extends ReloadableReplicationEndpoint {

    private final Logger logger = LoggerFactory.getLogger(MessageQueueReplicationEndpoint.class);
    protected static final String EMPTY_CONFIGURATION_FOUND = "emptyConfigurationFound";
    private static final String METRIC_PREFIX_KEY = "com.flipkart.yak.sep.";
    private static final String CIRCUIT_BREAKER_NAME = "yak-sep-produce";
    private int timeout = 5000;

    static int CB_SLIDING_WINDOW_SIZE = 100;

    static int CB_SLOW_CALL_RATE_THRESHOLD = 50;

    static int CB_FAILURE_RATE_THRESHOLD = 50;

    static double CB_SLOW_CALL_DURATION_THRESHOLD_TIMEOUT_FACTOR = 0.75;

    static long CB_MAX_WAIT_DURATION_IN_HALF_OPEN_STATE = 60000;

    protected static final String DEFAULT_STR = "default";
    private final Map<String, SepFilter<SepFilterBaseModel>> allSepFilters = new HashMap<>();
    protected Map<String , T> cfConfigMap;
    protected boolean propogateMutationFlag;
    protected CBConfig cbConfig = new CBConfig();

    private MetricRegistry registry = new MetricRegistry();
    private JmxReporter reporter;
    protected SepMetricsPublisher publisher;

    private CircuitBreaker circuitBreaker;
    private CircuitBreakerRegistry circuitBreakerRegistry;
    private MeterRegistry meterRegistry;
    private SecureRandom random = new SecureRandom();
    private UUID id;
    private UUID clusterId;


    @Override
    public final UUID getPeerUUID() {
        logger.debug("getPeer UUID {}", id);
        return this.id;
    }

    @Override
    public final void init() throws SepException {
        logger.info("Initialising {}", this.getReplicatorName());
        this.loadMetricPublisher();
        circuitBreakerInit();
        Configuration hbaseConfig = HBaseConfiguration.create(this.ctx.getConfiguration());
        String peerId = this.ctx.getPeerId();
        this.id = UUID.nameUUIDFromBytes(peerId.getBytes());
        this.clusterId = this.ctx.getClusterId();
        this.onStart(hbaseConfig);
        SepFilter<SepFilterBaseModel> sepFilter = new WALOriginBasedFilter();
        sepFilter.registerMetricPublisher(this.publisher);
        this.registerFilter(sepFilter);
    }

    @Override
    public final void notifyInitFailure(Throwable ex) {
        publisher.incrementErrorMetric(SepMetricsPublisher.REPLICATE_EXCEPTION, ex);
    }


    @Override
    protected final void doStop() {
        try {
            logger.info("Stopping {}", this.getReplicatorName());
            this.stopReplication();
            logger.info("Stopped {}", this.getReplicatorName());
            notifyStopped();
            this.reporter.stop();
            this.meterRegistry.close();
            this.circuitBreaker.transitionToClosedState();
        } catch (Exception e) {
            logger.error("Failed to stop {}", this.getReplicatorName(), e);
            notifyFailed(e);
        }
    }

    public void circuitBreakerInit() {
        logger.info("Circuit Breaking Configuration initialised with configurations  {}" , cbConfig.toString());
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom().
                failureRateThreshold(ObjectUtils.defaultIfNull(cbConfig.getCbFailureRateThreshold(), CB_FAILURE_RATE_THRESHOLD)).
                slowCallRateThreshold(ObjectUtils.defaultIfNull(cbConfig.getCbSlowCallRateThreshold(), CB_SLOW_CALL_RATE_THRESHOLD)).
                slidingWindowSize(ObjectUtils.defaultIfNull(cbConfig.getCbSlidingWindowSize(), CB_SLIDING_WINDOW_SIZE)).
                maxWaitDurationInHalfOpenState(Duration.ofMillis(ObjectUtils.defaultIfNull(cbConfig.getCbMaxWaitDurationInHalfOpenState(), CB_MAX_WAIT_DURATION_IN_HALF_OPEN_STATE))).
                slowCallDurationThreshold(Duration.ofMillis(Math.round(timeout * ObjectUtils.defaultIfNull(cbConfig.getCbSlowCallDurationThresholdTimeoutFactor(), CB_SLOW_CALL_DURATION_THRESHOLD_TIMEOUT_FACTOR)))).
                enableAutomaticTransitionFromOpenToHalfOpen().
                build();
        this.loadCircuitBreaker(circuitBreakerConfig, ObjectUtils.defaultIfNull(cbConfig.getCbSlowCallDurationThresholdTimeoutFactor(), CB_SLOW_CALL_DURATION_THRESHOLD_TIMEOUT_FACTOR));
    }

    private void loadCircuitBreaker(CircuitBreakerConfig circuitBreakerConfig, double slowCallFactor ) {
        long slowCallDurationThreshold = Math.round(this.timeout * slowCallFactor);
        logger.info("Got val {} for slowCallDurationThreshold {}", timeout, slowCallDurationThreshold);
        CircuitBreakerConfig breakerConfig = CircuitBreakerConfig.from(circuitBreakerConfig)
                .slowCallDurationThreshold(Duration.ofMillis(slowCallDurationThreshold)).build();
        this.circuitBreakerRegistry = Optional.ofNullable(this.circuitBreakerRegistry).orElse(CircuitBreakerRegistry.ofDefaults());
        this.circuitBreaker = this.circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME, breakerConfig);
        this.circuitBreaker.transitionToClosedState();
        this.circuitBreaker.getEventPublisher().onSlowCallRateExceeded(event -> logger.info("SlowCallRateExceeded Event: {}",event));
        this.circuitBreaker.getEventPublisher().onFailureRateExceeded(event -> logger.info("FailureRateExceeded Event {}",event));
        this.circuitBreaker.getEventPublisher().onStateTransition(event -> logger.info("StateTransition Event: {}",event));
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(this.circuitBreakerRegistry).bindTo(this.meterRegistry);
        logger.info("Starting replication with circuit breaker settings with {}", this.circuitBreaker.getCircuitBreakerConfig());
    }

    private void loadMetricPublisher() {
        this.reporter = JmxReporter.forRegistry(registry).build();
        this.meterRegistry = new JmxMeterRegistry(s -> null, Clock.SYSTEM, HierarchicalNameMapper.DEFAULT, registry, reporter);
        this.reporter.start();
        this.publisher = new SepMetricsPublisher(this.registry, METRIC_PREFIX_KEY);
    }

    private void registerFilter(SepFilter<SepFilterBaseModel> sepFilter) {
        this.allSepFilters.putIfAbsent(sepFilter.name(), sepFilter);
    }

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
        List<WAL.Entry> entries = null;
        publisher.incrementMetric(SepMetricsPublisher.REPLICATE_BATCH_INIT);
        Timer.Context timer = publisher.getTimer(SepMetricsPublisher.REPLICATE_TIMER);
        try {
            synchronized (lock) {
                entries = replicateContext.getEntries();
                Map<String, List<Future<R>>> batchFutures = new HashMap<>();
                if (this.skipAndFail(replicateContext)) {
                    return false;
                }
                if (this.skipAndAck(replicateContext)) {
                    publisher.incrementErrorMetric(SepMetricsPublisher.REPLICATE_EXCEPTION, EMPTY_CONFIGURATION_FOUND);
                    return true;
                }
                this.pushWalEntries(entries, batchFutures);
                logger.debug("Flushing futures {}:{}", replicateContext.getWalGroupId(), replicateContext.getSize());
                for (Map.Entry<String, List<Future<R>>> batchFuture : batchFutures
                        .entrySet()) {
                    List<Future<R>> lst = batchFuture.getValue();
                    for (Future<R> f : lst) {
                        this.circuitBreaker.executeCheckedSupplier(() -> f.get(timeout, TimeUnit.MILLISECONDS));
                        publisher.incrementMetric(propogateMutationFlag ? SepMetricsPublisher.REPLICATE_MUTATION_EVENT_COMPLETE : SepMetricsPublisher.REPLICATE_EVENT_COMPLETE);
                    }
                }
                logger.debug("Flushed futures");
            }
        }
        catch (CallNotPermittedException e) {
            if (random.nextInt(500) == 1) {
                logger.warn("Circuit is {} while replicating with message => {} ", this.circuitBreaker.getState(), e.getMessage());
            }
        }
        catch(Throwable e){
                publisher.incrementErrorMetric(SepMetricsPublisher.REPLICATE_EXCEPTION, (e.getCause() != null) ? e.getCause() : e);
                logger.error("Exception while replicating entries ", e);
                return false;
            } finally {
                publisher.incrementMetric(SepMetricsPublisher.REPLICATE_BATCH_COMPLETE);
                timer.close();
            }
        return true;
    }

    /**
     * Responsible for actually performing enqueue operation on a batch of {@link WAL.Entry}. This applies filters
     * on edits, and calls <code>send</code> for individual {@link Cell} edits.
     *
     * @param entries batch of WAL entries to be pushed.
     * @param batchFutures list to be populated with <code>Future</code> objects representing returns from each
     *                     <code>send</code> call of implementing classes.
     */
    private void pushWalEntries(List<WAL.Entry> entries, Map<String, List<Future<R>>> batchFutures) {

        for (WAL.Entry entry : entries) {
            WALKey key = WalEntryUtils.getWALKeyFromEntry(entry);
            WALEdit val = WalEntryUtils.getWALEditFromEntry(entry);
            TableName tableName = key.getTableName();
            SepMessageProto.SepTableName sepTable = WalEntryUtils.getSepTableName(entry);
            UUID originClusterId = WalEntryUtils.getOriginClusterIdFromWAL(entry);
            if( originClusterId == null) {
                originClusterId = clusterId;
                logger.warn("WALEdit doesn't have source cluster details. Not filtering based on origin cluster");
                publisher.incrementMetric(SepMetricsPublisher.FILTER_NO_ORIGIN);
            }
            logger.debug("Propogate Mutation Configured as {}", propogateMutationFlag);

            if(propogateMutationFlag) {
                if(!(val.getCells().isEmpty())) {
                    if(originClusterId.equals(clusterId)) {
                        publisher.incrementMetric(SepMetricsPublisher.REPLICATE_MUTATION_EVENT_INIT);
                        pushMutationEvent(batchFutures, key, val.getCells(), tableName, sepTable, originClusterId, true);
                    }
                    else {
                        publisher.incrementMetric(SepMetricsPublisher.REPLICATE_ROW_MUTATION_EVENT_INIT);
                        Map<String, List<Cell>> entriesByRowKey = val.getCells().stream().collect(Collectors.groupingBy(cell -> Bytes.toString(CellUtil.cloneRow(cell))));
                        for (Map.Entry<String, List<Cell>> rowData : entriesByRowKey.entrySet()) {
                            pushMutationEvent(batchFutures, key, rowData.getValue(), tableName, sepTable, originClusterId, false);
                        }
                    }
                }
                else {
                    logger.debug("WAL Edit does not contain any cell");
                    publisher.incrementMetric(SepMetricsPublisher.FILTER_NO_CELLS);
                }
            }

            else {
                for (Cell cell : val.getCells()) {
                    publisher.incrementMetric(SepMetricsPublisher.REPLICATE_EVENT_INIT);
                    String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                    boolean isFiltered = this.applyFilters(qualifier, columnFamily, cell, originClusterId);
                    logger.debug("For {}:{} isFiltered: {}", columnFamily, qualifier, isFiltered);
                    if (isFiltered) {
                        publisher.incrementMetric(SepMetricsPublisher.FILTER_ANY);
                        publisher.incrementMetric(SepMetricsPublisher.REPLICATE_EVENT_COMPLETE);
                        continue;
                    }
                    Optional<String> topicOptional = CFConfigUtils.getTopicNameFromConfig(tableName, columnFamily, qualifier, this.getColumnFamilyConfig(columnFamily));
                    if (!topicOptional.isPresent()) {
                        logger.debug("ignoring because there is no topic");
                        publisher.incrementMetric(SepMetricsPublisher.FILTER_NO_TOPIC);
                        publisher.incrementMetric(SepMetricsPublisher.REPLICATE_EVENT_COMPLETE);
                        continue;
                    }
                    String topic = topicOptional.get();
                    logger.debug("For {}:{} topic: {}", columnFamily, qualifier, topic);
                    SepMessageProto.SepMessage msg = SepMessageUtils.buildSepMessage(sepTable, cell, key);
                    Optional<Pair<String, Future<R>>> sendResponse = this.send(msg, cell, topic, columnFamily);
                    sendResponse.ifPresent(stringFuturePair -> {
                                batchFutures.putIfAbsent(stringFuturePair.getFirst(), new ArrayList<>());
                                batchFutures.get(stringFuturePair.getFirst()).add(stringFuturePair.getSecond());
                            }
                    );
                }
            }
        }
    }

    private void pushMutationEvent(Map<String, List<Future<R>>> batchFutures, WALKey key, List<Cell> cellList, TableName tableName, SepMessageProto.SepTableName sepTable, UUID originClusterId, boolean localOriginFlag) {
        SepMessageProto.SepMessageV2 message = SepMessageUtils.buildSepMessageV2(sepTable,key,cellList);
        Set<String> columnFamilySet = SepMessageUtils.getCFSet(cellList);
        for(String cfConfigKey : cfConfigMap.keySet()) {
            if((cfConfigKey  == DEFAULT_STR) || (columnFamilySet.contains(cfConfigKey))) {
                boolean isFiltered = this.applyFilters(cfConfigKey, originClusterId);
                logger.debug("For {} isFiltered: {}", cfConfigKey, isFiltered);
                if (isFiltered) {
                    publisher.incrementMetric(SepMetricsPublisher.FILTER_ANY);
                    publisher.incrementMetric(localOriginFlag ? SepMetricsPublisher.REPLICATE_MUTATION_EVENT_COMPLETE : SepMetricsPublisher.REPLICATE_ROW_MUTATION_EVENT_COMPLETE);
                    continue;
                }
                Optional<String> topicOptional = CFConfigUtils.getTopicNameFromConfig(tableName, cfConfigKey, this.getColumnFamilyConfig(cfConfigKey));
                if (!topicOptional.isPresent()) {
                    logger.debug("Ignoring because there is no topic");
                    publisher.incrementMetric(SepMetricsPublisher.FILTER_NO_TOPIC);
                    publisher.incrementMetric(localOriginFlag ? SepMetricsPublisher.REPLICATE_MUTATION_EVENT_COMPLETE : SepMetricsPublisher.REPLICATE_ROW_MUTATION_EVENT_COMPLETE);
                    continue;
                }
                String topic = topicOptional.get();
                logger.debug("For topic: {}", topic);
                Optional<Pair<String, Future<R>>> sendResponse = this.send(message, cellList.get(0), topic, cfConfigKey, localOriginFlag);
                sendResponse.ifPresent(stringFuturePair -> {
                            batchFutures.putIfAbsent(stringFuturePair.getFirst(), new ArrayList<>());
                            batchFutures.get(stringFuturePair.getFirst()).add(stringFuturePair.getSecond());
                        }
                );
            }
        }
    }

    /**
     * Applies all {@link SepFilter} on {@link Cell} iteratively registered for the instance. By default, {@link WALOriginBasedFilter}
     * is registered and this method applies on edits.
     *
     * @param qualifier column name for the cell
     * @param columnFamily column family for the cell
     * @param cell contains edit
     * @param originClusterId source cluster ID, required for {@link SepFilterBaseModel}
     * @return true if needs to be blocked
     */
    private boolean applyFilters(String qualifier, String columnFamily, Cell cell, UUID originClusterId) {
        boolean shouldBeFiltered = true;
        for (SepFilter<SepFilterBaseModel> sepFilter : this.allSepFilters.values()) {
            SepFilterBaseModel sepFilterBaseModel = new SepFilterModelBuilder().setCell(cell)
                    .setColumnFamily(columnFamily).setQualifier(qualifier).setOriginClusterId(originClusterId)
                    .setCurrentClusterId(clusterId).createSepFilterBaseModel();
            shouldBeFiltered = shouldBeFiltered && sepFilter.filter(sepFilterBaseModel, this.getColumnFamilyConfig(columnFamily));
        }
        return shouldBeFiltered;
    }

    private boolean applyFilters(String columnFamily, UUID originClusterId) {
        boolean shouldBeFiltered = true;
        for (SepFilter<SepFilterBaseModel> sepFilter : this.allSepFilters.values()) {
            SepFilterBaseModel sepFilterBaseModel = new SepFilterModelBuilder().setColumnFamily(columnFamily)
                    .setOriginClusterId(originClusterId)
                    .setCurrentClusterId(clusterId).createSepFilterBaseModel();
            shouldBeFiltered = shouldBeFiltered && sepFilter.filter(sepFilterBaseModel, this.getColumnFamilyConfig(columnFamily));
        }
        return shouldBeFiltered;
    }
    /**
     * An utility method to get {@link CFConfig} from {@link SepConfig}
     */
    private Optional<CFConfig> getColumnFamilyConfig(String columnFamily) {
        CFConfig cfConf;
        if (cfConfigMap.containsKey(columnFamily)) {
            cfConf = cfConfigMap.get(columnFamily);
        } else if (cfConfigMap.containsKey(DEFAULT_STR)) {
            cfConf = cfConfigMap.get(DEFAULT_STR);
        } else {
            return Optional.empty();
        }
        return Optional.of(cfConf);
    }

    @Override
    public final void start() {
        startAsync();
    }

    @Override
    public final void stop() {
        stopAsync();
    }

    /**
     * Implementation to enqueue <code>WAL Edits</code>.
     *
     * @param sepMessage De-serialised version of {@link Cell}.
     * @param cell The {@link Cell} data as well which is to be replicated.
     * @param topic The name topic to be replicated, this is fully-qualified topic name as given in config.
     * @param columnFamily The name of the column-family to which the {@link Cell} belong.
     * @return <code>Optional.empty()</code> if sends needless to be performed, else <code>Future</> wrapper of Response.
     */
    public abstract Optional<Pair<String, Future<R>>> send(SepMessageProto.SepMessage sepMessage, Cell cell, String topic, String columnFamily);

    public abstract Optional<Pair<String, Future<R>>> send(SepMessageProto.SepMessageV2 sepMessage,  Cell cell, String topic, String columnFamily, boolean localOriginFlag);

    /**
     * Init method, should be used to populate initial config and any resources to be prepared in advance.
     *
     * @param hbaseConfig {@link Configuration} instance representing HBASE config in current context.
     */
    public abstract void onStart(Configuration hbaseConfig) throws SepException;

    /**
     *
     * @param replicateContext {@link org.apache.hadoop.hbase.replication.ReplicationEndpoint.ReplicateContext} passed to
     *                         replicate method of {@link org.apache.hadoop.hbase.replication.ReplicationEndpoint} contains
     *                         WAL Edits.
     *
     * @return true if given {@link org.apache.hadoop.hbase.replication.ReplicationEndpoint.ReplicateContext} needs to
     *         be skipped but DON'T ack to Replicator, rather go for retry, may be due to corruption in publisher
     *
     */
    public abstract boolean skipAndFail(ReplicateContext replicateContext);

    /**
     *
     * @param replicateContext {@link org.apache.hadoop.hbase.replication.ReplicationEndpoint.ReplicateContext} passed to
     *                         replicate method of {@link org.apache.hadoop.hbase.replication.ReplicationEndpoint} contains
     *                         WAL Edits.
     *
     * @return true if given {@link org.apache.hadoop.hbase.replication.ReplicationEndpoint.ReplicateContext} needs to
     *         be skipped but ack to Replicator, dont retry even, implies irrelevant edits
     *
     */
    public abstract boolean skipAndAck(ReplicateContext replicateContext);

    /**
     * Information returned by this method will be used for segregating telemetry and logging mostly.
     *
     * @return Name of {@link MessageQueueReplicationEndpoint} as set by implementation
     */
    public abstract String getReplicatorName();

}

