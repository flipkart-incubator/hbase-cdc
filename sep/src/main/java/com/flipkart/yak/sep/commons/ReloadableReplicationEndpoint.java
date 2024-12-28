package com.flipkart.yak.sep.commons;

import com.flipkart.yak.sep.commons.exceptions.SepConfigException;
import com.flipkart.yak.sep.commons.exceptions.SepException;
import com.flipkart.yak.sep.commons.exceptions.SepFileWatchException;
import com.flipkart.yak.sep.commons.exceptions.SepRuntimeException;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Implementation of {@link BaseReplicationEndpoint} which enables any implementing classes to recognise
 * any file/config change and reload on the go. Implementing classes should be focusing on replication and
 * reload logic part.
 * <p>
 * <code>doStop</code> from {@link org.apache.hbase.thirdparty.com.google.common.util.concurrent.AbstractService}
 * and <code>stop</code> from {@link org.apache.hadoop.hbase.replication.ReplicationEndpoint} needs to implemented
 * by Implementing classes which may be different implementation from stopping replication before reloads.
 */
public abstract class ReloadableReplicationEndpoint extends BaseReplicationEndpoint {

    private final Logger logger = LoggerFactory.getLogger(ReloadableReplicationEndpoint.class);
    private ExecutorService fileWatcherThread;
    private static final String DEFAULT_WATCH_PATH ="/usr/share/yak";
    private static final String DEFAULT_WATCH_FILE ="reload.log";
    private static final String SEP_THREAD_NAME = "sep-file-watcher-thread";
    final Object lock = new Object();

    @Override
    protected final void doStart() {
        try {
            synchronized (lock) {
                this.init();
                this.watch();
                notifyStarted();
            }
        } catch (Exception e) {
            logger.error("Failed to start ReplicationEndpoint ", e);
            this.notifyInitFailure(e);
            notifyFailed(e);
        }
    }

    private void watch() {
        this.fileWatcherThread = Executors.newSingleThreadExecutor(r -> {
            Thread th = new Thread(r, SEP_THREAD_NAME);
            th.setDaemon(true);
            return th;
        });

        this.fileWatcherThread.execute(() -> {
            try {
                this.setup(DEFAULT_WATCH_PATH, DEFAULT_WATCH_FILE);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (SepConfigException | SepFileWatchException | SepException | IOException e) {
                throw new SepRuntimeException(e);
            }
        });
    }

    private void setup(String pathStr, String fileStr) throws InterruptedException, IOException, SepException, SepFileWatchException, SepConfigException {
        logger.info("Setting up file watcher");
        final Path path = FileSystems.getDefault().getPath(pathStr);
        try (final WatchService watchService = FileSystems.getDefault().newWatchService()) {
            path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
            while (true) {
                final WatchKey wk = watchService.take();
                for (WatchEvent<?> event : wk.pollEvents()) {
                    // we only register "ENTRY_MODIFY" so the context is always a Path.
                    final Path changed = (Path) event.context();
                    logger.debug("File Changed {}", changed.getFileName());
                    if (changed.getFileName().toString().equals(fileStr)) {
                        logger.info("Got Modify event for {} attempting to stop and start", fileStr);
                        try {
                            synchronized (lock) {
                                this.stopReplication();
                                this.reloadConfig();
                                logger.debug("release lock");
                            }

                        } catch (SepException | SepConfigException e) {
                            this.stopReplication();
                            notifyFailed(e);
                            throw e;
                        }
                    }
                }
                boolean valid = wk.reset();
                if (!valid) {
                    logger.info("Key has been unregistered");
                    throw new SepFileWatchException("Failed to register for future events");
                }
            }
        }
    }


    /**
     * Loads config from path populates config state. Clearing old config state is also responsibility of this method.
     * Implementing class is responsible for maintaining HADOOP {@link org.apache.hadoop.conf.Configuration} state,
     * reference to HADOOP config should be managed by implementing classes.
     *
     * @throws SepConfigException If configuration is malformed or not present at specified location.
     *                              Any illegal configuration should also result into this.
     * @see com.flipkart.yak.sep.KafkaReplicationEndPoint
     */
    public abstract void reloadConfig() throws SepConfigException, IOException;

    /**
     * Stops replication and RELEASES all resources those are held up to perform replication. Replication stopped upon
     * every re-start of replication due to any change. Implementing classes are expected to release resources carefully,
     * otherwise can lead to dead-lock.
     *
     * @throws SepException If replication could not be stopped as expected.
     */
    public abstract void stopReplication() throws SepException;

    /**
     * Will be called while bootstrapping {@link BaseReplicationEndpoint} along with <code>doStart</code> method. All
     * initialisations are expected to be part of this.
     * atomically.
     */
    public abstract void init() throws SepException;

    /**
     * Handle exception thrown during init
     * @param ex Thrown due to failure
     */
    public abstract void notifyInitFailure(Throwable ex);
}
