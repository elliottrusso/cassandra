/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.service.snapshot;


import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;

import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.ExecutorUtils;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.utils.FBUtilities.now;

public class SnapshotManager {

    private static final ScheduledExecutorPlus executor = executorFactory().scheduled(false, "SnapshotCleanup");

    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

    private final Supplier<Stream<TableSnapshot>> snapshotLoader;
    private final long initialDelaySeconds;
    private final long cleanupPeriodSeconds;

    @VisibleForTesting
    protected volatile ScheduledFuture cleanupTaskFuture;

    /**
     * Expiring ssnapshots ordered by expiration date, to allow only iterating over snapshots
     * that need to be removed on {@link this#clearExpiredSnapshots()}
     */
    private final PriorityQueue<TableSnapshot> expiringSnapshots = new PriorityQueue<>(Comparator.comparing(x -> x.getExpiresAt()));

    public SnapshotManager()
    {
        this(CassandraRelevantProperties.SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS.getInt(),
             CassandraRelevantProperties.SNAPSHOT_CLEANUP_PERIOD_SECONDS.getInt(),
             () -> StreamSupport.stream(Keyspace.all().spliterator(), false)
                                .flatMap(ks -> ks.getAllSnapshots()));
    }

    @VisibleForTesting
    protected SnapshotManager(long initialDelaySeconds, long cleanupPeriodSeconds,
                              Supplier<Stream<TableSnapshot>> snapshotLoader)
    {
        this.initialDelaySeconds = initialDelaySeconds;
        this.cleanupPeriodSeconds = cleanupPeriodSeconds;
        this.snapshotLoader = snapshotLoader;
    }

    public Collection<TableSnapshot> getExpiringSnapshots()
    {
        return expiringSnapshots;
    }

    public synchronized void start()
    {
        loadSnapshots();
        resumeSnapshotCleanup();
    }

    public synchronized void stop() throws InterruptedException, TimeoutException
    {
        expiringSnapshots.clear();
        if (cleanupTaskFuture != null)
        {
            cleanupTaskFuture.cancel(false);
            cleanupTaskFuture = null;
        }
    }

    public synchronized void addSnapshot(TableSnapshot snapshot)
    {
        // We currently only care about expiring snapshots
        if (snapshot.isExpiring())
        {
            logger.debug("Adding expiring snapshot {}", snapshot);
            expiringSnapshots.add(snapshot);
        }
    }

    @VisibleForTesting
    protected synchronized void loadSnapshots()
    {
        logger.debug("Loading snapshots");
        snapshotLoader.get().forEach(this::addSnapshot);
    }

    // TODO: Support pausing snapshot cleanup
    private synchronized void resumeSnapshotCleanup()
    {
        if (cleanupTaskFuture == null)
        {
            logger.info("Scheduling expired snapshot cleanup with initialDelaySeconds={} and cleanupPeriodSeconds={}");
            cleanupTaskFuture = executor.scheduleWithFixedDelay(this::clearExpiredSnapshots, initialDelaySeconds,
                                                                cleanupPeriodSeconds, TimeUnit.SECONDS);
        }
    }

    @VisibleForTesting
    protected synchronized void clearExpiredSnapshots()
    {
        Instant now = now();
        while (!expiringSnapshots.isEmpty() && expiringSnapshots.peek().isExpired(now))
        {
            TableSnapshot expiredSnapshot = expiringSnapshots.peek();
            logger.debug("Removing expired snapshot {}.", expiredSnapshot);
            clearSnapshot(expiredSnapshot);
        }
    }

    /**
     * Deletes snapshot and remove it from manager
     */
    protected void clearSnapshot(TableSnapshot snapshot)
    {
        for (File snapshotDir : snapshot.getDirectories())
        {
            Directories.removeSnapshotDirectory(DatabaseDescriptor.getSnapshotRateLimiter(), snapshotDir);
        }
        expiringSnapshots.remove(snapshot);
    }

    @VisibleForTesting
    public static void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownNowAndWait(timeout, unit, executor);
    }
}
