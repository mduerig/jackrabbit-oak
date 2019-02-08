/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.segment.scheduler;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.Revisions;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreStats;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

public class LockBasedSchedulerTest {

    private NodeState getRoot(Scheduler scheduler) {
        return scheduler.getHeadNodeState().getChildNode("root");
    }

    private static final Logger log = LoggerFactory.getLogger(LockBasedSchedulerCheckpointTest.class);

    /**
     * OAK-7162
     * <p>
     * This test guards against race conditions which may happen when the head
     * state in {@link Revisions} is changed from outside the scheduler. If a
     * race condition happens at that point, data from a single commit will be
     * lost.
     */
    @Test
    public void testSimulatedRaceOnRevisions() throws Exception {
        final MemoryStore ms = new MemoryStore();
        StatisticsProvider statsProvider = StatisticsProvider.NOOP;
        SegmentNodeStoreStats stats = new SegmentNodeStoreStats(statsProvider);
        final LockBasedScheduler scheduler = LockBasedScheduler.builder(ms.getRevisions(), ms.getReader(), stats)
                .build();

        final RecordId initialHead = ms.getRevisions().getHead();
        ExecutorService executorService = newFixedThreadPool(10);
        final AtomicInteger count = new AtomicInteger();
        final Random rand = new Random();

        try {
            Callable<PropertyState> commitTask = new Callable<PropertyState>() {
                @Override
                public PropertyState call() throws Exception {
                    String property = "prop" + count.incrementAndGet();
                    Commit commit = createCommit(scheduler, property, "value");
                    SegmentNodeState result = (SegmentNodeState) scheduler.schedule(commit);

                    return result.getProperty(property);
                }
            };

            Callable<Void> parallelTask = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    Thread.sleep(rand.nextInt(10));
                    ms.getRevisions().setHead(ms.getRevisions().getHead(), initialHead);
                    return null;
                }
            };

            List<Future<?>> results = newArrayList();
            for (int i = 0; i < 100; i++) {
                results.add(executorService.submit(commitTask));
                executorService.submit(parallelTask);
            }

            for (Future<?> result : results) {
                assertNotNull(
                        "PropertyState must not be null! The corresponding commit got lost because of a race condition.",
                        result.get());
            }
        } finally {
            new ExecutorCloser(executorService).close();
        }
    }

    private Commit createCommit(final Scheduler scheduler, final String property, String value) {
        NodeBuilder a = getRoot(scheduler).builder();
        a.setProperty(property, value);
        Commit commit = new Commit(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        return commit;
    }

    @Test
    public void testLockBasedSchedulerGC() throws Exception {
        MemoryStore ms = new MemoryStore();
        StatisticsProvider statsProvider = StatisticsProvider.NOOP;
        SegmentNodeStoreStats stats = new SegmentNodeStoreStats(statsProvider);
        final LockBasedScheduler scheduler = LockBasedScheduler.builder(ms.getRevisions(), ms.getReader(), stats)
                .build();

        createGarbage(scheduler);

        CountDownLatch readyToCompact = new CountDownLatch(1);
        CountDownLatch compactionCompleted = new CountDownLatch(1);
        CountDownLatch changesDone = new CountDownLatch(1);
        CountDownLatch afterChangesDone = new CountDownLatch(1);
        ArrayBlockingQueue<Integer> results = new ArrayBlockingQueue<>(2);

        NodeBuilder changes = getRoot(scheduler).builder();
        changes.setProperty("a", "a");
        changes.setProperty(new StringPropertyState("b", "b") {
            @Override
            public String getValue() {
                readyToCompact.countDown();
                awaitUninterruptibly(compactionCompleted);
                return super.getValue();
            }
        });

        NodeBuilder afterChanges = getRoot(scheduler).builder();
        afterChanges.setProperty("a", "a");

        // Overlap an ongoing write operation triggered by the call to getNodeState
        // with a full compaction.
        runAsync(() -> {
            log.info("1 is scheduled");
            NodeState ret = scheduler.schedule(new Commit(changes, EmptyHook.INSTANCE, CommitInfo.EMPTY));
            results.add(1);
            changesDone.countDown();
            log.info("1 is done");
            return ret;
        });

        readyToCompact.await();
        ms.gc();

        runAsync(() -> {
            log.info("2 is scheduled");
            NodeState ret = scheduler.schedule(new Commit(afterChanges, EmptyHook.INSTANCE, CommitInfo.EMPTY));
            results.add(2);
            afterChangesDone.countDown();
            log.info("2 is done");
            return ret;
        });

        afterChangesDone.await(1, TimeUnit.SECONDS);
        compactionCompleted.countDown();
        changesDone.await();
        assertArrayEquals(new Integer[]{2,1}, results.toArray(new Integer[]{}));
    }

    private void createGarbage(final Scheduler scheduler) {
        NodeBuilder a = getRoot(scheduler).builder();
        for (int i = 0; i < 1000000; ++i) {
            a.setProperty("foo", "bar" + i);
            Commit c = new Commit(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            try {
                scheduler.schedule(c);
            } catch (CommitFailedException e) {
            }
        }
    }

    private static <T> FutureTask<T> runAsync(Callable<T> callable) {
        FutureTask<T> task = new FutureTask<T>(callable);
        new Thread(task).start();
        return task;
    }
}
