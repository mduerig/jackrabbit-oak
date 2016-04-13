/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.plugins.segment.Record.fastEquals;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore.ROOT;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.segment.scheduler.Scheduler;
import org.apache.jackrabbit.oak.plugins.segment.scheduler.SchedulerOptions;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

class RetryScheduler implements Scheduler<SchedulerOptions> {

    private final Random random = new Random();

    private final boolean commitFairLock = Boolean.getBoolean("oak.segmentNodeStore.commitFairLock");

    private final Semaphore commitSemaphore = new Semaphore(1, commitFairLock);

    private final long maximumBackOff = MILLISECONDS.convert(10, SECONDS);

    private final int checkpointsLockWaitTime = Integer.getInteger("oak.checkpoints.lockWaitTime", 10);

    private final SegmentStore store;

    private final AtomicReference<SegmentNodeState> head;

    RetryScheduler(SegmentStore store) {
        this.store = store;
        this.head = new AtomicReference<SegmentNodeState>(store.getHead());
    }

    @Override
    public NodeState schedule(NodeBuilder changes, CommitHook hook, CommitInfo info, SchedulerOptions options) throws CommitFailedException {
        checkArgument(changes instanceof SegmentNodeBuilder);
        SegmentNodeBuilder snb = (SegmentNodeBuilder) changes;
        checkArgument(snb.isRootBuilder());
        checkNotNull(hook);

        try {
            commitSemaphore.acquire();
            try {
                NodeState merged = execute(snb, hook, info);
                snb.reset(merged);
                return merged;
            } finally {
                commitSemaphore.release();
            }
        } catch (InterruptedException e) {
            currentThread().interrupt();
            throw new CommitFailedException("Segment", 2, "Merge interrupted", e);
        } catch (SegmentOverflowException e) {
            throw new CommitFailedException("Segment", 3, "Merge failed", e);
        }
    }

    @Override
    public String addCheckpoint(long lifetime, Map<String, String> properties) {
        checkArgument(lifetime > 0);
        checkNotNull(properties);

        String name = UUID.randomUUID().toString();

        try {
            if (commitSemaphore.tryAcquire(checkpointsLockWaitTime, SECONDS)) {
                try {
                    lockedAddCheckpoint(name, lifetime, properties);
                } finally {
                    commitSemaphore.release();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return name;
    }

    @Override
    public boolean removeCheckpoint(String name) {
        checkNotNull(name);

        // try 5 times
        for (int i = 0; i < 5; i++) {
            if (commitSemaphore.tryAcquire()) {
                try {
                    refreshHead();

                    SegmentNodeState state = head.get();
                    SegmentNodeBuilder builder = state.builder();

                    NodeBuilder cp = builder.child("checkpoints").child(name);
                    if (cp.exists()) {
                        cp.remove();
                        SegmentNodeState newState = builder.getNodeState();
                        if (store.setHead(state, newState)) {
                            refreshHead();
                            return true;
                        }
                    }
                } finally {
                    commitSemaphore.release();
                }
            }
        }

        return false;
    }

    private void lockedAddCheckpoint(String name, long lifetime, Map<String, String> properties) {
        long now = System.currentTimeMillis();

        refreshHead();

        SegmentNodeState state = head.get();
        SegmentNodeBuilder builder = state.builder();

        NodeBuilder checkpoints = builder.child("checkpoints");

        for (String n : checkpoints.getChildNodeNames()) {
            NodeBuilder cp = checkpoints.getChildNode(n);
            PropertyState ts = cp.getProperty("timestamp");
            if (ts == null || ts.getType() != LONG || now > ts.getValue(LONG)) {
                cp.remove();
            }
        }

        NodeBuilder cp = checkpoints.child(name);
        cp.setProperty("timestamp", now + lifetime);
        cp.setProperty("created", now);

        NodeBuilder props = cp.setChildNode("properties");
        for (Map.Entry<String, String> p : properties.entrySet()) {
            props.setProperty(p.getKey(), p.getValue());
        }
        cp.setChildNode(ROOT, state.getChildNode(ROOT));

        store.setHead(state, builder.getNodeState());
    }

    private NodeState execute(SegmentNodeBuilder changes, CommitHook hook, CommitInfo info) throws CommitFailedException, InterruptedException {
        // only do the merge if there are some changes to commit
        if (!fastEquals(changes.getBaseState(), changes.getNodeState())) {
            long timeout = optimisticMerge(changes, hook, info);

            if (timeout >= 0) {
                pessimisticMerge(changes, hook, info, timeout);
            }
        }

        return head.get().getChildNode(ROOT);
    }

    private long optimisticMerge(SegmentNodeBuilder changes, CommitHook hook, CommitInfo info) throws CommitFailedException, InterruptedException {
        long timeout = 1;

        // use exponential backoff in case of concurrent commits
        for (long backOff = 1; backOff < maximumBackOff; backOff *= 2) {
            long start = System.nanoTime();

            refreshHead();

            SegmentNodeState state = head.get();

            if (!state.hasProperty("token") || state.getLong("timeout") < currentTimeMillis()) {
                SegmentNodeBuilder builder = prepare(state, changes, hook, info);
                // use optimistic locking to update the journal
                if (setHead(state, builder.getNodeState())) {
                    return -1;
                }
            }

            // someone else was faster, so wait a while and retry later
            Thread.sleep(backOff, random.nextInt(1000000));

            long stop = System.nanoTime();
            if (stop - start > timeout) {
                timeout = stop - start;
            }
        }

        return MILLISECONDS.convert(timeout, NANOSECONDS);
    }

    private void pessimisticMerge(SegmentNodeBuilder changes, CommitHook hook, CommitInfo info, long timeout) throws CommitFailedException, InterruptedException {
        while (true) {
            long now = currentTimeMillis();

            SegmentNodeState state = head.get();

            if (state.hasProperty("token") && state.getLong("timeout") >= now) {
                // locked by someone else, wait until unlocked or expired
                Thread.sleep(Math.min(state.getLong("timeout") - now, 1000), random.nextInt(1000000));
            } else {
                // attempt to acquire the lock
                SegmentNodeBuilder builder = state.builder();
                builder.setProperty("token", UUID.randomUUID().toString());
                builder.setProperty("timeout", now + timeout);

                if (setHead(state, builder.getNodeState())) {
                    // lock acquired; rebase, apply commit hooks, and unlock
                    builder = prepare(state, changes, hook, info);
                    builder.removeProperty("token");
                    builder.removeProperty("timeout");

                    // complete the commit
                    if (setHead(state, builder.getNodeState())) {
                        return;
                    }
                }
            }
        }
    }

    private void refreshHead() {
        SegmentNodeState state = store.getHead();

        if (!state.getRecordId().equals(head.get().getRecordId())) {
            head.set(state);
        }
    }

    private SegmentNodeBuilder prepare(SegmentNodeState state, SegmentNodeBuilder changes, CommitHook hook, CommitInfo info) throws CommitFailedException {
        SegmentNodeBuilder builder = state.builder();

        if (fastEquals(changes.getBaseState(), state.getChildNode(ROOT))) {
            // use a shortcut when there are no external changes
            NodeState baseRoot = changes.getBaseState();
            NodeState headRoot = changes.getNodeState();
            NodeState updatedRoot = hook.processCommit(baseRoot, headRoot, info);
            builder.setChildNode(ROOT, updatedRoot);
        } else {
            // there were some external changes, so do the full rebase
            NodeStateDiff diff = new ConflictAnnotatingRebaseDiff(builder.child(ROOT));
            changes.getNodeState().compareAgainstBaseState(changes.getBaseState(), diff);
            // apply commit hooks on the rebased changes
            NodeState baseRoot = builder.getBaseState().getChildNode(ROOT);
            NodeState headRoot = builder.getNodeState().getChildNode(ROOT);
            NodeState updatedRoot = hook.processCommit(baseRoot, headRoot, info);
            builder.setChildNode(ROOT, updatedRoot);
        }

        return builder;
    }

    private boolean setHead(SegmentNodeState before, SegmentNodeState after) {
        refreshHead();

        if (store.setHead(before, after)) {
            head.set(after);
            refreshHead();
            return true;
        }

        return false;
    }

}
