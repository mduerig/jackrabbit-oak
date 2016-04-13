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
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.segment.Record.fastEquals;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.plugins.segment.scheduler.Scheduler;
import org.apache.jackrabbit.oak.plugins.segment.scheduler.SchedulerOptions;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * The top level class for the segment store.
 * <p>
 * The root node of the JCR content tree is actually stored in the node "/root", and checkpoints are stored under
 * "/checkpoints".
 */
public class SegmentNodeStore implements NodeStore, Observable {

    static final String ROOT = "root";

    public static final String CHECKPOINTS = "checkpoints";

    private final SegmentStore store;

    private final ChangeDispatcher changeDispatcher;

    /**
     * Local copy of the head of the journal associated with this store.
     */
    private final AtomicReference<SegmentNodeState> head;

    private final Scheduler<SchedulerOptions> scheduler;

    @Nonnull
    public static SegmentNodeStoreBuilder newSegmentNodeStore(@Nonnull SegmentStore store) {
        return SegmentNodeStoreBuilder.newSegmentNodeStore(checkNotNull(store));
    }

    /**
     * @deprecated Use {@link SegmentNodeStore#newSegmentNodeStore(SegmentStore)} instead
     */
    @Deprecated
    public SegmentNodeStore(SegmentStore store) {
        this(store, false);
    }

    SegmentNodeStore(SegmentStore store, boolean internal) {
        this.store = store;
        this.head = new AtomicReference<SegmentNodeState>(store.getHead());
        this.changeDispatcher = new ChangeDispatcher(getRoot());
        this.scheduler = new RetryScheduler(store);
    }

    public SegmentNodeStore() throws IOException {
        this(new MemoryStore());
    }

    void setMaximumBackoff(long max) {
        // TODO - the back-off is now an implementation detail of the scheduler
        throw new UnsupportedOperationException("invalid");
    }

    /**
     * Execute the passed callable with trying to acquire this store's commit lock.
     *
     * @param c callable to execute
     * @return {@code false} if the store's commit lock cannot be acquired, the result of {@code c.call()} otherwise.
     * @throws Exception
     */
    boolean locked(Callable<Boolean> c) throws Exception {
        // TODO - locking and commit synchronization is now an implementation detail of the scheduler
        throw new UnsupportedOperationException("invalid");
    }

    /**
     * Execute the passed callable with trying to acquire this store's commit lock.
     *
     * @param timeout the maximum time to wait for the store's commit lock
     * @param unit    the time unit of the {@code timeout} argument
     * @param c       callable to execute
     * @return {@code false} if the store's commit lock cannot be acquired, the result of {@code c.call()} otherwise.
     * @throws Exception
     */
    boolean locked(Callable<Boolean> c, long timeout, TimeUnit unit) throws Exception {
        // TODO - locking and commit synchronization is now an implementation detail of the scheduler
        throw new UnsupportedOperationException("invalid");
    }

    /**
     * Refreshes the head state. Should only be called while holding a permit from the {@link #commitSemaphore}.
     */
    private SegmentNodeState refreshHead() {
        SegmentNodeState state = store.getHead();

        if (!state.getRecordId().equals(head.get().getRecordId())) {
            head.set(state);
            changeDispatcher.contentChanged(state.getChildNode(ROOT), null);
        }

        return state;
    }

    @Override
    public Closeable addObserver(Observer observer) {
        return changeDispatcher.addObserver(observer);
    }

    @Override
    @Nonnull
    public NodeState getRoot() {
        return getSuperRoot().getChildNode(ROOT);
    }

    @Nonnull
    public NodeState getSuperRoot() {
        return refreshHead();
    }

    @Override
    public NodeState merge(@Nonnull NodeBuilder builder, @Nonnull CommitHook commitHook, @Nonnull CommitInfo info) throws CommitFailedException {
        return scheduler.schedule(builder, commitHook, info, null);
    }

    @Override
    @Nonnull
    public NodeState rebase(@Nonnull NodeBuilder builder) {
        checkArgument(builder instanceof SegmentNodeBuilder);

        SegmentNodeBuilder snb = (SegmentNodeBuilder) builder;

        NodeState root = getRoot();
        NodeState before = snb.getBaseState();
        if (!fastEquals(before, root)) {
            SegmentNodeState after = snb.getNodeState();
            snb.reset(root);
            after.compareAgainstBaseState(
                    before, new ConflictAnnotatingRebaseDiff(snb));
        }

        return snb.getNodeState();
    }

    @Override
    @Nonnull
    public NodeState reset(@Nonnull NodeBuilder builder) {
        checkArgument(builder instanceof SegmentNodeBuilder);

        SegmentNodeBuilder snb = (SegmentNodeBuilder) builder;

        NodeState root = getRoot();
        snb.reset(root);

        return root;
    }

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return store.getTracker().getWriter().writeStream(stream);
    }

    @Override
    public Blob getBlob(@Nonnull String reference) {
        //Use of 'reference' here is bit overloaded. In terms of NodeStore API
        //a blob reference refers to the secure reference obtained from Blob#getReference()
        //However in SegmentStore terminology a blob is referred via 'external reference'
        //That 'external reference' would map to blobId obtained from BlobStore#getBlobId
        BlobStore blobStore = store.getBlobStore();
        if (blobStore != null) {
            String blobId = blobStore.getBlobId(reference);
            if (blobId != null) {
                return store.readBlob(blobId);
            }
            return null;
        }
        throw new IllegalStateException("Attempt to read external blob with blobId [" + reference + "] " +
                "without specifying BlobStore");
    }

    @Nonnull
    @Override
    public String checkpoint(long lifetime, @Nonnull Map<String, String> properties) {
        return scheduler.addCheckpoint(lifetime, properties);
    }

    @Override
    @Nonnull
    public String checkpoint(long lifetime) {
        return checkpoint(lifetime, Collections.<String, String>emptyMap());
    }

    @Nonnull
    @Override
    public Map<String, String> checkpointInfo(@Nonnull String checkpoint) {
        Map<String, String> properties = newHashMap();
        checkNotNull(checkpoint);
        NodeState cp = head.get()
                .getChildNode("checkpoints")
                .getChildNode(checkpoint)
                .getChildNode("properties");

        for (PropertyState prop : cp.getProperties()) {
            properties.put(prop.getName(), prop.getValue(STRING));
        }

        return properties;
    }

    @Override
    @CheckForNull
    public NodeState retrieve(@Nonnull String checkpoint) {
        checkNotNull(checkpoint);
        NodeState cp = head.get()
                .getChildNode("checkpoints")
                .getChildNode(checkpoint)
                .getChildNode(ROOT);
        if (cp.exists()) {
            return cp;
        }
        return null;
    }

    @Override
    public boolean release(@Nonnull String checkpoint) {
        return scheduler.removeCheckpoint(checkpoint);
    }

    NodeState getCheckpoints() {
        return head.get().getChildNode(CHECKPOINTS);
    }

    /**
     * Sets the number of seconds to wait for the attempt to grab the lock to create a checkpoint
     */
    void setCheckpointsLockWaitTime(int checkpointsLockWaitTime) {
        // TODO - synchronization of checkpoint creation is now an implementation detail of the scheduler
        throw new UnsupportedOperationException("invalid");
    }

}
