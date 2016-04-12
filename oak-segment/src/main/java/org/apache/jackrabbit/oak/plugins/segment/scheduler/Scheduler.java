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

package org.apache.jackrabbit.oak.plugins.segment.scheduler;

import java.util.Map;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A {@code Scheduler} instance transforms changes to the content tree
 * into a {@link ScheduledCommits backlog} of {@link Commit commits}.
 * <p>
 * An implementation is free to employ any scheduling strategy as long
 * as it guarantees all changes are applied atomically without changing
 * the semantics of the changes recorded in the {@code NodeBuilder} nor
 * the semantics of the {@code CommitHook} passed to the
 * {@link #schedule(NodeBuilder, CommitHook, CommitInfo, SchedulerOptions) schedule}
 * method.
 */
public interface Scheduler<S extends SchedulerOptions> {

    /**
     * Schedule {@code changes} for committing. This method blocks until the
     * {@code changes} have been processed and persisted. That is, until a call
     * to {@code SegmentStore.getHead} would return a node state reflecting those
     * changes.
     *
     * @param changes    changes to commit
     * @param hook       commit hook to run as part of the commit process
     * @param info       commit info pertaining to this commit
     * @param schedulingOptions       implementation specific scheduling options
     * @throws CommitFailedException  if the commit failed and none of the changes
     *                                have been applied.
     */
    NodeState schedule(NodeBuilder changes, CommitHook hook, CommitInfo info, S schedulingOptions) throws CommitFailedException;

    String addCheckpoint(long duration, Map<String, String> properties);

    boolean removeCheckpoint(String name);
}
