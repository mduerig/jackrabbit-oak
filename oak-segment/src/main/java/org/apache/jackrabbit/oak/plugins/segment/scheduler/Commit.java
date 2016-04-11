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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A {@code Commit} instance represents a set of related changes that atomically take a
 * {@link SegmentStore} instance from its current persisted state to the next persisted state.
 */
public interface Commit {

    /**
     * Apply the changes represented by this commit to the passed {@code store}.
     *
     * @param store  the {@code SegmentStore} instance to apply this commit to
     * @return       the resulting state from applying this commit
     * @throws CommitFailedException
     */
    NodeState apply(SegmentStore store) throws CommitFailedException;

}
