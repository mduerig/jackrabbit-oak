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

package org.apache.jackrabbit.oak.jcr.observation.queues;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.observation.Event;

/**
 * A hybrid {@code Queue} implementation that consists of a
 * {@link FixedQueue} and a {@link SoftQueue}.
 */
public class HybridQueue implements Queue {
    private final FixedQueue fixedQueue;
    private final Queue softQueue;

    /**
     * Create a new instance with the respective capacities for the
     * fixed and soft queues.
     * @param fixedCapacity  capacity of the fixed queue
     * @param softCapacity   capacity of the soft queue
     */
    public HybridQueue(int fixedCapacity, int softCapacity) {
        this.fixedQueue = new FixedQueue(fixedCapacity);
        this.softQueue = new SoftQueue(softCapacity);
    }

    @Override
    public boolean add(@Nonnull Event event) {
        return fixedQueue.add(checkNotNull(event)) || softQueue.add(event);
    }

    @Override
    @CheckForNull
    public Reader getReader() {
        final Reader fixed = fixedQueue.getReader();
        final Reader soft = softQueue.getReader();

        return soft == null ? null : new Reader() {
            @Override
            public long size() {
                return fixed.size() + soft.size();
            }

            @Override
            public boolean isEmpty() {
                return fixed.isEmpty() && soft.isEmpty();
            }

            @Override
            public void clear() {
                fixed.clear();
                soft.clear();
            }

            @Override
            public Event removeFirst() {
                if (!fixed.isEmpty()) {
                    return fixed.removeFirst();
                } else {
                    return soft.removeFirst();
                }
            }

            @Override
            public void removeFirst(long num) {
                if (num < fixedQueue.getCapacity()) {
                    fixed.removeFirst(num);
                } else {
                    fixed.clear();
                    soft.removeFirst(num - fixedQueue.getCapacity());
                }
            }
        };
    }

}
