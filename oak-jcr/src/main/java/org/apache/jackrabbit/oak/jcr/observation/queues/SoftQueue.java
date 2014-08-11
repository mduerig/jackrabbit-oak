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

import java.lang.ref.SoftReference;
import java.util.LinkedList;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.observation.Event;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A memory sensitive {@code Queue} implementation with a fixed capacity.
 * <p>
 * Once the number of elements in this queue reaches the capacity
 * of the queue adding further elements will cause the queue
 * to become full. In addition once there is not enough memory to retain this
 * queue on the heap, it will become full.
 */
public class SoftQueue implements Queue {
    private static final Logger LOG = LoggerFactory.getLogger(SoftQueue.class);

    private final SoftReference<LinkedList<Event>> queue =
            new SoftReference<LinkedList<Event>>(Lists.<Event>newLinkedList());

    private final long capacity;

    /**
     * Create a new instance with the given capacity
     * @param capacity
     */
    public SoftQueue(long capacity) {
        this.capacity = capacity;
    }

    @Override
    public boolean add(@Nonnull Event event) {
        LinkedList<Event> q = getQueue();
        if (q != null && q.size() < capacity) {
            q.add(checkNotNull(event));
            return true;
        }
        return false;
    }

    @Override
    @CheckForNull
    public Reader getReader() {
        LinkedList<Event> q = getQueue();
        return q != null
            ? new LinkedListReader(q)
            : null;
    }

    private LinkedList<Event> getQueue() {
        LinkedList<Event> q = queue.get();
        if (q == null) {
            LOG.debug("Discarding event queue due to memory pressure. Free memory: {}",
                    Runtime.getRuntime().freeMemory());
        }
        return q;

    }

}
