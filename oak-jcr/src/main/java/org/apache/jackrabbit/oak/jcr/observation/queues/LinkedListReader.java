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

import static java.lang.Math.min;

import java.util.LinkedList;

import javax.annotation.Nonnull;
import javax.jcr.observation.Event;

import org.apache.jackrabbit.oak.jcr.observation.queues.Queue.Reader;

/**
 * A {@code Reader} implementation base on a linked list.
 */
class LinkedListReader implements Reader {
    private final LinkedList<Event> queue;

    /**
     * Create a new instance based on the passed linked list.
     * @param queue
     */
    public LinkedListReader(LinkedList<Event> queue) {
        this.queue = queue;
    }

    @Override
    public long size() {
        return queue.size();
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public void clear() {
        queue.clear();
    }

    @Override
    @Nonnull
    public Event removeFirst() {
        return queue.removeFirst();
    }

    @Override
    public void removeFirst(long num) {
        queue.subList(0, (int) min(num, Integer.MAX_VALUE)).clear();
    }
}
