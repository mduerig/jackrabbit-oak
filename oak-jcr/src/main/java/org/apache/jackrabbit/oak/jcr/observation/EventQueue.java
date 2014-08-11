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
package org.apache.jackrabbit.oak.jcr.observation;

import static java.lang.Integer.getInteger;

import java.util.NoSuchElementException;

import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;

import org.apache.jackrabbit.oak.jcr.observation.queues.HybridQueue;
import org.apache.jackrabbit.oak.jcr.observation.queues.Queue;
import org.apache.jackrabbit.oak.jcr.observation.queues.Queue.Reader;
import org.apache.jackrabbit.oak.plugins.observation.EventGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Queue of JCR Events generated from a given content change.
 * <p>
 * This implementation delegates back to a  {@link HybridQueue} to store the
 * actual events. Once that queue becomes it is discarded to free up memory
 * and {@link #isFull()} returns {@code true}.
 */
class EventQueue {
    private static final Logger LOG = LoggerFactory.getLogger(EventQueue.class);
    private static final int SOFT_QUEUE_CAPACITY =
            getInteger("observation.soft-queue-capacity", Integer.MAX_VALUE);

    /** The backing queue */
    private Queue queue;

    /**
     * Create a new event queue.
     */
    EventQueue() {
        clear();
    }

    /**
     * @return  {@code true} if full {@code false} otherwise.
     */
    public boolean isFull() {
        return queue == null;
    }

    /**
     * Remove all elements from the queue
     */
    public final void clear() {
        this.queue = new HybridQueue(EventGenerator.MAX_CHANGES_PER_CONTINUATION, SOFT_QUEUE_CAPACITY);
    }

    /**
     * Called by the {@link QueueingHandler} to add new events to the queue.
     * Does nothing if this queue is full.
     */
    public void addEvent(Event event) {
        if (queue != null) {
            if (!queue.add(event)) {
                queue = null;
            }
        }
    }

    //-----------------------------------------------------< EventIterator >--

    private Reader getReader() {
        return queue == null ? null : this.queue.getReader();
    }

    /**
     * Create a event iterator for this queue using the passed generator
     * @param generator  the generator for generating the events for this queue
     * @return  a new event iterator or {@code null} if the queue is full.
     */
    public EventIterator getEvents(final EventGenerator generator) {
        final Reader queueReader = getReader();
        return queueReader == null ? null : new EventIterator() {
            private long position = 0;

            @Override
            public long getSize() {
                if (generator.isDone()) {
                    // no more new events will be generated, so count just those
                    // that have already been iterated and those left in the queue
                    return position + queueReader.size();
                } else {
                    // the generator is not yet done, so there's no way
                    // to know how many events may still be generated
                    return -1;
                }
            }

            @Override
            public long getPosition() {
                return position;
            }

            @Override
            public boolean hasNext() {
                while (queueReader.isEmpty()) {
                    if (generator.isDone()) {
                        return false;
                    } else {
                        generator.generate();
                    }
                }
                return true;
            }

            @Override
            public void skip(long skipNum) {
                // generate events until all events to skip have been queued
                while (skipNum > queueReader.size()) {
                    // drop all currently queued events as we're skipping them all
                    position += queueReader.size();
                    skipNum -= queueReader.size();
                    queueReader.clear();

                    // generate more events if possible, otherwise fail
                    if (!generator.isDone()) {
                        generator.generate();
                    } else {
                        throw new NoSuchElementException("Not enough events to skip");
                    }
                }

                // the remaining events to skip are guaranteed to all be in the
                // queue, so we can just drop those events and advance the position
                queueReader.removeFirst(skipNum);
                position += skipNum;
            }

            @Override
            public Event nextEvent() {
                if (hasNext()) {
                    position++;
                    return queueReader.removeFirst();
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public Event next() {
                return nextEvent();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

}
