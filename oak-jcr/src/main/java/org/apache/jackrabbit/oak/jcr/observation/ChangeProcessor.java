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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.OBSERVATION_EVENT_COUNTER;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.OBSERVATION_EVENT_DURATION;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.observation.filter.VisibleFilter.VISIBLE_FILTER;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerObserver;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.scheduleWithFixedDelay;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;

import com.google.common.util.concurrent.Monitor;
import com.google.common.util.concurrent.Monitor.Guard;
import org.apache.jackrabbit.api.jmx.EventListenerMBean;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.jcr.observation.temp.ListenerTracker;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.observation.CommitRateLimiter;
import org.apache.jackrabbit.oak.plugins.observation.CompositeHandler;
import org.apache.jackrabbit.oak.plugins.observation.EventGenerator;
import org.apache.jackrabbit.oak.plugins.observation.EventHandler;
import org.apache.jackrabbit.oak.plugins.observation.FilteredHandler;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventFilter;
import org.apache.jackrabbit.oak.plugins.observation.filter.FilterProvider;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.whiteboard.CompositeRegistration;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.apache.jackrabbit.oak.stats.StatisticManager;
import org.apache.jackrabbit.stats.TimeSeriesMax;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code ChangeProcessor} generates observation {@link javax.jcr.observation.Event}s
 * based on a {@link FilterProvider filter} and delivers them to the registered
 * {@link EventListener}.
 * <p>
 * After instantiation a {@code ChangeProcessor} must be started in order to start
 * delivering observation events and stopped to stop doing so.
 */
class ChangeProcessor implements Observer {
    private static final Logger LOG = LoggerFactory.getLogger(ChangeProcessor.class);

    /**
     * Fill ratio of the revision queue at which commits should be delayed
     * (conditional of {@code commitRateLimiter} being non {@code null}).
     */
    public static final double DELAY_THRESHOLD = 0.8;

    /**
     * Maximal number of milli seconds a commit is delayed once {@code DELAY_THRESHOLD}
     * kicks in.
     */
    public static final int MAX_DELAY = 10000;

    private final Set<Listener> listeners = newConcurrentHashSet();
    private final Whiteboard whiteboard;
    private final String sessionId;
    private final NamePathMapper namePathMapper;
    private final AtomicLong eventCount;
    private final AtomicLong eventDuration;
    private final TimeSeriesMax maxQueueLength;
    private final int queueLength;
    private final CommitRateLimiter commitRateLimiter;

    private CompositeRegistration registration;
    private BackgroundObserver observer;
    private volatile NodeState previousRoot;

    /**
     * {@code ListenerRegistration} instances represent listeners
     * registered with a {@code ChangeProcessor}.
     */
    public interface ListenerRegistration extends Registration {

        /**
         * Atomically change the filter condition for the
         * registered listener.
         * @param filterProvider  filter condition
         */
        void setFilterProvider(FilterProvider filterProvider);
    }

    public ChangeProcessor(
            Whiteboard whiteboard,
            ContentSession contentSession,
            NamePathMapper namePathMapper,
            StatisticManager statisticManager,
            int queueLength,
            CommitRateLimiter commitRateLimiter) {
        this.whiteboard = whiteboard;
        this.sessionId = contentSession.toString();
        this.namePathMapper = namePathMapper;
        this.eventCount = statisticManager.getCounter(OBSERVATION_EVENT_COUNTER);
        this.eventDuration = statisticManager.getCounter(OBSERVATION_EVENT_DURATION);
        this.maxQueueLength = statisticManager.maxQueLengthRecorder();
        this.queueLength = queueLength;
        this.commitRateLimiter = commitRateLimiter;
    }

    /**
     * Start this change processor
     * @throws IllegalStateException if started already
     */
    public synchronized void start() {
        checkState(registration == null, "Change processor started already");
        final WhiteboardExecutor executor = new WhiteboardExecutor();
        executor.start(whiteboard);
        observer = createObserver(executor);
        registration = new CompositeRegistration(
            registerObserver(whiteboard, observer),
            new Registration() {
                @Override
                public void unregister() {
                    observer.close();
                }
            },
            new Registration() {
                @Override
                public void unregister() {
                    executor.stop();
                }
            },
            scheduleWithFixedDelay(whiteboard, new Runnable() {
                @Override
                public void run() {
                    tracker.recordOneSecond();
                }
            }, 1)
        );
    }

    private BackgroundObserver createObserver(final WhiteboardExecutor executor) {
        return new BackgroundObserver(this, executor, queueLength) {
            private volatile long delay;
            private volatile boolean blocking;

            @Override
            protected void added(int queueSize) {
                maxQueueLength.recordValue(queueSize);
                tracker.recordQueueLength(queueSize);

                if (queueSize == queueLength) {
                    if (commitRateLimiter != null) {
                        if (!blocking) {
                            LOG.warn("Revision queue is full. Further commits will be blocked.");
                        }
                        commitRateLimiter.blockCommits();
                    } else if (!blocking) {
                        LOG.warn("Revision queue is full. Further revisions will be compacted.");
                    }
                    blocking = true;
                } else {
                    double fillRatio = (double) queueSize / queueLength;
                    if (fillRatio > DELAY_THRESHOLD) {
                        if (commitRateLimiter != null) {
                            if (delay == 0) {
                                LOG.warn("Revision queue is becoming full. Further commits will be delayed.");
                            }

                            // Linear backoff proportional to the number of items exceeding
                            // DELAY_THRESHOLD. Offset by 1 to trigger the log message in the
                            // else branch once the queue falls below DELAY_THRESHOLD again.
                            int newDelay = 1 + (int) ((fillRatio - DELAY_THRESHOLD) / (1 - DELAY_THRESHOLD) * MAX_DELAY);
                            if (newDelay > delay) {
                                delay = newDelay;
                                commitRateLimiter.setDelay(delay);
                            }
                        }
                    } else {
                        if (commitRateLimiter != null) {
                            if (delay > 0) {
                                LOG.debug("Revision queue becoming empty. Unblocking commits");
                                commitRateLimiter.setDelay(0);
                                delay = 0;
                            }
                            if (blocking) {
                                LOG.debug("Revision queue becoming empty. Stop delaying commits.");
                                commitRateLimiter.unblockCommits();
                                blocking = false;
                            }
                        }
                    }
                }
            }
        };
    }

    /**
     * Add a listener to this change processor
     * @param tracker  the listener
     * @param filterProvider  the filter for the listener
     * @return  a listener registration instance
     */
    public ListenerRegistration addListener(ListenerTracker tracker, FilterProvider filterProvider) {
        final Listener listener = new Listener(whiteboard, tracker, filterProvider);
        observer.addCallback(new Runnable() {
            // Make sure the listener is only enabled once we reach the "current revision"
            // as otherwise we might receive events from the past.
            @Override
            public void run() {
                listener.setEnabled(true);
            }
        });
        return listener;
    }

    private final Monitor runningMonitor = new Monitor();
    private final RunningGuard running = new RunningGuard(runningMonitor);

    /**
     * Try to stop this change processor if running. This method will wait
     * the specified time for a pending event listener to complete. If
     * no timeout occurred no further events will be delivered after this
     * method returns.
     * <p>
     * Does nothing if stopped already.
     *
     * @param timeOut time this method will wait for an executing event
     *                listener to complete.
     * @param unit    time unit for {@code timeOut}
     * @return {@code true} if no time out occurred and this change processor
     *         could be stopped, {@code false} otherwise.
     * @throws IllegalStateException if not yet started
     */
    public synchronized boolean stopAndWait(int timeOut, TimeUnit unit) {
        checkState(registration != null, "Change processor not started");
        if (running.stop()) {
            if (runningMonitor.enter(timeOut, unit)) {
                unregisterAll();
                runningMonitor.leave();
                return true;
            } else {
                // Timed out
                return false;
            }
        } else {
            // Stopped already
            return true;
        }
    }

    /**
     * Stop this change processor after all pending events have been
     * delivered. In contrast to {@link #stopAndWait(int, java.util.concurrent.TimeUnit)}
     * this method returns immediately without waiting for pending listeners to
     * complete.
     */
    public synchronized void stop() {
        checkState(registration != null, "Change processor not started");
        if (running.stop()) {
            unregisterAll();
            runningMonitor.leave();
        }
    }

    private void unregisterAll() {
        registration.unregister();
        for (Listener listener : listeners) {
            listener.unregister();
        }
    }

    @Override
    public void contentChanged(@Nonnull NodeState root, @Nullable CommitInfo info) {
        if (previousRoot != null) {
            EventFactory eventFactory = new EventFactory(namePathMapper, info);
            Set<EventDispatcher> dispatchers = newHashSet();
            for (Listener listener : listeners) {
                EventDispatcher dispatcher = listener.createDispatcher(root, info, eventFactory);
                if (dispatcher != null) {
                    dispatchers.add(dispatcher);
                }
            }

            dispatch(root, dispatchers);
        }
        previousRoot = root;
    }

    private void dispatch(NodeState root, Set<EventDispatcher> dispatchers) {
        // Create a composite event handler for all dispatchers and use a single generator
        // for generating the events. This causes the events for the first dispatcher
        // to be generated as the client iterates through the passed node iterator. The
        // events for all other dispatchers will be cached while piggy backing on the
        // associated of the first dispatcher.
        if (!dispatchers.isEmpty()) {
            EventGenerator generator = createGenerator(root, dispatchers);
            for (EventDispatcher dispatcher : newHashSet(dispatchers)) {
                try {
                    if (dispatcher.dispatchEvents(generator)) {
                        dispatchers.remove(dispatcher);
                    }
                } catch (Exception e) {
                    LOG.warn("Error while dispatching observation events to " + dispatcher, e);
                }
            }
        }

        // Split the set of dispatcher for which dispatching failed
        // into two equally sized sets and retry.
        if (!dispatchers.isEmpty()) {
            Set<EventDispatcher> d1 = newHashSet();
            Set<EventDispatcher> d2 = newHashSet();
            for (EventDispatcher dispatcher : dispatchers) {
                if (d1.size() * 2 < dispatchers.size()) {
                    d1.add(dispatcher);
                } else {
                    d2.add(dispatcher);
                }
            }
            dispatch(root, d1);
            dispatch(root, d2);
        }
    }

    private EventGenerator createGenerator(NodeState root, Iterable<EventDispatcher> dispatchers) {
        // Note: Making EventDispatcher implement EventHandler and direct composition would be
        // cleaner here but but it seems prohibitively expensive given all the extra instances
        // necessary.
        ArrayList<EventHandler> handlers = newArrayList();
        Set<String> subTrees = newHashSet();
        for (EventDispatcher dispatcher : dispatchers) {
            handlers.add(dispatcher.createEventHandler());
            addAll(subTrees, dispatcher.subTrees);
        }
        NodeState before = preFilter(previousRoot, subTrees);
        NodeState after = preFilter(root, subTrees);
        EventHandler handler = new FilteredHandler(VISIBLE_FILTER, CompositeHandler.create(handlers));
        return new EventGenerator(before, after, handler);
    }

    /**
     * Create a synthetic tree only consisting of the subtrees at the
     * specified paths.
     */
    private static NodeState preFilter(NodeState root, Set<String> paths) {
        if (paths.contains("/")) {
            return root;
        }

        NodeBuilder rootBuilder = EMPTY_NODE.builder();
        for (String path : paths) {
            NodeState child = root;
            NodeBuilder builder = rootBuilder;
            for (String name : elements(getParentPath(path))) {
                child = child.getChildNode(name);
                builder = builder.child(name);
            }
            String name = getName(path);
            builder.setChildNode(name, child.getChildNode(name));
        }
        return rootBuilder.getNodeState();
    }

    private static class RunningGuard extends Guard {
        private boolean stopped;

        public RunningGuard(Monitor monitor) {
            super(monitor);
        }

        @Override
        public boolean isSatisfied() {
            return !stopped;
        }

        /**
         * @return  {@code true} if this call set this guard to stopped,
         *          {@code false} if another call set this guard to stopped before.
         */
        public boolean stop() {
            boolean wasStopped = stopped;
            stopped = true;
            return !wasStopped;
        }
    }

    private class Listener implements ListenerRegistration {
        private final EventListener eventListener;
        private final AtomicReference<FilterProvider> filterProvider;
        private final Registration mBean;

        private volatile boolean enabled;

        public Listener(Whiteboard whiteboard, ListenerTracker tracker, FilterProvider filterProvider) {
            this.eventListener = tracker.getTrackedListener();
            this.filterProvider = new AtomicReference<FilterProvider>(filterProvider);
            mBean = registerMBean(whiteboard, EventListenerMBean.class,
                    tracker.getListenerMBean(), "EventListener", tracker.toString());
            listeners.add(this);
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        /**
         * Create an event dispatcher for this listener using its current
         * filter conditions.
         *
         * @param root    root node state whose changes to dispatch
         * @param info    commit info associate with the changes
         * @param eventFactory
         * @return  a new event dispatcher
         */
        public EventDispatcher createDispatcher(NodeState root, CommitInfo info,
                EventFactory eventFactory) {
            FilterProvider provider = filterProvider.get();
            if (enabled && provider.includeCommit(sessionId, info)) {
                EventFilter filter = provider.getFilter(previousRoot, root);
                return new EventDispatcher(provider.getSubTrees(), root, filter, eventFactory, eventListener);
            } else {
                return null;
            }
        }

        @Override
        public void setFilterProvider(FilterProvider filterProvider) {
            this.filterProvider.set(filterProvider);
        }

        @Override
        public void unregister() {
            mBean.unregister();
            listeners.remove(this);
        }
    }

    private class EventDispatcher {
        private final EventQueue queue = new EventQueue();
        private final Iterable<String> subTrees;
        private final NodeState root;
        private final EventHandler handler;
        private final EventListener listener;

        private int attempts;

        public EventDispatcher(Iterable<String> subTrees, NodeState root, EventFilter filter,
                EventFactory eventFactory, EventListener listener) {
            this.subTrees = subTrees;
            this.root = root;
            this.handler = new FilteredHandler(filter,
                    new QueueingHandler(queue, eventFactory, previousRoot, root));
            this.listener = listener;
        }

        /**
         * Create a new event handler for this dispatcher.
         * @return
         */
        public EventHandler createEventHandler() {
            queue.clear();
            return handler;
        }

        /**
         * Dispatch events from the passed generator.
         * @param generator  generator to generate the events
         * @return  {@code true} on success, {@code false} otherwise.
         */
        public boolean dispatchEvents(EventGenerator generator) {
            attempts++;
            EventIterator events = queue.getEvents(generator);
            if (events == null) {
                LOG.warn("{}: event queue overflowed during piggyback diffing (attempt {}). " +
                        "Retrying...", listener, attempts);
                return false;
            }

            if (events.hasNext() && runningMonitor.enterIf(running)) {
                try {
                    CountingIterator countingEvents = new CountingIterator(events);
                    listener.onEvent(countingEvents);
                    countingEvents.updateCounters(eventCount, eventDuration);
                    LOG.debug("{}: diffing succeeded after {} attempts.", listener, attempts);
                } finally {
                    runningMonitor.leave();
                }
            }
            return true;
        }

        @Override
        public String toString() {
            return listener.toString();
        }
    }

    private static class CountingIterator implements EventIterator {
        private final long t0 = System.nanoTime();
        private final EventIterator events;
        private long eventCount;
        private long sysTime;

        public CountingIterator(EventIterator events) {
            this.events = events;
        }

        public void updateCounters(AtomicLong eventCount, AtomicLong eventDuration) {
            checkState(this.eventCount >= 0);
            eventCount.addAndGet(this.eventCount);
            eventDuration.addAndGet(System.nanoTime() - t0 - sysTime);
            this.eventCount = -1;
        }

        @Override
        public Event next() {
            if (eventCount == -1) {
                LOG.warn("Access to EventIterator outside the onEvent callback detected. This will " +
                        "cause observation related values in RepositoryStatistics to become unreliable.");
                eventCount = -2;
            }

            long t0 = System.nanoTime();
            try {
                return events.nextEvent();
            } finally {
                eventCount++;
                sysTime += System.nanoTime() - t0;
            }
        }

        @Override
        public boolean hasNext() {
            long t0 = System.nanoTime();
            try {
                return events.hasNext();
            } finally {
                sysTime += System.nanoTime() - t0;
            }
        }

        @Override
        public Event nextEvent() {
            return next();
        }

        @Override
        public void skip(long skipNum) {
            long t0 = System.nanoTime();
            try {
                events.skip(skipNum);
            } finally {
                sysTime += System.nanoTime() - t0;
            }
        }

        @Override
        public long getSize() {
            return events.getSize();
        }

        @Override
        public long getPosition() {
            return events.getPosition();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
