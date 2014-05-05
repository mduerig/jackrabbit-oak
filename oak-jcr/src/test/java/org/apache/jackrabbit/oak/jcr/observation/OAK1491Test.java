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

import static com.google.common.base.Objects.equal;
import static java.util.Collections.synchronizedList;
import static java.util.Collections.synchronizedSet;
import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_MOVED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PERSIST;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_CHANGED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.ObservationManager;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.jcr.NodeStoreFixture;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAK1491Test extends AbstractRepositoryTest {
    private static final Logger log = LoggerFactory.getLogger(OAK1491Test.class);

    public static final int ALL_EVENTS = NODE_ADDED | NODE_REMOVED | NODE_MOVED | PROPERTY_ADDED |
            PROPERTY_REMOVED | PROPERTY_CHANGED | PERSIST;
    private static final String TEST_NODE = "test_node";
    private static final String TEST_PATH = '/' + TEST_NODE;
    public static final int TIME_OUT = 4;

    private Session observingSession;
    private ObservationManager observationManager;

    public OAK1491Test(NodeStoreFixture fixture) {
        super(fixture);
    }

    @BeforeClass
    public static void beforeClass() {
        log.info("Start running {} tests", OAK1491Test.class);
    }

    @AfterClass
    public static void afterClass() {
        log.info("Done running {} tests", OAK1491Test.class);
    }

    @Before
    public void setup() throws RepositoryException {
        Session session = getAdminSession();
        Node n = session.getRootNode().addNode(TEST_NODE);
        session.save();

        observingSession = createAdminSession();
        observationManager = observingSession.getWorkspace().getObservationManager();
    }

    @After
    public void tearDown() {
        observingSession.logout();
    }


    @Test
    public void observation() throws RepositoryException, InterruptedException, ExecutionException {
        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null, null, false);
        try {
            Node n = getNode(TEST_PATH);
            listener.expectAdd(n.addNode("n1"));
            getAdminSession().save();

            Stopwatch watch = Stopwatch.createStarted();
            List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            System.out.println(watch.stop());

            assertTrue("Missing events: " + missing, missing.isEmpty());
            List<Event> unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());

            listener.expectAdd(n.addNode("n2"));
            listener.expectRemove(n.getNode("n1")).remove();
            getAdminSession().save();

            watch = Stopwatch.createStarted();
            missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            System.out.println(watch.stop());

            assertTrue("Missing events: " + missing, missing.isEmpty());
            unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        }
        finally {
            observationManager.removeEventListener(listener);
        }
    }

    //------------------------------------------------------------< private >---

    private Node getNode(String path) throws RepositoryException {
        return getAdminSession().getNode(path);
    }

    //------------------------------------------------------------< ExpectationListener >---

    private static class Expectation extends ForwardingListenableFuture<Event> {
        private final SettableFuture<Event> future = SettableFuture.create();
        private final String name;

        private volatile boolean enabled = true;

        Expectation(String name, boolean enabled) {
            this.name = name;
            this.enabled = enabled;
        }

        Expectation(String name) {
            this(name, true);
        }

        @Override
        protected ListenableFuture<Event> delegate() {
            return future;
        }

        public void enable(boolean enabled) {
            this.enabled = enabled;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void complete(Event event) {
            future.set(event);
        }

        public void fail(Exception e) {
            future.setException(e);
        }

        public boolean wait(long timeout, TimeUnit unit) {
            try {
                future.get(timeout, unit);
                return true;
            }
            catch (Exception e) {
                return false;
            }
        }

        public boolean onEvent(Event event) throws Exception {
            return true;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static class ExpectationListener implements EventListener {
        private final Set<Expectation> expected = synchronizedSet(
                Sets.<Expectation>newCopyOnWriteArraySet());
        private final List<Event> unexpected = synchronizedList(
                Lists.<Event>newCopyOnWriteArrayList());

        private volatile Exception failed;

        public Expectation expect(Expectation expectation) {
            if (failed != null) {
                expectation.fail(failed);
            }
            expected.add(expectation);
            return expectation;
        }

        public Future<Event> expect(final String path, final int type) {
            return expect(new Expectation("path = " + path + ", type = " + type) {
                @Override
                public boolean onEvent(Event event) throws RepositoryException {
                    return type == event.getType() && equal(path, event.getPath());
                }
            });
        }

        public Node expectAdd(Node node) throws RepositoryException {
            expect(node.getPath(), NODE_ADDED);
            expect(node.getPath() + "/jcr:primaryType", PROPERTY_ADDED);
            return node;
        }

        public Node expectRemove(Node node) throws RepositoryException {
            expect(node.getPath(), NODE_REMOVED);
            expect(node.getPath() + "/jcr:primaryType", PROPERTY_REMOVED);
            return node;
        }

        public Property expectAdd(Property property) throws RepositoryException {
            expect(property.getPath(), PROPERTY_ADDED);
            return property;
        }

        public Property expectRemove(Property property) throws RepositoryException {
            expect(property.getPath(), PROPERTY_REMOVED);
            return property;
        }

        public Property expectChange(Property property) throws RepositoryException {
            expect(property.getPath(), PROPERTY_CHANGED);
            return property;
        }

        public void expectMove(final String src, final String dst) {
            expect(new Expectation('>' + src + ':' + dst){
                @Override
                public boolean onEvent(Event event) throws Exception {
                    return event.getType() == NODE_MOVED &&
                            equal(dst, event.getPath()) &&
                            equal(src, event.getInfo().get("srcAbsPath")) &&
                            equal(dst, event.getInfo().get("destAbsPath"));
                }
            });
        }

        public List<Expectation> getMissing(int time, TimeUnit timeUnit)
                throws ExecutionException, InterruptedException {
            List<Expectation> missing = Lists.newArrayList();
            long t0 = System.nanoTime();
            try {
                Futures.allAsList(expected).get(time, timeUnit);
            }
            catch (TimeoutException e) {
                long dt = System.nanoTime() - t0;
                // TODO remove again once OAK-1491 is fixed
                assertTrue("Spurious wak-up after " + dt,
                        dt > 0.8*TimeUnit.NANOSECONDS.convert(time, timeUnit));
                for (Expectation exp : expected) {
                    if (!exp.isDone()) {
                        missing.add(exp);
                    }
                }
            }
            return missing;
        }

        public List<Event> getUnexpected() {
            return Lists.newArrayList(unexpected);
        }

        @Override
        public void onEvent(EventIterator events) {
            try {
                while (events.hasNext() && failed == null) {
                    Event event = events.nextEvent();
                    boolean found = false;
                    for (Expectation exp : expected) {
                        if (exp.isEnabled() && exp.onEvent(event)) {
                            found = true;
                            exp.complete(event);
                        }
                    }
                    if (!found) {
                        unexpected.add(event);
                    }

                }
            } catch (Exception e) {
                for (Expectation exp : expected) {
                    exp.fail(e);
                }
                failed = e;
            }
        }

        private static String key(String path, int type) {
            return path + ':' + type;
        }
    }

}
