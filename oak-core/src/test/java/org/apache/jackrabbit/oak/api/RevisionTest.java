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

package org.apache.jackrabbit.oak.api;

import org.apache.jackrabbit.oak.NodeStoreFixture;
import org.apache.jackrabbit.oak.OakBaseTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RevisionTest extends OakBaseTest {

    private ContentSession session;

    public RevisionTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setUp() {
        this.session = createContentSession();
    }

    @After
    public void tearDown() {
        this.session = null;
    }

    @Test
    public void testValidRevision() throws Exception {
        Assert.assertNotNull(session.getLatestRoot().getRevision());
    }

    @Test
    public void testValidRevisionString() throws Exception {
        Assert.assertNotNull(session.getLatestRoot().getRevision().asString());
    }

    @Test
    public void testReadRootFromRevision() throws Exception {
        Root root = session.getLatestRoot();
        root.getTree("/").addChild("test");
        root.commit();

        Revision revision = root.getRevision();

        ContentSession anotherSession = createContentSession();
        Root anotherRoot = anotherSession.getRoot(revision.asString());
        Assert.assertTrue(anotherRoot.getTree("/test").exists());
    }

    @Test
    public void testReadRootFromRevisionString() throws Exception {
        Root root = session.getLatestRoot();
        root.getTree("/").addChild("test");
        root.commit();

        String revision = root.getRevision().asString();

        ContentSession anotherSession = createContentSession();
        Root anotherRoot = anotherSession.getRoot(revision);
        Assert.assertTrue(anotherRoot.getTree("/test").exists());
    }

    public void testInvalidRevision() throws Exception {
        Assert.assertNull(session.getRoot("any"));
    }

}
