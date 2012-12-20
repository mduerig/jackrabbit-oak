/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MicroKernelImplTest {
    
    private File homeDir;
    private MicroKernelImpl mk;
    
    @Before
    public void setup() throws Exception {
        homeDir = new File("target/mk");
        if (homeDir.exists()) {
            FileUtils.cleanDirectory(homeDir);
        }
        mk = new MicroKernelImpl(homeDir.getPath());
    }
    
    @After
    public void tearDown() throws Exception {
        if (mk != null) {
            mk.dispose();
        }
    }
    
    /**
     * OAK-276: potential clash of commit id's after restart.
     */
    @Test
    public void potentialClashOfCommitIds() {
        String headRev = mk.commit("/", "+\"a\" : {}", mk.getHeadRevision(), null);
        String branchRev = mk.branch(mk.getHeadRevision());
        
        mk.dispose();
        mk = new MicroKernelImpl(homeDir.getPath());
        assertEquals("Stored head should be equal", headRev, mk.getHeadRevision());

        headRev = mk.commit("/", "+\"b\" : {}", mk.getHeadRevision(), null);
        assertFalse("Commit must not have same id as branch", headRev.equals(branchRev));
    }

    @Test
    public void rebaseWithoutChanges() {
        String branch = mk.branch(null);
        String rebased = mk.rebase(branch, null);
        assertEquals(branch, rebased);
    }

    @Test
    public void fastForwardRebase() {
        String branch = mk.branch(null);
        branch = mk.commit("", "+\"/a\":{}", branch, null);
        String rebased = mk.rebase(branch, null);
        assertEquals(branch, rebased);
    }

    @Test
    public void rebaseEmptyBranch() {
        String branch = mk.branch(null);
        String trunkRev = mk.commit("", "+\"/a\":{}", null, null);
        String rebased = mk.rebase(branch, null);

        assertEquals("{\":childNodeCount\":1,\"a\":{}}", mk.getNodes("/", rebased, 0, 0, -1, null));
        assertEquals("{\":childNodeCount\":1,\"a\":{}}", mk.getNodes("/", null, 0, 0, -1, null));
        assertEquals(trunkRev, mk.getHeadRevision());
        assertFalse((trunkRev.equals(rebased)));
    }

    @Test
    public void rebaseAddNode() {
        mk.commit("", "+\"/x\":{}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "+\"/x/b\":{}", branch, null);
        String trunkRev = mk.commit("", "+\"/x/a\":{}", null, null);
        String rebased = mk.rebase(branch, null);

        assertEquals(1, mk.getChildNodeCount("/x", null));
        assertNotNull(mk.getNodes("/x/a", null, 0, 0, -1, null));

        assertEquals(1, mk.getChildNodeCount("/x", branch));
        assertNotNull(mk.getNodes("/x/b", branch, 0, 0, -1, null));

        assertEquals(2, mk.getChildNodeCount("/x", rebased));
        assertNotNull(mk.getNodes("/x/a", rebased, 0, 0, -1, null));
        assertNotNull(mk.getNodes("/x/b", rebased, 0, 0, -1, null));
    }

    @Test
    public void rebaseRemoveNode() {
        mk.commit("", "+\"/x\":{\"y\":{}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "-\"/x/y\"", branch, null);
        String trunkRev = mk.commit("", "+\"/x/a\":{}", null, null);
        String rebased = mk.rebase(branch, null);

        assertEquals(2, mk.getChildNodeCount("/x", null));
        assertNotNull(mk.getNodes("/x/a", null, 0, 0, -1, null));
        assertNotNull(mk.getNodes("/x/y", null, 0, 0, -1, null));

        assertEquals(0, mk.getChildNodeCount("/x", branch));

        assertEquals(1, mk.getChildNodeCount("/x", rebased));
        assertNotNull(mk.getNodes("/x/a", rebased, 0, 0, -1, null));
    }

    @Test
    public void rebaseAddProperty() {
        mk.commit("", "+\"/x\":{\"y\":{}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "^\"/x/y/p\":42", branch, null);
        String trunkRev = mk.commit("", "^\"/x/y/q\":99", null, null);
        String rebased = mk.rebase(branch, null);

        String branchNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertTrue(branchNode.contains("\"p\":42"));
        assertFalse(branchNode.contains("\"q\":99"));

        String rebasedNode = mk.getNodes("/x/y", rebased, 0, 0, -1, null);
        assertTrue(rebasedNode.contains("\"p\":42"));
        assertTrue(rebasedNode.contains("\"q\":99"));

        String trunkNode = mk.getNodes("/x/y", null, 0, 0, -1, null);
        assertFalse(trunkNode.contains("\"p\":42"));
        assertTrue(trunkNode.contains("\"q\":99"));
    }

    @Test
    public void rebaseRemoveProperty() {
        mk.commit("", "+\"/x\":{\"y\":{\"p\":42}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "^\"/x/y/p\":null", branch, null);
        String trunkRev = mk.commit("", "^\"/x/y/q\":99", null, null);
        String rebased = mk.rebase(branch, null);

        String branchNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertFalse(branchNode.contains("\"p\":42"));
        assertFalse(branchNode.contains("\"q\":99"));

        String rebasedNode = mk.getNodes("/x/y", rebased, 0, 0, -1, null);
        assertFalse(rebasedNode.contains("\"p\":42"));
        assertTrue(rebasedNode.contains("\"q\":99"));

        String trunkNode = mk.getNodes("/x/y", null, 0, 0, -1, null);
        assertTrue(trunkNode.contains("\"p\":42"));
        assertTrue(trunkNode.contains("\"q\":99"));
    }

    @Test
    public void rebaseChangeProperty() {
        mk.commit("", "+\"/x\":{\"y\":{\"p\":42}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "^\"/x/y/p\":41", branch, null);
        String trunkRev = mk.commit("", "^\"/x/y/q\":99", null, null);
        String rebased = mk.rebase(branch, null);

        String branchNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertTrue(branchNode.contains("\"p\":41"));
        assertFalse(branchNode.contains("\"q\":99"));

        String rebasedNode = mk.getNodes("/x/y", rebased, 0, 0, -1, null);
        assertTrue(rebasedNode.contains("\"p\":41"));
        assertTrue(rebasedNode.contains("\"q\":99"));

        String trunkNode = mk.getNodes("/x/y", null, 0, 0, -1, null);
        assertTrue(trunkNode.contains("\"p\":42"));
        assertTrue(trunkNode.contains("\"q\":99"));
    }

    @Test
    public void rebaseChangePropertyWithSameValue() {
        mk.commit("", "+\"/x\":{\"y\":{\"p\":42}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "^\"/x/y/p\":99", branch, null);
        String trunkRev = mk.commit("", "^\"/x/y/p\":99", null, null);
        String rebased = mk.rebase(branch, null);

        String branchNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertTrue(branchNode.contains("\"p\":99"));

        String rebasedNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertTrue(rebasedNode.contains("\"p\":99"));

        String trunkNode = mk.getNodes("/x/y", null, 0, 0, -1, null);
        assertTrue(trunkNode.contains("\"p\":99"));
    }

    @Test
    public void rebaseAddNodeConflict() {
        mk.commit("", "+\"/x\":{}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "+\"/x/a\":{}", branch, null);
        String trunkRev = mk.commit("", "+\"/x/a\":{}", null, null);
        try {
            mk.rebase(branch, null);
            fail("Expected MicroKernelException for conflict at '/x/a'");
        }
        catch (MicroKernelException e) {
            assertSame(Exception.class, e.getCause().getClass());
        }

        assertEquals(1, mk.getChildNodeCount("/x", null));
        assertNotNull(mk.getNodes("/x/a", null, 0, 0, -1, null));

        assertEquals(1, mk.getChildNodeCount("/x", branch));
        assertNotNull(mk.getNodes("/x/a", branch, 0, 0, -1, null));
    }

    @Test
    public void rebaseAddPropertyConflict() {
        mk.commit("", "+\"/x\":{\"y\":{}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "^\"/x/y/p\":42", branch, null);
        String trunkRev = mk.commit("", "^\"/x/y/p\":99", null, null);
        try {
            mk.rebase(branch, null);
            fail("Expected MicroKernelException for conflict at '/x/y/p'");
        }
        catch (MicroKernelException e) {
            assertSame(Exception.class, e.getCause().getClass());
        }

        String branchNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertTrue(branchNode.contains("\"p\":42"));

        String trunkNode = mk.getNodes("/x/y", null, 0, 0, -1, null);
        assertTrue(trunkNode.contains("\"p\":99"));
    }

    @Test
    public void rebaseRemovePropertyConflict() {
        mk.commit("", "+\"/x\":{\"y\":{\"p\":42}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "^\"/x/y/p\":99", branch, null);
        String trunkRev = mk.commit("", "^\"/x/y/p\":null", null, null);
        try {
            mk.rebase(branch, null);
            fail("Expected MicroKernelException for conflict at '/x/y/p'");
        }
        catch (MicroKernelException e) {
            assertSame(Exception.class, e.getCause().getClass());
        }

        String branchNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertTrue(branchNode.contains("\"p\":99"));

        String trunkNode = mk.getNodes("/x/y", null, 0, 0, -1, null);
        assertFalse(trunkNode.contains("\"p\":42"));
        assertFalse(trunkNode.contains("\"p\":99"));
    }

    @Test
    public void rebaseRemoveChangedPropertyConflict() {
        mk.commit("", "+\"/x\":{\"y\":{\"p\":42}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "^\"/x/y/p\":null", branch, null);
        String trunkRev = mk.commit("", "^\"/x/y/p\":99", null, null);
        try {
            mk.rebase(branch, null);
            fail("Expected MicroKernelException for conflict at '/x/y/p'");
        }
        catch (MicroKernelException e) {
            assertSame(Exception.class, e.getCause().getClass());
        }

        String branchNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertFalse(branchNode.contains("\"p\":42"));
        assertFalse(branchNode.contains("\"p\":99"));

        String trunkNode = mk.getNodes("/x/y", null, 0, 0, -1, null);
        assertTrue(trunkNode.contains("\"p\":99"));
    }

    @Test
    public void rebaseChangedChangedPropertyConflict() {
        mk.commit("", "+\"/x\":{\"y\":{\"p\":42}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "^\"/x/y/p\":41", branch, null);
        String trunkRev = mk.commit("", "^\"/x/y/p\":99", null, null);
        try {
            mk.rebase(branch, null);
            fail("Expected MicroKernelException for conflict at '/x/y/p'");
        }
        catch (MicroKernelException e) {
            assertSame(Exception.class, e.getCause().getClass());
        }

        String branchNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertTrue(branchNode.contains("\"p\":41"));

        String trunkNode = mk.getNodes("/x/y", null, 0, 0, -1, null);
        assertTrue(trunkNode.contains("\"p\":99"));
    }

    @Test
    public void rebaseRemoveChangedNodeConflict() {
        mk.commit("", "+\"/x\":{\"y\":{}}", null, null);
        String branch = mk.branch(null);
        mk.getNodes("/x/y", branch, 0, 0, -1, null);
        branch = mk.commit("", "-\"/x/y\"", branch, null);
        String trunkRev = mk.commit("", "^\"/x/y/p\":42", null, null);
        try {
            mk.rebase(branch, null);
            fail("Expected MicroKernelException for conflict at '/x/y");
        }
        catch (MicroKernelException e) {
            assertSame(Exception.class, e.getCause().getClass());
        }

        assertFalse(mk.nodeExists("/x/y", branch));

        String trunkNode = mk.getNodes("/x/y", null, 0, 0, -1, null);
        assertTrue(trunkNode.contains("\"p\":42"));
    }

    @Test
    public void rebaseChangeRemovedNodeConflict() {
        mk.commit("", "+\"/x\":{\"y\":{}}", null, null);
        String branch = mk.branch(null);
        String trunkRev = mk.commit("", "-\"/x\"", null, null);
        branch = mk.commit("", "^\"/x/p\":42", branch, null);
        try {
            mk.rebase(branch, null);
            fail("Expected MicroKernelException for conflict at '/x");
        }
        catch (MicroKernelException e) {
            assertSame(Exception.class, e.getCause().getClass());
        }

        assertFalse(mk.nodeExists("/x", null));

        String branchNode = mk.getNodes("/x", branch, 0, 0, -1, null);
        assertTrue(branchNode.contains("\"p\":42"));
    }

    @Test
    public void mergeRebased() {
        mk.commit("", "+\"/x\":{\"y\":{}}", null, null);
        String branch = mk.branch(null);
        String trunkRev = mk.commit("", "^\"/x/p\":42", null, null);
        branch = mk.commit("", "^\"/x/q\":43", branch, null);
        branch = mk.rebase(branch, null);

        String branchNode = mk.getNodes("/x", branch, 0, 0, -1, null);
        assertTrue(branchNode.contains("\"p\":42"));
        assertTrue(branchNode.contains("\"q\":43"));

        mk.merge(branch, null);
        String trunkNode = mk.getNodes("/x", branch, 0, 0, -1, null);
        assertTrue(trunkNode.contains("\"p\":42"));
        assertTrue(trunkNode.contains("\"q\":43"));
    }

}
