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
package org.apache.jackrabbit.oak.kernel;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.TreeImpl;
import org.apache.jackrabbit.oak.kernel.JsopOp.Set;
import org.apache.jackrabbit.oak.plugins.commit.ConflictHandlerWrapper;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.skip;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

/**
 * {@code NodeStoreBranch} based on {@link MicroKernel} branching and merging.
 * This implementation keeps changes in memory up to a certain limit and writes
 * them back when the to the Microkernel branch when the limit is exceeded.
 */
class KernelNodeStoreBranch implements NodeStoreBranch {

    /** The underlying store to which this branch belongs */
    private final KernelNodeStore store;

    /** Base state of this branch */
    private final NodeState base;

    /** Revision of the base state of this branch */
    private final String baseRevision;

    /** Head revision of this branch, null if not yet branched */
    private String headRevision;

    /** Root state of the head revision of this branch */
    private NodeState currentRoot;

    /** Last state which was committed to this branch */
    private NodeState committed;

    KernelNodeStoreBranch(KernelNodeStore store, KernelNodeState root) {
        this.store = store;
        this.baseRevision = root.getRevision();
        this.currentRoot = root;
        this.base = root;
        this.committed = currentRoot;
    }

    @Override
    public NodeState getBase() {
        return base;
    }

    @Override
    public NodeState getRoot() {
        checkNotMerged();
        return currentRoot;
    }

    @Override
    public void setRoot(NodeState newRoot) {
        checkNotMerged();
        if (!currentRoot.equals(newRoot)) {
            currentRoot = newRoot;
            JsopDiff diff = new JsopDiff(store.getKernel());
            currentRoot.compareAgainstBaseState(committed, diff);
            commit(diff.toString());
        }
    }

    @Override
    public boolean move(String source, String target) {
        checkNotMerged();
        if (getNode(source) == null) {
            // source does not exist
            return false;
        }
        NodeState destParent = getNode(getParentPath(target));
        if (destParent == null) {
            // parent of destination does not exist
            return false;
        }
        if (destParent.getChildNode(getName(target)) != null) {
            // destination exists already
            return false;
        }

        commit(">\"" + source + "\":\"" + target + '"');
        return true;
    }

    @Override
    public boolean copy(String source, String target) {
        checkNotMerged();
        if (getNode(source) == null) {
            // source does not exist
            return false;
        }
        NodeState destParent = getNode(getParentPath(target));
        if (destParent == null) {
            // parent of destination does not exist
            return false;
        }
        if (destParent.getChildNode(getName(target)) != null) {
            // destination exists already
            return false;
        }

        commit("*\"" + source + "\":\"" + target + '"');
        return true;
    }

    @Override
    public NodeState merge() throws CommitFailedException {
        checkNotMerged();
        CommitHook commitHook = store.getHook();
        NodeState toCommit = commitHook.processCommit(base, currentRoot);
        NodeState oldRoot = currentRoot;
        setRoot(toCommit);

        try {
            if (headRevision == null) {
                // Nothing was written to this branch: return initial node state.
                headRevision = null;
                currentRoot = null;
                return committed;
            }
            else {
                MicroKernel kernel = store.getKernel();
                String mergedRevision = kernel.merge(headRevision, null);
                headRevision = null;
                currentRoot = null;
                return store.getRootState(mergedRevision);
            }
        }
        catch (MicroKernelException e) {
            setRoot(oldRoot);
            throw new CommitFailedException(e);
        }
    }

    @Override
    public NodeStoreBranch rebase(ConflictHandler conflictHandler) {
        checkNotMerged();

        NodeStoreBranch rebasedBranch = store.branch();
        if (headRevision != null) {  // Nothing has been written to this branch if null
            NodeBuilder rootBuilder = rebasedBranch.getRoot().builder();
            for (JsopOp jsopOp : getJsopOps(store.getKernel(), baseRevision, headRevision)) {
                jsopOp.apply(getBase(), rootBuilder, new ChildOrderConflictHandler(conflictHandler));
            }
            rebasedBranch.setRoot(rootBuilder.getNodeState());
        }

        return rebasedBranch;
    }

    private static Iterable<JsopOp> getJsopOps(final MicroKernel kernel, String fromRevision, String toRevision) {
        String journal = kernel.getJournal(fromRevision, toRevision, null);
        Iterable<String> changes = skip(JournalIterator.getChanges(journal), 1);  // skip fromRevision

        return Iterables.concat(Iterables.transform(changes, new Function<String, Iterable<JsopOp>>() {
            @Override
            public Iterable<JsopOp> apply(String entry) {
                return JsopIterator.getJsopOps(kernel, entry);
            }
        }));
    }

    //------------------------------------------------------------< private >---

    private void checkNotMerged() {
        checkState(currentRoot != null, "Branch has already been merged");
    }

    private NodeState getNode(String path) {
        checkArgument(path.startsWith("/"));
        NodeState node = getRoot();
        for (String name : elements(path)) {
            node = node.getChildNode(name);
            if (node == null) {
                break;
            }
        }

        return node;
    }

    //------------------------------------------------------------< ChildOrderConflictHandler >---

    private void commit(String jsop) {
        MicroKernel kernel = store.getKernel();
        if (headRevision == null) {
            // create the branch if this is the first commit
            headRevision = kernel.branch(baseRevision);
        }

        headRevision = kernel.commit("", jsop, headRevision, null);
        currentRoot = store.getRootState(headRevision);
        committed = currentRoot;
    }

    private static class ChildOrderConflictHandler extends ConflictHandlerWrapper {
        public ChildOrderConflictHandler(ConflictHandler conflictHandler) {
            super(conflictHandler);
        }

        @Override
        public void parentNotFound(Set set, NodeState base, NodeBuilder rootBuilder) {
            if (!TreeImpl.OAK_CHILD_ORDER.equals(PathUtils.getName(set.path))) {
                // Node has been removed, conflict on child order is irrelevant: ignore
                super.propertyValueConflict(set, base, rootBuilder);
            }
        }

        @Override
        public void propertyValueConflict(Set set, NodeState base, NodeBuilder rootBuilder) {
            if (!TreeImpl.OAK_CHILD_ORDER.equals(PathUtils.getName(set.path))) {
                // concurrent orderBefore(), other changes win
                super.propertyValueConflict(set, base, rootBuilder);
            }
        }

    }
}
