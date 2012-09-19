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
package org.apache.jackrabbit.oak.core;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class TreeDecoratorTest extends AbstractCoreTest {

    @Override
    protected NodeState createInitialState(MicroKernel microKernel) {
        try {
            RootImpl root = createRootImpl(null);
            Tree tree = root.getTree("/");
            tree.addChild("a").addChild("b").addChild("1");
            tree.addChild("1").addChild("2");
            root.commit(DefaultConflictHandler.OURS);
            return store.getRoot();
        }
        catch (CommitFailedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void testRead() {
        RootImpl root = createRootImpl(null);
        Tree tree = root.getTree("/");
        Tree mount = tree.getChild("1");
        Tree mounted = TreeDecorator.mount(tree, mount, "a/b/1");

        Tree a = mounted.getChild("a");
        System.out.println(a.getPath());

        Tree b = a.getChild("b");
        System.out.println(b.getPath());

        Tree one = b.getChild("1");
        System.out.println(one.getPath());

        Tree two = one.getChild("2");
        System.out.println(two.getPath());

        one = two.getParent();
        System.out.println(one.getPath());

        b = one.getParent();
        System.out.println(b.getPath());

        a = b.getParent();
        System.out.println(a.getPath());

        Tree r = a.getParent();
        System.out.println(r.getPath());
    }

    @Test
    public void traverseAndAdd() {
        RootImpl root = createRootImpl(null);
        Tree tree = root.getTree("/");
        Tree mount = tree.getChild("1");
        Tree mounted = TreeDecorator.mount(tree, mount, "a/b/1");

        Tree a = mounted.getChild("a");
        traverse(a);
        traverseAndAdd(a, "m");
        traverse(a);
        traverseAndAdd(a, "n");
        traverse(a);

        traverse(tree);
    }

    private static void traverseAndAdd(Tree tree, String name) {
        for (Tree child : tree.getChildren()) {
            traverseAndAdd(child, name);
        }
        tree.addChild(name);
    }

    private static void traverse(Tree tree) {
        System.out.println(tree.getPath());
        for (Tree child : tree.getChildren()) {
            traverse(child);
        }
    }
}
