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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Tree;

public class TreeDecorator extends AbstractTreeDecorator {
    private final Tree mount;
    private final String mountPoint;

    private TreeDecorator(Tree tree, Tree mount, String mountPoint) {
        super(tree);
        this.mount = mount;
        this.mountPoint = mountPoint;
    }

    /**
     * Mount the tree {@code mount} onto the host tree {@code host} at
     * the path given by {@code mountPoint}.
     */
    public static Tree mount(Tree host, Tree mount, String mountPoint) {
        return host == null
            ? null
            : new TreeDecorator(host, mount, mountPoint);
    }

    @Override
    public String getPath() {
        if (isRoot()) {
            return "/";
        }
        else if (getParent().isRoot()) {
            return '/' + getName();
        }
        else {
            return getParent().getPath() + '/' + getName();
        }
    }

    @Override
    public Tree getParent() {
        String head = head(mountPoint);
        String tail = tail(mountPoint);

        if (isMountPoint()) {
            return mount(mount, getTree(), getName());
        }
        else {
            if ("..".equals(head)) {
                return mount(getTree().getParent(), mount, tail);
            }
            else {
                return mount(getTree().getParent(), mount, cons(getName(), mountPoint));
            }
        }
    }

    @Override
    public Tree getChild(String name) {
        return mountChild(getTree().getChild(name));
    }

    @Override
    public Iterable<Tree> getChildren() {
        return Iterables.transform(super.getChildren(), new Function<Tree, Tree>() {
            @Override
            public Tree apply(Tree child) {
                return mountChild(child);
            }
        });
    }

    private Tree mountChild(Tree child) {
        String head = head(mountPoint);
        String tail = tail(mountPoint);

        if (head.equals(child.getName())) {
            if (tail.isEmpty()) {
                return mount(mount, getTree(), "");
            }
            else {
                return mount(child, mount, tail);
            }
        }
        else {
            return mount(child, mount, cons("..", mountPoint));
        }
    }

    private boolean isMountPoint() {
        return mountPoint.isEmpty();
    }

    @Override
    public boolean remove() {
        return !isMountPoint() && super.remove();
    }

    private static String cons(String name, String path) {
        return path.isEmpty()
            ? name
            : name + '/' + path;
    }

    private static String head(String path) {
        int k = path.indexOf('/');
        return k == -1
            ? path
            : path.substring(0, k);
    }

    private static String tail(String path) {
        int k = path.indexOf('/');
        return k == -1
            ? ""
            : path.substring(k + 1, path.length());
    }

}
