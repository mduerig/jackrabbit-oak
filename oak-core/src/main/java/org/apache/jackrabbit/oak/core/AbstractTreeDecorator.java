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

import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.TreeLocation;

public abstract class AbstractTreeDecorator implements Tree {
    private final Tree tree;

    protected AbstractTreeDecorator(Tree tree) {
        this.tree = tree;
    }

    protected Tree getTree(){
        return tree;
    }

    @Override
    public String getName() {
        return getTree().getName();
    }

    @Override
    public boolean isRoot() {
        return getTree().isRoot();
    }

    @Override
    public String getPath() {
        return getTree().getPath();
    }

    @Override
    public Status getStatus() {
        return getTree().getStatus();
    }

    @Override
    public TreeLocation getLocation() {
        return getTree().getLocation();
    }

    @Override
    public Tree getParent() {
        return getTree().getParent();
    }

    @Override
    public PropertyState getProperty(String name) {
        return getTree().getProperty(name);
    }

    @Override
    public Status getPropertyStatus(String name) {
        return getTree().getPropertyStatus(name);
    }

    @Override
    public boolean hasProperty(String name) {
        return getTree().hasProperty(name);
    }

    @Override
    public long getPropertyCount() {
        return getTree().getPropertyCount();
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return getTree().getProperties();
    }

    @Override
    public Tree getChild(String name) {
        return getTree().getChild(name);
    }

    @Override
    public boolean hasChild(String name) {
        return getTree().hasChild(name);
    }

    @Override
    public long getChildrenCount() {
        return getTree().getChildrenCount();
    }

    @Override
    public Iterable<Tree> getChildren() {
        return getTree().getChildren();
    }

    @Override
    public boolean remove() {
        return getTree().remove();
    }

    @Nonnull
    @Override
    public Tree addChild(String name) {
        return getTree().addChild(name);
    }

    @Override
    public PropertyState setProperty(String name, @Nonnull CoreValue value) {
        return getTree().setProperty(name, value);
    }

    @Override
    public PropertyState setProperty(String name, @Nonnull List<CoreValue> values) {
        return getTree().setProperty(name, values);
    }

    @Override
    public void removeProperty(String name) {
        getTree().removeProperty(name);
    }
}
