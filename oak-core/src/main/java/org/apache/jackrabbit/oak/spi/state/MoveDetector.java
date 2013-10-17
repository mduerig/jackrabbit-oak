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

package org.apache.jackrabbit.oak.spi.state;

import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;

public class MoveDetector implements NodeStateDiff {
    private final MoveAwareNodeStateDiff diff;
    private final String path;
    private final Set<String> movedPaths;

    public MoveDetector(MoveAwareNodeStateDiff diff, String path, Set<String> movedPaths) {
        this.diff = diff;
        this.path = path;
        this.movedPaths = movedPaths;
    }

    public static Set<String> findMovedPaths(NodeState before, NodeState after) {
        final Set<String> movedPaths = Sets.newHashSet();

        class MoveFindingDiff extends DefaultNodeStateDiff {
            private final String path;

            public MoveFindingDiff(String path) {
                this.path = path;
            }

            @Override
            public boolean childNodeAdded(String name, NodeState after) {
                PropertyState sourceProperty = after.getProperty(":source-path");
                if (sourceProperty != null) {
                    movedPaths.add(sourceProperty.getValue(STRING));
                }
                return true;
            }

            @Override
            public boolean childNodeChanged(String name, NodeState before, NodeState after) {
                return after.compareAgainstBaseState(before, new MoveFindingDiff(concat(path, name)));
            }
        }

        after.compareAgainstBaseState(before, new MoveFindingDiff("/"));
        return movedPaths;
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        return diff.propertyAdded(after);
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        return diff.propertyChanged(before, after);
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        return diff.propertyDeleted(before);
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        PropertyState sourceProperty = after.getProperty(":source-path");
        if (sourceProperty == null) {
            return diff.childNodeAdded(name, after);
        } else {
            String sourcePath = sourceProperty.getValue(STRING);
            String destPath = concat(path, name);
            return diff.move(sourcePath, destPath, after);
        }
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        return diff.childNodeChanged(name, before, after);
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        if (movedPaths.contains(concat(path, name))) {
            return true;
        } else {
            return diff.childNodeDeleted(name, before);
        }
    }
}
