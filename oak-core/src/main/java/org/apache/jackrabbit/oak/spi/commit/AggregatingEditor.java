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

package org.apache.jackrabbit.oak.spi.commit;

import static org.apache.jackrabbit.oak.api.Type.LONG;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * POC of an editor implementing an atomic counter.
 *
 * Deltas to the counter are recorded within properties whose name start with
 * {@code delta-}. Once this editor runs it will add the sum of all {@code delta-}
 * properties to the value of the {@code count} property and remove the individual
 * {@code delta-} properties.
 */
public class AggregatingEditor extends DefaultEditor {
    private final NodeBuilder builder;

    private boolean update;
    private long count;

    public AggregatingEditor(NodeBuilder builder) {
        this.builder = builder;
        if (builder.hasProperty("count")) {
            count = builder.getProperty("count").getValue(LONG);
        }
    }

    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        if (update) {
            builder.setProperty("count", count);
        }
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        String name = after.getName();
        if (name.startsWith("delta-") && after.getType() == LONG) {
            count += after.getValue(LONG);
            update = true;
            builder.removeProperty(name);
        }
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
        return new AggregatingEditor(builder.getChildNode(name));
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        return new AggregatingEditor(builder.getChildNode(name));
    }
}
