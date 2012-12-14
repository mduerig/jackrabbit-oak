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

package org.apache.jackrabbit.oak.kernel;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Iterator for the {@link JsopOp} operations in a jsop string.
 */
class JsopIterator implements Iterator<JsopOp> {
    private final MicroKernel kernel;
    private final JsopTokenizer reader;

    private int current;

    /**
     * An iterable for the {@code Jsop} operations in a jsop string.
     * @param kernel  Microkernel for retrieving the binaries.
     * @param jsop  jsop string
     * @return  iterable of {@code Jsop} operations.
     */
    public static Iterable<JsopOp> getJsopOps(final MicroKernel kernel, final String jsop) {
        return new Iterable<JsopOp>() {
            @Override
            public Iterator<JsopOp> iterator() {
                return new JsopIterator(kernel, jsop);
            }
        };
    }

    /**
     * New iterator for the {@code Jsop} operations in a jsop string.
     * @param kernel  Microkernel for retrieving the binaries.
     * @param jsop  jsop string
     */
    public JsopIterator(MicroKernel kernel, String jsop) {
        this.kernel = checkNotNull(kernel);
        reader = new JsopTokenizer(checkNotNull(jsop));
        current = reader.read();
    }

    @Override
    public boolean hasNext() {
        return current != JsopReader.END;
    }

    @Override
    public JsopOp next() {
        int op = current;
        String path = reader.readString();
        switch (op) {
            case '+': {
                reader.read(':');
                reader.read('{');
                NodeState nodeState = readNodeState(reader);
                current = reader.read();
                return JsopOp.add(path, nodeState);
            }
            case '-': {
                current = reader.read();
                return JsopOp.remove(path);
            }
            case '^': {
                reader.read(':');
                String name = PathUtils.getName(path);
                PropertyState propertyState = readPropertyState(name, reader);
                current = reader.read();
                return JsopOp.set(path, propertyState);
            }
            case '>': {
                reader.read(':');
                String target = reader.readString();
                current = reader.read();
                return JsopOp.move(path, target);
            }
            case '*': {
                reader.read(':');
                String target = reader.readString();
                current = reader.read();
                return JsopOp.copy(path, target);
            }
            default:
                throw new IllegalStateException();
        }
    }

    private PropertyState readPropertyState(String name, JsopTokenizer reader) {
        if (reader.matches(JsopReader.NULL)) {
            return null;
        }
        else if (reader.matches('[')) {
            return KernelNodeState.readArrayProperty(name, reader, kernel);
        }
        else {
            return KernelNodeState.readProperty(name, reader, kernel);
        }
    }

    private NodeState readNodeState(JsopTokenizer reader) {
        HashMap<String, PropertyState> properties = new LinkedHashMap<String, PropertyState>();
        HashMap<String, NodeState> nodes = new LinkedHashMap<String, NodeState>();
        while (!reader.matches('}')) {
            String name = StringCache.get(reader.readString());
            reader.read(':');
            if (reader.matches('{')) {
                nodes.put(name, readNodeState(reader));
            }
            else if (reader.matches('[')) {
                properties.put(name, KernelNodeState.readArrayProperty(name, reader, kernel));
            }
            else {
                properties.put(name, KernelNodeState.readProperty(name, reader, kernel));
            }
            reader.matches(',');
        }
        return MemoryNodeState.create(properties, nodes);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }
}
