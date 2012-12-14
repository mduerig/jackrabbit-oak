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

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;

/**
 * An iterator for the entries of {@link org.apache.jackrabbit.mk.api.MicroKernel#getJournal(String, String, String)}
 * Each element of the iterator contains the "changes" element of the corresponding element
 * of the journal or {@code null} if the journal does not contain "changes".
 */
class JournalIterator implements Iterator<String> {
    private final JsopReader reader;

    private boolean hasNext;

    /**
     * An iterable for the entries of {@code MicroKernel#getJournal()}
     * @param journal  the journal in the format returned by the {@code MicroKernel#getJournal()}.
     * @return  iterable over the "change" entries in {@code journal}.
     */
    public static Iterable<String> getChanges(final String journal) {
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return new JournalIterator(journal);
            }
        };
    }

    /**
     * New iterator for the entries of {@code MicroKernel#getJournal()}
     * @param journal  the journal in the format returned by the {@code MicroKernel#getJournal()}.
     */
    public JournalIterator(String journal) {
        reader = new JsopTokenizer(journal);
        reader.read('[');
        if (!(hasNext = reader.matches('{'))) {
            reader.read(']');
        }
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public String next() {
        if (!hasNext) {
            throw new NoSuchElementException();
        }

        String next = null;
        while (!reader.matches('}')) {
            String name = reader.readString();
            reader.read(':');
            if ("changes".equals(name)) {
                next = reader.readString();
            }
            else {
                reader.read();
            }
            reader.matches(',');
        }

        if (hasNext = reader.matches(',')) {
            reader.read('{');
        }
        else {
            reader.read(']');
        }

        return next;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }
}
