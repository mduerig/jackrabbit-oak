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

package org.apache.jackrabbit.oak.plugins.segment;

import org.apache.jackrabbit.oak.plugins.segment.Segment.Reader;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

/**
 * michid document
 */
public final class Page {
    private RecordId recordId;  // (re-)resolve on snfe

    private Page(RecordId recordId) {
        this.recordId = recordId;
    }

    public static boolean contains(SegmentStore store, Page page) {
        return false; // michid implement contains
    }

    public SegmentStore getNodeStore() {
        return null;
    }

    public BlobStore getBlobStore() {
        return null; // michid implement getBlobStore
    }

    public String getContentIdentity() {
        return null;   // michid implement getContentIdentity. See Blob.getContentIdentity
    }

    public Reader getReader() {
        return null;  // michid implement getReader
    }

    public Reader getReader(int offset) {
        return null; // michid implement getReader
    }

    public Reader getReader(int offset, int ids) {
        return null; // michid implement getReader
    }

    public String readString(int offset) {
        return null; // michid implement readString
    }

    public int readInt(int offset) {
        return 0; // michid implement readInt
    }

    public long readLength(int offset) {
        return 0; // michid implement readLength
    }

    public byte readByte(int offset) {
        return 0; // michid implement readByte
    }

    public Page readPage() {
        return null; // michid implement readPage
    }

    public Page readPage(int offset, int ids) {
        // michid implement readPage
        return null;
    }

    public Template readTemplate() {
        return null; // michid implement readTemplate
    }

    public MapRecord readMap(int offset, int ids) {
        return null; // michid implement readMap
    }

    public MapRecord readMap() {
        return null; // michid implement readMap
    }

    public void readBytes(int position, byte[] buffer, int offset, int length) {
        // michid implement readBytes
    }

    // michid equals iff recordId1 == recordId2 || record1 == record2 (re-resolve on default?)
    // michid we need hash code for MapEntry.compare. how to implement?
}
