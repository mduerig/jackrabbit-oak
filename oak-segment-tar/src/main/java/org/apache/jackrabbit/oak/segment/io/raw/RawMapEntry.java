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

package org.apache.jackrabbit.oak.segment.io.raw;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Objects;

public class RawMapEntry {

    public static RawMapEntry of(int hash, RawRecordId key, RawRecordId value) {
        return new RawMapEntry(hash, checkNotNull(key, "key"), checkNotNull(value, "value"));
    }

    private final int hash;

    private final RawRecordId key;

    private final RawRecordId value;

    private RawMapEntry(int hash, RawRecordId key, RawRecordId value) {
        this.hash = hash;
        this.key = key;
        this.value = value;
    }

    public RawRecordId getKey() {
        return key;
    }

    public RawRecordId getValue() {
        return value;
    }

    public int getHash() {
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        return equals((RawMapEntry) o);
    }

    private boolean equals(RawMapEntry that) {
        return hash == that.hash && Objects.equals(key, that.key) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hash, key, value);
    }

    @Override
    public String toString() {
        return String.format("RawMapEntry{hash=%s, key=%s, value=%s}", hash, key, value);
    }

}
