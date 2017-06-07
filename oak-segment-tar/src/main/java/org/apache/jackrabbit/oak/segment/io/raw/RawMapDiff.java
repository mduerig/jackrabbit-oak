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

public class RawMapDiff {

    public static RawMapDiff of(RawRecordId base, RawMapEntry entry) {
        return new RawMapDiff(checkNotNull(base), checkNotNull(entry));
    }

    private final RawRecordId base;

    private final RawMapEntry entry;

    private RawMapDiff(RawRecordId base, RawMapEntry entry) {
        this.base = base;
        this.entry = entry;
    }

    public RawRecordId getBase() {
        return base;
    }

    public RawMapEntry getEntry() {
        return entry;
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
        return equals((RawMapDiff) o);
    }

    private boolean equals(RawMapDiff that) {
        return Objects.equals(base, that.base) && Objects.equals(entry, that.entry);
    }

    @Override
    public int hashCode() {
        return Objects.hash(base, entry);
    }

    @Override
    public String toString() {
        return String.format("RawMapDiff{base=%s, entry=%s}", base, entry);
    }

}
