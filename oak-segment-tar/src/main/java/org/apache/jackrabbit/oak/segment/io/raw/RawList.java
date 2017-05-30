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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Objects;

public class RawList {

    public static RawList empty() {
        return new RawList(0, null);
    }

    public static RawList of(int size, RawRecordId bucket) {
        checkArgument(size > 0, "size");
        checkArgument(bucket != null, "bucket");
        return new RawList(size, bucket);
    }

    private final int size;

    private final RawRecordId bucket;

    private RawList(int size, RawRecordId bucket) {
        this.size = size;
        this.bucket = bucket;
    }

    public int getSize() {
        return size;
    }

    public RawRecordId getBucket() {
        return bucket;
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
        return equals((RawList) o);
    }

    private boolean equals(RawList that) {
        return size == that.size && Objects.equals(bucket, that.bucket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(size, bucket);
    }

    @Override
    public String toString() {
        return String.format("RawList{size=%d, bucket=%s}", size, bucket);
    }

}
