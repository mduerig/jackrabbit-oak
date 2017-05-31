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
import static com.google.common.base.Preconditions.checkState;

import java.util.Objects;

public class RawNode {

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private RawRecordId template;

        private RawRecordId child;

        private RawRecordId childrenMap;

        private RawRecordId propertiesList;

        private RawRecordId stableId;

        private Builder() {
            // Prevent external instantiation.
        }

        public Builder withTemplate(RawRecordId template) {
            this.template = checkNotNull(template);
            return this;
        }

        public Builder withChild(RawRecordId child) {
            this.child = checkNotNull(child);
            return this;
        }

        public Builder withChildrenMap(RawRecordId childrenMap) {
            this.childrenMap = checkNotNull(childrenMap);
            return this;
        }

        public Builder withPropertiesList(RawRecordId propertiesList) {
            this.propertiesList = checkNotNull(propertiesList);
            return this;
        }

        public Builder withStableId(RawRecordId stableId) {
            this.stableId = checkNotNull(stableId);
            return this;
        }

        public RawNode build() {
            checkState(template != null, "template not specified");
            checkState(!(child != null && childrenMap != null), "both child and children map specified");
            return new RawNode(this);
        }

    }

    private final RawRecordId template;

    private final RawRecordId child;

    private final RawRecordId childrenMap;

    private final RawRecordId propertiesList;

    private final RawRecordId stableId;

    private RawNode(Builder builder) {
        this.template = builder.template;
        this.child = builder.child;
        this.childrenMap = builder.childrenMap;
        this.propertiesList = builder.propertiesList;
        this.stableId = builder.stableId;
    }

    public RawRecordId getTemplate() {
        return template;
    }

    public RawRecordId getChild() {
        return child;
    }

    public RawRecordId getChildrenMap() {
        return childrenMap;
    }

    public RawRecordId getPropertiesList() {
        return propertiesList;
    }

    public RawRecordId getStableId() {
        return stableId;
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
        return equals((RawNode) o);
    }

    private boolean equals(RawNode that) {
        return Objects.equals(template, that.template) &&
                Objects.equals(child, that.child) &&
                Objects.equals(childrenMap, that.childrenMap) &&
                Objects.equals(propertiesList, that.propertiesList) &&
                Objects.equals(stableId, that.stableId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(template, child, childrenMap, propertiesList, stableId);
    }

    @Override
    public String toString() {
        return String.format("RawNode{template=%s, child=%s, childrenMap=%s, propertiesList=%s, stableId=%s}", template, child, childrenMap, propertiesList, stableId);
    }

}
