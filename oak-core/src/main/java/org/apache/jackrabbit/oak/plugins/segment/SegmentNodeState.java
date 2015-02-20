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
package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.spi.state.AbstractNodeState.checkValidName;

import java.util.Collections;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.segment.Segment.Reader;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * A record of type "NODE". This class can read a node record from a segment. It
 * currently doesn't cache data (but the template is fully loaded).
 */
public class SegmentNodeState implements NodeState {
    private final Page page;

    private volatile Page templatePage = null;

    private volatile Template template = null;

    public SegmentNodeState(@Nonnull Page page) {
        this.page = checkNotNull(page);
    }

    public Page getPage() {
        return page;
    }

    public static boolean fastEquals(Object a, Object b) {
        return a instanceof SegmentNodeState && fastEquals((SegmentNodeState) a, b);
    }

    public static boolean fastEquals(SegmentNodeState a, Object b) {
        return b instanceof SegmentNodeState && fastEquals(a, (SegmentNodeState) b);
    }

    public static boolean fastEquals(SegmentNodeState a, SegmentNodeState b) {
        return Record.fastEquals(a.page, b.page);
    }

    Page getTemplatePage() {
        if (templatePage == null) {
            // no problem if updated concurrently,
            // as each concurrent thread will just get the same value
            templatePage = page.readPage();
        }
        return templatePage;
    }

    Template getTemplate() {
        if (template == null) {
            // no problem if updated concurrently,
            // as each concurrent thread will just get the same value
            template = getTemplatePage().readTemplate();
        }
        return template;
    }

    MapRecord getChildNodeMap() {
        return page.readMap(0, 1);
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public long getPropertyCount() {
        Template template = getTemplate();
        long count = template.getPropertyTemplates().length;
        if (template.getPrimaryType() != null) {
            count++;
        }
        if (template.getMixinTypes() != null) {
            count++;
        }
        return count;
    }

    @Override
    public boolean hasProperty(String name) {
        checkNotNull(name);
        Template template = getTemplate();
        if (JCR_PRIMARYTYPE.equals(name)) {
            return template.getPrimaryType() != null;
        } else if (JCR_MIXINTYPES.equals(name)) {
            return template.getMixinTypes() != null;
        } else {
            return template.getPropertyTemplate(name) != null;
        }
    }

    @Override @CheckForNull
    public PropertyState getProperty(String name) {
        checkNotNull(name);
        Template template = getTemplate();
        PropertyState property = null;
        if (JCR_PRIMARYTYPE.equals(name)) {
            property = template.getPrimaryType();
        } else if (JCR_MIXINTYPES.equals(name)) {
            property = template.getMixinTypes();
        }
        if (property != null) {
            return property;
        }

        PropertyTemplate propertyTemplate =
                template.getPropertyTemplate(name);
        if (propertyTemplate != null) {
            int ids = 1 + propertyTemplate.getIndex();
            if (template.getChildName() != Template.ZERO_CHILD_NODES) {
                ids++;
            }
            return new SegmentPropertyState(page.readPage(0, ids), propertyTemplate);
        } else {
            return null;
        }
    }

    @Override @Nonnull
    public Iterable<PropertyState> getProperties() {
        Template template = getTemplate();
        PropertyTemplate[] propertyTemplates = template.getPropertyTemplates();
        List<PropertyState> list =
                newArrayListWithCapacity(propertyTemplates.length + 2);

        PropertyState primaryType = template.getPrimaryType();
        if (primaryType != null) {
            list.add(primaryType);
        }

        PropertyState mixinTypes = template.getMixinTypes();
        if (mixinTypes != null) {
            list.add(mixinTypes);
        }

        int ids = 1;
        if (template.getChildName() != Template.ZERO_CHILD_NODES) {
            ids++;
        }
        for (PropertyTemplate propertyTemplate : propertyTemplates) {
            list.add(new SegmentPropertyState(page.readPage(0, ids), propertyTemplate));
        }

        return list;
    }

    @Override
    public boolean getBoolean(String name) {
        return Boolean.TRUE.toString().equals(getValueAsString(name, BOOLEAN));
    }

    @Override
    public long getLong(String name) {
        String value = getValueAsString(name, LONG);
        if (value != null) {
            return Long.parseLong(value);
        } else {
            return 0;
        }
    }

    @Override @CheckForNull
    public String getString(String name) {
        return getValueAsString(name, STRING);
    }

    @Override @Nonnull
    public Iterable<String> getStrings(String name) {
        return getValuesAsStrings(name, STRINGS);
    }

    @Override @CheckForNull
    public String getName(String name) {
        return getValueAsString(name, NAME);
    }

    @Override @Nonnull
    public Iterable<String> getNames(String name) {
        return getValuesAsStrings(name, NAMES);
    }

    /**
     * Optimized value access method. Returns the string value of a property
     * of a given non-array type. Returns {@code null} if the named property
     * does not exist, or is of a different type than given.
     *
     * @param name property name
     * @param type property type
     * @return string value of the property, or {@code null}
     */
    @CheckForNull
    private String getValueAsString(String name, Type<?> type) {
        checkArgument(!type.isArray());

        Template template = getTemplate();
        if (JCR_PRIMARYTYPE.equals(name)) {
            PropertyState primary = template.getPrimaryType();
            if (primary != null) {
                if (type == NAME) {
                    return primary.getValue(NAME);
                } else {
                    return null;
                }
            }
        } else if (JCR_MIXINTYPES.equals(name)
                && template.getMixinTypes() != null) {
            return null;
        }

        PropertyTemplate propertyTemplate =
                template.getPropertyTemplate(name);
        if (propertyTemplate == null
                || propertyTemplate.getType() != type) {
            return null;
        }

        int ids = 1 + propertyTemplate.getIndex();
        if (template.getChildName() != Template.ZERO_CHILD_NODES) {
            ids++;
        }
        return page.readPage(0, ids).readString(0);
    }

    /**
     * Optimized value access method. Returns the string values of a property
     * of a given array type. Returns an empty iterable if the named property
     * does not exist, or is of a different type than given.
     *
     * @param name property name
     * @param type property type
     * @return string values of the property, or an empty iterable
     */
    @Nonnull
    private Iterable<String> getValuesAsStrings(String name, Type<?> type) {
        checkArgument(type.isArray());

        Template template = getTemplate();
        if (JCR_MIXINTYPES.equals(name)) {
            PropertyState mixin = template.getMixinTypes();
            if (type == NAMES && mixin != null) {
                return mixin.getValue(NAMES);
            } else if (type == NAMES || mixin != null) {
                return emptyList();
            }
        } else if (JCR_PRIMARYTYPE.equals(name)
                && template.getPrimaryType() != null) {
            return emptyList();
        }

        PropertyTemplate propertyTemplate =
                template.getPropertyTemplate(name);
        if (propertyTemplate == null
                || propertyTemplate.getType() != type) {
            return emptyList();
        }

        int ids = 1 + propertyTemplate.getIndex();
        if (template.getChildName() != Template.ZERO_CHILD_NODES) {
            ids++;
        }

        Reader reader = page.readPage(0, ids).getReader();
        int size = reader.readInt();
        if (size == 0) {
            return emptyList();
        }

        Page valuePage = reader.readPage();
        if (size == 1) {
            return singletonList(valuePage.readString(0));
        }

        List<String> values = newArrayListWithCapacity(size);
        ListRecord list = new ListRecord(valuePage, size);
        for (Page entry : list.getEntries()) {
            values.add(entry.readString(0));
        }
        return values;
    }

    @Override
    public long getChildNodeCount(long max) {
        String childName = getTemplate().getChildName();
        if (childName == Template.ZERO_CHILD_NODES) {
            return 0;
        } else if (childName == Template.MANY_CHILD_NODES) {
            return getChildNodeMap().size();
        } else {
            return 1;
        }
    }

    @Override
    public boolean hasChildNode(String name) {
        String childName = getTemplate().getChildName();
        if (childName == Template.ZERO_CHILD_NODES) {
            return false;
        } else if (childName == Template.MANY_CHILD_NODES) {
            return getChildNodeMap().getEntry(name) != null;
        } else {
            return childName.equals(name);
        }
    }

    @Override @Nonnull
    public NodeState getChildNode(String name) {
        String childName = getTemplate().getChildName();
        if (childName == Template.MANY_CHILD_NODES) {
            MapEntry child = getChildNodeMap().getEntry(name);
            if (child != null) {
                return child.getNodeState();
            }
        } else if (childName != Template.ZERO_CHILD_NODES
                && childName.equals(name)) {
            Page childPage = page.readPage(0, 1);
            return new SegmentNodeState(childPage);
        }
        checkValidName(name);
        return MISSING_NODE;
    }

    @Override @Nonnull
    public Iterable<String> getChildNodeNames() {
        String childName = getTemplate().getChildName();
        if (childName == Template.ZERO_CHILD_NODES) {
            return Collections.emptyList();
        } else if (childName == Template.MANY_CHILD_NODES) {
            return getChildNodeMap().getKeys();
        } else {
            return Collections.singletonList(childName);
        }
    }

    @Override @Nonnull
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        String childName = getTemplate().getChildName();
        if (childName == Template.ZERO_CHILD_NODES) {
            return Collections.emptyList();
        } else if (childName == Template.MANY_CHILD_NODES) {
            return getChildNodeMap().getEntries();
        } else {
            Page childPage = page.readPage(0, 1);
            return Collections.singletonList(new MemoryChildNodeEntry(
                    childName, new SegmentNodeState(childPage)));
        }
    }

    @Override @Nonnull
    public SegmentNodeBuilder builder() {
        return new SegmentNodeBuilder(this);
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (this == base || fastEquals(this, base)) {
             return true; // no changes
        } else if (base == EMPTY_NODE || !base.exists()) { // special case
            return EmptyNodeState.compareAgainstEmptyState(this, diff);
        } else if (!(base instanceof SegmentNodeState)) { // fallback
            return AbstractNodeState.compareAgainstBaseState(this, base, diff);
        }

        SegmentNodeState that = (SegmentNodeState) base;

        Template beforeTemplate = that.getTemplate();
        Page beforePage = that.getPage();

        Template afterTemplate = getTemplate();
        Page afterPage = getPage();

        // Compare type properties
        if (!compareProperties(
                beforeTemplate.getPrimaryType(), afterTemplate.getPrimaryType(),
                diff)) {
            return false;
        }
        if (!compareProperties(
                beforeTemplate.getMixinTypes(), afterTemplate.getMixinTypes(),
                diff)) {
            return false;
        }

        // Compare other properties, leveraging the ordering
        int beforeIndex = 0;
        int afterIndex = 0;
        PropertyTemplate[] beforeProperties =
                beforeTemplate.getPropertyTemplates();
        PropertyTemplate[] afterProperties =
                afterTemplate.getPropertyTemplates();
        while (beforeIndex < beforeProperties.length
                && afterIndex < afterProperties.length) {
            int d = Integer.valueOf(afterProperties[afterIndex].hashCode())
                    .compareTo(Integer.valueOf(beforeProperties[beforeIndex].hashCode()));
            if (d == 0) {
                d = afterProperties[afterIndex].getName().compareTo(
                        beforeProperties[beforeIndex].getName());
            }
            PropertyState beforeProperty = null;
            PropertyState afterProperty = null;
            if (d < 0) {
                afterProperty = afterTemplate.getProperty(afterPage, afterIndex++);
            } else if (d > 0) {
                beforeProperty = beforeTemplate.getProperty(beforePage, beforeIndex++);
            } else {
                afterProperty = afterTemplate.getProperty(afterPage, afterIndex++);
                beforeProperty = beforeTemplate.getProperty(beforePage, beforeIndex++);
            }
            if (!compareProperties(beforeProperty, afterProperty, diff)) {
                return false;
            }
        }
        while (afterIndex < afterProperties.length) {
            if (!diff.propertyAdded(
                    afterTemplate.getProperty(afterPage, afterIndex++))) {
                return false;
            }
        }
        while (beforeIndex < beforeProperties.length) {
            PropertyState beforeProperty =
                    beforeTemplate.getProperty(beforePage, beforeIndex++);
            if (!diff.propertyDeleted(beforeProperty)) {
                return false;
            }
        }

        String beforeChildName = beforeTemplate.getChildName();
        String afterChildName = afterTemplate.getChildName();
        if (afterChildName == Template.ZERO_CHILD_NODES) {
            if (beforeChildName != Template.ZERO_CHILD_NODES) {
                for (ChildNodeEntry entry
                        : beforeTemplate.getChildNodeEntries(beforePage)) {
                    if (!diff.childNodeDeleted(
                            entry.getName(), entry.getNodeState())) {
                        return false;
                    }
                }
            }
        } else if (afterChildName != Template.MANY_CHILD_NODES) {
            NodeState afterNode = afterTemplate.getChildNode(afterChildName, afterPage);
            NodeState beforeNode = beforeTemplate.getChildNode(afterChildName, beforePage);
            if (!beforeNode.exists()) {
                if (!diff.childNodeAdded(afterChildName, afterNode)) {
                    return false;
                }
            } else if (!fastEquals(afterNode, beforeNode)) {
                if (!diff.childNodeChanged(
                        afterChildName, beforeNode, afterNode)) {
                    return false;
                }
            }
            if (beforeChildName == Template.MANY_CHILD_NODES
                    || (beforeChildName != Template.ZERO_CHILD_NODES
                        && !beforeNode.exists())) {
                for (ChildNodeEntry entry
                        : beforeTemplate.getChildNodeEntries(beforePage)) {
                    if (!afterChildName.equals(entry.getName())) {
                        if (!diff.childNodeDeleted(
                                entry.getName(), entry.getNodeState())) {
                            return false;
                        }
                    }
                }
            }
        } else if (beforeChildName == Template.ZERO_CHILD_NODES) {
            for (ChildNodeEntry entry
                    : afterTemplate.getChildNodeEntries(afterPage)) {
                if (!diff.childNodeAdded(
                        entry.getName(), entry.getNodeState())) {
                    return false;
                }
            }
        } else if (beforeChildName != Template.MANY_CHILD_NODES) {
            for (ChildNodeEntry entry
                    : afterTemplate.getChildNodeEntries(afterPage)) {
                String childName = entry.getName();
                NodeState afterChild = entry.getNodeState();
                if (beforeChildName.equals(childName)) {
                    NodeState beforeChild =
                            beforeTemplate.getChildNode(beforeChildName, beforePage);
                    if (beforeChild.exists()) {
                        if (!fastEquals(afterChild, beforeChild)
                                && !diff.childNodeChanged(
                                        childName, beforeChild, afterChild)) {
                            return false;
                        }
                    } else {
                        if (!diff.childNodeAdded(childName, afterChild)) {
                            return false;
                        }
                    }
                } else if (!diff.childNodeAdded(childName, afterChild)) {
                    return false;
                }
            }
        } else {
            MapRecord afterMap = afterTemplate.getChildNodeMap(afterPage);
            MapRecord beforeMap = beforeTemplate.getChildNodeMap(beforePage);
            return afterMap.compare(beforeMap, diff);
        }

        return true;
    }

    private static boolean compareProperties(
            PropertyState before, PropertyState after, NodeStateDiff diff) {
        if (before == null) {
            return after == null || diff.propertyAdded(after);
        } else if (after == null) {
            return diff.propertyDeleted(before);
        } else {
            return before.equals(after) || diff.propertyChanged(before, after);
        }
    }

    //------------------------------------------------------------< Object >--

    @Override
    public boolean equals(Object object) {
        if (this == object || fastEquals(this, object)) {
            return true;
        } else if (object instanceof SegmentNodeState) {
            SegmentNodeState that = (SegmentNodeState) object;
            Template template = getTemplate();
            return template.equals(that.getTemplate())
                    && template.compare(getPage(), that.getPage());
        } else {
            return object instanceof NodeState
                    && AbstractNodeState.equals(this, (NodeState) object); // TODO
        }
    }

    @Override
    public int hashCode() {
        return 31 * page.hashCode() + templatePage.hashCode();
    }

    @Override
    public String toString() {
        return AbstractNodeState.toString(this);
    }

}
