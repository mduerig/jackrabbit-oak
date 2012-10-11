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
package org.apache.jackrabbit.mongomk.query;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.model.NodeMongo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * FIXME - This should either merge or at least share a base class with
 * {@code FetchNodesByPathAndDepthQuery}
 *
 * An query for fetching nodes for a specific revision.
 */
public class FetchNodesForRevisionQuery extends AbstractQuery<List<NodeMongo>> {

    private static final Logger LOG = LoggerFactory.getLogger(FetchNodesForRevisionQuery.class);

    private final Set<String> paths;
    private final Long revisionId;

    private String branchId;

    /**
     * Constructs a new {@code FetchNodesForRevisionQuery}.
     *
     * @param mongoConnection The {@link MongoConnection}.
     * @param paths The paths to fetch.
     * @param revisionId The revision id.
     * @param branchId
     */
    public FetchNodesForRevisionQuery(MongoConnection mongoConnection, Set<String> paths,
            Long revisionId) {
        super(mongoConnection);
        this.paths = paths;
        this.revisionId = revisionId;
    }

    /**
     * Sets the branchId for the query.
     *
     * @param branchId Branch id.
     */
    public void setBranchId(String branchId) {
        this.branchId = branchId;
    }

    /**
     * FIXME - Consider removing this.
     * Constructs a new {@code FetchNodesForRevisionQuery}.
     *
     * @param mongoConnection The {@link MongoConnection}.
     * @param paths The paths to fetch.
     * @param revisionId The revision id.
     */
    public FetchNodesForRevisionQuery(MongoConnection mongoConnection, String[] paths,
            Long revisionId) {
        this(mongoConnection, new HashSet<String>(Arrays.asList(paths)), revisionId);
    }

    @Override
    public List<NodeMongo> execute() {
        List<Long> validRevisions = new FetchValidRevisionsQuery(mongoConnection, revisionId).execute();
        DBCursor dbCursor = retrieveAllNodes();
        List<NodeMongo> nodes = QueryUtils.getMostRecentValidNodes(dbCursor, validRevisions);
        return nodes;
    }

    private DBCursor retrieveAllNodes() {
        DBCollection nodeCollection = mongoConnection.getNodeCollection();
        QueryBuilder queryBuilder = QueryBuilder.start(NodeMongo.KEY_PATH).in(paths)
                .and(NodeMongo.KEY_REVISION_ID)
                .lessThanEquals(revisionId);

        if (branchId == null) {
            DBObject query = new BasicDBObject(NodeMongo.KEY_BRANCH_ID, new BasicDBObject("$exists", false));
            queryBuilder = queryBuilder.and(query);
        } else {
            queryBuilder = queryBuilder.and(NodeMongo.KEY_BRANCH_ID).is(branchId);
        }

        DBObject query = queryBuilder.get();
        LOG.debug(String.format("Executing query: %s", query));

        return nodeCollection.find(query);
    }
}