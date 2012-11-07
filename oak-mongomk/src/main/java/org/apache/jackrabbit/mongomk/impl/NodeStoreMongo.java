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
package org.apache.jackrabbit.mongomk.impl;

import java.util.Arrays;

import org.apache.jackrabbit.mongomk.api.NodeStore;
import org.apache.jackrabbit.mongomk.api.command.Command;
import org.apache.jackrabbit.mongomk.api.command.CommandExecutor;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.action.FetchCommitAction;
import org.apache.jackrabbit.mongomk.impl.command.CommitCommand;
import org.apache.jackrabbit.mongomk.impl.command.DefaultCommandExecutor;
import org.apache.jackrabbit.mongomk.impl.command.DiffCommand;
import org.apache.jackrabbit.mongomk.impl.command.GetHeadRevisionCommand;
import org.apache.jackrabbit.mongomk.impl.command.GetJournalCommand;
import org.apache.jackrabbit.mongomk.impl.command.GetNodesCommand;
import org.apache.jackrabbit.mongomk.impl.command.GetRevisionHistoryCommand;
import org.apache.jackrabbit.mongomk.impl.command.MergeCommand;
import org.apache.jackrabbit.mongomk.impl.command.NodeExistsCommand;
import org.apache.jackrabbit.mongomk.impl.command.WaitForCommitCommand;
import org.apache.jackrabbit.mongomk.impl.model.CommitMongo;
import org.apache.jackrabbit.mongomk.impl.model.NodeMongo;
import org.apache.jackrabbit.mongomk.impl.model.SyncMongo;
import org.apache.jackrabbit.mongomk.util.MongoUtil;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * Implementation of {@link NodeStore} for the {@code MongoDB}.
 */
public class NodeStoreMongo implements NodeStore {

    public static final String INITIAL_COMMIT_MESSAGE = "This is an autogenerated initial commit";
    public static final String INITIAL_COMMIT_PATH = "";
    public static final String INITIAL_COMMIT_DIFF = "+\"/\" : {}";

    private static final String COLLECTION_COMMITS = "commits";
    private static final String COLLECTION_NODES = "nodes";
    private static final String COLLECTION_SYNC = "sync";

    private final CommandExecutor commandExecutor;
    private final DB db;

    /**
     * Constructs a new {@code NodeStoreMongo}.
     *
     * @param db Mongo DB.
     */
    public NodeStoreMongo(DB db) {
        commandExecutor = new DefaultCommandExecutor();
        this.db = db;
    }

    @Override
    public String commit(Commit commit) throws Exception {
        Command<Long> command = new CommitCommand(this, commit);
        Long revisionId = commandExecutor.execute(command);
        return MongoUtil.fromMongoRepresentation(revisionId);
    }

    @Override
    public String diff(String fromRevision, String toRevision, String path, int depth)
            throws Exception {
        Command<String> command = new DiffCommand(this, fromRevision, toRevision, path, depth);
        return commandExecutor.execute(command);
    }

    @Override
    public String getHeadRevision() throws Exception {
        GetHeadRevisionCommand command = new GetHeadRevisionCommand(this);
        long revisionId = commandExecutor.execute(command);
        return MongoUtil.fromMongoRepresentation(revisionId);
    }

    @Override
    public Node getNodes(String path, String revisionId, int depth, long offset,
            int maxChildNodes, String filter) throws Exception {
        GetNodesCommand command = new GetNodesCommand(this, path,
                MongoUtil.toMongoRepresentation(revisionId));
        command.setBranchId(getBranchId(revisionId));
        command.setDepth(depth);
        return commandExecutor.execute(command);
    }

    @Override
    public String merge(String branchRevisionId, String message) throws Exception {
        MergeCommand command = new MergeCommand(this, branchRevisionId, message);
        return commandExecutor.execute(command);
    }

    @Override
    public boolean nodeExists(String path, String revisionId) throws Exception {
        NodeExistsCommand command = new NodeExistsCommand(this, path,
                MongoUtil.toMongoRepresentation(revisionId));
        String branchId = getBranchId(revisionId);
        command.setBranchId(branchId);
        return commandExecutor.execute(command);
    }

    @Override
    public String getJournal(String fromRevisionId, String toRevisionId, String path)
            throws Exception {
        GetJournalCommand command = new GetJournalCommand(this, fromRevisionId, toRevisionId, path);
        return commandExecutor.execute(command);
    }

    @Override
    public String getRevisionHistory(long since, int maxEntries, String path)
            throws Exception {
        GetRevisionHistoryCommand command = new GetRevisionHistoryCommand(this,
                since, maxEntries, path);
        return commandExecutor.execute(command);
    }

    @Override
    public String waitForCommit(String oldHeadRevisionId, long timeout) throws Exception {
        WaitForCommitCommand command = new WaitForCommitCommand(this,
                oldHeadRevisionId, timeout);
        long revisionId = commandExecutor.execute(command);
        return MongoUtil.fromMongoRepresentation(revisionId);
    }

    /**
     * Initializes the underlying DB.
     *
     * @param force If true, clears the DB before initializing the collections.
     * Use with caution.
     */
    public void initializeDB(boolean force) {
        if (force) {
            clearDB();
        }
        initCommitCollection(force);
        initNodeCollection(force);
        initSyncCollection(force);
    }

    /**
     * Drops the collections of the underlying DB.
     */
    public void clearDB() {
        getNodeCollection().drop();
        getCommitCollection().drop();
        getSyncCollection().drop();
    }

    /**
     * Returns the commit {@link DBCollection}.
     *
     * @return The commit {@link DBCollection}.
     */
    public DBCollection getCommitCollection() {
        DBCollection commitCollection = db.getCollection(COLLECTION_COMMITS);
        commitCollection.setObjectClass(CommitMongo.class);
        return commitCollection;
    }

    /**
     * Returns the sync {@link DBCollection}.
     *
     * @return The sync {@link DBCollection}.
     */
    public DBCollection getSyncCollection() {
        DBCollection syncCollection = db.getCollection(COLLECTION_SYNC);
        syncCollection.setObjectClass(SyncMongo.class);
        return syncCollection;
    }

    /**
     * Returns the node {@link DBCollection}.
     *
     * @return The node {@link DBCollection}.
     */
    public DBCollection getNodeCollection() {
        DBCollection nodeCollection = db.getCollection(COLLECTION_NODES);
        nodeCollection.setObjectClass(NodeMongo.class);
        return nodeCollection;
    }

    private void initCommitCollection(boolean force) {
        if (!force && db.collectionExists(COLLECTION_COMMITS)){
            return;
        }
        DBCollection commitCollection = getCommitCollection();
        DBObject index = new BasicDBObject();
        index.put(CommitMongo.KEY_REVISION_ID, 1L);
        DBObject options = new BasicDBObject();
        options.put("unique", Boolean.TRUE);
        commitCollection.ensureIndex(index, options);
        CommitMongo commit = new CommitMongo();
        commit.setAffectedPaths(Arrays.asList(new String[] { "/" }));
        commit.setBaseRevisionId(0L);
        commit.setDiff(INITIAL_COMMIT_DIFF);
        commit.setMessage(INITIAL_COMMIT_MESSAGE);
        commit.setRevisionId(0L);
        commit.setPath(INITIAL_COMMIT_PATH);
        commitCollection.insert(commit);
    }

    private void initNodeCollection(boolean force) {
        if (!force && db.collectionExists(COLLECTION_NODES)){
            return;
        }
        DBCollection nodeCollection = getNodeCollection();
        DBObject index = new BasicDBObject();
        index.put(NodeMongo.KEY_PATH, 1L);
        index.put(NodeMongo.KEY_REVISION_ID, 1L);
        DBObject options = new BasicDBObject();
        options.put("unique", Boolean.TRUE);
        nodeCollection.ensureIndex(index, options);
        NodeMongo root = new NodeMongo();
        root.setRevisionId(0L);
        root.setPath("/");
        nodeCollection.insert(root);
    }

    private void initSyncCollection(boolean force) {
        if (!force && db.collectionExists(COLLECTION_SYNC)){
            return;
        }
        DBCollection headCollection = getSyncCollection();
        SyncMongo headMongo = new SyncMongo();
        headMongo.setHeadRevisionId(0L);
        headMongo.setNextRevisionId(1L);
        headCollection.insert(headMongo);
    }

    private String getBranchId(String revisionId) throws Exception {
        if (revisionId == null) {
            return null;
        }

        CommitMongo baseCommit = new FetchCommitAction(this,
                MongoUtil.toMongoRepresentation(revisionId)).execute();
        return baseCommit.getBranchId();
    }
}