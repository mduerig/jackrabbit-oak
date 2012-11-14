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
package org.apache.jackrabbit.mongomk.impl.command;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.mongomk.api.instruction.Instruction;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.action.FetchCommitAction;
import org.apache.jackrabbit.mongomk.impl.action.FetchCommitsAction;
import org.apache.jackrabbit.mongomk.impl.action.FetchHeadRevisionIdAction;
import org.apache.jackrabbit.mongomk.impl.action.FetchNodesAction;
import org.apache.jackrabbit.mongomk.impl.action.ReadAndIncHeadRevisionAction;
import org.apache.jackrabbit.mongomk.impl.action.SaveAndSetHeadRevisionAction;
import org.apache.jackrabbit.mongomk.impl.action.SaveCommitAction;
import org.apache.jackrabbit.mongomk.impl.action.SaveNodesAction;
import org.apache.jackrabbit.mongomk.impl.command.exception.ConflictingCommitException;
import org.apache.jackrabbit.mongomk.impl.instruction.CommitCommandInstructionVisitor;
import org.apache.jackrabbit.mongomk.impl.model.MongoCommit;
import org.apache.jackrabbit.mongomk.impl.model.MongoNode;
import org.apache.jackrabbit.mongomk.impl.model.MongoSync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.WriteResult;

/**
 * {@code Command} for {@code MongoMicroKernel#commit(String, String, String, String)}
 */
public class CommitCommand extends BaseCommand<Long> {

    private static final Logger logger = LoggerFactory.getLogger(CommitCommand.class);

    private final MongoCommit commit;

    private Set<String> affectedPaths;
    private List<MongoNode> existingNodes;
    private MongoSync mongoSync;
    private Set<MongoNode> nodes;
    private Long revisionId;
    private String branchId;

    /**
     * Constructs a new {@code CommitCommandMongo}.
     *
     * @param nodeStore Node store.
     * @param commit {@link Commit}
     */
    public CommitCommand(MongoNodeStore nodeStore, Commit commit) {
        super(nodeStore);
        this.commit = (MongoCommit)commit;
    }

    @Override
    public Long execute() throws Exception {
        logger.debug(String.format("Trying to commit: %s", commit.getDiff()));

        mongoSync = new ReadAndIncHeadRevisionAction(nodeStore).execute();
        revisionId = mongoSync.getNextRevisionId() - 1;
        readBranchIdFromBaseCommit();
        createMongoNodes();
        prepareCommit();
        readExistingNodes();
        mergeNodes();
        prepareMongoNodes();
        new SaveNodesAction(nodeStore, nodes).execute();
        new SaveCommitAction(nodeStore, commit).execute();
        boolean success = saveAndSetHeadRevision();

        logger.debug(String.format("Success was: %b", success));

        if (!success) {
            markAsFailed();
            throw new ConflictingCommitException();
        }

        return revisionId;
    }

    @Override
    public int getNumOfRetries() {
        return 20;
    }

    @Override
    public boolean needsRetry(Exception e) {
        return e instanceof ConflictingCommitException;
    }

    private void readBranchIdFromBaseCommit() throws Exception {
        String commitBranchId = commit.getBranchId();
        if (commitBranchId != null) {
            // This is a newly created branch, so no need to check the base
            // commit's branch id.
            branchId = commitBranchId;
            return;
        }

        Long baseRevisionId = commit.getBaseRevisionId();
        if (baseRevisionId == null) {
            return;
        }

        MongoCommit baseCommit = new FetchCommitAction(nodeStore, baseRevisionId).execute();
        branchId = baseCommit.getBranchId();
    }

    private void createMongoNodes() throws Exception {
        CommitCommandInstructionVisitor visitor = new CommitCommandInstructionVisitor(
                nodeStore, mongoSync.getHeadRevisionId());
        visitor.setBranchId(branchId);

        for (Instruction instruction : commit.getInstructions()) {
            instruction.accept(visitor);
        }

        Map<String, MongoNode> pathNodeMap = visitor.getPathNodeMap();
        affectedPaths = pathNodeMap.keySet();
        nodes = new HashSet<MongoNode>(pathNodeMap.values());
    }

    private void prepareCommit() throws Exception {
        commit.setAffectedPaths(new LinkedList<String>(affectedPaths));
        commit.setBaseRevisionId(mongoSync.getHeadRevisionId());
        commit.setRevisionId(revisionId);
        if (commit.getBranchId() == null && branchId != null) {
            commit.setBranchId(branchId);
        }
        commit.removeField("_id"); // In case this is a retry.
    }

    private void readExistingNodes() {
        FetchNodesAction action = new FetchNodesAction(nodeStore, affectedPaths,
                mongoSync.getHeadRevisionId());
        action.setBranchId(branchId);
        existingNodes = action.execute();
    }

    private void mergeNodes() {
        for (MongoNode existingNode : existingNodes) {
            for (MongoNode committingNode : nodes) {
                if (existingNode.getPath().equals(committingNode.getPath())) {
                    logger.debug(String.format("Found existing node to merge: %s", existingNode.getPath()));
                    logger.debug(String.format("Existing node: %s", existingNode));
                    logger.debug(String.format("Committing node: %s", committingNode));

                    Map<String, Object> existingProperties = existingNode.getProperties();
                    if (!existingProperties.isEmpty()) {
                        committingNode.setProperties(existingProperties);

                        logger.debug(String.format("Merged properties for %s: %s", existingNode.getPath(),
                                existingProperties));
                    }

                    List<String> existingChildren = existingNode.getChildren();
                    if (existingChildren != null) {
                        committingNode.setChildren(existingChildren);

                        logger.debug(String.format("Merged children for %s: %s", existingNode.getPath(), existingChildren));
                    }

                    committingNode.setBaseRevisionId(existingNode.getRevisionId());

                    logger.debug(String.format("Merged node for %s: %s", existingNode.getPath(), committingNode));

                    break;
                }
            }
        }
    }

    private void prepareMongoNodes() {
        for (MongoNode committingNode : nodes) {
            logger.debug(String.format("Preparing children (added and removed) of %s", committingNode.getPath()));
            logger.debug(String.format("Committing node: %s", committingNode));

            List<String> children = committingNode.getChildren();
            if (children == null) {
                children = new LinkedList<String>();
            }

            List<String> addedChildren = committingNode.getAddedChildren();
            if (addedChildren != null) {
                children.addAll(addedChildren);
            }

            List<String> removedChildren = committingNode.getRemovedChildren();
            if (removedChildren != null) {
                children.removeAll(removedChildren);
            }

            if (!children.isEmpty()) {
                Set<String> temp = new HashSet<String>(children); // remove all duplicates
                committingNode.setChildren(new LinkedList<String>(temp));
            } else {
                committingNode.setChildren(null);
            }

            Map<String, Object> properties = committingNode.getProperties();

            Map<String, Object> addedProperties = committingNode.getAddedProps();
            if (addedProperties != null) {
                properties.putAll(addedProperties);
            }

            Map<String, Object> removedProperties = committingNode.getRemovedProps();
            if (removedProperties != null) {
                for (Map.Entry<String, Object> entry : removedProperties.entrySet()) {
                    properties.remove(entry.getKey());
                }
            }

            if (!properties.isEmpty()) {
                committingNode.setProperties(properties);
            } else {
                committingNode.setProperties(null);
            }

            committingNode.setRevisionId(revisionId);
            if (branchId != null) {
                committingNode.setBranchId(branchId);
            }

            logger.debug(String.format("Prepared committing node: %s", committingNode));
        }
    }

    /**
     * Protected for testing purposed only.
     *
     * @return True if the operation was successful.
     * @throws Exception If an exception happens.
     */
    protected boolean saveAndSetHeadRevision() throws Exception {
        long assumedHeadRevision = this.mongoSync.getHeadRevisionId();
        MongoSync mongoSync = new SaveAndSetHeadRevisionAction(nodeStore,
                assumedHeadRevision, revisionId).execute();
        if (mongoSync == null) { // There has been update(s) in the meantime.
            // FIXME - Add
            //if (conflictingCommitsExist(assumedHeadRevision)) {
                logger.warn(String.format("Encounterd a conflicting update, thus can't commit"
                        + " revision %s and will be retried with new revision", revisionId));
                return false;
            //}
        }
        return true;
    }

    private boolean conflictingCommitsExist(long assumedHeadRevision) throws Exception {
        // Grab the real head revision
        long realHeadRevisionId = new FetchHeadRevisionIdAction(nodeStore).execute();

        // Grab commits between assumed head revision and real head revision
        List<MongoCommit> commits = new FetchCommitsAction(nodeStore, assumedHeadRevision,
                realHeadRevisionId).execute();

        // Check affected paths
        for (MongoCommit commit : commits) {
            if (commit.getRevisionId().equals(assumedHeadRevision)) {
                // We're only interested in commits after the assumed head revision.
                continue;
            }
            for (String affectedPath : commit.getAffectedPaths()) {
                if (affectedPaths.contains(affectedPath)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void markAsFailed() throws Exception {
        DBCollection commitCollection = nodeStore.getCommitCollection();
        DBObject query = QueryBuilder.start("_id").is(commit.getObjectId("_id")).get();
        DBObject update = new BasicDBObject("$set", new BasicDBObject(MongoCommit.KEY_FAILED, Boolean.TRUE));
        WriteResult writeResult = commitCollection.update(query, update);
        if (writeResult.getError() != null) {
            // FIXME This is potentially a bug that we need to handle.
            throw new Exception(String.format("Update wasn't successful: %s", writeResult));
        }
    }
}