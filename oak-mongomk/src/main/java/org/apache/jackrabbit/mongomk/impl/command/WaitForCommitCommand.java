package org.apache.jackrabbit.mongomk.impl.command;

import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.action.FetchHeadRevisionIdAction;
import org.apache.jackrabbit.mongomk.util.MongoUtil;

/**
 * A {@code Command} for {@code MongoMicroKernel#waitForCommit(String, long)}
 */
public class WaitForCommitCommand extends BaseCommand<Long> {

    private static final long WAIT_FOR_COMMIT_POLL_MILLIS = 1000;

    private final String oldHeadRevisionId;
    private final long timeout;

    /**
     * Constructs a {@code WaitForCommitCommandMongo}
     *
     * @param nodeStore Node store.
     * @param oldHeadRevisionId Id of earlier head revision
     * @param timeout The maximum time to wait in milliseconds
     */
    public WaitForCommitCommand(MongoNodeStore nodeStore, String oldHeadRevisionId,
            long timeout) {
        super(nodeStore);
        this.oldHeadRevisionId = oldHeadRevisionId;
        this.timeout = timeout;
    }

    @Override
    public Long execute() throws Exception {
        long startTimestamp = System.currentTimeMillis();
        Long initialHeadRevisionId = getHeadRevision();

        if (timeout <= 0) {
            return initialHeadRevisionId;
        }

        Long oldHeadRevision = MongoUtil.toMongoRepresentation(oldHeadRevisionId);
        if (oldHeadRevision != initialHeadRevisionId) {
            return initialHeadRevisionId;
        }

        long waitForCommitPollMillis = Math.min(WAIT_FOR_COMMIT_POLL_MILLIS, timeout);
        while (true) {
            long headRevisionId = getHeadRevision();
            long now = System.currentTimeMillis();
            if (headRevisionId != initialHeadRevisionId || now - startTimestamp >= timeout) {
                return headRevisionId;
            }
            Thread.sleep(waitForCommitPollMillis);
        }
    }

    private long getHeadRevision() throws Exception {
        FetchHeadRevisionIdAction query = new FetchHeadRevisionIdAction(nodeStore);
        return query.execute();
    }
}