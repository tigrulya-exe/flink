package org.apache.flink.streaming.api.connector.source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkNotNull;

// todo we can change it to no-op
/** todo. */
public class FromElementsSplitEnumerator
        implements SplitEnumerator<FromElementsSplit, FromElementsSplit> {

    private final SplitEnumeratorContext<FromElementsSplit> context;
    private final Queue<FromElementsSplit> remainingSplits;

    public FromElementsSplitEnumerator(
            SplitEnumeratorContext<FromElementsSplit> context, FromElementsSplit split) {
        this.context = checkNotNull(context);
        this.remainingSplits = new ArrayDeque<>();
        remainingSplits.add(split);
    }

    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        final FromElementsSplit nextSplit = remainingSplits.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<FromElementsSplit> splits, int subtaskId) {
        remainingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {}

    @Override
    public FromElementsSplit snapshotState(long checkpointId) throws Exception {
        return remainingSplits.poll();
    }

    @Override
    public void close() throws IOException {}
}
