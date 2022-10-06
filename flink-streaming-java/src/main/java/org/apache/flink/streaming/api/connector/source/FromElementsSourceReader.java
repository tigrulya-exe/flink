package org.apache.flink.streaming.api.connector.source;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** todo. */
public class FromElementsSourceReader<T> implements SourceReader<T, FromElementsSplit> {

    private final SourceReaderContext context;

    private final TypeSerializer<T> serializer;

    private volatile boolean isRunning = true;

    /** The availability future. This reader is available as soon as a split is assigned. */
    private final CompletableFuture<Void> availability;

    private final int elementsCount;
    private int currentOffset = 0;

    private final DataInputViewStreamWrapper serializedElementsStream;

    public FromElementsSourceReader(
            SourceReaderContext context,
            TypeSerializer<T> serializer,
            byte[] serializedElements,
            int elementsCount) {
        this.context = context;
        this.serializer = serializer;
        this.elementsCount = elementsCount;
        this.availability = new CompletableFuture<>();
        this.serializedElementsStream =
                new DataInputViewStreamWrapper(new ByteArrayInputStream(serializedElements));
    }

    @Override
    public void start() {
        // todo: tmp
        context.sendSplitRequest();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        if (isRunning && currentOffset < elementsCount) {
            try {
                T record = serializer.deserialize(serializedElementsStream);
                output.collect(record);
                ++currentOffset;
                return InputStatus.MORE_AVAILABLE;
            } catch (Exception e) {
                throw new IOException("Failed to deserialize an element from the source.", e);
            }
        }
        return InputStatus.END_OF_INPUT;
    }

    @Override
    public List<FromElementsSplit> snapshotState(long checkpointId) {
        return Collections.singletonList(new FromElementsSplit(currentOffset));
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return availability;
    }

    @Override
    public void addSplits(List<FromElementsSplit> splits) {
        // source should be used only with parallelism 1
        Preconditions.checkArgument(splits.size() == 1);
        currentOffset = splits.get(0).getOffset();
        skipElements(currentOffset);
        // set availability so that pollNext is actually called
        availability.complete(null);
    }

    @Override
    public void notifyNoMoreSplits() {
        // set availability so that pollNext is actually called
        availability.complete(null);
    }

    @Override
    public void close() throws Exception {
        isRunning = false;
    }

    private void skipElements(int elementsToSkip) {
        try {
            // todo add local var instead of elementsToSkip
            serializedElementsStream.reset();
            while (elementsToSkip > 0) {
                serializer.deserialize(serializedElementsStream);
                elementsToSkip--;
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to deserialize an element from the source. "
                            + "If you are using user-defined serialization (Value and Writable types), check the "
                            + "serialization functions.\nSerializer is "
                            + serializer,
                    e);
        }
    }
}
