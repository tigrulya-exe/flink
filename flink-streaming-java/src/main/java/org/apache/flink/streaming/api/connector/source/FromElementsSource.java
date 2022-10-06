package org.apache.flink.streaming.api.connector.source;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class FromElementsSource<T> implements Source<T,
        CollectionSplit<T>,
        Collection<CollectionSplit<T>>> {
    private @Nullable

    TypeSerializer<T> serializer;
    /** The actual data elements, in serialized form. */
    private byte[] elementsSerialized;
    private final transient Iterable<T> elements;
    private final int numSplits;

    public FromElementsSource(TypeSerializer<T> serializer, Collection<T> collection) throws IOException {
        this(serializer, collection, 1);
    }

    public FromElementsSource(TypeSerializer<T> serializer, Collection<T> collection, int numSplits) throws IOException {
        Preconditions.checkArgument(numSplits > 0,
                "Splits count should be positive");
        Preconditions.checkArgument(numSplits <= collection.size(),
                "Splits count should be lesser than collection size");
        this.elements = Preconditions.checkNotNull(collection);
        this.numSplits = numSplits;

        serializeElements();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<T, CollectionSplit<T>> createReader(SourceReaderContext readerContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<CollectionSplit<T>, Collection<CollectionSplit<T>>> createEnumerator(
            SplitEnumeratorContext<CollectionSplit<T>> enumContext) throws Exception {
        Collection<CollectionSplit<T>> splits = createSplits(collection, numSplits);
        return new FromElementsSplitEnumerator<>(enumContext, splits);
    }

    @Override
    public SplitEnumerator<CollectionSplit<T>, Collection<CollectionSplit<T>>> restoreEnumerator(
            SplitEnumeratorContext<CollectionSplit<T>> enumContext,
            Collection<CollectionSplit<T>> restoredSplits) throws Exception {
        return new FromElementsSplitEnumerator<>(enumContext, restoredSplits);
    }

    @Override
    public SimpleVersionedSerializer<CollectionSplit<T>> getSplitSerializer() {
        return new CollectionSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<Collection<CollectionSplit<T>>> getEnumeratorCheckpointSerializer() {
        // todo
        return null;
    }

    private Collection<CollectionSplit<T>> createSplits(Collection<T> collection, int numSplits) {
        int chunkSize = collection.size() / numSplits;
        int remainder = collection.size() % numSplits;

        List<CollectionSplit<T>> collectionSplits = new ArrayList<>(numSplits);

        Iterator<T> collectionIter = collection.iterator();
        for (int splitId = 0; splitId < numSplits; ++splitId) {
            int currentChunkSize = splitId < remainder ? chunkSize : chunkSize + 1;

            List<T> chunk = new ArrayList<>(currentChunkSize);
            for (int i = 0; i < currentChunkSize; ++i) {
                chunk.add(collectionIter.next());
            }
            collectionSplits.add(new CollectionSplit<>(chunk));
        }

        return collectionSplits;
    }

    private void serializeElements() throws IOException {
        Preconditions.checkState(serializer != null, "serializer not set");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
        try {
            for (T element : elements) {
                serializer.serialize(element, wrapper);
            }
        } catch (Exception e) {
            throw new IOException("Serializing the source elements failed: " + e.getMessage(), e);
        }
        this.elementsSerialized = baos.toByteArray();
    }

    public static class CollectionSplitSerializer<T> implements SimpleVersionedSerializer<CollectionSplit<T>> {

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(CollectionSplit<T> obj) throws IOException {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            try (ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream)) {
                outputStream.writeObject(obj);
            }
            return byteArrayOutputStream.toByteArray();
        }

        @Override
        public CollectionSplit<T> deserialize(int version, byte[] serialized) throws IOException {
            try (ObjectInputStream inputStream =
                         new ObjectInputStream(new ByteArrayInputStream(serialized))) {
                return (CollectionSplit<T>) inputStream.readObject();
            } catch (ClassNotFoundException exc) {
                throw new IOException("Failed to deserialize CollectionSplit", exc);
            }
        }
    }
}
