package org.apache.flink.streaming.api.connector.source.splittable;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReader;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/** todo. */
public class FromElementsSource<T>
        implements Source<T, ElementsSplit<T>, Collection<ElementsSplit<T>>> {
    private final transient Iterable<T> elements;

    private final TypeSerializer<T> serializer;

    private final int elementsCount;

    public FromElementsSource(TypeSerializer<T> serializer, Iterable<T> elements) {
        this.elements = Preconditions.checkNotNull(elements);
        this.serializer = Preconditions.checkNotNull(serializer);
        this.elementsCount = Iterables.size(elements);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<T, ElementsSplit<T>> createReader(SourceReaderContext readerContext)
            throws Exception {
        return new IteratorSourceReader<>(readerContext);
    }

    @Override
    public SplitEnumerator<ElementsSplit<T>, Collection<ElementsSplit<T>>> createEnumerator(
            SplitEnumeratorContext<ElementsSplit<T>> enumContext) throws Exception {
        Collection<ElementsSplit<T>> splits =
                createSplits(elements, enumContext.currentParallelism());
        return new IteratorSourceEnumerator<>(enumContext, splits);
    }

    @Override
    public SplitEnumerator<ElementsSplit<T>, Collection<ElementsSplit<T>>> restoreEnumerator(
            SplitEnumeratorContext<ElementsSplit<T>> enumContext,
            Collection<ElementsSplit<T>> checkpoint)
            throws Exception {
        return new IteratorSourceEnumerator<>(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<ElementsSplit<T>> getSplitSerializer() {
        return new ElementsSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<Collection<ElementsSplit<T>>>
            getEnumeratorCheckpointSerializer() {
        return new ElementsSplitsSerializer<>();
    }

    private Collection<ElementsSplit<T>> createSplits(Iterable<T> collection, int numSplits) {
        int chunkSize = elementsCount / numSplits;
        int remainder = elementsCount % numSplits;

        List<ElementsSplit<T>> collectionSplits = new ArrayList<>(numSplits);

        Iterator<T> collectionIter = collection.iterator();
        for (int splitId = 0; splitId < numSplits; ++splitId) {
            int currentChunkSize = splitId < remainder ? chunkSize : chunkSize + 1;

            List<T> chunk = new ArrayList<>(currentChunkSize);
            for (int i = 0; i < currentChunkSize; ++i) {
                chunk.add(collectionIter.next());
            }
            collectionSplits.add(
                    new ElementsSplit<>(serializeElements(chunk), currentChunkSize, serializer));
        }

        return collectionSplits;
    }

    private byte[] serializeElements(Iterable<T> elements) {
        Preconditions.checkState(serializer != null, "serializer not set");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
        try {
            for (T element : elements) {
                serializer.serialize(element, wrapper);
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Serializing the source elements failed: " + e.getMessage(), e);
        }
        return baos.toByteArray();
    }

    /** todo. */
    public static class ElementsSplitSerializer<E>
            implements SimpleVersionedSerializer<ElementsSplit<E>> {
        private static final int VERSION = 1;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(ElementsSplit<E> obj) throws IOException {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            try (ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream)) {
                outputStream.writeObject(obj);
            }
            return byteArrayOutputStream.toByteArray();
        }

        @Override
        public ElementsSplit<E> deserialize(int version, byte[] serialized) throws IOException {
            Preconditions.checkArgument(version == VERSION);
            try (ObjectInputStream inputStream =
                    new ObjectInputStream(new ByteArrayInputStream(serialized))) {
                return (ElementsSplit<E>) inputStream.readObject();
            } catch (ClassNotFoundException exc) {
                throw new IOException("Failed to deserialize FromElementsSplit", exc);
            }
        }
    }

    /** todo. */
    public static class ElementsSplitsSerializer<E>
            implements SimpleVersionedSerializer<Collection<ElementsSplit<E>>> {
        private static final int VERSION = 1;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(Collection<ElementsSplit<E>> splits) throws IOException {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            try (ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream)) {
                outputStream.writeInt(splits.size());
                for (ElementsSplit<E> split : splits) {
                    outputStream.writeObject(split);
                }
            }
            return byteArrayOutputStream.toByteArray();
        }

        @Override
        public Collection<ElementsSplit<E>> deserialize(int version, byte[] serialized)
                throws IOException {
            Preconditions.checkArgument(version == VERSION);
            try (ObjectInputStream inputStream =
                    new ObjectInputStream(new ByteArrayInputStream(serialized))) {
                int size = inputStream.readInt();
                List<ElementsSplit<E>> splits = new ArrayList<>();

                while (size > 0) {
                    splits.add((ElementsSplit<E>) inputStream.readObject());
                    --size;
                }
                return splits;
            } catch (ClassNotFoundException exc) {
                throw new IOException("Failed to deserialize FromElementsSplit", exc);
            }
        }
    }
}
