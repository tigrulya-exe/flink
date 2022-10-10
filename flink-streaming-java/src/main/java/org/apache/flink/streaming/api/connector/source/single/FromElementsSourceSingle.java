package org.apache.flink.streaming.api.connector.source.single;

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
import java.util.Collections;
import java.util.List;

/** todo. */
public class FromElementsSourceSingle<T>
        implements Source<T, ElementsSplit<T>, Collection<ElementsSplit<T>>> {
    private final transient Iterable<T> elements;

    private byte[] serializedElements;

    private final TypeSerializer<T> serializer;

    private final int elementsCount;

    public FromElementsSourceSingle(TypeSerializer<T> serializer, Iterable<T> elements) {
        this.elements = Preconditions.checkNotNull(elements);
        this.serializer = Preconditions.checkNotNull(serializer);
        this.elementsCount = Iterables.size(elements);
        this.serializedElements = serializeElements(elements);
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
        ElementsSplit<T> split = new ElementsSplit<>(serializedElements, elementsCount, serializer);
        return new IteratorSourceEnumerator<>(enumContext, Collections.singletonList(split));
    }

    @Override
    public SplitEnumerator<ElementsSplit<T>, Collection<ElementsSplit<T>>> restoreEnumerator(
            SplitEnumeratorContext<ElementsSplit<T>> enumContext,
            Collection<ElementsSplit<T>> restoredSplits)
            throws Exception {
        Preconditions.checkArgument(restoredSplits.size() == 1);

        ElementsSplit<T> split =
                new ElementsSplit<>(
                        serializedElements,
                        elementsCount,
                        serializer,
                        restoredSplits.iterator().next().getCurrentOffset());
        return new IteratorSourceEnumerator<>(enumContext, Collections.singletonList(split));
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
