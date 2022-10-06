package org.apache.flink.streaming.api.connector.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;

/** todo. */
public class FromElementsSource<T> implements Source<T, FromElementsSplit, FromElementsSplit> {

    /** The actual data elements, in serialized form. */
    private byte[] elementsSerialized;

    private final transient Iterable<T> elements;
    private final TypeSerializer<T> serializer;
    private final int elementsCount;

    public FromElementsSource(TypeSerializer<T> serializer, Collection<T> collection)
            throws IOException {
        this.elements = Preconditions.checkNotNull(collection);
        this.serializer = Preconditions.checkNotNull(serializer);
        this.elementsCount = Iterables.size(elements);
        serializeElements();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<T, FromElementsSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        return new FromElementsSourceReader(
                readerContext, serializer, elementsSerialized, elementsCount);
    }

    @Override
    public SplitEnumerator<FromElementsSplit, FromElementsSplit> createEnumerator(
            SplitEnumeratorContext<FromElementsSplit> enumContext) throws Exception {
        FromElementsSplit startSplit = new FromElementsSplit(0);
        return new FromElementsSplitEnumerator(enumContext, startSplit);
    }

    @Override
    public SplitEnumerator<FromElementsSplit, FromElementsSplit> restoreEnumerator(
            SplitEnumeratorContext<FromElementsSplit> enumContext, FromElementsSplit restoredSplits)
            throws Exception {
        return new FromElementsSplitEnumerator(enumContext, restoredSplits);
    }

    @Override
    public SimpleVersionedSerializer<FromElementsSplit> getSplitSerializer() {
        return new FromElementsSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<FromElementsSplit> getEnumeratorCheckpointSerializer() {
        return new FromElementsSplitSerializer();
    }

    //    private Collection<FromElementsSplit> createSplits(Collection<T> collection, int
    // numSplits) {
    //        int chunkSize = collection.size() / numSplits;
    //        int remainder = collection.size() % numSplits;
    //
    //        List<FromElementsSplit> collectionSplits = new ArrayList<>(numSplits);
    //
    //        Iterator<T> collectionIter = collection.iterator();
    //        for (int splitId = 0; splitId < numSplits; ++splitId) {
    //            int currentChunkSize = splitId < remainder ? chunkSize : chunkSize + 1;
    //
    //            List<T> chunk = new ArrayList<>(currentChunkSize);
    //            for (int i = 0; i < currentChunkSize; ++i) {
    //                chunk.add(collectionIter.next());
    //            }
    //            collectionSplits.add(new CollectionSplit<>(chunk));
    //        }
    //
    //        return collectionSplits;
    //    }

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

    /** todo. */
    public static class FromElementsSplitSerializer
            implements SimpleVersionedSerializer<FromElementsSplit> {

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(FromElementsSplit obj) throws IOException {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            try (ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream)) {
                outputStream.writeObject(obj);
            }
            return byteArrayOutputStream.toByteArray();
        }

        @Override
        public FromElementsSplit deserialize(int version, byte[] serialized) throws IOException {
            try (ObjectInputStream inputStream =
                    new ObjectInputStream(new ByteArrayInputStream(serialized))) {
                return (FromElementsSplit) inputStream.readObject();
            } catch (ClassNotFoundException exc) {
                throw new IOException("Failed to deserialize FromElementsSplit", exc);
            }
        }
    }
}
