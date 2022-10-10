package org.apache.flink.streaming.api.connector.source;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;

/** todo. */
public class ElementsSplit<E> implements IteratorSourceSplit<E, Iterator<E>>, Serializable {
    private static final String SPLIT_ID = "0";

    private final byte[] serializedData;
    private final TypeSerializer<E> serializer;
    private final int elementsCount;

    private int currentOffset;

    public ElementsSplit(byte[] serializedData, int elementsCount, TypeSerializer<E> serializer) {
        this(serializedData, elementsCount, serializer, 0);
    }

    public ElementsSplit(
            byte[] serializedData,
            int elementsCount,
            TypeSerializer<E> serializer,
            int currentOffset) {
        this.serializer = serializer;
        this.elementsCount = elementsCount;
        this.serializedData = serializedData;
        this.currentOffset = currentOffset;
    }

    @Override
    public String splitId() {
        return SPLIT_ID;
    }

    @Override
    public Iterator<E> getIterator() {
        return new ElementsSplitIterator(currentOffset);
    }

    @Override
    public IteratorSourceSplit<E, Iterator<E>> getUpdatedSplitForIterator(Iterator<E> iterator) {
        return new ElementsSplit<>(serializedData, elementsCount, serializer, currentOffset);
    }

    public int getCurrentOffset() {
        return currentOffset;
    }

    public byte[] getSerializedData() {
        return serializedData;
    }

    public TypeSerializer<E> getSerializer() {
        return serializer;
    }

    public int getElementsCount() {
        return elementsCount;
    }

    /** todo. */
    public class ElementsSplitIterator implements Iterator<E> {
        private final DataInputViewStreamWrapper serializedElementsStream;

        public ElementsSplitIterator(int currentOffset) {
            this.serializedElementsStream =
                    new DataInputViewStreamWrapper(new ByteArrayInputStream(serializedData));
            skipElements(currentOffset);
        }

        @Override
        public boolean hasNext() {
            return currentOffset < elementsCount;
        }

        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            try {
                E record = serializer.deserialize(serializedElementsStream);
                ++currentOffset;
                return record;
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize an element from the source.", e);
            }
        }

        private void skipElements(int elementsToSkip) {
            int toSkip = elementsToSkip;
            try {
                serializedElementsStream.reset();
                while (toSkip > 0) {
                    serializer.deserialize(serializedElementsStream);
                    toSkip--;
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize an element from the source.", e);
            }
        }

        public int getCurrentOffset() {
            return currentOffset;
        }
    }
}
