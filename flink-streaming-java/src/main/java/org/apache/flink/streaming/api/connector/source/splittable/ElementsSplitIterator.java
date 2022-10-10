package org.apache.flink.streaming.api.connector.source.splittable;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

/** todo. */
public class ElementsSplitIterator<E> implements Iterator<E> {

    private final int elementsCount;
    private final TypeSerializer<E> serializer;

    private final DataInputViewStreamWrapper serializedElementsStream;

    private int currentOffset;

    public ElementsSplitIterator(
            int elementsCount, byte[] serializedElements, TypeSerializer<E> serializer) {
        this(elementsCount, serializedElements, serializer, 0);
    }

    public ElementsSplitIterator(
            int elementsCount,
            byte[] serializedElements,
            TypeSerializer<E> serializer,
            int currentOffset) {
        this.elementsCount = elementsCount;
        this.serializer = serializer;
        this.currentOffset = currentOffset;
        this.serializedElementsStream =
                new DataInputViewStreamWrapper(new ByteArrayInputStream(serializedElements));
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
            throw new RuntimeException(
                    "Failed to deserialize an element from the source. "
                            + "If you are using user-defined serialization (Value and Writable types), check the "
                            + "serialization functions.\nSerializer is "
                            + serializer,
                    e);
        }
    }

    public int getCurrentOffset() {
        return currentOffset;
    }
}
