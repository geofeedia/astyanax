package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.SetType;

/**
 * Serializer implementation for generic sets.
 * 
 * @author vermes
 * 
 * @param <T>
 *            element type
 */
public class SetSerializer<T> extends AbstractSerializer<Set<T>> {

    private final SetType<T> mySet;

    /**
     * @param elements
     * @param isMultiCell
     */
    public SetSerializer(AbstractType<T> elements, boolean isMultiCell) {
        mySet = SetType.getInstance(elements, isMultiCell);
    }

    @Override
    public Set<T> fromByteBuffer(ByteBuffer arg0) {
        return arg0 == null ? null : mySet.compose(arg0);
    }

    @Override
    public ByteBuffer toByteBuffer(Set<T> arg0) {
        return arg0 == null ? null : mySet.decompose(arg0);
    }
}