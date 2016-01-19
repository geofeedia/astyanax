package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;

/**
 * Serializer implementation for generic lists.
 * 
 * @author vermes
 * 
 * @param <T>
 *            element type
 */
public class ListSerializer<T> extends AbstractSerializer<List<T>> {

    private final ListType<T> myList;

    /**
     * @param elements
     * @param isMultiCell
     */
    public ListSerializer(AbstractType<T> elements, boolean isMultiCell) {
        myList = ListType.getInstance(elements, isMultiCell);
    }

    @Override
    public List<T> fromByteBuffer(ByteBuffer arg0) {
        if (arg0 ==  null) return null;
        ByteBuffer dup = arg0.duplicate();
        return myList.compose(dup);
    }

    @Override
    public ByteBuffer toByteBuffer(List<T> arg0) {
        return arg0 == null ? null : myList.decompose(arg0);
    }
}