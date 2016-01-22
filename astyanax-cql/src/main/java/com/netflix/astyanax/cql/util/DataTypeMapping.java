package com.netflix.astyanax.cql.util;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Row;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

public class DataTypeMapping {

	// TODO implement complex types
	public static <T> Object getDynamicColumn(Row row, String columnName, DataType dataType) {

		switch(dataType.getName()) {

		case LIST:
		    throw new UnsupportedOperationException("Collection objects not supported for column: " + columnName);
		case SET:
		    throw new UnsupportedOperationException("Collection objects not supported for column: " + columnName);
		case MAP:
			List<DataType> types = dataType.getTypeArguments();
			return row.getMap(columnName, getTypeClass(types.get(0)), getTypeClass(types.get(1)));

		default:
			return row.get(columnName, getTypeClass(dataType));
		}
	}

	private static Class<?> getTypeClass(DataType dataType) {
		switch(dataType.getName()) {

			case ASCII:
				return String.class;
			case BIGINT:
				return Long.class;
			case BLOB:
				return ByteBuffer.class;
			case BOOLEAN:
				return Boolean.class;
			case COUNTER:
				return Long.class;
			case DECIMAL:
				return BigDecimal.class;
			case DOUBLE:
				return Double.class;
			case FLOAT:
				return Float.class;
			case INET:
				return InetAddress.class;
			case INT:
				return Integer.class;
			case TEXT:
				return String.class;
			case TIMESTAMP:
				return LocalDate.class;
			case UUID:
				return UUID.class;
			case VARCHAR:
				return String.class;
			case VARINT:
				return Long.class;
			case TIMEUUID:
				return UUID.class;
			case LIST:
				return List.class;
			case SET:
				return Set.class;
			case MAP:
				return Map.class;

			default:
				throw new UnsupportedOperationException("Unrecognized object for DataType: " + dataType.getName());
		}
	}

}
