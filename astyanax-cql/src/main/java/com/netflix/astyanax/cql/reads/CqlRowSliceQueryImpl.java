package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.desc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.reads.model.CqlColumnSlice;
import com.netflix.astyanax.cql.reads.model.CqlRangeBuilder;
import com.netflix.astyanax.cql.reads.model.CqlRangeImpl;
import com.netflix.astyanax.cql.reads.model.CqlRowListImpl;
import com.netflix.astyanax.cql.reads.model.CqlRowSlice;
import com.netflix.astyanax.cql.reads.model.CqlRowSlice.RowRange;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.cql.writes.CqlColumnListMutationImpl.ColumnFamilyMutationContext;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.RowSliceColumnCountQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.CompositeRangeBuilder;
import com.netflix.astyanax.serializers.CompositeRangeBuilder.CompositeByteBufferRange;
import com.netflix.astyanax.serializers.CompositeRangeBuilder.RangeQueryOp;
import com.netflix.astyanax.serializers.CompositeRangeBuilder.RangeQueryRecord;

@SuppressWarnings("unchecked")
public class CqlRowSliceQueryImpl<K, C> implements RowSliceQuery<K, C> {

	private final KeyspaceContext ksContext;
	private final ColumnFamilyMutationContext<K,C> cfContext;

	private final CqlRowSlice<K> rowSlice;
	private CqlColumnSlice<C> columnSlice = new CqlColumnSlice<C>();
	private CompositeByteBufferRange compositeRange = null;
	
	public CqlRowSliceQueryImpl(KeyspaceContext ksCtx, ColumnFamilyMutationContext<K,C> cfCtx, CqlRowSlice<K> rSlice) {
		this.ksContext = ksCtx;
		this.cfContext = cfCtx;
		this.rowSlice = rSlice;
	}
	
	@Override
	public OperationResult<Rows<K, C>> execute() throws ConnectionException {
		return new InternalRowQueryExecutionImpl().execute();
	}

	@Override
	public ListenableFuture<OperationResult<Rows<K, C>>> executeAsync() throws ConnectionException {
		return new InternalRowQueryExecutionImpl().executeAsync();
	}
	
	@Override
	public RowSliceQuery<K, C> withColumnSlice(C... columns) {
		return withColumnSlice(Arrays.asList(columns));
	}

	@Override
	public RowSliceQuery<K, C> withColumnSlice(Collection<C> columns) {
		this.columnSlice = new CqlColumnSlice<C>(columns);
		return this;
	}

	@Override
	public RowSliceQuery<K, C> withColumnSlice(ColumnSlice<C> columns) {
		this.columnSlice = new CqlColumnSlice<C>(columns);
		return this;
	}

	@Override
	public RowSliceQuery<K, C> withColumnRange(C startColumn, C endColumn, boolean reversed, int count) {
		this.columnSlice = new CqlColumnSlice<C>(new CqlRangeBuilder<C>()
				.setColumn("column1")
				.setStart(startColumn)
				.setEnd(endColumn)
				.setReversed(reversed)
				.setLimit(count)
				.build());
		return this;
	}

	@Override
	public RowSliceQuery<K, C> withColumnRange(ByteBuffer startColumn, ByteBuffer endColumn, boolean reversed, int limit) {
		Serializer<C> colSerializer = cfContext.getColumnFamily().getColumnSerializer();
		C start = (startColumn != null && startColumn.capacity() > 0) ? colSerializer.fromByteBuffer(startColumn) : null;
		C end = (endColumn != null && endColumn.capacity() > 0) ? colSerializer.fromByteBuffer(endColumn) : null;
		return this.withColumnRange(start, end, reversed, limit);
	}

	@Override
	public RowSliceQuery<K, C> withColumnRange(ByteBufferRange range) {
		
		if (range instanceof CompositeByteBufferRange) {
			this.compositeRange = (CompositeByteBufferRange) range;
			
		} else if (range instanceof CompositeRangeBuilder) {
			this.compositeRange = ((CompositeRangeBuilder)range).build();
			
		} else if (range instanceof CqlRangeImpl) {
			this.columnSlice.setCqlRange((CqlRangeImpl<C>) range);
		} else {
			return this.withColumnRange(range.getStart(), range.getEnd(), range.isReversed(), range.getLimit());
		}
		return this;
	}

	@Override
	public RowSliceColumnCountQuery<K> getColumnCounts() {
		Query query = new InternalRowQueryExecutionImpl().getQuery();
		return new CqlRowSliceColumnCountQueryImpl<K>(ksContext, cfContext, query);
	}
	
	private class InternalRowQueryExecutionImpl extends CqlAbstractExecutionImpl<Rows<K, C>> {

		private final CqlColumnFamilyDefinitionImpl cfDef = (CqlColumnFamilyDefinitionImpl) cf.getColumnFamilyDefinition();
		private final String keyColumnAlias = cfDef.getPrimaryKeyColumnDefinition().getName();
		private final String[] allColumnNames = cfDef.getAllColumnNames();
		private final List<ColumnDefinition> pkCols = cfDef.getPartitionKeyColumnDefinitionList();

		public InternalRowQueryExecutionImpl() {
			super(ksContext, cfContext);
		}

		@Override
		public Query getQuery() {

			if (rowSlice.isCollectionQuery()) {
				
				if (compositeRange != null) {
					return selectCompositeColumnRangeForRowKeys(rowSlice.getKeys(), compositeRange);
				}
				
				switch(columnSlice.getQueryType()) {
				case SELECT_ALL:
					return selectAllColumnsForRowKeys(rowSlice.getKeys());
				case COLUMN_COLLECTION:
					return selectColumnSetForRowKeys(rowSlice.getKeys(), columnSlice.getColumns());
				case COLUMN_RANGE:
					return selectColumnRangeForRowKeys(rowSlice.getKeys(), columnSlice);
				default:
					throw new IllegalStateException();
				}
			} else {
				
				if (compositeRange != null) {
					return selectCompositeColumnRangeForRowRange(rowSlice.getRange(), compositeRange);
				}

				switch(columnSlice.getQueryType()) {
				case SELECT_ALL:
					return selectAllColumnsForRowRange(rowSlice.getRange());
				case COLUMN_COLLECTION:
					return selectColumnSetForRowRange(rowSlice.getRange(), columnSlice.getColumns());
				case COLUMN_RANGE:
					return selectColumnRangeForRowRange(rowSlice.getRange(), columnSlice);
				default:
					throw new IllegalStateException();
				}
			}
		}

		@Override
		public Rows<K, C> parseResultSet(ResultSet rs) {
			
			List<com.datastax.driver.core.Row> rows = rs.all();
			if (rows == null || rows.isEmpty()) {
				throw new RuntimeException("Empty result set");
			}
			return new CqlRowListImpl<K, C>(rows, (ColumnFamily<K, C>) cf);
		}

		@Override
		public CassandraOperationType getOperationType() {
			return CassandraOperationType.GET_ROW;
		}
		
		private Query selectAllColumnsForRowKeys(Collection<K> rowKeys) {
				return QueryBuilder.select(allColumnNames)
						.from(keyspace, cf.getName())
						.where(in(keyColumnAlias, rowKeys.toArray()));
			}
			
		private Query selectColumnRangeForRowKeys(Collection<K> rowKeys, CqlColumnSlice<C> columnSlice) {
				Where where = QueryBuilder.select().all()
						.from(keyspace, cf.getName())
						.where(in(keyColumnAlias, rowKeys.toArray()));
				where = addWhereClauseForColumn(where, columnSlice);
				return where;
			}

		private Query selectColumnSetForRowKeys(Collection<K> rowKeys, Collection<C> cols) {
					
				if (pkCols.size() == 1) {

					// THIS IS A SIMPLE QUERY WHERE THE INDIVIDUAL COLS ARE BEING SELECTED E.G NAME, AGE ETC
					Select.Selection select = QueryBuilder.select();
					select.column(keyColumnAlias);

					for (C col : cols) {
						select.column((String)col);
					}

					return select.from(keyspace, cf.getName()).where(in(keyColumnAlias, rowKeys.toArray()));

				} else if (pkCols.size() == 2) {

					// THIS IS A QUERY WHERE THE COLUMN NAME IS DYNAMIC  E.G TIME SERIES
					Object[] columns = cols.toArray(new Object[cols.size()]); 

					String pkColName = pkCols.get(1).getName();

					return QueryBuilder.select(allColumnNames)
							.from(keyspace, cf.getName())
							.where(in(keyColumnAlias, rowKeys.toArray()))
							.and(in(pkColName, columns));
				} else {
					throw new RuntimeException("Composite col query - todo");
				}
			}

		private Query selectCompositeColumnRangeForRowKeys(Collection<K> rowKeys, CompositeByteBufferRange compositeRange) {
				
				Where stmt = QueryBuilder.select(allColumnNames)
							.from(keyspace, cf.getName())
							.where(in(keyColumnAlias, rowKeys.toArray()));

				stmt = addWhereClauseForCompositeColumnRange(stmt, compositeRange);
				return stmt;
			}

		private Query selectAllColumnsForRowRange(RowRange<K> range) {

				Select select = QueryBuilder.select(allColumnNames)
						.from(keyspace, cf.getName());
				return addWhereClauseForRowKey(keyColumnAlias, select, range);
			}

		private Query selectColumnSetForRowRange(RowRange<K> range, Collection<C> cols) {

				if (pkCols.size() == 1) {

					// THIS IS A SIMPLE QUERY WHERE THE INDIVIDUAL COLS ARE BEING SELECTED E.G NAME, AGE ETC
					Select.Selection select = QueryBuilder.select();
					select.column(keyColumnAlias);

					for (C col : cols) {
						select.column((String)col);
					}

					Select select2 = select.from(keyspace, cf.getName());
					Where where = addWhereClauseForRowKey(keyColumnAlias, select2, range);
					return where;

				} else if (pkCols.size() == 2) {

					// THIS IS A QUERY WHERE THE COLUMN NAME IS DYNAMIC  E.G TIME SERIES

					String pkColName = pkCols.get(1).getName();
					Object[] columns = cols.toArray(new Object[cols.size()]); 

					Select select = QueryBuilder.select(allColumnNames).from(keyspace, cf.getName());
					if (columns != null && columns.length > 0) {
						select.allowFiltering();
					}
					Where where = addWhereClauseForRowKey(keyColumnAlias, select, range);
					where.and(in(pkColName, columns));
				
					return where;
				} else {
					throw new RuntimeException("Invalid row slice query combination");
				}
			}

		private Query selectColumnRangeForRowRange(RowRange<K> range, CqlColumnSlice<C> columnSlice) {

				Select select = QueryBuilder.select().all().from(keyspace, cf.getName());
				if (columnSlice != null && columnSlice.isRangeQuery()) {
					select.allowFiltering();
				}

				Where where = addWhereClauseForRowKey(keyColumnAlias, select, range);			
				where = addWhereClauseForColumn(where, columnSlice);
				return where;
			}

		private Query selectCompositeColumnRangeForRowRange(RowRange<K> range, CompositeByteBufferRange compositeRange) {

				Select select = QueryBuilder.select().all().from(keyspace, cf.getName());
				if (compositeRange != null) {
					select.allowFiltering();
				}

				Where where = addWhereClauseForRowKey(keyColumnAlias, select, range);	
				where = addWhereClauseForCompositeColumnRange(where, compositeRange);
				return where;
			}
			
			private Where addWhereClauseForColumn(Where where, CqlColumnSlice<C> columnSlice) {

				String pkColName = pkCols.get(1).getName();

				if (!columnSlice.isRangeQuery()) {
					return where;
				}
				if (columnSlice.getStartColumn() != null) {
					where.and(gte(pkColName, columnSlice.getStartColumn()));
				}
				if (columnSlice.getEndColumn() != null) {
					where.and(lte(pkColName, columnSlice.getEndColumn()));
				}

				if (columnSlice.getReversed()) {
					where.orderBy(desc(pkColName));
				}

				if (columnSlice.getLimit() != -1) {
					where.limit(columnSlice.getLimit());
				}

				return where;
			}
			
			private Where addWhereClauseForCompositeColumnRange(Where stmt, CompositeByteBufferRange compositeRange) {

				List<RangeQueryRecord> records = compositeRange.getRecords();
				int componentIndex = 1; 

				for (RangeQueryRecord record : records) {

					for (RangeQueryOp op : record.getOps()) {

						String columnName = pkCols.get(componentIndex).getName();

						switch (op.getOperator()) {

						case EQUAL:
							stmt.and(eq(columnName, op.getValue()));
							componentIndex++;
							break;
						case LESS_THAN :
							stmt.and(lt(columnName, op.getValue()));
							break;
						case LESS_THAN_EQUALS:
							stmt.and(lte(columnName, op.getValue()));
							break;
						case GREATER_THAN:
							stmt.and(gt(columnName, op.getValue()));
							break;
						case GREATER_THAN_EQUALS:
							stmt.and(gte(columnName, op.getValue()));
							break;
						default:
							throw new RuntimeException("Cannot recognize operator: " + op.getOperator().name());
						}; // end of switch stmt
					} // end of inner for for ops for each range query record
				}
				return stmt;
			}

			private Where addWhereClauseForRowKey(String keyAlias, Select select, RowRange<K> rowRange) {

				Where where = null;

				boolean keyIsPresent = false;
				boolean tokenIsPresent = false; 
				
				if (rowRange.getStartKey() != null || rowRange.getEndKey() != null) {
					keyIsPresent = true;
				}
				if (rowRange.getStartToken() != null || rowRange.getEndToken() != null) {
					tokenIsPresent = true;
				}
				
				if (keyIsPresent && tokenIsPresent) {
					throw new RuntimeException("Cannot provide both token and keys for range query");
				}
				
				if (keyIsPresent) {
					if (rowRange.getStartKey() != null && rowRange.getEndKey() != null) {

						where = select.where(gte(keyAlias, rowRange.getStartKey()))
								.and(lte(keyAlias, rowRange.getEndKey()));

					} else if (rowRange.getStartKey() != null) {				
						where = select.where(gte(keyAlias, rowRange.getStartKey()));

					} else if (rowRange.getEndKey() != null) {
						where = select.where(lte(keyAlias, rowRange.getEndKey()));
					}
					
				} else if (tokenIsPresent) {
					String tokenOfKey ="token(" + keyAlias + ")";

					BigInteger startToken = rowRange.getStartToken() != null ? new BigInteger(rowRange.getStartToken()) : null; 
					BigInteger endToken = rowRange.getEndToken() != null ? new BigInteger(rowRange.getEndToken()) : null; 
					
					if (startToken != null && endToken != null) {

						where = select.where(gte(tokenOfKey, startToken))
										.and(lte(tokenOfKey, endToken));

					} else if (startToken != null) {
						where = select.where(gte(tokenOfKey, startToken));

					} else if (endToken != null) {
						where = select.where(lte(tokenOfKey, endToken));
					}
					
				} else { 
					where = select.where();
				}

				if (rowRange.getCount() > 0) {
					where.limit(rowRange.getCount());
				}

				return where; 
			}

		
	}
}


