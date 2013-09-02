package CassandraHector;

import java.util.Iterator;
import java.util.List;

import me.prettyprint.cassandra.model.QuorumAllConsistencyLevelPolicy;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

public class Dumper
{
	private final Cluster cluster;
	private final Keyspace keyspace;

	public Dumper()
	{
		this.cluster = HFactory.getOrCreateCluster("test-cluster", "localhost:9160");
		this.keyspace = HFactory.createKeyspace("write_test", cluster,
				new QuorumAllConsistencyLevelPolicy());
	}

	public void close()
	{
		cluster.getConnectionManager().shutdown();
	}

	// retrieve message by iterator
	public void run1()
	{
		int row_count = 10000;
		int count = 0;
		RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory
				.createRangeSlicesQuery(keyspace, StringSerializer.get(), StringSerializer.get(),
						StringSerializer.get()).setColumnFamily("Attribute")
				.setRange(null, null, false, 999999).setRowCount(row_count);
		// .setRange(null, null, false, 10).setRowCount(row_count);

		String last_key = null;

		while (true)
		{
			rangeSlicesQuery.setKeys(last_key, null);
			System.out.println(" > " + last_key);
			System.out.println(" >> The count " + count);
			QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
			OrderedRows<String, String, String> rows = result.get();
			Iterator<Row<String, String, String>> rowsIterator = rows.iterator();

			// we'll skip this first one, since it is the same as the last one
			// from previous time we executed
			if (last_key != null && rowsIterator != null)
				rowsIterator.next();
			if (!rowsIterator.hasNext())
				break;
			while (rowsIterator.hasNext())
			{
				Row<String, String, String> row = rowsIterator.next();
				last_key = row.getKey();
				System.out.println(" > " + last_key);
				if (row.getColumnSlice().getColumns().isEmpty())
				{
					continue;
				}
				count++;
				// System.out.println(row);
				// if (count % 1000 == 0)
				System.out.println(count);
			}

		}
	}

	// retrieve message in batch all
	public void run2()
	{
		int row_count = 19000;
		int count = 0;
		RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory
				.createRangeSlicesQuery(keyspace, StringSerializer.get(), StringSerializer.get(),
						StringSerializer.get()).setColumnFamily("Attribute")
				.setRange(null, null, false, 99).setRowCount(row_count);

		QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();

		OrderedRows rows = result.get();
		List<Row> rowlist = rows.getList();
		for (Row row : rowlist)
		{
			// System.out.println(row);

		}

		System.out.println(rowlist.size());
	}

	private void run3()
	{

		String lastKeyForMissing = "";
		StringSerializer s = StringSerializer.get();
		RangeSlicesQuery<String, String, String> allRowsQuery = HFactory.createRangeSlicesQuery(
				keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
		allRowsQuery.setColumnFamily("Attribute");
		allRowsQuery.setRange("", "", false, 99); // retrieve 3 columns, no
		System.out.println(1);
		// reverse
		// allRowsQuery.setReturnKeysOnly(); //enable this line if we want key
		// only

		int rowCnt = 0;
		int total = 15000;
		allRowsQuery.setRowCount(total);
		while (rowCnt < total)
		{
			allRowsQuery.setKeys(lastKeyForMissing, "");
			QueryResult<OrderedRows<String, String, String>> res = allRowsQuery.execute();
			// System.out.println(rowCnt);
			OrderedRows<String, String, String> rows = res.get();
			lastKeyForMissing = rows.peekLast().getKey();
			for (Row<String, String, String> aRow : rows)
			{

			}
			rowCnt++;

		}
		System.out.println(rowCnt);

	}

	public static void main(String[] args)
	{

		Long startTime = System.nanoTime();
		new Dumper().run1();
		Long endTime = System.nanoTime();
		System.out.println("Total time is: " + (endTime - startTime) / 1e9);
	}
}