import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import parsing.ParseMSG;
import parsing.kvPair;
import CassandraHector.CassandraConnect;

public class test_hector
{
	static Cluster cluster;
	static CassandraConnect cdc;

	public static void main(String[] args)
	{
		Long startTime = System.nanoTime();
		cassandraWriteTest();
		Long endTime = System.nanoTime();
		System.out.println("Total time is: " + (endTime - startTime) / 1e9);
	}

	private static void cassandraWriteTest()
	{
		String filename = "/home/user/workspace/data/msg.txt";
		cdc = new CassandraConnect();
		insertMSGfromFile(filename);
	}

	private static void insertMSGfromFile(String filename)
	{
		BufferedReader br = null;

		try
		{
			String sCurrentLine;
			br = new BufferedReader(new FileReader(filename));
			int count = 0;
			while ((sCurrentLine = br.readLine()) != null)
			{
				// System.out.println(sCurrentLine);
				if (!sCurrentLine.startsWith("<Message:"))
					continue;
				if (count % 1000 == 0)
					System.out.println(count);
				if (count >= 100000)
					break;

				try
				{
					List<kvPair> lmsg = ParseMSG.parseString(sCurrentLine);
					cdc.insert1row(lmsg);
					// insertMSG2idxtable(lmsg, idxfamiliy, idxAttributes);
				}
				catch (Exception ex)
				{
					System.out.println(sCurrentLine);
				}
				count = count + 1;
			}
			System.out.println(count);
			System.out.println("Done!");

		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				if (br != null)
					br.close();
			}
			catch (IOException ex)
			{
				ex.printStackTrace();
			}
		}

	}

	private static void test()
	{
		Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9160");
		Keyspace keyspace = HFactory.createKeyspace("NOAH", cluster);

		RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory
				.createRangeSlicesQuery(keyspace, StringSerializer.get(), StringSerializer.get(),
						StringSerializer.get()).setColumnFamily("Users")
				.setRange(null, null, false, 10);

		rangeSlicesQuery.setKeys(null, null);

		QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
		OrderedRows<String, String, String> rows = result.get();
		Iterator<Row<String, String, String>> rowsIterator = rows.iterator();

		while (rowsIterator.hasNext())
		{
			Row<String, String, String> row = rowsIterator.next();

			if (row.getColumnSlice().getColumns().isEmpty())
			{
				continue;
			}

			System.out.println(row);
		}

	}

	private static void test2()
	{
		cluster = HFactory.getOrCreateCluster("test-cluster", "localhost:9160");
		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace("MyKeyspace");

		// If keyspace does not exist, the CFs don't exist either. => create
		// them.
		if (keyspaceDef == null)
		{
			createSchema();
		}
		Keyspace ksp = HFactory.createKeyspace("MyKeyspace", cluster);
		String columnFamily = "cf";
		ColumnFamilyTemplate<String, String> template = new ThriftColumnFamilyTemplate<String, String>(
				ksp, columnFamily, StringSerializer.get(), StringSerializer.get());

		// <String, String> correspond to key and Column name.
		ColumnFamilyUpdater<String, String> updater = template.createUpdater("a key");
		updater.setString("domain", "www.datastax.com");
		updater.setLong("time", System.currentTimeMillis());

		try
		{
			template.update(updater);
		}
		catch (HectorException e)
		{
			// do something ...
		}
	}

	private static void test3()
	{
		Cluster tutorialCluster = HFactory.getOrCreateCluster("Test Cluster", "127.0.0.1:9160");
		ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();

		ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);

		Keyspace tutorialKeyspace = HFactory.createKeyspace("MyKeyspace", tutorialCluster, ccl);
		Mutator<String> mutator = HFactory.createMutator(tutorialKeyspace, StringSerializer.get());

		mutator.addInsertion("CA Burlingame", "ColumnFamilyName",
				HFactory.createStringColumn("test1", "v1"));

		MutationResult mr = mutator.execute();

		Mutator<String> mutator2 = HFactory.createMutator(tutorialKeyspace, StringSerializer.get());
		List<HColumn<String, String>> colsList = new ArrayList<HColumn<String, String>>();
		colsList.add(HFactory.createStringColumn("name", "tarun"));
		colsList.add(HFactory.createStringColumn("age", "25"));
		mutator2.addInsertion("deviceId", "scf", HFactory.createSuperColumn("10000", colsList,
				StringSerializer.get(), StringSerializer.get(), StringSerializer.get()));

		mutator2.execute();

	}

	private static void test4()
	{
		String keyspace = "MyKeyspace";
		String column1 = "ColumnFamilyName2";
		ColumnFamilyDefinition columnFamily1 = HFactory.createColumnFamilyDefinition(keyspace,
				column1, ComparatorType.UTF8TYPE);
		List columns = new ArrayList();
		columns.add(columnFamily1);
		KeyspaceDefinition testKeyspace = HFactory.createKeyspaceDefinition(keyspace,
				ThriftKsDef.DEF_STRATEGY_CLASS, 1, columns);
		KeyspaceDefinition testKeyspace2 = HFactory.createKeyspaceDefinition(keyspace,
				ThriftKsDef.DEF_STRATEGY_CLASS, 1, columns);
		cluster.addKeyspace(testKeyspace, true);

	}

	private static void createSuperCF()
	{
		String keyspace = "testKeyspace";
		String column1 = "testcolumn";
		ColumnFamilyDefinition columnFamily1 = HFactory.createColumnFamilyDefinition(keyspace,
				column1, ComparatorType.UTF8TYPE);
		List columns = new ArrayList();
		columns.add(columnFamily1);
		KeyspaceDefinition testKeyspace = HFactory.createKeyspaceDefinition(keyspace,
				ThriftKsDef.DEF_STRATEGY_CLASS, 1, columns);
		cluster.addKeyspace(testKeyspace, true);

	}

	private static void createSchema()
	{
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition("MyKeyspace",
				"ColumnFamilyName", ComparatorType.BYTESTYPE);
		int replicationFactor = 1;
		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition("MyKeyspace",
				ThriftKsDef.DEF_STRATEGY_CLASS, replicationFactor, Arrays.asList(cfDef));
		// Add the schema to the cluster.
		// "true" as the second param means that Hector will block until all
		// nodes see the change.
		cluster.addKeyspace(newKeyspace, true);
	}
}
