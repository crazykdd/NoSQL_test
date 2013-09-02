/**
 * 
 */
package CassandraHector;

import java.util.List;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import parsing.kvPair;

/** @author user */
public class CassandraConnect
{
	Keyspace thisKeyspace;
	Mutator<String> mutator;
	static final int maxBatchNum = 1000;
	String ColumnFamilyName = "";

	public CassandraConnect()
	{
		Cluster cluster = HFactory.getOrCreateCluster("test-cluster", "localhost:9160");
		ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();
		ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
		Keyspace thisKeyspace = HFactory.createKeyspace("write_test", cluster, ccl);
		mutator = HFactory.createMutator(thisKeyspace, StringSerializer.get());
		ColumnFamilyName = "Attribute";
	}

	public void insert1row(List<kvPair> lkvPair)
	{
		String rowKey = lkvPair.get(0).value;

		for (int i = 1; i < lkvPair.size(); i++)
			mutator.addInsertion(rowKey, ColumnFamilyName,
					HFactory.createStringColumn(lkvPair.get(i).key, lkvPair.get(i).value));
		mutator.execute();
		// executeChecking();

	}

	private void executeChecking()
	{
		if (mutator.getPendingMutationCount() >= maxBatchNum)
			mutator.execute();
	}

}
