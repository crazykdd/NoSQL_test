package hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseConnect
{
	protected static Configuration conf = null;
	HTable table = null;
	String tableName;
	String family;
	/** Initialization */
	static
	{
		conf = HBaseConfiguration.create();
	}

	/* create a table */
	public HBaseConnect(String tableName, String family) throws IOException
	{
		this.tableName = tableName;

		this.family = family;
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName))
		{
			System.out.println(tableName + "connected.");
		}
		else
		{
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			tableDesc.addFamily(new HColumnDescriptor(family));
			admin.createTable(tableDesc);
			System.out.println(tableName + " created.");
		}
		admin.close();
		table = new HTable(conf, tableName);
	}

	public void close() throws IOException
	{
		table.close();
	}

	/** Create a table
	 * 
	 * @throws IOException */
	protected static void creatTable(String tableName, String[] familys) throws Exception
	{
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName))
		{
			System.out.println("table already exists!");
		}
		else
		{
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			for (int i = 0; i < familys.length; i++)
			{
				tableDesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("create table " + tableName + " ok.");
		}
		admin.close();
	}

	/** Delete a table */
	public void deleteTable() throws Exception
	{
		try
		{
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("delete table " + tableName + " ok.");
			admin.close();
		}
		catch (MasterNotRunningException e)
		{
			e.printStackTrace();
		}
		catch (ZooKeeperConnectionException e)
		{
			e.printStackTrace();
		}

	}

	/** Put (or insert) a row */
	public void addRecord(String rowKey, String family, String qualifier, String value)
			throws Exception
	{
		try
		{
			table.put(buildOneRecord(rowKey, family, qualifier, value));
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	/** Put (or insert) a row */
	public void addRecord(byte[] rowKey, String family, String qualifier, String value)
			throws Exception
	{
		try
		{
			table.put(buildOneRecord(rowKey, family, qualifier, value));
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	protected Put buildOneRecord(String rowKey, String family, String qualifier, String value)
	{
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		return put;
	}

	protected Put buildOneRecord(byte[] rowKey, String family, String qualifier, String value)
	{
		Put put = new Put(rowKey);
		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		return put;
	}

	protected List<String> getQualifiers(String rowId) throws IOException
	{
		List<String> qualifiers = new ArrayList<String>();
		Get get = new Get(rowId.getBytes());
		Result rs = table.get(get);
		for (KeyValue kv : rs.raw())
		{
			qualifiers.add(new String(kv.getQualifier()));
		}
		return qualifiers;
	}

	/** Delete a row */
	protected void delRecord(String rowKey) throws IOException
	{
		List<Delete> list = new ArrayList<Delete>();
		Delete del = new Delete(rowKey.getBytes());
		list.add(del);
		table.delete(list);
		System.out.println("del recored " + rowKey + " ok.");
	}

	/* public List<String> retrieveMSGID(String tableName, String rowKey) throws
	 * IOException { List<String> rowID = new ArrayList<String>(); Get get = new
	 * Get(rowKey.getBytes()); Result rs = table.get(get); for (KeyValue kv :
	 * rs.raw()) { rowID.add(new String(kv.getQualifier())); } return rowID; } */

	protected Result[] getRecordsBatch(List<String> rowKeys) throws IOException
	{

		List<Get> lget = new ArrayList<Get>();
		for (String rowKey : rowKeys)
			lget.add(new Get(rowKey.getBytes()));

		Result[] rs = table.get(lget);

		return rs;

	}

	protected Result[] getRecordsBatch(List<String> rowKeys, String qualifier) throws IOException
	{

		List<Get> lget = new ArrayList<Get>();
		for (String rowKey : rowKeys)
		{
			Get _get = new Get(rowKey.getBytes());
			_get.addColumn("Attribute".getBytes(), qualifier.getBytes());
			lget.add(_get);
		}

		Result[] rs = table.get(lget);

		return rs;

	}

	protected Result[] getRecordsBatch(List<String> rowKeys, long maxStamp) throws IOException
	{

		List<Get> lget = new ArrayList<Get>();
		for (String rowKey : rowKeys)
		{
			Get get = new Get(rowKey.getBytes());
			get.setTimeRange(0, maxStamp);
			lget.add(get);
		}

		Result[] rs = table.get(lget);

		return rs;

	}

	protected Result getOneRecord(String rowKey) throws IOException
	{

		Get get = new Get(rowKey.getBytes());
		Result rs = table.get(get);
		return rs;

	}

	/** TO DELETE Scan (or list) a table */
	/* private void scanTableTest(int max) { ResultScanner ss = null; int count
	 * = 0; try { Scan s = new Scan(); // s.setFilter(new FirstKeyOnlyFilter());
	 * // s.setFilter(new KeyOnlyFilter()); // s.setCaching(50); //
	 * s.setCacheBlocks(false); ss = table.getScanner(s); for (Result rr =
	 * ss.next(); rr != null; rr = ss.next()) { if (count > max) break;
	 * List<kvPair> lmsg = rst2kvPair(rr); String msg =
	 * ParseMSG.reconstructMSG(lmsg); count++; } } catch (IOException e) {
	 * e.printStackTrace(); } // System.out.println(count - 1); ss.close(); } */
	protected List<String> listRowKey(int maxnum)
	{
		ResultScanner ss = null;
		int count = 0;
		List<String> lrowKey = new ArrayList<String>();
		try
		{

			Scan s = new Scan();

			s.setFilter(new FirstKeyOnlyFilter());
			s.setFilter(new KeyOnlyFilter());
			ss = table.getScanner(s);
			for (Result rr = ss.next(); rr != null; rr = ss.next())
			{
				if (count > maxnum)
					break;
				lrowKey.add(new String(rr.getRow()));
				// List<kvPair> kv = rst2kvPair(rr);
				count++;
			}

		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		ss.close();
		return lrowKey;
	}

	void testscan(int maxnum)
	{

		ResultScanner ss = null;
		int count = 0;
		List<String> lrowKey = new ArrayList<String>();
		try
		{
			Long startTime, endTime;

			ResultScanner ss2 = null;
			count = 0;
			startTime = System.nanoTime();
			Scan s2 = new Scan();
			Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("Attribute"),
					Bytes.toBytes("Num"), CompareOp.LESS_OR_EQUAL, Bytes.toBytes("50792"));
			s2.setFilter(filter1);
			ss2 = table.getScanner(s2);
			for (Result rr = ss2.next(); rr != null; rr = ss2.next())
			{
				count++;
				if (count > maxnum)
					break;
				// List<kvPair> kv = rst2kvPair(rr);

			}
			endTime = System.nanoTime();
			// System.out.println("Total time is: " + (endTime - startTime) /
			// 1e9);
			// System.out.println("Total number is: " + count);

			count = 0;
			double time = 0;

			ResultScanner ss3 = null;
			for (int i = 0; i < 10; i++)
			{
				count = 0;
				startTime = System.nanoTime();
				Scan s3 = new Scan();
				Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("Attribute"),
						Bytes.toBytes("Num"), CompareOp.LESS_OR_EQUAL, Bytes.toBytes("50792"));
				s3.setFilter(filter2);
				ss3 = table.getScanner(s3);
				for (Result rr = ss3.next(); rr != null; rr = ss3.next())
				{
					count++;
					if (count > maxnum)
						break;
					// List<kvPair> kv = rst2kvPair(rr);

				}
				endTime = System.nanoTime();
				time = (time + (endTime - startTime) / 1e9);
			}
			System.out.println("Total time is: " + time);
			// System.out.println("Total number is: " + count);

			long max = 9372263027062L;
			time = 0;
			for (int i = 0; i < 10; i++)
			{
				count = 0;

				startTime = System.nanoTime();
				Scan s = new Scan();

				s.setTimeRange(0, max);
				s.setFilter(new FirstKeyOnlyFilter());
				s.setFilter(new KeyOnlyFilter());
				ss = table.getScanner(s);
				// maxnum = 3;
				for (Result rr = ss.next(); rr != null; rr = ss.next())
				{
					count++;
					if (count > maxnum)
						break;
					// List<kvPair> kv = rst2kvPair(rr);

				}
				endTime = System.nanoTime();
				time = (time + (endTime - startTime) / 1e9);
			}
			System.out.println("Total time is: " + time);
			// System.out.println("Total number is: " + count);
			ss.close();

			int a = 1;
			int b = a;

		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		ss.close();

	}

	protected static List<tserver.gen.kvPair> rst2kvPair2(Result rst)
	{
		List<tserver.gen.kvPair> lmsg = new ArrayList<tserver.gen.kvPair>();
		lmsg.add(new tserver.gen.kvPair("rowID", new String(rst.getRow())));
		String AttributeIndex = "";
		for (KeyValue kv : rst.raw())
		{
			String attributeName = new String(kv.getQualifier());
			String attributeValue = new String(kv.getValue());
			if (attributeName.equals("AttributeIndex"))
				AttributeIndex = attributeValue;
			else
				lmsg.add(new tserver.gen.kvPair(attributeName, attributeValue));
		}
		lmsg.add(new tserver.gen.kvPair("AttributeIndex", AttributeIndex));
		return lmsg;

	}

	protected static List<kvPair> rst2kvPair(Result rst)
	{
		List<kvPair> lmsg = new ArrayList<kvPair>();
		lmsg.add(new kvPair("rowID", new String(rst.getRow())));
		String AttributeIndex = "";
		for (KeyValue kv : rst.raw())
		{
			String attributeName = new String(kv.getQualifier());
			String attributeValue = new String(kv.getValue());
			if (attributeName.equals("AttributeIndex"))
				AttributeIndex = attributeValue;
			else
				lmsg.add(new kvPair(attributeName, attributeValue));
		}
		lmsg.add(new kvPair("AttributeIndex", AttributeIndex));
		return lmsg;

	}

	public void msgFromRowId(List<String> rowId) throws IOException
	{
		for (String str : rowId)
		{
			Result rsts = getOneRecord(str);
			List<kvPair> lmsg = rst2kvPair(rsts);
			String msg = ParseMSG.reconstructMSG(lmsg);
		}
	}

	public void msgFromRowIdBatch(List<String> rowId) throws IOException
	{
		int maxBatchSize = 100;
		for (int i = 0; i < rowId.size(); i = i + maxBatchSize)
		{
			Result[] rsts = getRecordsBatch(rowId.subList(i,
					Math.min(i + maxBatchSize - 1, rowId.size() - 1)));
			for (Result rst : rsts)
			{
				List<kvPair> lmsg = rst2kvPair(rst);
				String msg = ParseMSG.reconstructMSG(lmsg);
			}
		}

	}

	private String msgFromRst(Result rst) throws IOException
	{
		List<kvPair> lmsg = rst2kvPair(rst);
		String msg = ParseMSG.reconstructMSG(lmsg);
		return msg;
	}

	public static final byte[] intToByteArray(int value)
	{
		return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8),
				(byte) value };
	}

	public static final int byteArrayToInt(byte[] bytes)
	{
		return bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8
				| (bytes[3] & 0xFF);
	}

	/* public static final byte[] intToByteArray2(int value) { return
	 * ByteBuffer.allocate(4).putInt(value).array(); } public static final int
	 * byteArrayToInt2(byte[] bytes) { return ByteBuffer.wrap(bytes).getInt(); } */
}
