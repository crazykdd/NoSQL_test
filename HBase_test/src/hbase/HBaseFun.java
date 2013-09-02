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
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseFun
{

	private static Configuration conf = null;
	/** Initialization */

	static HTable table;
	static String family;

	public static void ini(String tableName, String[] familys) throws Exception
	{
		conf = HBaseConfiguration.create();
		creatTable(tableName, familys);
		conf.set("hbase.client.scanner.caching", "100");
		table = new HTable(conf, tableName);
		family = familys[0];
	}

	public static void ini(String tableName, String[] familys, String confName, String confValue)
			throws Exception
	{
		conf = HBaseConfiguration.create();
		creatTable(tableName, familys);
		conf.set(confName, confValue);
		conf.set("hbase.client.scanner.caching", "128");
		table = new HTable(conf, tableName);
		family = familys[0];
	}

	public static void close() throws IOException
	{
		table.close();
		conf.clear();
	}

	/** Create a table */
	public static void creatTable(String tableName, String[] familys) throws Exception
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
	}

	/** Delete a table */
	public static void deleteTable(String tableName) throws Exception
	{
		try
		{
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("delete table " + tableName + " ok.");
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
	public static void addRecord(String tableName, String rowKey, String family, String qualifier,
			String value) throws Exception
	{
		try
		{
			HTable table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			table.put(put);
			System.out.println("insert recored " + rowKey + " to table " + tableName + " ok.");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	public static Put buildOneRecord(String rowKey, String family, String qualifier, String value)
	{
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		return put;
	}

	// build batch record for one MSG
	public static Put buildBatchRecord(List<kvPair> lmsg, String family)
	{
		String rowId = lmsg.get(0).value;
		Put put = new Put(Bytes.toBytes(rowId));
		for (int i = 1; i < lmsg.size(); i++)
			put.add(Bytes.toBytes(family), Bytes.toBytes(lmsg.get(i).key),
					Bytes.toBytes(lmsg.get(i).value));
		return put;
	}

	public static Put buildBatchRecord(List<kvPair> lmsg)
	{
		String rowId = lmsg.get(0).value;
		Put put = new Put(Bytes.toBytes(rowId));
		for (int i = 1; i < lmsg.size(); i++)
			put.add(Bytes.toBytes(family), Bytes.toBytes(lmsg.get(i).key),
					Bytes.toBytes(lmsg.get(i).value));
		return put;
	}

	public static void insert1msg(List<kvPair> lmsg) throws IOException
	{
		table.put(buildBatchRecord(lmsg, family));
	}

	/** Delete a row */
	public static void delRecord(String tableName, String rowKey) throws IOException
	{
		HTable table = new HTable(conf, tableName);
		List<Delete> list = new ArrayList<Delete>();
		Delete del = new Delete(rowKey.getBytes());
		list.add(del);
		table.delete(list);
		System.out.println("del recored " + rowKey + " ok.");
	}

	/** Get a row */
	public static void getOneRecord(String tableName, String rowKey) throws IOException
	{
		HTable table = new HTable(conf, tableName);
		Get get = new Get(rowKey.getBytes());
		Result rs = table.get(get);
		for (KeyValue kv : rs.raw())
		{
			System.out.print(new String(kv.getRow()) + " ");
			System.out.print(new String(kv.getFamily()) + ":");
			System.out.print(new String(kv.getQualifier()) + " ");
			System.out.print(kv.getTimestamp() + " ");
			System.out.println(new String(kv.getValue()));
		}
		table.close();
	}

	public static List<String> retrieveMSGID(String tableName, String rowKey) throws IOException
	{
		List<String> rowID = new ArrayList<String>();
		HTable table = new HTable(conf, tableName);
		Get get = new Get(rowKey.getBytes());
		Result rs = table.get(get);
		for (KeyValue kv : rs.raw())
		{
			rowID.add(new String(kv.getQualifier()));
		}
		table.close();
		return rowID;
	}

	public static Result[] getRecordsBatch(String tableName, List<String> rowKeys)
			throws IOException
	{
		HTable table = new HTable(conf, tableName);
		List<Get> lget = new ArrayList<Get>();
		for (String rowKey : rowKeys)
			lget.add(new Get(rowKey.getBytes()));

		Result[] rs = table.get(lget);
		table.close();

		return rs;

	}

	/** Scan (or list) a table */
	public static ResultScanner scanTable(String tableName)
	{
		ResultScanner ss = null;
		try
		{
			HTable table = new HTable(conf, tableName);
			Scan s = new Scan();
			Long startTime = System.nanoTime();
			ss = table.getScanner(s);
			Long endTime = System.nanoTime();
			System.out.println("time to scan the dataset is: " + (endTime - startTime) / 1e9);
			/* for (Result r : ss) { for (KeyValue kv : r.raw()) {
			 * System.out.print(new String(kv.getRow()) + " ");
			 * System.out.print(new String(kv.getFamily()) + ":");
			 * System.out.print(new String(kv.getQualifier()) + " ");
			 * System.out.print(kv.getTimestamp() + " "); System.out.println(new
			 * String(kv.getValue())); } } */
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		return ss;
	}

	public static void sequetialScan(String tableName, int maxnum) throws IOException
	{
		ResultScanner ss = null;
		int count = 0;
		try
		{
			count = 0;

			Scan s = new Scan();
			ss = table.getScanner(s);
			for (Result rr = ss.next(); rr != null; rr = ss.next())
			{
				count++;
				if (count > maxnum)
					break;
				List<kvPair> kv = rst2kvPair(rr);
				String a = ParseMSG.reconstructMSG(kv);
				// System.out.println(a);
			}

			ss.close();

		}
		catch (IOException e)
		{
			e.printStackTrace();
			ss.close();

		}
		System.out.println("count =" + count);
	}

	public static void createIdxTable(String tableName, String[] attributes,
			String[] idxtablenames, String[] idxfamilys) throws Exception
	{
		// ini index table
		List<HTable> lidxtable = new ArrayList<HTable>();
		for (int i = 0; i < attributes.length; i++)
		{
			HBaseFun.creatTable(idxtablenames[i], idxfamilys);
			lidxtable.add(new HTable(conf, idxtablenames[i]));
		}

		// get the scanner
		HTable table = new HTable(conf, tableName);
		Scan s = new Scan();

		ResultScanner ss = table.getScanner(s);

		Long startTime = System.nanoTime();
		// build index
		for (Result r : ss)
		{
			for (int i = 0; i < attributes.length; i++)
			{
				for (KeyValue kv : r.raw())
				{
					String attributeName = new String(kv.getQualifier());
					{
						if (attributeName.equals(attributes[i]))
						{
							String attributeValue = new String(kv.getValue());
							String rowID = new String(kv.getRow());
							lidxtable.get(i).put(
									buildOneRecord(attributeValue, idxfamilys[0], rowID, ""));
						}
					}

				}
			}
		}
		Long endTime = System.nanoTime();
		System.out.println("time to build index table(s) is: " + (endTime - startTime) / 1e9);
		for (int i = 0; i < lidxtable.size(); i++)
		{
			lidxtable.get(i).close();
		}
		table.close();

	}

	public static List<kvPair> rst2kvPair(Result rst)
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

	public static void main(String[] agrs) throws Exception
	{
		String[] tableName = new String[] { "shortmsg_40000001",
				"shortmsg_40000001_idx_Collection", "shortmsg_40000001_idx_GroupId",
				"shortmsg_40000001_idx_Id", "shortmsg_40000001_idx_Num",
				"shortmsg_40000001_idx_ObjectName", "shortmsg_all_single_ts" };
		for (String str : tableName)
			deleteTable(str);
	}

}