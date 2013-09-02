package hbase;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

public class HBaseTest
{
	static HTable table;
	private static Configuration conf = null;
	/**
	 * Initialization
	 */
	static
	{
		conf = HBaseConfiguration.create();
	}

	public static void main(String[] agrs) throws Exception
	{
		testwrite();
		// testread();
	}

	private static void testwrite() throws Exception
	{
		String tablename = "MSG0";// all: means 400k version
		String[] familys = { "Attribute" };
		HBaseFun.creatTable(tablename, familys);
		String filename = "/home/user/workspace/data/MSGs.txt";
		table = new HTable(conf, tablename);
		Long startTime = System.nanoTime();
		// System.out.print("Start time: " + startTime);
		insertMSG(filename, familys[0]);
		Long endTime = System.nanoTime();
		System.out.print("End time: " + (endTime - startTime) / 1e9);
		table.close();
		conf.clear();
	}

	private static void testread() throws Exception
	{
		String tablename = "MSG0";
		String rowID = "40000001-1-44516";
		HBaseFun.getOneRecord(tablename, rowID);
	}

	private static void insertMSG(String filename, String familiy)
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
				if (count >= 100000)
					break;
				try
				{
					List<kvPair> lmsg = ParseMSG.parseString(sCurrentLine);
					insertMSG2HBase(lmsg, familiy);
				}
				catch (Exception ex)
				{
					System.out.println(sCurrentLine);
				}
				count = count + 1;
			}
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

	private static void insertMSG2HBase(List<kvPair> lmsg, String family)
			throws IOException
	{

		table.put(HBaseFun.buildBatchRecord(lmsg, family));
	}

}
