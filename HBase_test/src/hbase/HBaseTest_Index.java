package hbase;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

public class HBaseTest_Index
{
	static HBaseConnect hbc;
	static DataConnect dc;
	static IndexConnect ic;
	static HBaseConnect hbcq;
	static HTable datatable;
	static List<HTable> lidxtable;
	static List<DataConnect> ldc;
	private static Configuration conf = null;
	/** Initialization */
	static
	{
		conf = HBaseConfiguration.create();
	}

	public static void main(String[] agrs) throws Exception
	{

		// testInsertperformance();
		// testwrite();
		// testread();
		// testscan();
		// testidxtable();
		// testTS();
		// generateQuery();
		// testreadFromfile();
		// testSequentialScan();
		// testRandomScan();
		// build_idxtable();
		// testintbyte();

		// testDataNTSTables();
		// insertdata2alltables();
		// insertshortdata2alltables();
		// testqueryperformance();
		testSequentialScan();
		// testSequentialScan_test();
		// testParseMSG_para();
		// testParseMSG_reconstruct_para();

		// testInsertperformance();
	}

	private static void testParseMSG_para() throws IOException
	{

		Long startTime, endTime;
		int[] maxCounts = new int[4];
		maxCounts[0] = 1000 * 1;
		maxCounts[1] = 1000 * 10;
		maxCounts[2] = 1000 * 100;
		maxCounts[3] = 1000 * 400;
		List<String> filenames = new ArrayList<String>();
		filenames.add("/home/user/workspace/data/msg_40000001.txt");
		filenames.add("/home/user/workspace/data/msg_40000002.txt");
		filenames.add("/home/user/workspace/data/msg_40000003.txt");
		filenames.add("/home/user/workspace/data/msg_40000004.txt");
		for (int i = 0; i < 4; i++)
		{
			List<String> MSGs = testReadMSG(filenames.get(i), maxCounts[i]);
			startTime = System.nanoTime();
			testParsingMSG(MSGs, maxCounts[i]);
			endTime = System.nanoTime();
			System.out.println((endTime - startTime) / 1e9);
		}
	}

	private static void testParseMSG_reconstruct_para() throws IOException
	{

		Long startTime, endTime;
		int[] maxCounts = new int[4];
		maxCounts[0] = 1000 * 1;
		maxCounts[1] = 1000 * 10;
		maxCounts[2] = 1000 * 100;
		maxCounts[3] = 1000 * 400;
		List<String> filenames = new ArrayList<String>();
		filenames.add("/home/user/workspace/data/msg_40000001.txt");
		filenames.add("/home/user/workspace/data/msg_40000002.txt");
		filenames.add("/home/user/workspace/data/msg_40000003.txt");
		filenames.add("/home/user/workspace/data/msg_40000004.txt");
		for (int i = 0; i < 3; i++)
		{
			List<String> MSGs = testReadMSG(filenames.get(i), maxCounts[i]);
			List<List<kvPair>> llmsg = testParsingMSG2(MSGs, maxCounts[i]);
			startTime = System.nanoTime();
			for (List<kvPair> lmsg : llmsg)
				ParseMSG.reconstructMSG(lmsg);
			endTime = System.nanoTime();
			System.out.println((endTime - startTime) / 1e9);
		}
	}

	private static void testParsingMSG(List<String> MSGs, int maxCount)
	{
		for (int i = 0; i < maxCount; i++)
			ParseMSG.parseString(MSGs.get(i));

	}

	private static List<List<kvPair>> testParsingMSG2(List<String> MSGs, int maxCount)
	{
		List<List<kvPair>> llmsg = new ArrayList<List<kvPair>>();
		for (int i = 0; i < maxCount; i++)
			llmsg.add(ParseMSG.parseString(MSGs.get(i)));
		return llmsg;

	}

	private static List<String> testReadMSG(String filename, int maxCount) throws IOException
	{
		List<String> MSGs = new ArrayList<String>();
		BufferedReader br = null;

		String sCurrentLine;
		br = new BufferedReader(new FileReader(filename));
		int count = 0;
		while ((sCurrentLine = br.readLine()) != null)
		{
			// System.out.println(sCurrentLine);
			if (!sCurrentLine.startsWith("<Message:"))
				continue;
			// if (count % 40000 == 0)
			// System.out.println(count);
			if (count >= maxCount)
				break;
			count++;
			// ParseMSG.parseString(sCurrentLine);
			MSGs.add(sCurrentLine);

		}
		br.close();
		return MSGs;

	}

	private static void testInsertperformance() throws IOException
	{
		// String filename = "/home/user/workspace/data2/msg_0.txt";
		String filename = "/home/user/workspace/data/msg_40000001.txt";
		String datatableName = "datatable_Final_javaAPI400k";
		String dataFamily = "Attribute";
		Long startTime = System.nanoTime();
		dc = new DataConnect(datatableName, dataFamily, "", "");
		insertMSGfromFile(filename);
		Long endTime = System.nanoTime();
		System.out.println("Total time is: " + (endTime - startTime) / 1e9);
		dc.close();
	}

	// test performance for multiple tables and single giant table
	private static void testqueryperformance() throws IOException
	{

		int type = 2;// 1:single table//2 multiple table
		if (type == 1)
		{
			// single giant table
			String filename = "/home/user/workspace/data/shortqry.txt";
			String datatableName = "shortmsg_all";
			String dataFamily = "Attribute";
			dc = new DataConnect(datatableName, dataFamily, "", "");
			String indextableName = "shortmsg_all_idx_ObjectName";
			String indexFamily = "RowID";
			ic = new IndexConnect(indextableName, indexFamily);
			Long startTime = System.nanoTime();

			int totalMSGs = operationFromFile(filename, 1);
			// without reconstruction
			// int totalMSGs = operationFromFile(filename, 9);
			Long endTime = System.nanoTime();
			System.out.println("Total retrieved MSGs number is :" + totalMSGs);
			System.out.println("Total time is: " + (endTime - startTime) / 1e9);

			dc.close();
			ic.close();
		}
		else
		{
			ldc = new ArrayList<DataConnect>();
			String filename = "/home/user/workspace/data/shortqry.txt";
			String datatableName = "shortmsg_all";
			String dataFamily = "Attribute";
			for (int i = 1; i < 22; i++)
			{
				datatableName = "shortmsg_" + String.valueOf(40000000 + i);
				ldc.add(new DataConnect(datatableName, dataFamily, "", ""));
			}
			String indextableName = "shortmsg_all_idx_ObjectName";
			String indexFamily = "RowID";
			ic = new IndexConnect(indextableName, indexFamily);
			Long startTime = System.nanoTime();
			// int totalMSGs = operationFromFile(filename, 3);
			// one by one multiple tables
			int totalMSGs = operationFromFile(filename, 2);
			Long endTime = System.nanoTime();
			System.out.println("Total retrieved MSGs number is :" + totalMSGs);
			System.out.println("Total time is: " + (endTime - startTime) / 1e9);

			for (int i = 1; i < 22; i++)
			{
				ldc.get(i - 1).close();
			}
			ic.close();
		}

	}

	// smaller size data, into one table
	private static void insertshortdata2alltables() throws IOException
	{
		String datatableName = "";// "test_all_testMSG_1";
		String dataFamily = "Attribute";
		String tstableName = "shortmsg_all_ts";
		String tsFamily = "Attribute";
		String[] attributeNames = { "GroupId", "Id", "Num", "Collection", "ObjectName" };
		// String[] attributeNames = { "GroupId", "ObjectName" };
		int[] types = { 1, 1, 1, 2, 2 };// 1:int, 2:String
		// int[] types = { 1, 2 };// 1:int, 2:String
		String filename = "";// "/home/user/workspace/data/short/shortmsg.txt";
		datatableName = "shortmsg_all";
		dc = new DataConnect(datatableName, dataFamily, tstableName, tsFamily, attributeNames,
				types);
		for (int i = 0; i < 21; i++)
		{
			System.out.println("currentfile" + i);
			filename = "/home/user/workspace/data/short/shortmsg_" + String.valueOf(40000001 + i)
					+ ".txt";
			insertMSGfromFile(filename);

		}
		dc.close();
	}

	private static void insertdata2alltables() throws IOException
	{
		String datatableName = "";// "test_all_testMSG_1";
		String dataFamily = "Attribute";
		String tstableName = "shortmsg_all_single_ts";
		String tsFamily = "Attribute";
		String[] attributeNames = { "GroupId", "Id", "Num", "Collection", "ObjectName" };
		// String[] attributeNames = { "GroupId", "ObjectName" };
		int[] types = { 1, 1, 1, 2, 2 };// 1:int, 2:String
		// int[] types = { 1, 2 };// 1:int, 2:String
		String filename = "";// "/home/user/workspace/data/msg.txt";
		for (int i = 0; i < 21; i++)
		{
			datatableName = "shortmsg_" + String.valueOf(40000001 + i);
			dc = new DataConnect(datatableName, dataFamily, tstableName, tsFamily, attributeNames,
					types);
			filename = "/home/user/workspace/data/short/shortmsg_" + String.valueOf(40000001 + i)
					+ ".txt";
			insertMSGfromFile(filename);
			dc.close();
		}
	}

	private static void testDataNTSTables() throws IOException
	{
		String datatableName = "all_testMSG_0";
		String dataFamily = "Attribute";
		String tstableName = "all_testMSG_0_ts";
		String tsFamily = "Attribute";
		dc = new DataConnect(datatableName, dataFamily, tstableName, tsFamily);

		String filename = "/home/user/workspace/data/msg.txt";
		insertMSGfromFile(filename);
		dc.close();
	}

	// test int byte conversion
	private static void testintbyte()
	{
		int v = 9438934;
		int num = 10000000;
		Long startTime, endTime;

		startTime = System.nanoTime();
		for (int i = 0; i < num; i++)
		{
			// byte[] tmp = DataConnect.intToByteArray2(v);
			// DataConnect.byteArrayToInt2(tmp);
		}
		endTime = System.nanoTime();
		System.out.println("Total time for 2 is: " + (endTime - startTime) / 1e9);

		startTime = System.nanoTime();
		for (int i = 0; i < num; i++)
		{
			byte[] tmp = DataConnect.intToByteArray(v);
			DataConnect.byteArrayToInt(tmp);
		}
		endTime = System.nanoTime();
		System.out.println("Total time for 1 is: " + (endTime - startTime) / 1e9);

		startTime = System.nanoTime();
		for (int i = 0; i < num; i++)
		{
			// byte[] tmp = DataConnect.intToByteArray2(v);
			// DataConnect.byteArrayToInt2(tmp);
		}
		endTime = System.nanoTime();
		System.out.println("Total time for 2 is: " + (endTime - startTime) / 1e9);

		startTime = System.nanoTime();
		for (int i = 0; i < num; i++)
		{
			byte[] tmp = DataConnect.intToByteArray(v);
			DataConnect.byteArrayToInt(tmp);
		}
		endTime = System.nanoTime();
		System.out.println("Total time for 1 is: " + (endTime - startTime) / 1e9);

		startTime = System.nanoTime();
		for (int i = 0; i < num; i++)
		{
			// byte[] tmp = DataConnect.intToByteArray2(v);
			// DataConnect.byteArrayToInt2(tmp);
		}
		endTime = System.nanoTime();
		System.out.println("Total time for 2 is: " + (endTime - startTime) / 1e9);

	}

	// test the retrieval efficiency comparison between ts and qualifier
	// constraint
	private static void build_idxtable() throws Exception
	{
		String datatableName = "all_testMSG";
		DataConnect dbc = new DataConnect(datatableName, "", null, null);
		String[] attributes = new String[] { "GroupId", "Id", "Num" };
		int[] types = new int[] { 1, 1, 1 };
		String[] idxtablenames = new String[] { "all_test_idx_GroupId_bytes2",
				"all_test_idx_Id_bytes2", "all_test_idx_Num_bytes2" };
		String[] idxfamilys = new String[] { "RowID" };
		dbc.createIdxTableOffline_type(attributes, types, idxtablenames, idxfamilys);
		dbc.close();
	}

	private static void testRandomScan() throws IOException
	{
		Long startTime, startTime2, endTime, endTime2;
		// Random
		// String datatableName = "all_testMSG";
		String datatableName = "msg_40000001";
		hbc = new HBaseConnect(datatableName, "");
		for (int l = 0; l < 1; l++)
		{
			System.out.println("");
			int max = 100000;
			int num = 100000;

			startTime = System.nanoTime();
			// hbc.testscan(max);
			List<String> rowKeys = hbc.listRowKey(max);
			endTime = System.nanoTime();
			System.out
					.println("(Listing 100k rowID) Total time is: " + (endTime - startTime) / 1e9);

			startTime = System.nanoTime();
			// hbc.scanTableTest(max);
			endTime = System.nanoTime();
			System.out.println("(Real sequential) Total time is: " + (endTime - startTime) / 1e9);

			startTime = System.nanoTime();
			int[] idx = randomNum(max, num);
			List<String> randomKeys = selectbyIdx(rowKeys, idx);
			endTime = System.nanoTime();
			System.out.println("(random select 100k rowID) Total time is: " + (endTime - startTime)
					/ 1e9);
			startTime = System.nanoTime();
			// hbc.msgFromRowId(randomKeys);
			hbc.msgFromRowIdBatch(randomKeys);
			endTime = System.nanoTime();
			System.out.println("(Random retrieval) Total time is: " + (endTime - startTime) / 1e9);

			// sequential
			startTime = System.nanoTime();
			List<String> seKeys = rowKeys.subList(0, num);
			endTime = System.nanoTime();
			System.out.println("(sequential select 100k rowID) Total time is: "
					+ (endTime - startTime) / 1e9);

			startTime2 = System.nanoTime();
			// hbc.msgFromRowId(seKeys);
			hbc.msgFromRowIdBatch(seKeys);
			endTime2 = System.nanoTime();
			System.out.println("(Sequential retrieval) Total time is: " + (endTime2 - startTime2)
					/ 1e9);
		}
		hbc.close();
	}

	/* private static void testscan() { // ong startTime = System.nanoTime(); //
	 * System.out.print("Start time: " + startTime); String datatablename =
	 * "all_testMSG";// all: means 400k version
	 * HBaseFun.scanTable(datatablename); // Long endTime = System.nanoTime();
	 * // System.out.print("time: " + (endTime - startTime) / 1e9); } */
	/* private static void testidxtable() throws Exception { String tableName =
	 * "all_testMSG"; String[] idxAttributes = { "Collection", "ObjectName" };
	 * String idxtablename = "all_test_idx_Collection_ObjectName1"; String[]
	 * idxfamilys = { "RowID" }; HBaseConnect hbc = new HBaseConnect(tableName);
	 * hbc.createJointIdxTable(tableName, idxAttributes, idxtablename,
	 * idxfamilys); } */

	private static void testSequentialScan() throws Exception
	{
		String confName = "hbase.client.scanner.caching";
		String datatablename = "all_testMSG_100k";
		HBaseFun.ini(datatablename, new String[] { "" });
		Long startTime = System.nanoTime();
		// HBaseFun.sequetialScan(datatablename, 100000);
		HBaseFun.sequetialScan(datatablename, 19000);
		Long endTime = System.nanoTime();
		System.out.println("time: " + (endTime - startTime) / 1e9);
	}

	private static void testSequentialScan_test() throws Exception
	{
		String confName = "hfile.block.cache.size";// hfile.block.cache.size
		String datatablename = "all_testMSG_100k";
		int maxPowerNum = 9;
		int maxRun = 5;
		Long startTime, endTime;
		String[] confValue = new String[] { "0", "0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7",
				"0.8" };
		// for (int i = 0; i < maxPowerNum; i++)
		// confValue[i] = String.valueOf((int) Math.pow(2, i));
		double[] timefinal = new double[maxPowerNum];
		double[] stdfinal = new double[maxPowerNum];
		for (int run = 0; run < maxPowerNum; run++)
		{
			double[] timeall = new double[maxRun];
			for (int i = 0; i < maxRun; i++)
			{
				HBaseFun.ini(datatablename, new String[] { "" }, confName, confValue[run]);
				startTime = System.nanoTime();
				HBaseFun.sequetialScan(datatablename, 100000);
				endTime = System.nanoTime();
				double costTime = (endTime - startTime) / 1e9;
				System.out.println("time: " + costTime);
				System.out.println(run + "\t" + i);
				timeall[i] = costTime;
				timefinal[run] = mean(timeall);
				stdfinal[run] = std(timeall);
				HBaseFun.close();
			}
			writeFile("block_time_rst_mean.txt", new double[] { timefinal[run] });
			writeFile("block_time_rst_std.txt", new double[] { stdfinal[run] });
			writeFile("block_time_rst_power.txt", new String[] { confValue[run] });
		}

	}

	private static void testwrite() throws Exception
	{
		String datatablename = "all_testMSG_100k2";// all: means 400k version
		String[] datafamilys = { "Attribute" };
		// HBaseFun.creatTable(datatablename, datafamilys);
		String filename = "/home/user/workspace/data/msg.txt";
		// ini datatable
		// datatable = new HTable(conf, datatablename);

		// ini idx table
		// attributes need to be built in idx
		String[] idxAttributes = { "Collection", "ObjectName", "Object.Id" };
		String[] idxtablename = new String[idxAttributes.length];
		String[] idxfamilys = { "RowID" };// original RowID
		lidxtable = new ArrayList<HTable>();
		for (int i = 0; i < idxAttributes.length; i++)
		{
			idxtablename[i] = "all_test_idx_" + idxAttributes[i];
			HBaseFun.creatTable(idxtablename[i], idxfamilys);
			lidxtable.add(new HTable(conf, idxtablename[i]));
		}
		dc = new DataConnect(datatablename, datafamilys[0], "", "");
		Long startTime = System.nanoTime();
		// System.out.print("Start time: " + startTime);
		insertMSGfromFile(filename, datafamilys[0], idxfamilys[0], idxAttributes);
		Long endTime = System.nanoTime();
		System.out.print("End time: " + (endTime - startTime) / 1e9);
		// datatable.close();
		dc.close();
		conf.clear();
		for (int i = 0; i < idxAttributes.length; i++)
		{
			lidxtable.get(i).close();
		}
	}

	private static void testread() throws Exception
	{
		String qattribute = "ObjectName";

		String qvalue = "2";
		String qtablename = "test_idx_" + qattribute;
		String datatableName = "testMSG0";

		List<String> rowID = HBaseFun.retrieveMSGID(qtablename, qvalue);
		Result[] rsts = HBaseFun.getRecordsBatch(datatableName, rowID);
		for (Result rs : rsts)
		{
			for (KeyValue kv : rs.raw())
			{
				System.out.print(new String(kv.getRow()) + " ");
				System.out.print(new String(kv.getFamily()) + ":");
				System.out.print(new String(kv.getQualifier()) + " ");
				System.out.print(kv.getTimestamp() + " ");
				System.out.println(new String(kv.getValue()));
			}
			List<kvPair> lmsg = HBaseFun.rst2kvPair(rs);
			// System.out.println("");
			// ParseMSG.printLMSG(lmsg);
			String msg = ParseMSG.reconstructMSG(lmsg);
			System.out.println(msg);
		}
	}

	/* private static void testreadFromfile() throws Exception { String
	 * qattribute = "Object.Id"; String qtablename = "all_test_idx_" +
	 * qattribute; String datatableName = "all_testMSG"; String filename =
	 * "/home/user/workspace/data/qry.txt"; hbc = new
	 * HBaseConnect(datatableName); hbcq = new HBaseConnect(qtablename); Long
	 * startTime = System.nanoTime();
	 * System.out.println("total retrieved MSG number is: " +
	 * testreadfromFile(filename, qtablename, datatableName)); Long endTime =
	 * System.nanoTime(); hbc.close(); hbcq.close();
	 * System.out.println("Total time is: " + (endTime - startTime) / 1e9); } */
	// test the default hbase timestamp
	// conclustion: hbase use System.currentTimeMillis() as the timestamp
	private static void testTS() throws Exception
	{
		String tablename = "test";
		HBaseConnect hbc = new HBaseConnect(tablename, "Attribute");
		hbc.addRecord("row1", "cf", "qualifier1", "value1");
		System.out.println(System.currentTimeMillis());
		Result rst = hbc.getOneRecord("row1");
		long ts = rst.raw()[0].getTimestamp();
		System.out.println(ts);
		hbc.close();

	}

	private static void insertMSGfromFile(String filename, String datafamiliy, String idxfamiliy,
			String[] idxAttributes)
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
					// insertMSG2Datatable(lmsg);
					insertMSG2Datatable2(lmsg);
					// insertMSG2idxtable(lmsg, idxfamiliy, idxAttributes);
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
				if (count >= 400000)
					break;

				try
				{
					List<kvPair> lmsg = ParseMSG.parseString(sCurrentLine);
					insertMSG2Datatable2(lmsg);
					// insertMSG2idxtable(lmsg, idxfamiliy, idxAttributes);
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

	private static int operationFromFile(String filename, int operation)
	{
		BufferedReader br = null;
		int totalRetrievedMSGs = 0;

		try
		{
			String sCurrentLine;
			br = new BufferedReader(new FileReader(filename));
			int count = 0;
			while ((sCurrentLine = br.readLine()) != null)
			{
				// System.out.println(sCurrentLine);
				if (count >= 50)
					break;
				// if (count % 10 == 0)
				System.out.println(count);

				try
				{
					// 1 qry
					if (operation == 1)
					{
						sCurrentLine = "0";
						List<String> rowIds = ic.getMSGId(sCurrentLine);
						List<String> msgs = dc.retrieveMSGs(rowIds);
						totalRetrievedMSGs = totalRetrievedMSGs + msgs.size();
						if (msgs.size() == 0)
							System.out.println("No msg found!");

					}
					// retrieve results only (No Reconstruction)
					else if (operation == 9)
					{
						sCurrentLine = "0";
						List<String> rowIds = ic.getMSGId(sCurrentLine);
						Result[] rst = dc.retrieveRstOnly(rowIds);
						// dc.retrieveMSGs(rowIds);
						// totalRetrievedMSGs = totalRetrievedMSGs + rst.length;

					}
					// one by one multiple tables
					else if (operation == 2)
					{
						sCurrentLine = "0";
						List<String> rowIds = ic.getMSGId(sCurrentLine);
						for (String row : rowIds)
						{
							int GroupId = Integer.parseInt(row.split("-")[0]);
							int idx = GroupId - 40000001;
							String msg = ldc.get(idx).retrieveMSGs(row);
							totalRetrievedMSGs++;
							if (msg == null)
								System.out.println("No msg found!");
						}

					}

					else if (operation == 3)
					{

						sCurrentLine = "0";
						List<String> rowIds = ic.getMSGId(sCurrentLine);
						List<List<String>> organizedRST = new ArrayList<List<String>>();
						for (int i = 1; i < 22; i++)
							organizedRST.add(new ArrayList<String>());
						for (String row : rowIds)
						{
							for (int i = 1; i < 22; i++)
								if (row.startsWith(String.valueOf(40000000 + i)))
									organizedRST.get(i - 1).add(row);
						}
						for (int i = 1; i < 22; i++)
						{
							List<String> tmplist = organizedRST.get(i - 1);
							List<String> msgs = ldc.get(i - 1).retrieveMSGs(tmplist);
							totalRetrievedMSGs = totalRetrievedMSGs + msgs.size();
						}

					}
					// insertMSG2idxtable(lmsg, idxfamiliy, idxAttributes);
				}
				catch (Exception ex)
				{
					System.out.println(sCurrentLine);
					System.out.println(ex);
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
		return totalRetrievedMSGs;

	}

	/* private static int testreadfromFile(String filename, String qtablename,
	 * String datatableName) { BufferedReader br = null; int numMSGs = 0; try {
	 * String sCurrentLine; br = new BufferedReader(new FileReader(filename));
	 * int count = 0; while ((sCurrentLine = br.readLine()) != null) { //
	 * System.out.println(sCurrentLine); if (count % 1000 == 0)
	 * System.out.println(count); try { List<String> rowID =
	 * hbcq.retrieveMSGID(qtablename, sCurrentLine); Result[] rsts =
	 * hbc.getRecordsBatch(rowID); numMSGs = numMSGs + rsts.length; for (Result
	 * rs : rsts) { List<kvPair> lmsg = HBaseFun.rst2kvPair(rs); String msg =
	 * ParseMSG.reconstructMSG(lmsg); } } catch (Exception ex) {
	 * System.out.println(sCurrentLine); } count = count + 1; }
	 * System.out.println("Done!"); } catch (IOException e) {
	 * e.printStackTrace(); } finally { try { if (br != null) br.close(); }
	 * catch (IOException ex) { ex.printStackTrace(); } return numMSGs; } } */

	private static void insertMSG2idxtable(List<kvPair> lmsg, String idxfamiliy,
			String[] idxAttributes) throws IOException
	{
		// a list of kvpair to be inserted into idx table
		// List<kvPair> idxkv = new ArrayList<kvPair>();
		String rowID = lmsg.get(0).value;
		for (int i = 0; i < idxAttributes.length; i++)
			for (int j = 1; j < lmsg.size() - 1; j++)
			{
				String attname = lmsg.get(j).key;
				if (attname.equals(idxAttributes[i]))
				{
					// attribute value as new rowID
					// original rowId as qualifier
					// value is empty
					String attvalue = lmsg.get(j).value;
					lidxtable.get(i).put(HBaseFun.buildOneRecord(attvalue, idxfamiliy, rowID, ""));
					break;

				}
			}

	}

	private static void insertMSG2Datatable(List<kvPair> lmsg) throws Exception
	{
		String newTrans = null, newId = null, groupId = null;
		for (kvPair kv : lmsg)
		{
			if (kv.key.equals("TranStartNum"))
				newTrans = kv.value;
			else if (kv.key.equals("Id"))
				newId = kv.value;
			else if (kv.key.equals("GroupId"))
				groupId = kv.value;
		}
		// if null, use the num as the trans num
		if (newTrans == null)
		{
			String[] arrRowId = lmsg.get(0).value.split("-");
			newTrans = arrRowId[2];
		}
		// receive a msg from different groups or nodes
		if (((Integer.parseInt(newId) != Integer.parseInt(dc.getlastId())) || (Integer
				.parseInt(groupId) != Integer.parseInt(dc.getlastGroupId())))
				&& (!dc.getlastGroupId().equals("-1")))
		{
			dc.setlastTransNum("-1");
		}

		if (Integer.parseInt(newTrans) < Integer.parseInt(dc.getlastTransNum()))
			return;
		Long ts = dc.insert1msg(lmsg, !newTrans.equals(dc.getlastTransNum()));
		if (ts != null)
			dc.setlastTransNum(newTrans);

		// dc.buildBatchRecord(lmsg, family);
	}

	private static void insertMSG2Datatable2(List<kvPair> lmsg) throws Exception
	{

		Long ts = dc.insert1msg(lmsg);

		// dc.buildBatchRecord(lmsg, family);
	}

	private static void generateQuery() throws IOException
	{
		String filename = "/home/user/workspace/data/shortmsg.txt";
		String qfilename = "/home/user/workspace/data/shortqry.txt";
		int numQuerry = 10000;
		// int max = 423642;
		int max = 25646;
		String qAttribute = "ObjectName";
		int[] randomQuerry = randomNum(max, numQuerry);

		BufferedReader br = null;
		BufferedWriter bw = new BufferedWriter(new FileWriter(qfilename));

		try
		{
			String sCurrentLine;
			br = new BufferedReader(new FileReader(filename));
			int count = -1;
			int icount = 0;
			while ((sCurrentLine = br.readLine()) != null)
			{
				count = count + 1;
				// System.out.println(sCurrentLine);
				if (!sCurrentLine.startsWith("<Message:"))
					continue;
				if (count % 1000 == 0)
					System.out.println(count);
				if (count != randomQuerry[icount])
					continue;
				icount++;
				try
				{
					List<kvPair> lmsg = ParseMSG.parseString(sCurrentLine);
					// ParseMSG.printLMSG(lmsg);
					for (int i = 0; i < lmsg.size(); i++)

						if (lmsg.get(i).key.equals(qAttribute))
						{
							bw.write(lmsg.get(i).value);
							bw.newLine();
							break;
						}

				}
				catch (Exception ex)
				{
					System.out.println(sCurrentLine);
				}

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
				bw.close();
			}
			catch (IOException ex)
			{
				ex.printStackTrace();
			}
		}

	}

	private static int[] randomNum(int max, int num)
	{
		// no duplicates
		// max: the max number of the range [0,max)
		// num: the total number of random numbers we want
		Random rnd = new Random();
		Set<Integer> generated = new HashSet<Integer>();
		while (generated.size() < num)
		{
			Integer next = rnd.nextInt(max);
			generated.add(next);
		}
		int[] rst = new int[num];
		int j = 0;
		for (Integer i : generated)
		{
			rst[j] = (int) i;
			j++;
		}
		Arrays.sort(rst);
		return rst;
	}

	private static double mean(double[] vec)
	{
		double ave = 0;
		int num = vec.length;
		for (double _value : vec)
			ave += _value;
		return ave / num;
	}

	public static double std(double[] data)
	{
		final int n = data.length;
		if (n < 2)
		{
			return Double.NaN;
		}
		double avg = data[0];
		double sum = 0;
		for (int i = 1; i < data.length; i++)
		{
			double newavg = avg + (data[i] - avg) / (i + 1);
			sum += (data[i] - avg) * (data[i] - newavg);
			avg = newavg;
		}
		return Math.sqrt(sum / n);
	}

	private static void writeFile(String filename, double[] timefinal) throws IOException
	{
		FileWriter fw = new FileWriter(filename, true);
		for (double _value : timefinal)
			fw.write(String.valueOf(_value) + "\r\n");
		fw.flush();
		fw.close();
	}

	private static void writeFile(String filename, String[] timefinal) throws IOException
	{
		FileWriter fw = new FileWriter(filename, true);
		for (String _value : timefinal)
			fw.write(String.valueOf(_value) + "\r\n");
		fw.close();
	}

	private static List<String> selectbyIdx(List<String> list, int[] idx)
	{
		List<String> rlist = new ArrayList<String>();
		for (int i = 0; i < idx.length; i++)
			rlist.add(list.get(idx[i]));
		return rlist;
	}
}
