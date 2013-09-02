package hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class DataConnect extends HBaseConnect
{
	String idxfamiliy = "RowID";
	List<IndexConnect> lidxtable;
	String[] attributeNames;
	int[] types;
	TSConnect tsTable = null;
	private String lastTransNum = "-1", lastId = "-1", lastGroupId = "-1";

	// create both tables
	@SuppressWarnings("null")
	public DataConnect(String datatableName, String datafamily, String tstableName, String itsfamily)
			throws IOException
	{
		super(datatableName, datafamily);
		if ((tstableName != null) && (!tstableName.equals("")))
			tsTable = new TSConnect(tstableName, itsfamily);
	}

	// construct function including index table
	public DataConnect(String datatableName, String datafamily, String tstableName,
			String itsfamily, String[] attributeNames, int[] types) throws IOException
	{
		super(datatableName, datafamily);
		if ((tstableName != null) && (!tstableName.equals("")))
			tsTable = new TSConnect(tstableName, itsfamily);
		String idxtablename = "";
		lidxtable = new ArrayList<IndexConnect>();
		for (String attrname : attributeNames)
		{
			idxtablename = datatableName + "_idx_" + attrname;
			lidxtable.add(new IndexConnect(idxtablename, idxfamiliy));
		}
		this.attributeNames = attributeNames;
		this.types = types;
	}

	/* insert a single msg */
	public Long insert1msg(List<kvPair> lmsg, boolean newTrans) throws Exception
	{
		if (lastTransNum.equals("-1"))
		{
			lastTransNum = tsTable.getLatestNum(lmsg.get(0).value);

			if (lastTransNum != null)
				return null;
		}
		table.put(buildBatchRecord(lmsg));
		Long ts = System.currentTimeMillis();
		if (newTrans)
			tsTable.updateTSTable(lmsg.get(0).value, ts);
		if (lidxtable != null)
			insertMSG2idxtable(lmsg);
		lastGroupId = lmsg.get(0).value.split("-")[0];
		lastId = lmsg.get(0).value.split("-")[1];
		return ts;
	}

	public Long insert1msg(List<kvPair> lmsg) throws Exception
	{

		table.put(buildBatchRecord(lmsg));
		Long ts = System.currentTimeMillis();

		return ts;
	}

	public List<String> retrieveMSGs(List<String> rowIds) throws IOException
	{
		Result[] rsts = getRecordsBatch(rowIds);
		List<String> MSGs = new ArrayList<String>();
		for (Result rs : rsts)
		{
			List<kvPair> lmsg = rst2kvPair(rs);
			String msg = ParseMSG.reconstructMSG(lmsg);
			MSGs.add(msg);
		}
		return MSGs;
	}

	public Result[] retrieveRstOnly(List<String> rowIds) throws IOException
	{
		// Result[] rsts = getRecordsBatch(rowIds);
		Result[] rsts = getRecordsBatch(rowIds, "CRC");
		return rsts;
	}

	public List<List<tserver.gen.kvPair>> retrieveLMSGsOnly(List<String> rowIds) throws IOException
	{
		Result[] rsts = getRecordsBatch(rowIds);
		List<List<tserver.gen.kvPair>> LMSGs = new ArrayList<List<tserver.gen.kvPair>>();
		for (Result rs : rsts)
		{
			List<tserver.gen.kvPair> lmsg = rst2kvPair2(rs);
			LMSGs.add(lmsg);
		}
		return LMSGs;
	}

	public String retrieveMSGs(String rowId) throws IOException
	{
		Result rst = getOneRecord(rowId);
		List<kvPair> lmsg = rst2kvPair(rst);
		String msg = ParseMSG.reconstructMSG(lmsg);
		return msg;
	}

	@Override
	public void close() throws IOException
	{
		if (table != null)
			table.close();
		if (tsTable != null)
			tsTable.close();
		if (lidxtable != null)
		{
			for (IndexConnect ic : lidxtable)
				ic.close();
		}
	}

	public void createIdxTableOffline(String tableName, String[] attributes,
			String[] idxtablenames, String[] idxfamilys) throws Exception
	{
		List<HTable> lidxtable = new ArrayList<HTable>();
		for (int i = 0; i < attributes.length; i++)
		{
			creatTable(idxtablenames[i], idxfamilys);
			lidxtable.add(new HTable(conf, idxtablenames[i]));
		}

		// get the scanner
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

	}

	public void createIdxTableOffline_type(String[] attributes, int[] types,
			String[] idxtablenames, String[] idxfamilys) throws Exception
	{
		List<HTable> lidxtable = new ArrayList<HTable>();
		for (int i = 0; i < attributes.length; i++)
		{
			creatTable(idxtablenames[i], idxfamilys);
			lidxtable.add(new HTable(conf, idxtablenames[i]));
		}

		// get the scanner
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
							switch (types[i])
							{
							case 1:// int
								int tmpvalue = Integer.parseInt(attributeValue);
								byte[] value = intToByteArray(tmpvalue);
								// tmpvalue = byteArrayToInt(value);
								lidxtable.get(i).put(
										buildOneRecord(value, idxfamilys[0], rowID, ""));
								break;
							case 2:// "string"
								lidxtable.get(i).put(
										buildOneRecord(attributeValue, idxfamilys[0], rowID, ""));
								break;
							case 3:// "date":
								;
								break;
							default:
								break;

							}

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

	}

	public void createJointIdxTableOffline(String tableName, String[] attributes,
			String idxtablename, String[] idxfamilys) throws Exception
	{
		// joint index table for two attributes
		// hard coded version number inside the function

		// creatTable(idxtablename, idxfamilys);

		HBaseConnect idxtable = new HBaseConnect(idxtablename, "");
		// get the scanner
		Scan s = new Scan();

		ResultScanner ss = table.getScanner(s);

		Long startTime = System.nanoTime();
		int nAtt = attributes.length;
		// build index
		for (Result r : ss)
		{
			String rowId = "";
			String[] arrattributeValue = new String[nAtt];
			String versionNum = "";
			boolean hasAttribute = true;
			for (int i = 0; i < nAtt; i++)
			{

				byte[] tmp = r.getValue(Bytes.toBytes("Attribute"), Bytes.toBytes(attributes[i]));
				// String a = new String(tmp);
				if (tmp == null)
					break;
				else
					arrattributeValue[i] = new String(tmp);

				/* hasAttribute = false; for (KeyValue kv : r.raw()) { String
				 * attributeName = new String(kv.getQualifier()); { if
				 * (attributeName.equals(attributes[i])) { hasAttribute = true;
				 * arrattributeValue[i] = new String(kv.getValue()); } // get
				 * version number and rowId else if ((i == nAtt - 1) &&
				 * (attributeName.equals("Version"))) { versionNum = new
				 * String(kv.getValue()); rowId = new String(kv.getRow()); } } }
				 * if (!hasAttribute) break; */
			}
			rowId = new String(r.getRow());
			byte[] tmp1 = r.getValue(Bytes.toBytes("Attribute"), Bytes.toBytes("Version"));
			if (tmp1 == null)
				continue;
			else
				versionNum = new String(tmp1);

			// build the new rowId for the index table
			String newRowId = arrattributeValue[0];
			for (int i = 1; i < nAtt; i++)
			{
				newRowId = newRowId + "><" + arrattributeValue[i];
			}

			// check the version number and compare
			Result rst = idxtable.getOneRecord(newRowId);
			if (rst.isEmpty())
			{
				// insert
				idxtable.addRecord(newRowId, idxfamilys[0], rowId, versionNum);
			}
			else
			{
				// compare
				// there is only one column in each row, so return the value of
				// the first column
				String tmpVN = new String(rst.value());
				int originalVersionNumber = Integer.parseInt(tmpVN);
				int newVersionNumber = Integer.parseInt(versionNum);
				if (newVersionNumber > originalVersionNumber)
					// insert
					idxtable.addRecord(newRowId, idxfamilys[0], rowId, versionNum);
			}
		}
		Long endTime = System.nanoTime();
		System.out.println("time to build index table(s) is: " + (endTime - startTime) / 1e9);
		idxtable.close();

	}

	public String getlastTransNum()
	{
		return lastTransNum;
	}

	public void setlastTransNum(String lastTransNum)
	{
		this.lastTransNum = lastTransNum;
	}

	private void insertMSG2idxtable(List<kvPair> lmsg) throws Exception
	{
		// a list of kvpair to be inserted into idx table
		String rowID = lmsg.get(0).value;
		for (int i = 0; i < attributeNames.length; i++)
			for (int j = 1; j < lmsg.size() - 1; j++)
			{
				String attname = lmsg.get(j).key;
				if (attname.equals(attributeNames[i]))
				{
					// attribute value as new rowID
					// original rowId as qualifier
					// value is empty
					String attributeValue = lmsg.get(j).value;
					switch (types[i])
					{
					case 1:// string
						int tmpvalue = Integer.parseInt(attributeValue);
						byte[] value = intToByteArray(tmpvalue);
						lidxtable.get(i).addRecord(value, idxfamiliy, rowID, "");
						break;
					case 2:// int
						lidxtable.get(i).addRecord(attributeValue, idxfamiliy, rowID, "");
						break;
					default:
						break;
					}
					break;
				}
			}

	}

	// build batch record for one MSG
	private Put buildBatchRecord(List<kvPair> lmsg)
	{
		String rowId = lmsg.get(0).value;
		Put put = new Put(Bytes.toBytes(rowId));
		for (int i = 1; i < lmsg.size(); i++)
			put.add(Bytes.toBytes(family), Bytes.toBytes(lmsg.get(i).key),
					Bytes.toBytes(lmsg.get(i).value));
		return put;
	}

	public String getlastId()
	{
		return lastId;
	}

	public String getlastGroupId()
	{
		// TODO Auto-generated method stub
		return lastGroupId;
	}
}
