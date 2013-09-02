package hbase;
import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;

//timestamp table, row: GroupId-Id, column: "TS"=ts value, column: "Num"=num value;
public class TSConnect extends HBaseConnect
{
	String rowId = "";
	String num = "";
	String family = "Attribute";
	String qualifierTs = "ts";
	String qualifierNum = "num";

	public TSConnect(String tableName, String family) throws IOException
	{
		super(tableName, family);
	}

	public String getTS(String GroupId, String Id)
	{
		String ts = null;
		return ts;
	}

	/* Build a TS table */
	void updateTSTable(String originalRowId, Long ts) throws Exception
	{
		// input: last inserted msg and last successful inserted ts
		// figure out the GroupId and Id
		idArray(originalRowId);

		addRecord(rowId, family, qualifierTs, ts.toString());
		addRecord(rowId, family, qualifierNum, num);
	}

	public String getLatestTS(String rowKey) throws IOException
	{
		String latestTS;
		idArray(rowKey);
		Result rst = getOneRecord(rowId);
		if (!rst.isEmpty())
		{
			latestTS = new String(rst.getValue(family.getBytes(),
					qualifierTs.getBytes()));
			return latestTS;
		}
		else
			return null;
	}

	public String getLatestNum(String rowKey) throws IOException
	{
		String num;
		idArray(rowKey);
		Result rst = getOneRecord(rowId);
		if (!rst.isEmpty())
		{
			num = new String(rst.getValue(family.getBytes(),
					qualifierNum.getBytes()));
			return num;
		}
		else
			return null;
	}

	private void idArray(String originalRowId)
	{
		String[] originalRowIdarr = originalRowId.split("-");
		String GroupId = originalRowIdarr[0];
		String Id = originalRowIdarr[1];
		rowId = GroupId + "-" + Id;
		this.num = originalRowIdarr[2];
	}

}