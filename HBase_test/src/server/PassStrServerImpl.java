package server;

import hbase.DataConnect;
import hbase.HBaseFun;
import hbase.IndexConnect;
import hbase.ParseMSG;
import hbase.kvPair;

import java.io.IOException;
import java.util.List;

import org.apache.thrift.TException;

import tserver.gen.PassStrServer;

public class PassStrServerImpl implements PassStrServer.Iface
{
	DataConnect dc;
	IndexConnect ic;

	@Override
	public int passStr(String st) throws Exception
	{
		// TODO Auto-generated method stub
		int a = 0;
		insertMSG(st);
		return a;
	}

	private void insertMSG(String msg) throws Exception
	{
		List<kvPair> lmsg = ParseMSG.parseString(msg);
		HBaseFun.insert1msg(lmsg);
	}

	@Override
	public List<List<tserver.gen.kvPair>> querry(String attribute, String value)
			throws TException, IOException
	{
		String qrryStr = "0";
		List<String> rowIds = ic.getMSGId(qrryStr);
		List<List<tserver.gen.kvPair>> rst = dc.retrieveLMSGsOnly(rowIds);
		return rst;
	}

	@Override
	public int connectDatabase(List<String> paras) throws TException,
			IOException
	{
		String filename = "/home/user/workspace/data/shortqry.txt";
		String datatableName = "shortmsg_all";
		String dataFamily = "Attribute";
		dc = new DataConnect(datatableName, dataFamily, "", "");
		String indextableName = "shortmsg_all_idx_ObjectName";
		String indexFamily = "RowID";
		ic = new IndexConnect(indextableName, indexFamily);
		return 0;
	}
}
