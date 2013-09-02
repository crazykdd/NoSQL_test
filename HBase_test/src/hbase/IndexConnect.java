package hbase;
import java.io.IOException;
import java.util.List;

public class IndexConnect extends HBaseConnect
{

	public IndexConnect(String tableName, String family) throws IOException
	{
		super(tableName, family);
	}

	public List<String> getMSGId(String attributeValue) throws IOException
	{
		return getQualifiers(attributeValue);
	}

}
