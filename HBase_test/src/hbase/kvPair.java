package hbase;

public class kvPair
{
	String key;
	String value;

	public kvPair(String key, String value)
	{
		this.key = key;
		this.value = value;
	}

	public kvPair()
	{
		this.key = "";
		this.value = "";
	}
}