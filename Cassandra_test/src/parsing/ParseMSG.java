package parsing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseMSG
{

	/** @param args
	 * @throws Exception */
	public static void main(String[] args) throws Exception
	{
		boolean debug = true;
		Long startTime = System.nanoTime();
		for (int i = 0; i < 1; i++)
			testDebug(debug);
		// testString();
		Long endTime = System.nanoTime();
		System.out.println("Total time is: " + (endTime - startTime) / 1e9);
	}

	private static void testString()
	{
		String msg = "<Message:";
		for (int i = 0; i < msg.length(); i++)
		{
			String a = String.valueOf(msg.charAt(i));
		}

	}

	private static void testDebug(boolean debug)
	{
		if (debug)
		{

			String msg2 = "";
			String msg = "";

			Long startTime = System.nanoTime();
			for (int i = 0; i < 10000; i++)
			{
				msg = "<Message:<GroupId:40000001><Id:1><Num:138><Num:138><Num:138><Num:138><Num:138><Num:138><Num:138><Num:138><Date:27-Sep-2012><Time:01:43:07><Expiry:90><TranStartNum:124><Object:<Id:2625a01-1-8a><Parent:2625a01-1-7b><Name:Kiosk12_background.jpg><Version:1>><Contents:<Data:<FileName:Kiosk12_background.jpg><ElementName:Kiosk12_background.jpg><FileSigner:Escher1><FileVersion:><FileSignature:rYiDLswpTd4VyMngpB6fKbK1BgjAcWp8GmhMSS8tLC1kqgFanULhl6Vt5UH9z13Vdoi?85ulukEGs0FW3MkLoE><PathName:%WEBRIPOSTEDIR%\\Image><Signer:EscherConfig>><Signature:suS7LgTHhE3cGK5EZReIj6Lm@bkLjWQzWqEMP5IBbo@4mXUtEknKEfxR5MpRhbE0PL18?lWOLlf?hLe3ZivD4U>><CRC:4BFF7788>>";
				// String msg = "<Message:<X:<A:<B:C><D:E><F:G><H:I>>>>";
				List<kvPair> lmsg = parseString(msg);

				// List<kvPair> lmsg = getKVs(msg);
				msg2 = reconstructMSG(lmsg);
				// inverseReorganizeLMSG(lmsg);

			}
			Long endTime = System.nanoTime();
			System.out.println("Total time is: " + (endTime - startTime) / 1e9);

			// System.out.print(msg2 + "\r\n");
			// System.out.print(msg + "\r\n");
			System.out.println(msg2.equals(msg));
			/*
			 * DataConnect dc = new DataConnect("faketable", "fake1", "", "");
			 * String newTrans = null; for (kvPair kv : lmsg) { if
			 * (kv.key.equals("TranStartNum")) newTrans = kv.value; } if
			 * (newTrans == null) newTrans = lmsg.get(0).value.split("-")[2];
			 * boolean a = !newTrans.equals(dc.getlastTransNum());
			 * dc.insert1msg(lmsg, a); dc.close();
			 */
		}
		else
			test();
	}

	public static void test()
	{

		BufferedReader br = null;

		try
		{
			String sCurrentLine;
			br = new BufferedReader(new FileReader(
					"/mnt/hgfs/Yang/ParseMSG/data/MSGs.txt"));
			int count = 0;
			while ((sCurrentLine = br.readLine()) != null)
			{
				// System.out.println(sCurrentLine);
				if (!sCurrentLine.startsWith("<Message:"))
					continue;
				try
				{
					if (checkMSG(sCurrentLine))
						;// System.out.println(count);
					else
						System.out.println(sCurrentLine);
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

	public static List<kvPair> parseString(String msg)
	{

		List<kvPair> lmsg = getKVs(msg);
		// printLMSG(lmsg);
		lmsg = reorganizeLMSG(lmsg);
		// printLMSG(lmsg);
		cvrowID(lmsg);
		// printLMSG(lmsg);
		lmsg = sortLMSGbyName(lmsg);
		// printLMSG(lmsg);

		return lmsg;

	}

	private static List<kvPair> reorganizeLMSG(List<kvPair> lmsg)
	{
		List<kvPair> lmsg2 = new ArrayList<kvPair>();
		String attridx = lmsg.get(lmsg.size() - 1).value;
		String[] attridxarr = attridx.split("\\.");
		List<String> nattridxarr = new ArrayList<String>();
		nattridxarr.add(attridxarr[0]);
		nattridxarr.add(attridxarr[1]);
		int duplicateidx = -1;
		for (int i = 0; i < lmsg.size() - 1; i++)
		{
			boolean isduplicate = false;
			String key = lmsg.get(i).key;
			String value = lmsg.get(i).value;
			// first insertion
			if (lmsg2.isEmpty())
			{
				lmsg2.add(new kvPair(key, value));
				continue;
			}

			for (int j = 0; j < lmsg2.size(); j++)
			{
				if (lmsg2.get(j).key.equals(key))
				{
					isduplicate = true;
					duplicateidx = j;
					break;
				}

			}
			if (isduplicate == false)
			{
				lmsg2.add(new kvPair(key, value));

				nattridxarr.add(attridxarr[i + 1]);
			}
			else
			{
				String tmpvalue = lmsg2.get(duplicateidx).value;
				if (tmpvalue.contains("<"))
					lmsg2.set(duplicateidx, new kvPair(key, tmpvalue + "<"
							+ value + ">") {});
				else
					lmsg2.set(duplicateidx, new kvPair(key, "<" + tmpvalue
							+ ">" + "<" + value + ">"));

				nattridxarr.set(duplicateidx + 1,
						nattridxarr.get(duplicateidx + 1) + "_"
								+ attridxarr[i + 1]);
				duplicateidx = -1;
			}

		}

		lmsg2.add(new kvPair(lmsg.get(lmsg.size() - 1).key, strlist2str(
				nattridxarr, ".")));
		return lmsg2;

	}

	private static List<kvPair> inverseReorganizeLMSG(List<kvPair> lmsg)
	{
		List<kvPair> lmsg2 = new ArrayList<kvPair>();
		String attridx = lmsg.get(lmsg.size() - 1).value;
		String[] attridxarr = attridx.split("\\.");
		List<String> nattridxarr = new ArrayList<String>();
		nattridxarr.add(attridxarr[0]);
		for (int i = 0; i < lmsg.size() - 1; i++)
		{
			List<String> lvalues = new ArrayList<String>();
			String key = lmsg.get(i).key;
			String value = lmsg.get(i).value;
			if (attridxarr[i + 1].contains("_"))
			{
				lvalues = parseValue(value);
				String[] cruattridxarr = attridxarr[i + 1].split("_");
				for (int j = 0; j < lvalues.size(); j++)
				{
					lmsg2.add(new kvPair(key, lvalues.get(j)));
					nattridxarr.add(cruattridxarr[j]);
				}
			}
			else
			{
				lmsg2.add(new kvPair(key, value));
				nattridxarr.add(attridxarr[i + 1]);
			}

		}
		lmsg2.add(new kvPair(lmsg.get(lmsg.size() - 1).key, strlist2str(
				nattridxarr, ".")));
		lmsg2 = sortLMSG(lmsg2);

		return lmsg2;

	}

	private static List<kvPair> getKVs(String msg)
	{
		List<kvPair> lmsg = new ArrayList<kvPair>();
		try
		{
			boolean haschild = false;
			int parentlayer = 1;
			Stack<Integer> startstack = new Stack<Integer>();
			Stack<Integer> colonstack = new Stack<Integer>();
			int len = msg.length();
			// first element stores the length of the original message
			String attridx = String.valueOf(len);
			for (int i = 0; i < len; i++)
			{

				if (msg.charAt(i) == '<')
				{
					startstack.push(i);
				}
				else if (msg.charAt(i) == ':')
				{
					// one ":" corresponds to one "<"
					if (colonstack.size() == startstack.size() - 1)
						colonstack.push(i);
				}
				else if (msg.charAt(i) == '>')
				{
					int startidx = startstack.lastElement();
					int colonidx = colonstack.lastElement();
					int lastidx = i;
					startstack.pop();
					colonstack.pop();
					// ignore "<" and ":"
					String key = msg.substring(startidx + 1, colonidx);

					// ignore "<" and ":"
					String value = msg.substring(colonidx + 1, lastidx);

					if (startstack.size() >= 2)
					{
						if ((haschild == true)
								&& (parentlayer > startstack.size()))
						// parent is greater than the current stack size means
						// it has children popped up

						{
							parentlayer = startstack.size();
							continue;
						}
						// ignore the first stack element "Message"
						// can be optimized by using a list of parent strings
						for (int j = startstack.size(); j > 1; j--)
						{
							String tmpstr = msg.substring(
									startstack.get(j - 1) + 1,
									colonstack.get(j - 1));
							key = tmpstr + "." + key;
						}
						lmsg.add(new kvPair(key, value));
						attridx = attridx + "." + String.valueOf(startidx);
						haschild = true;
						parentlayer = startstack.size();
					}
					// first layer other than "Message"
					else if (startstack.size() == 1)
					{
						if (haschild == true)
						{
							haschild = false;
							continue;
						}
						attridx = attridx + "." + String.valueOf(startidx);
						// System.out.print(key + ":"+value +"\r\n");
						lmsg.add(new kvPair(key, value));
					}
				}
				else
					continue;
			}
			// System.out.print("AttributeIndex" + ":"+attridx +"\r\n");
			lmsg.add(new kvPair("AttributeIndex", attridx));
		}
		catch (Exception ex)
		{
			System.out.print(ex);
		}
		return lmsg;
	}

	private static void cvrowID(List<kvPair> lmsg)
	{
		String rowIDkey = "rowID";
		// concatenate
		String rowIDvalue = lmsg.get(0).value + "-" + lmsg.get(1).value + "-"
				+ lmsg.get(2).value;
		lmsg.add(0, new kvPair(rowIDkey, rowIDvalue));
		// lmsg.remove(1);
		// lmsg.remove(1);

	}

	private static List<kvPair> sortLMSGbyName(List<kvPair> lmsg)
	{
		List<kvPair> lmsg2 = new ArrayList<kvPair>();

		String[] keys = new String[lmsg.size() - 2];// exclude rowID and
													// AttributeIndex
		for (int i = 0; i < keys.length; i++)
			keys[i] = lmsg.get(i + 1).key;
		ArrayIndexComparator comparator = new ArrayIndexComparator(keys);
		Integer[] indexes = comparator.createIndexArray();
		Arrays.sort(indexes, comparator);
		int[] iindex = new int[indexes.length];
		// convert Integer to int
		for (int i = 0; i < iindex.length; i++)
			iindex[i] = (int) indexes[i];
		// add sorted list of msgs
		lmsg2.add(lmsg.get(0));// first one stays the same
		// ini AttributeIndex
		String attrindex = lmsg.get(lmsg.size() - 1).value;
		String[] arrattrindex = attrindex.split("\\.");
		String[] narrattrindex = new String[arrattrindex.length];
		narrattrindex[0] = arrattrindex[0];
		// narrattrindex[1] = arrattrindex[1];
		// narrattrindex[2] = arrattrindex[2];
		// narrattrindex[3] = arrattrindex[3];
		for (int i = 0; i < iindex.length; i++)
		{
			lmsg2.add(lmsg.get(iindex[i] + 1));
			narrattrindex[i + 1] = arrattrindex[iindex[i] + 1];
		}
		lmsg2.add(new kvPair(lmsg.get(lmsg.size() - 1).key, strarr2str(
				narrattrindex, ".")));
		return lmsg2;
	}

	public static String reconstructMSG(List<kvPair> lmsg)
	{
		lmsg.remove(0);
		lmsg = inverseReorganizeLMSG(lmsg);
		// printLMSG(lmsg);
		String msg = kv2string(lmsg);
		return msg;
	}

	private static String kv2string(List<kvPair> lmsg)
	{
		String attridx = lmsg.get(lmsg.size() - 1).value;
		String[] attridxarr = attridx.split("\\.");
		char[] msg = new char[Integer.parseInt(attridxarr[0])];
		String startmsg = "<Message:";
		String[] parentlayer = new String[] {};
		String[] attrs = new String[] {};
		int lastupdateidx = startmsg.length() + 1;
		for (int i = 0; i < msg.length; i++)
		{
			if (i < startmsg.length())
				msg[i] = startmsg.charAt(i);
			else
				msg[i] = 'x';
		}
		msg[msg.length - 1] = '>';

		for (int i = 0; i < lmsg.size() - 1; i++)
		{
			String key = lmsg.get(i).key;
			// System.out.println(key);
			String value = lmsg.get(i).value;
			int attStartidx = -1;

			String curidxstr = attridxarr[i + 1];
			attStartidx = Integer.parseInt(curidxstr);
			if (key.contains("."))
				attrs = key.split("\\.");
			else
				attrs = new String[] {};

			// String tmp = strarr2str(msg, "");

			if (parentlayer.length != 0)
			{
				// System.out.println("***before***\r\n"
				// + tmp.substring(0, lastupdateidx));
				int numd = numDiff(parentlayer, attrs);

				for (int j = 0; j < numd; j++)
					msg[lastupdateidx + j] = '>';
				// tmp = strarr2str(msg, "");
				// System.out.println(tmp.substring(0, lastupdateidx + numd));
			}
			insert1row(msg, key, value, attStartidx, lastupdateidx);

			parentlayer = attrs;
			if (attrs.length == 0)
				lastupdateidx = attStartidx + key.length() + value.length() + 3;// original
																				// 4
			else
				lastupdateidx = attStartidx + attrs[attrs.length - 1].length()
						+ value.length() + 3;//

			// tmp = strarr2str(msg, "");

		}
		/*
		 * for (int i = 0; i < msg.length; i++) if (msg[i] == "x") msg[i] = ">";
		 */
		String msgstr = chararr2str(msg, "");
		// System.out.print(msgstr+"\r\n");

		return msgstr;
	}

	private static String kv2string_old(List<kvPair> lmsg)
	{
		String attridx = lmsg.get(lmsg.size() - 1).value;
		String[] attridxarr = attridx.split("\\.");
		String[] msg = new String[Integer.parseInt(attridxarr[0])];
		String startmsg = "<Message:";
		String[] parentlayer = new String[] {};
		String[] attrs = new String[] {};
		int lastupdateidx = startmsg.length() + 1;
		for (int i = 0; i < msg.length; i++)
		{
			if (i < startmsg.length())
				msg[i] = String.valueOf(startmsg.charAt(i));
			else
				msg[i] = "x";
		}
		msg[msg.length - 1] = ">";

		for (int i = 0; i < lmsg.size() - 1; i++)
		{
			String key = lmsg.get(i).key;
			// System.out.println(key);
			String value = lmsg.get(i).value;
			int attStartidx = -1;

			String curidxstr = attridxarr[i + 1];
			attStartidx = Integer.parseInt(curidxstr);
			if (key.contains("."))
				attrs = key.split("\\.");
			else
				attrs = new String[] {};

			String tmp = strarr2str(msg, "");

			if (parentlayer.length != 0)
			{
				// System.out.println("***before***\r\n"
				// + tmp.substring(0, lastupdateidx));
				int numd = numDiff(parentlayer, attrs);

				for (int j = 0; j < numd; j++)
					msg[lastupdateidx + j] = ">";
				// tmp = strarr2str(msg, "");
				// System.out.println(tmp.substring(0, lastupdateidx + numd));
			}
			insert1row(msg, key, value, attStartidx, lastupdateidx);

			parentlayer = attrs;
			if (attrs.length == 0)
				lastupdateidx = attStartidx + key.length() + value.length() + 3;// original
																				// 4
			else
				lastupdateidx = attStartidx + attrs[attrs.length - 1].length()
						+ value.length() + 3;//

			tmp = strarr2str(msg, "");

		}
		/*
		 * for (int i = 0; i < msg.length; i++) if (msg[i] == "x") msg[i] = ">";
		 */
		String msgstr = strarr2str(msg, "");
		// System.out.print(msgstr+"\r\n");

		return msgstr.toString();
	}

	private static void insert1row(String[] msg, String key, String value,
			int attStartidx, int lastupdateidx)
	{
		int attEndidx;
		String[] attrs = new String[] {};
		if (key.contains("."))
		{
			// has children
			attrs = key.split("\\.");
			// insert the child
			int lastidx = key.lastIndexOf('.');

			key = key.substring(lastidx + 1);
			String attr = key + ":" + value;
			insertstr(msg, attr, attStartidx);
			attEndidx = attStartidx + attr.length() + 1;
			msg[attStartidx] = "<";
			msg[attEndidx] = ">";
			attStartidx = attStartidx - 1;

			// insert parents
			for (int j = attrs.length - 2; j >= 0; j--)
			{
				String tmpattr = "<" + attrs[j] + ":";
				attStartidx = attStartidx - tmpattr.length();
				boolean suc = insertstr(msg, tmpattr, attStartidx);
				if (suc)
				{
					// msg[attStartidx] = "<";
					// attStartidx = attStartidx - 1;
				}
				else
					break;
				attEndidx = attEndidx + 1;
				// attEndidx = Math.min(attEndidx, msg.length - 1);
				// msg[attEndidx] =">";
			}

		}
		else
		{
			String attr = key + ":" + value;
			insertstr(msg, attr, attStartidx);
			attEndidx = attStartidx + attr.length() + 1;
			msg[attStartidx] = "<";
			msg[attEndidx] = ">";
		}
		// return attrs;
	}

	private static void insert1row(char[] msg, String key, String value,
			int attStartidx, int lastupdateidx)
	{
		int attEndidx;
		String[] attrs = new String[] {};
		if (key.contains("."))
		{
			// has children
			attrs = key.split("\\.");
			// insert the child
			key = attrs[attrs.length - 1];
			String attr = key + ":" + value;
			insertstr(msg, attr, attStartidx);
			attEndidx = attStartidx + attr.length() + 1;
			msg[attStartidx] = '<';
			msg[attEndidx] = '>';
			attStartidx = attStartidx - 1;

			// insert parents

			for (int j = attrs.length - 2; j >= 0; j--)
			{
				String tmpattr = "<" + attrs[j] + ":";
				attStartidx = attStartidx - tmpattr.length();
				boolean suc = insertstr(msg, tmpattr, attStartidx);
				if (suc)
				{
					// msg[attStartidx] = "<";
					// attStartidx = attStartidx - 1;
				}
				else
					break;
				attEndidx = attEndidx + 1;
				// attEndidx = Math.min(attEndidx, msg.length - 1);
				// msg[attEndidx] =">";
			}

		}
		else
		{
			String attr = key + ":" + value;
			insertstr(msg, attr, attStartidx);
			attEndidx = attStartidx + attr.length() + 1;
			msg[attStartidx] = '<';
			msg[attEndidx] = '>';
		}
		// return attrs;
	}

	private static boolean insertstr(String[] msg, String attr, int attstartidx)
	{

		int attrlen = attr.length();
		int j = 0;
		// check
		for (j = 0; j < attrlen; j++)
			if (msg[attstartidx + j + 1] != "x")
				return false;
		for (j = 0; j < attrlen; j++)
			msg[attstartidx + j + 1] = String.valueOf(attr.charAt(j));
		//
		return true;

	}

	private static boolean insertstr(char[] msg, String attr, int attstartidx)
	{

		int attrlen = attr.length();
		int j = 0;
		// check
		for (j = 0; j < attrlen; j++)
			if (msg[attstartidx + j + 1] != 'x')
				return false;
		for (j = 0; j < attrlen; j++)
			msg[attstartidx + j + 1] = attr.charAt(j);
		//
		return true;

	}

	private static boolean checkMSG(String msg)
	{
		List<kvPair> lmsg = parseString(msg);

		String msg2 = reconstructMSG(lmsg);
		if (msg.equals(msg2))
			return true;
		else
			return false;
	}

	private static List<String> parseValue(String value)
	{
		// parse the <value> into a list
		List<String> lvalue = new ArrayList<String>();
		int startidx = -1;
		int endidx = -1;
		for (int i = 0; i < value.length(); i++)
		{
			if (value.charAt(i) == '<')
				startidx = i + 1;
			else if (value.charAt(i) == '>')
			{
				endidx = i;
				lvalue.add(value.substring(startidx, endidx));
			}

		}
		return lvalue;

	}

	private static List<String> parseValue_old(String value)
	{
		// parse the <value> into a list
		// use regular expression
		Pattern p = Pattern.compile("<(.*?)>");
		Matcher m = p.matcher(value);
		List<String> lvalue = new ArrayList<String>();
		while (m.find())
			lvalue.add(m.group(1));
		return lvalue;

	}

	// /sort list of msg using the ascending order of their attribute indices
	private static List<kvPair> sortLMSG(List<kvPair> lmsg)
	{
		List<kvPair> lmsg2 = new ArrayList<kvPair>();
		String attridx = lmsg.get(lmsg.size() - 1).value;
		String[] attridxarr = attridx.split("\\.");
		String[] nattridxarr = new String[attridxarr.length];
		nattridxarr[0] = attridxarr[0];
		float[] intattridxarr = new float[attridxarr.length - 1];
		int[] idxarr = new int[intattridxarr.length];
		for (int i = 0; i < intattridxarr.length; i++)
		{
			intattridxarr[i] = Integer.parseInt(attridxarr[i + 1]);
			idxarr[i] = i;
		}
		sort.quicksort(intattridxarr, idxarr);
		for (int i = 0; i < idxarr.length; i++)
		{
			lmsg2.add(new kvPair(lmsg.get(idxarr[i]).key,
					lmsg.get(idxarr[i]).value));
		}

		// assign attribute index
		for (int i = 1; i < nattridxarr.length; i++)
			nattridxarr[i] = attridxarr[idxarr[i - 1] + 1];
		lmsg2.add(new kvPair(lmsg.get(lmsg.size() - 1).key, strarr2str(
				nattridxarr, ".")));
		return lmsg2;
	}

	private static String strarr2str(String[] msg, String seperator)
	{
		StringBuilder builder = new StringBuilder();
		for (String s : msg)
		{
			builder.append(s + seperator);
		}
		if (seperator.length() > 0)
			builder.deleteCharAt(builder.length() - seperator.length());
		return builder.toString();
	}

	private static String chararr2str(char[] msg, String seperator)
	{
		StringBuilder builder = new StringBuilder();
		for (char s : msg)
		{
			builder.append(String.valueOf(s) + seperator);
		}
		if (seperator.length() > 0)
			builder.deleteCharAt(builder.length() - seperator.length());
		return builder.toString();
	}

	private static String strlist2str(List<String> msg, String seperator)
	{
		StringBuilder builder = new StringBuilder();
		for (String s : msg)
		{
			builder.append(s + seperator);
		}
		builder.deleteCharAt(builder.length() - seperator.length());
		return builder.toString();
	}

	private static int numDiff(String[] a, String[] b)
	{
		// count the number of differences from the beginning:
		// discard the last element of a and b
		// e.g., a = [m,n,p];b=[m,n,p,q], return 0;
		// a = [a,m,n],b=[b,a,m,n], return 3;
		int count = 0;
		int num = a.length - 1;
		int num2 = Math.min(num, b.length - 1);
		for (int i = 0; i < num2; i++)
			if (!a[i].equals(b[i]))
				break;
			else
				count++;
		return num - count;
	}

	public static void printLMSG(List<kvPair> lmsg)
	{
		for (int i = 0; i < lmsg.size(); i++)
			System.out
					.print(lmsg.get(i).key + ":" + lmsg.get(i).value + "\r\n");
	}

}
