package server;

import hbase.HBaseFun;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;

import tserver.gen.PassStrServer;

public class Server
{
	private void start() throws Exception
	{
		try
		{
			TServerSocket serverTransport = new TServerSocket(7911);
			// TimeServer.Processor processor = new TimeServer.Processor(new
			// TimeServerImpl());
			// Factory protFactory = new TBinaryProtocol.Factory(true, true);
			// TServer server = new TSimpleServer(new
			// Args(serverTransport).processor(processor));

			// ini
			HBaseFun.ini("thrift_test3_final", new String[] { "Attribute" });

			PassStrServer.Processor processor = new PassStrServer.Processor(new PassStrServerImpl());
			TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));

			// TServer server = new TThreadPoolServer(processor,
			// serverTransport, protFactory);
			System.out.println("Starting server on port 7911 ...");
			server.serve();
			HBaseFun.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			HBaseFun.close();
			System.out.println("Server closed.");
		}
	}

	public static void main(String args[]) throws Exception
	{
		Server srv = new Server();
		srv.start();
	}
}