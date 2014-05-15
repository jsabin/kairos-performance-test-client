package org.kairosdb.perftest;

import org.kairosdb.client.HttpClient;
import org.kairosdb.client.TelnetClient;
import org.kairosdb.client.builder.Metric;
import org.kairosdb.client.builder.MetricBuilder;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Random;

public class Main
{
	public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException
	{
		String host = args[0];
		String clientType = args[1];
		int numThreads = Integer.parseInt(args[2]);
		int minutes = Integer.parseInt(args[3]);
		long count = 0;
//		Client client = new HttpClient("http://localhost:8080");

		long startTime = System.currentTimeMillis();

		DataBlaster[] threads = new DataBlaster[numThreads];
		for (int i = 0; i < threads.length; i++)
		{
			threads[i] = new DataBlaster(host, clientType, minutes);
		}

		for (Thread thread : threads)
		{
			thread.start();
		}

		for (DataBlaster thread : threads)
		{
			thread.join();
			System.out.println(thread.dpCount);
			count += thread.getDpCount();
		}

//		QueryBuilder query = QueryBuilder.getInstance();
//		query.setStart(new Date(startTime));
//		query.setEnd(new Date(System.currentTimeMillis()));
//		QueryMetric metric = query.addMetric("jeff_test");
//		metric.addAggregator(AggregatorFactory.createCountAggregator(1, TimeUnit.MINUTES));
//		metric.addAggregator(AggregatorFactory.createDivAggregator(60));
//
//		QueryResponse response = client.query(query);
//		List<String> errors = response.getErrors();
//		if (errors.size() > 0)
//		{
//			for (String error : errors)
//			{
//				System.out.println(error);
//			}
//			return;
//		}
//		for (Queries queries : response.getQueries())
//		{
//			for (Results results : queries.getResults())
//			{
//				System.out.println(results.getName());
//				for (DataPoint dataPoint : results.getDataPoints())
//				{
//					if (dataPoint.isInteger())
//					{
//						System.out.println(((LongDataPoint) dataPoint).getValue());
//					}
//					else
//					{
//						System.out.println(((DoubleDataPoint) dataPoint).getValue());
//					}
//				}
//			}
//		}
		System.out.println("Data points/sec = " + ((count * 1000) / (System.currentTimeMillis() - startTime)));

//		client.shutdown();
	}

	private static class DataBlaster extends Thread
	{
		private String clientType;
		private HttpClient httpClient;
		private TelnetClient telnetClient;
		private int minutes;
		private long dpCount;

		private DataBlaster(String host, String clientType, int minutes) throws IOException
		{
			this.clientType = clientType;
			this.minutes = minutes;

			if (clientType.equals("http"))
			{
				httpClient = new HttpClient(host);
			}
			else
			{
				telnetClient = new TelnetClient(host, 4242);
			}
		}

		public long getDpCount()
		{
			return dpCount;
		}

		@Override
		public void run()
		{
			try
			{
				Random random = new Random();

				long startTime = System.currentTimeMillis();
				long timestamp = 0;
				while (timestamp < (startTime + 1000 * 60 * minutes))
				{
					MetricBuilder builder = MetricBuilder.getInstance();

					for (int i = 0; i < 1000; i++)
					{
						Metric metric = builder.addMetric("jeff_test");
						metric.addTag("host", getName() + i);
						timestamp = i == 0 ? startTime : System.currentTimeMillis();
						metric.addDataPoint(timestamp, random.nextLong());
//						Thread.sleep(1);
						dpCount++;
					}

					if (clientType.equals("http"))
					{
						httpClient.pushMetrics(builder);
					}
					else
					{
						telnetClient.pushMetrics(builder);
					}
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			try
			{
				if (clientType.equals("http"))
				{
					httpClient.shutdown();
				}
				else
				{
					telnetClient.shutdown();
				}
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}
}
