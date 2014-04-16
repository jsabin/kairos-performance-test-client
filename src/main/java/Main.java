import org.kairosdb.client.Client;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.*;
import org.kairosdb.client.response.Queries;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Results;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class Main
{
	public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException
	{
		// todo Want a telnet client as well - add this the client

		long count = 0;
		Client client = new HttpClient("http://localhost:8080");

		long startTime = System.currentTimeMillis();

		DataBlaster[] threads = new DataBlaster[80];
		for(int i = 0; i < threads.length; i++)
		{
			threads[i] = new DataBlaster();
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

		client.shutdown();
	}

	private static class DataBlaster extends Thread
	{
		private Client client;
		private long dpCount;

		private DataBlaster()
		{
			client = new HttpClient("http://localhost:8080");
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
				while (timestamp < (startTime + 1000 * 60 * 5)) // go for five minutes
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

					client.pushMetrics(builder);
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			try
			{
				client.shutdown();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}
}
