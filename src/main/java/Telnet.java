import org.kairosdb.client.TelnetClient;
import org.kairosdb.client.builder.Metric;
import org.kairosdb.client.builder.MetricBuilder;

import java.io.IOException;
import java.util.Random;

public class Telnet
{

	public static void main(String[] args) throws IOException, InterruptedException
	{
		long count = 0;
		long startTime = System.currentTimeMillis();
		DataBlaster[] threads = new DataBlaster[40];
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

		System.out.println("Data points/sec = " + ((count * 1000) / (System.currentTimeMillis() - startTime)));
	}

	private static class DataBlaster extends Thread
	{
		private long dpCount;

		private DataBlaster()
		{
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
				TelnetClient client = new TelnetClient("localhost", 4242);

				long startTime = System.currentTimeMillis();
				long timestamp = 0;
				while (timestamp < (startTime + 1000 * 60 * 5)) // go for five minutes
				{
					MetricBuilder builder = MetricBuilder.getInstance();

					for (int i = 0; i < 1000; i++)
					{
						Metric metric = builder.addMetric("telnet_test");
						metric.addTag("host", getName()+ i);
						timestamp = System.currentTimeMillis();
						metric.addDataPoint(timestamp, random.nextLong());
						dpCount++;
					}

					client.pushMetrics(builder);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}
}
