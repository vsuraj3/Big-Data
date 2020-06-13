import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class OnSchedule
{

class OnSchedule_Mapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
	public void mapping(LongWritable k, Text val, Context context) throws NumberFormatException, IOException, InterruptedException
	{
		String split[]=val.toString().split(",");
		if(split[0]!=null)
		{
			String carrier=split[8];
			String delay=split[14];
			if(carrier!=null && !carrier.trim().equals("") && delay!=null && delay.trim().equals(""))
			{
				int delayInteger=Integer.parseInt(delay);
				context.write(new Text("S-" + carrier), new LongWritable(Integer.parseInt("1")));
				if(delayInteger>10) 
				{
					context.write(new Text("T-"+carrier), new LongWritable(Integer.parseInt("1")));
				}
			}
		}
	}
}
public class OnSchedule_Reducer extends Reducer<Text, LongWritable, Text, DoubleWritable>
{
	Map<String, Long> map = new HashMap<>();
	double high1 = 0L;
	double low1 = Double.MAX_VALUE;
	double high2 = 0L;
	double low2 = Double.MAX_VALUE;
	double high3 = 0L;
	double low3 = Double.MAX_VALUE;
	Text high1AirlineCode,low1AirlineCode,high2AirlineCode,low2AirlineCode,high3AirlineCode,
	 low3AirlineCode=new Text();
	DoubleWritable res;
	public void reduce(Text key, Iterable<LongWritable> val, Context context) throws IOException, InterruptedException 
	{
		double allSum=0;
		String str=key.toString();
		if(str.contains("S-")) 
		{
			for(LongWritable values:val)
			{
				allSum=allSum+values.get();
			}
			String carrier=str.replace("S-", "");
			map.put(carrier, (long) allSum);	
		}
		else 
		{
			long add=0;
			String carrier=str.replace("T-", "");
			long totalAdd =map.get(carrier);
			for(LongWritable values :val)
			{
				add=add+values.get();
			}
			long delay= add/totalAdd;
			
			if(high1<delay)
			{
				high3 = high2;
				high3AirlineCode.set(high2AirlineCode.toString());
				high2 = high1;
				high2AirlineCode.set(high1AirlineCode.toString());
				high1 = delay;
				high1AirlineCode.set(carrier);
			}
			else if (high2 < delay) {
				high3 = high2;
				high3AirlineCode.set(high2AirlineCode.toString());
				high2 = delay;
				high2AirlineCode.set(carrier);
			} 
			else if (high3 < delay) {
				high3 = delay;
				high3AirlineCode.set(carrier);
			}
			
			if (low1 > delay) {
				low3 = low2;
				low3AirlineCode.set(low2AirlineCode.toString());
				low2 = low1;
				low2AirlineCode.set(low1AirlineCode.toString());
				low1 = delay;
				low1AirlineCode.set(carrier);
			} else if (low2 > delay) {
				low3 = low2;
				low3AirlineCode.set(low2AirlineCode.toString());
				low2 = delay;
				low2AirlineCode.set(carrier);
			} 
			else if (low3 > delay) 
			{
				low3 = delay;
				low3AirlineCode.set(carrier);
			}
			res.set(add);
			context.write(key, res);
		}
		
		
	}
	protected void show(Context context) throws IOException, InterruptedException
	{

		context.write(new Text("low probability of flight for being on schedule"), null);
		context.write(high1AirlineCode, new DoubleWritable(high1));
		context.write(high2AirlineCode, new DoubleWritable(high2));
		context.write(high3AirlineCode, new DoubleWritable(high3));
		context.write(new Text("high probability of flight for being on schedule"), null);
		context.write(low1AirlineCode, new DoubleWritable(low1));
		context.write(low2AirlineCode, new DoubleWritable(low2));
		context.write(low3AirlineCode, new DoubleWritable(low3));
	}
}
}