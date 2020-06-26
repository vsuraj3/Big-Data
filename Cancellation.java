import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class Cancellation{
	

class Cancellation_Mapper extends Mapper<LongWritable, Text, Text, LongWritable>
{a
	public void mapping(LongWritable k, Text val, Context context)
	{
		String split[]=val.toString().split(",");
		if(split[0]!=null) 
		{
			String cancellation=split[22];
			if(cancellation!=null && !cancellation.trim().equals(""))
			{
				try {
					context.write(new Text(cancellation), new LongWritable(Integer.parseInt("1")));
				} catch (NumberFormatException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}

class Cancellation_Reducer extends Reducer<Text, LongWritable, Text, LongWritable>
{
	long num;
	Text code;
	LongWritable res;
	public void reduce(Text key, Iterable<LongWritable> val, Context context) 
	{
		long sum=0;
		for(LongWritable values:val)
		{
			sum=sum+values.get();
		}
		if(sum>=num)
		{
			code.set(key);
			num=sum;
		}
		res.set(sum);
		try {
			context.write(key, res);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
}


