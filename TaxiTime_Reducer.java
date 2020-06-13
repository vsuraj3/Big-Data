import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class TaxiTime_Reducer extends Reducer<Text, LongWritable, Text, DoubleWritable>
{
	public void reduce(Text key, Iterable<LongWritable> val, Context context) 
	{
		DoubleWritable res = new DoubleWritable();
		long ans;
		long add=0;
		long cnt=0;
		for(LongWritable values:val)
		{
			add=add+values.get();
			cnt++;
		}
		if(cnt>0)
		{
			ans=add/cnt;
			res.set(ans);
			try {
				context.write(key, res);
			} catch (IOException | InterruptedException e) 
			{
				e.printStackTrace();
			}
		}
	}
}
