import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Top_Combiner  extends Reducer<DoubleWritable, Text, DoubleWritable, Text>  
{
	int cnt=0;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		cnt = 0;
	}
	public void reduce(DoubleWritable key, Iterable<Text> val, Context context) throws IOException, InterruptedException 
	{
		if(cnt>3)
		{
			for(Text values:val)
			{
				context.write(key, values);
				cnt++;
			}
		}
	}

}
