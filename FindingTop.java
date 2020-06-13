import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class FindingTop
{
class Top_Mapper extends Mapper<LongWritable, Text, DoubleWritable, Text>
{
	public void mapping(LongWritable k, Text val, Context context) 
	{
		String[] split = val.toString().split("\t");
		try {
			context.write(new DoubleWritable(Double.parseDouble(split[1].trim())), new Text(split[0]));
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
public class Top_Reducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable>
{
	int cnt;
	public void reduce(DoubleWritable key, Iterable<Text> val, Context context) throws IOException, InterruptedException 
	{
		if(cnt<3)
		{
			for (Text values: val) 
			{
				context.write(values, key);
				cnt++;
			}
		}
	}
}

}


