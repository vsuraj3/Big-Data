
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class InTaxiTime_Mapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
	public void mapping(LongWritable k, Text val, Context context) throws IOException, InterruptedException
	{
		if(val!=null)
		{
			String split[]=val.toString().split(",");
			if (split != null && split.length>19 && split[0]!=null && split[16]!=null && split[19]!=null)
			{
				if(!split[0].equals("Year")&&split[0]!=null)
				{
					String arr=split[16];
					String taxiIn=split[19];
					if (arr!=null &&!arr.trim().equals("") && taxiIn!=null&&!taxiIn.trim().equals(""))
					{
						double inTime=Double.parseDouble(taxiIn);
						context.write(new Text(arr), new LongWritable((long) inTime));
					}
				}
			}
		}
	}
	
}
