import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class OutTaxiTime_Mapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
	public void mapping(LongWritable k, Text val, Context context) throws IOException, InterruptedException
	{
		if(val!=null)
		{
			String split[]=val.toString().split(",");
			if (split != null && split.length>19 && split[0]!=null && split[17]!=null && split[20]!=null)
			{
				if(!split[0].equals("Year")&&split[0]!=null)
				{
					String destination=split[17];
					String taxiOut=split[20];
					if (destination!=null &&!destination.trim().equals("") && taxiOut!=null&&!taxiOut.trim().equals(""))
					{
						double outTime=Double.parseDouble(taxiOut);
						context.write(new Text(destination), new LongWritable((long) outTime));
					}
				}
			}
		}
	}
	
}

