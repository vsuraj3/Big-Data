import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FlightAnalysis_Main extends Configured implements Tool {

	public static class Comparing extends WritableComparator
	{
		public Comparing() 
		{
			super(Text.class);
		}
	}
	

	public int run(String[] temp) {
		try {

			FileSystem file = FileSystem.get(getConf());
			Job j1 = Job.getInstance(getConf());
			j1.setJarByClass(getClass());

			FileInputFormat.setInputPaths(j1, new Path(temp[0]));

			j1.setMapperClass(Cancellation.Cancellation_Mapper.class);
			j1.setReducerClass(Cancellation.Cancellation_Reducer.class);
			
			j1.setMapOutputKeyClass(Text.class);
			j1.setOutputKeyClass(Text.class);
			
			j1.setMapOutputValueClass(LongWritable.class);
			j1.setOutputValueClass(LongWritable.class);

			FileOutputFormat.setOutputPath(j1, new Path(temp[1]));

			j1.waitForCompletion(true);

			Job j2 = Job.getInstance(getConf());
			j2.setJarByClass(getClass());

			j2.setMapperClass(InTaxiTime_Mapper.class);
			j2.setReducerClass(TaxiTime_Reducer.class);
			
			j2.setMapOutputKeyClass(Text.class);
			j2.setOutputKeyClass(Text.class);
			
			j2.setMapOutputValueClass(LongWritable.class);
			j2.setOutputValueClass(DoubleWritable.class);

			FileInputFormat.addInputPath(j2, new Path(temp[0]));
			FileOutputFormat.setOutputPath(j2, new Path("output_1"));

			j2.waitForCompletion(true);
			
			Job j3 = Job.getInstance(getConf());
			j3.setJarByClass(getClass());

			j3.setMapperClass(FindingTop.Top_Mapper.class);
			j3.setCombinerClass(Top_Combiner.class);
			j3.setReducerClass(FindingTop.Top_Reducer.class);
			
			j3.setMapOutputKeyClass(DoubleWritable.class);
			j3.setOutputValueClass(DoubleWritable.class);
			
			j3.setMapOutputValueClass(Text.class);
			j3.setOutputKeyClass(Text.class);

			j3.setNumReduceTasks(1);
			j3.setSortComparatorClass(LongWritable.DecreasingComparator.class);

			FileInputFormat.addInputPath(j3, new Path("output_1"));
			FileOutputFormat.setOutputPath(j3, new Path(temp[1] + "InTaxiTime_High"));

			j3.waitForCompletion(true);
			Job j4 = Job.getInstance(getConf());
			j4.setJarByClass(getClass());

			j4.setMapperClass(FindingTop.Top_Mapper.class);
			j4.setCombinerClass(Top_Combiner.class);
			j4.setReducerClass(FindingTop.Top_Reducer.class);
			
			j4.setMapOutputKeyClass(DoubleWritable.class);
			j4.setOutputValueClass(DoubleWritable.class);
			
			j4.setOutputKeyClass(Text.class);
			j4.setMapOutputValueClass(Text.class);

			j4.setNumReduceTasks(1);

			j4.setSortComparatorClass(DoubleWritable.Comparator.class);

			FileInputFormat.addInputPath(j4, new Path("output_1"));
			
			FileOutputFormat.setOutputPath(j4, new Path(temp[1] + "InTaxitime_Low"));

			j4.waitForCompletion(true);

			Job j5 = Job.getInstance(getConf());
			j5.setJarByClass(getClass());

			j5.setMapperClass(OutTaxiTime_Mapper.class);
			j5.setReducerClass(TaxiTime_Reducer.class);
			
			j5.setMapOutputKeyClass(Text.class);
			j5.setOutputKeyClass(Text.class);
			
			j5.setMapOutputValueClass(LongWritable.class);
			j5.setOutputValueClass(DoubleWritable.class);

			FileInputFormat.addInputPath(j5, new Path(temp[0]));
		
			FileOutputFormat.setOutputPath(j5, new Path("output_2"));

			j5.waitForCompletion(true);

			Job j6 = Job.getInstance(getConf());
			j6.setJarByClass(getClass());

			j6.setMapperClass(FindingTop.Top_Mapper.class);
			j6.setCombinerClass(Top_Combiner.class);
			j6.setReducerClass(FindingTop.Top_Reducer.class);
			
			j6.setMapOutputKeyClass(DoubleWritable.class);
			j6.setOutputValueClass(DoubleWritable.class);
			
			j6.setMapOutputValueClass(Text.class);
			j6.setOutputKeyClass(Text.class);
	
			j6.setNumReduceTasks(1);

			
			j6.setSortComparatorClass(LongWritable.DecreasingComparator.class);

			FileInputFormat.addInputPath(j6, new Path("output_2"));
		
			FileOutputFormat.setOutputPath(j6, new Path(temp[1] + "OutTaxiTime_High"));

			j6.waitForCompletion(true);
		
			Job j7 = Job.getInstance(getConf());
			j7.setJarByClass(getClass());

			j7.setMapperClass(FindingTop.Top_Mapper.class);
			j7.setCombinerClass(Top_Combiner.class);
			j7.setReducerClass(FindingTop.Top_Reducer.class);
			
			j7.setMapOutputKeyClass(DoubleWritable.class);
			j7.setOutputValueClass(DoubleWritable.class);
			
			j7.setMapOutputValueClass(Text.class);
			j7.setOutputKeyClass(Text.class);

			j7.setNumReduceTasks(1);
			
			j7.setSortComparatorClass(DoubleWritable.Comparator.class);

			FileInputFormat.addInputPath(j7, new Path("output_2"));
			FileOutputFormat.setOutputPath(j7, new Path(temp[1] + "OutTaxiTime_Low"));

			j7.waitForCompletion(true);

			Job j8 = Job.getInstance(getConf());
			j8.setJarByClass(getClass());

			j8.setMapperClass(OnSchedule.OnSchedule_Mapper.class);
			j8.setReducerClass(OnSchedule.OnSchedule_Reducer.class);
			
			j8.setMapOutputKeyClass(Text.class);
			j8.setOutputKeyClass(Text.class);
			
			j8.setMapOutputValueClass(LongWritable.class);
			j8.setOutputValueClass(LongWritable.class);

			j8.setSortComparatorClass(Comparing.class);

			FileInputFormat.setInputPaths(j8, new Path(temp[0]));

			FileOutputFormat.setOutputPath(j8, new Path(temp[1] + "OnSchedule"));

			return j8.waitForCompletion(true) ? 0 : 1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new FlightAnalysis_Main(), args));
	}

	


}