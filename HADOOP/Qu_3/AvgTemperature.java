import java.io.IOException;
//import java.util.StringTokenizer;
//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgTemperature {

  public static class AvgTemperatureMapper
       extends Mapper<LongWritable, Text, Text, IntWritable>{
    private static final int MISSING = 9999;
    @Override
    public void map(LongWritable key, Text value, Context context)
 	throws IOException, InterruptedException {
      String line  = value.toString();
      String year = line.substring(15, 19);
      int airTemperature;

      if (line.charAt(87) == '+') {
	airTemperature = Integer.parseInt(line.substring(88, 92));
       } else {
        airTemperature = Integer.parseInt(line.substring(87,92));
       }
  String quality = line.substring(92, 93);
 	if(airTemperature != MISSING && quality.matches("[01459]")) {
 	 context.write(new Text(year), new IntWritable(airTemperature));
 	}
}
}

     
  public static class AvgTemperatureReducer
       extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
 throws IOException, InterruptedException {
      int sum = 0;
      int count = 0;
      for (IntWritable val : values) {
        sum += val.get();
        count += 1;   
      }
      double avg = sum / count;
      result.set(avg);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Job job = new Job();
    job.setJarByClass(AvgTemperature.class);
    job.setJobName("Average Temperature");
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(AvgTemperatureMapper.class);
    job.setReducerClass(AvgTemperatureReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
