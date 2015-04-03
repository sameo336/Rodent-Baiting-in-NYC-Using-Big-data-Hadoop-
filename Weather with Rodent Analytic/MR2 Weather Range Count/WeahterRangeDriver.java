/*This code, along with the mapper and the reducer, is used to aggregate the rodent complaints based on the 'temperature range' of the day when the complaint was made */


package RodentWeatherRange;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;


public class WeahterRangeDriver {

  @SuppressWarnings("deprecation")
public static void main(String[] args) throws Exception {

    
    if (args.length != 2) {
      System.err.println("Usage: PageRank <input dir> <output dir>\n");
      System.exit(-1);
    }
    /*hadoop jar Pagerank.jar assignment3.PageRankJob /user/cloudera/input1 /user/cloudera/output1*/
    
    Job job = new Job();
    job.setJarByClass(WeahterRangeDriver.class);
    job.setJobName("Rodent");
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(WeatherRangeMapper.class);
    job.setReducerClass(WeatherRAngeReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}