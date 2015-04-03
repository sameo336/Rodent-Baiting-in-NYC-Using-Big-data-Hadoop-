/*This code, along with the mapper and the reducer, is used to change the format of the dates given in the Rodent complaints database. This is done because the format given in the weather database is very different from this and we need a uniform date format to perform the join*/


package Weather_date;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;


public class RodentDriver {

  @SuppressWarnings("deprecation")
public static void main(String[] args) throws Exception {

    
    if (args.length != 2) {
      System.err.println("Usage: PageRank <input dir> <output dir>\n");
      System.exit(-1);
    }
    /*hadoop jar Pagerank.jar assignment3.PageRankJob /user/cloudera/input1 /user/cloudera/output1*/
    
    Job job = new Job();
    job.setJarByClass(RodentDriver.class);
    job.setJobName("Rodent");
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(RodentMapper.class);
    job.setReducerClass(RodentReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}