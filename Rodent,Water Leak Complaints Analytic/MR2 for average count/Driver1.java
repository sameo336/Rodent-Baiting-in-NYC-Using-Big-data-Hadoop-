//This file, along with the mapper and the reducer, aggregates the output of the previous MapReduce task. For each zip code the average of the number of rodent calls recieved, a week prior and after, is calculated.

package project1;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;


public class Driver1 {

  @SuppressWarnings("deprecation")
public static void main(String[] args) throws Exception {

    
    if (args.length != 2) {
      System.err.println("Usage: PageRank <input dir> <output dir>\n");
      System.exit(-1);
    }
    /*hadoop jar Pagerank.jar assignment3.PageRankJob /user/cloudera/input1 /user/cloudera/output1*/
    
    Job job = new Job();
    job.setJarByClass(Driver1.class);
    job.setJobName("Rodent");
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(AveragePositiveMapper.class);
    job.setReducerClass(AveragePositiveReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
