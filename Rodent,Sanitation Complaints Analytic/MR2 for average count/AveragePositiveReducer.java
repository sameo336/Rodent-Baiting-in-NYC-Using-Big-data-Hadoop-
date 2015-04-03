package project1;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AveragePositiveReducer extends Reducer<Text, Text, Text, FloatWritable> 
{

  @Override
 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
  {

	  float count=0;
	  float sum=0;
	  for (Text value : values)
	  {
		  
		  
		  String a= value.toString();
		  int b= Integer.parseInt(a);
		  /*if (b>0){*/
			  count++;
		  /*}*/
		  sum=sum+b;
	  }
	  
	  float average= sum/count;
	  
	  
	  
		  context.write(new Text(key),  new FloatWritable(average) );
		  
	  }
  }

