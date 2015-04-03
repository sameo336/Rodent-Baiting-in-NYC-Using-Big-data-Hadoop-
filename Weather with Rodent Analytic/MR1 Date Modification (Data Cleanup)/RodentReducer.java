package Weather_date;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RodentReducer extends Reducer<Text, Text, NullWritable, Text> 
{

  @Override
 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
  {

	// float count=0;
	 // float sum=0;
	  for (Text value : values)
	  {
		  
		  
		  String a= value.toString();
		  
		  
	  									//sum=sum+b;
	  	//}
	  
	 // float average= sum/count;
	  
	 
	  
		  context.write(null,  new Text(a) );
		  
	  }
  }
}
