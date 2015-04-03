package RodentWeatherRange;
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

public class WeatherRAngeReducer extends Reducer<Text, Text, Text, FloatWritable> 
{

  @Override
 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
  {

	 float count=0;
	 	  for (Text value : values)
	  {
		  
		 count=count+1; 
	  
	 
	  	}
		  context.write(key,  new FloatWritable(count) );
		  
	  
  }
}
