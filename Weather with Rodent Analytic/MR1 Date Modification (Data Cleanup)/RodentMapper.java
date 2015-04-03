package Weather_date;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RodentMapper extends Mapper<LongWritable, Text, Text, Text>
{

public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
{
String line=value.toString();
String[] split1= line.split(" ");

//String line2=split1[1];
//String[] split2= line2.split(" ");

String Key= split1[1];
String value3=split1[0];

context.write(new Text(Key),new Text(value3));

}
}