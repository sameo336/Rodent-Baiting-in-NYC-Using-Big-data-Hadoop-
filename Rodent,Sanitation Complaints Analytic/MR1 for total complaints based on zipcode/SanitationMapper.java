package project;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SanitationMapper extends Mapper<LongWritable, Text, Text, Text>
{

public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
{
String line=value.toString();
String[] ary = line.split("\t",2);
String line1= ary[1];
String[] ary1= line1.split(" ",2);

String key1=ary[0]+","+ary1[0];
String value1=ary1[1]; 

String[] ary2=value1.split("\t",2);
String value2=ary2[1];

StringBuilder sb = new StringBuilder(value2);
int len=sb.length();
sb.deleteCharAt(len-1);
sb.deleteCharAt(0);
String b=sb.toString();
//{} have been removed

String[] valar = b.split(",");
//Splits the dates into round brackets array

StringBuilder z = null;
for(int i=0;i<valar.length;i++)
{
	
	try {
		
	
	z=new StringBuilder(valar[i]);
	int len2=z.length();
	if (len2>0){
	z.deleteCharAt(len2-1);
	z.deleteCharAt(0);
	valar[i]=z.toString();
	}
		}
	catch (Exception e) {
		System.out.println("this value"+z+"this value");
		e.printStackTrace();
	}
}
//valar is an array with () removed

String joinedString = StringUtils.join(valar, ",");
/*System.out.println(joinedString);
System.out.println(key1);*/

context.write(new Text(key1),new Text(joinedString));

}
}
