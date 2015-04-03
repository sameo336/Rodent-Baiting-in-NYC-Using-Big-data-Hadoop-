package project;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SanitationReducer extends Reducer<Text, Text, Text, Text> 
{

  @Override
 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
  {

	  String line1=key.toString();
	  String[] keyar = line1.split(",");
	  //keyar[0] is zip. keyar[1] is sanitation date.
	  int positive=0,negative=0;
	  
	  for (Text value : values)
	  {
		  
		  int i; 
		  String line=value.toString();
		  String[] valar=line.split(",");
		  
		  //Removes the round brackets. Valar[] now contains individual dates
		  
		  //Using java inbuilt types to convert string values of valar[] into date type. Do same for keyar[1].
		  
		  
		  
		  try {
			Date SanitationDate = new SimpleDateFormat("MM/dd/yyyy", Locale.ENGLISH).parse(keyar[1]);
		
		  for(i=0;i<valar.length;i++)
		  {
			  if(valar[i].length()>0){
			  Date RodentDate = new SimpleDateFormat("MM/dd/yyyy", Locale.ENGLISH).parse(valar[i]);
			  
			  long diff = (RodentDate.getTime() - SanitationDate.getTime());
			  long diffDays = diff / (24 * 60 * 60 * 1000);
			  
			  if (diffDays>=0 && diffDays <=7){
				  /*System.out.println(RodentDate);
				  System.out.println(SanitationDate);*/
				  positive++;
				  }
			  else if (diffDays<0 && diffDays >= -7)
				  negative++;
			  
			  }
		  }
		  } catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		  
		  //check1 is number of times rodent spotted after garbage complaint within a week
		  
		  //check2 is a value for comparison with check1 i.e. check1 should ideally be greater than check2.
		  
		  String value4= Integer.toString(positive)+","+Integer.toString(negative);
		  /*System.out.println(keyar[0]);
		  System.out.println(value4);*/
		  context.write(new Text(keyar[0]),  new Text(value4) );
		  
	  }
  }




}	  