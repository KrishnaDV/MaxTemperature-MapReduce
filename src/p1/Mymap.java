package p1;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//15-19
//88-93
public class Mymap extends Mapper<LongWritable,Text,IntWritable,FloatWritable> {
	@Override
	protected void map(	LongWritable offset,Text line,Context context)
			throws IOException, InterruptedException {
		
		//System.out.println("Offset :"+offset+"value:"+line);
		
		//convert year & temp into integer
		
		int year = Integer.parseInt(line.toString().substring(15, 19));
		
		float temp = Float.parseFloat(line.toString().substring(88,92));
		
		float temperature = temp/10;
		
		//use cont.write to write them to reducer

		context.write(new IntWritable(year), new FloatWritable(temperature));
		
		
	}
	
}
