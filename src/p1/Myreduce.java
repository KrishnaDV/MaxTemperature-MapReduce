package p1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Myreduce extends Reducer<IntWritable,FloatWritable,IntWritable,FloatWritable>{

@Override
protected void reduce(
		IntWritable key,Iterable<FloatWritable> value,Context ctx)
		throws IOException, InterruptedException {

   System.out.println("reduce(-,-,-)");
   
   System.out.println(key.toString());
   
 //set the maximum value initially to some value
   
   float currentMax = -100;
   
  // Use Iterator to access  elements in values  
   Iterator<FloatWritable> it = value.iterator();
   
   while(it.hasNext()){

	 FloatWritable f = it.next();  
	   
	 currentMax = Math.max(currentMax,f.get());
	
	 System.out.println(currentMax);
 
   	}

 ctx.write(key, new FloatWritable(currentMax));

}
	
}
