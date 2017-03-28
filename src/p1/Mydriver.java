package p1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Mydriver {

public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	
	//Configuration class pointing to default  configuration 
	Configuration con = new Configuration();
	
	//Prepare a job Object
	Job job = new Job(con,"MaxTempAssjob");
	
	//linking driver class to job
	job.setJarByClass(Mydriver.class);
	
	//link Mapper class to job
	job.setMapperClass(Mymap.class);
	
	//link Reducer class to job
	job.setReducerClass(Myreduce.class);
	
	job.setMapOutputKeyClass(IntWritable.class);
	
	job.setMapOutputValueClass(FloatWritable.class);
	
	//Set Final datatype of Output Key
	job.setOutputKeyClass(IntWritable.class);
	
	//Set Final datatype of Output Value
	job.setOutputValueClass(FloatWritable.class);
	
	
	
	//set the input path of the job
	Path input_dir=new Path("hdfs://localhost:54310/input/");
	
	FileInputFormat.addInputPath(job, input_dir);

	//set the output path of the job
	Path output_dir=new Path("hdfs://localhost:54310/output/");

	FileOutputFormat.setOutputPath(job,output_dir );

	//Run the program
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	
}
	
}
