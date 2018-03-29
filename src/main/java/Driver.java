/**
 * 132B Basic inverted index
 * Group number: cs132g2
 * Group name: Yet Another Mapreduce Program (YAMP)
 * Group members: Shuai Yu, Jinli Yu, Chuanxiong Yi, Minghui Zhu
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Driver {
	public static void main(String[] args) 
		throws IOException, ClassNotFoundException, InterruptedException {
//		Path inp00 = new Path(args[1]);
//		Path inp01 = new Path(args[1]);
//		Path inp02 = new Path(args[2]);

//		if running locally, use the line below
		Path outp = new Path(args[1]);
		
//		if running on cluster, use the line below
//		Path outp = new Path(args[2]);
		
		BasicConfigurator.configure();
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Inverted Index");
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputDirRecursive(job, true);
//		if running locally, use the line below
		FileInputFormat.addInputPath(job, new Path(args[0]));
//		if running on cluster, use the line below
//		FileInputFormat.addInputPath(job, new Path(args[1]));
//		TextInputFormat.addInputPath(job, inp00);
//		TextInputFormat.addInputPath(job, inp01);
//		TextInputFormat.addInputPath(job, inp02);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outp);
		
		job.setJarByClass(Driver.class);
		job.setMapperClass(WordsMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(MergeReducer.class);
		
		job.waitForCompletion(true);
	}

}
