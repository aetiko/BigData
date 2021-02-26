import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class handson3Local extends Configured implements Tool {

	public int run(String[] args) throws Exception  {

		if(args.length !=2) {

			System.err.println("Usage:  Handson3 <input path> <outputpath>");
            //example: specify the following command line parameters
			// /home/bigdata/eclipse/java-mars/eclipse/workspace/hadoopdemo/input /home/bigdata/eclipse/java-mars/eclipse/workspace/hadoopdemo/output
			System.exit(-1);

		}

//		Job job = new Job();
//		job.setJobName("Handson 3");
//		
//		job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.seperator", ",");
//		
//		job.setInputFormatClass(KeyValueTextInputFormat.class);
//
//		KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job,new Path(args[1]));
//
//		job.setMapperClass(handson3Mapper.class);
//		job.setReducerClass(Handson3Reducer.class);
//
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//
//		System.exit(job.waitForCompletion(true) ? 0:1);
//		boolean success = job.waitForCompletion(true);
//
//		return success ? 0 : 1;
		Job job = new Job();
		job.setJobName("Handson 3");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		job.setMapperClass(Handson3Mapper.class);
		job.setReducerClass(Handson3Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0:1);
		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;

	}

	
	public static void main(String[] args) throws Exception {

		handson3Local driver = new handson3Local();
		
		int exitCode = ToolRunner.run(driver, args);
		
		System.out.println("Exit code " + exitCode);

		System.exit(exitCode);

	}

}
