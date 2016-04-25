
package extractor.valueextractor;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ValueExtractor extends Configured implements Tool {

	Configuration conf;

	@Override
	public int run(String[] args) throws Exception {
		conf = getConf();

		int length = args.length;
		int out_pos = length - 3;
		int ref_pos = length - 2;
		int pos_pos = length - 1;

		if (args.length < 4) {

			System.out.println("<USAGE> :<in....> <out> <ref> <pos1|pos2>");
			System.out
					.println("performs an AND operation on the records to be extracted");
			System.exit(-1);
		}
		// set pos
		conf.set("POS", args[pos_pos]);

		FileSystem fs = FileSystem.get(conf);
		Job job = new Job(conf, "FieldValueExtractor");
		// set out
		FileOutputFormat.setOutputPath(job, new Path(args[out_pos]));
		// set inPaths
		for (int i = 0; i < out_pos; i++) {
			if (fs.exists(new Path(args[i]))) {
				FileInputFormat.addInputPaths(job, args[i]);
			}
		}

		job.setJarByClass(ValueExtractor.class);
		job.setMapperClass(ValueExtractorMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		// add ref
		DistributedCache.addCacheFile(new URI(args[ref_pos]),
				job.getConfiguration());

		return (job.waitForCompletion(true)) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ValueExtractor driver = new ValueExtractor();
		int jobStatus = ToolRunner.run(driver, args);
		System.out.println("jobStatus : " + jobStatus);
		System.exit(jobStatus);
	}
}
