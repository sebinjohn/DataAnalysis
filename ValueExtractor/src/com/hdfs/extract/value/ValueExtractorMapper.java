
package com.hdfs.extract.value;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ValueExtractorMapper extends
		Mapper<LongWritable, Text, Text, NullWritable> {

	List<String> lookup;
	String[] positions;

	public enum RecordCounter {
		MATCHED_RECORD_COUNTER, INPUT_RECORD_COUNTER
	}

	public ValueExtractorMapper() {
		lookup = new ArrayList<String>();
		positions = new String[1];
	}

	protected void setup(Context context) throws IOException,
			InterruptedException {
		/*Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());*/
		 
		URI[] cacheFiles = DistributedCache.getCacheFiles(context
				.getConfiguration());
		BufferedReader reader = new BufferedReader(new FileReader(
				cacheFiles[0].toString()));
		String line = null;
		while ((line = reader.readLine()) != null) {
			lookup.add(line.trim());
		}
		reader.close();
		Configuration conf = context.getConfiguration();
		positions = conf.get("POS").split("\\|", -1);

	}

	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {

		context.getCounter(RecordCounter.INPUT_RECORD_COUNTER).increment(1);
		String[] splitValue = value.toString().split("\\|", -1);

		for (int i = 0; i < lookup.size(); i++) {
			boolean StringMatches = false;
			String rowTocheck = lookup.get(i);
			String[] valuesToCheck = rowTocheck.split("\\|", -1);
			int searchPosition;
			for (int j = 0; j < positions.length; j++) {
				String valueToCheck;
				String positionToCheck = positions[j];
				if (StringUtils.isNotBlank(positionToCheck)) {
					searchPosition = Integer.parseInt(positionToCheck);
					if (searchPosition <= splitValue.length) {
						if (valuesToCheck == null) {
							valuesToCheck = new String[1];
							valuesToCheck[0] = "";
						}
						valueToCheck = valuesToCheck[j];
						if (splitValue[searchPosition - 1].trim()
								.equalsIgnoreCase(valueToCheck)) {
							StringMatches = true;
						} else {
							StringMatches = false;							
						}
					}
				}
			}
			if (StringMatches) {
				try {
					context.getCounter(RecordCounter.MATCHED_RECORD_COUNTER)
							.increment(1);
					context.write(value, NullWritable.get());
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}

	}
}
