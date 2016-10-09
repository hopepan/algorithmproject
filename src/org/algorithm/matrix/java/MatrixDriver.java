package org.algorithm.matrix.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class MatrixDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		// ����Ĵ�С�����������a[i][j]*b[x][y]��Ҫi = y�����ֵ��Ӧ�ľ�����������ͬ��ֵ
		conf.setInt("mapred.matrix.size", Integer.parseInt(arg0[0]));

		Job job = new Job(conf, "Matrix Job");
		job.setJarByClass(getClass());
		// ʹ��һ��reduce�����Է��㽫��������һ���ļ���
		job.setNumReduceTasks(1);

		Path input = new Path(arg0[1]);// ����·��
		Path output = new Path(arg0[2]);// ���·��

		FileSystem fs = FileSystem.get(job.getConfiguration());
		if (fs.exists(output)) {
			fs.delete(output, true);
		}

		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, input);
		job.setOutputFormatClass(MatrixOutputFormat.class);
		FileOutputFormat.setOutputPath(job, output);

		job.setGroupingComparatorClass(MatrixPairGroupComparator.class);

		job.setMapperClass(MatrixMapper.class);
		job.setMapOutputKeyClass(MatrixPair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(MatrixReducer.class);
		job.setOutputKeyClass(Matrix.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 1 : 0;
	}

}
