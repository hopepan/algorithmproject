package org.algorithm.pagerank.java;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

public class JobRunner {
	private static final Log log = LogFactory.getLog(JobRunner.class);

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.out
					.println("please just input one parameter for appoint how many times to count pagerank, such as 10.");
			System.exit(0);
		}
		int totalCounts = Integer.parseInt(args[0]);// ����ѭ������PageRank�Ĵ���
		int result = 0;

		String input = "/study/algorithm/pagerank/files/input/";
		String output = "/study/algorithm/pagerank/files/output/";
		String temp = "";

		Configuration conf = new Configuration();

		// �������ļ��ӱ����ϴ��ļ���hdfs
		FileSystem fs = FileSystem.get(conf);
		Path dis = new Path("/study/algorithm/pagerank/files/input/");
		Path src = new Path("/develop/files/page_rank_input_1.txt");
		if (fs.exists(dis)) {
			log.info(dis.toString() + " is exists,delete it first...");
			fs.delete(dis, true);
		}
		fs.mkdirs(dis);
		log.info("create dir " + dis.toString() + " success...");
		fs.copyFromLocalFile(src, dis);
		log.info("upload input files to " + dis.toString() + " success...");

		PageRankDriver driver;
		for (int i = 0; i < totalCounts; i++) {
			driver = new PageRankDriver();
			driver.setConf(conf);

			result = ToolRunner.run(driver, new String[] { String.valueOf(i),
					input, output });
			// jobִ�гɹ���������һ��ѭ���������������·������
			// ���ɹ��������ִ�иô�ѭ��
			if (result > 0) {
				temp = input;
				input = output;
				output = temp;
				log.info("job_" + i + " is success ...");
			} else {
				log.info("job_" + i + " is failed ...");
			}
		}
	}
}
