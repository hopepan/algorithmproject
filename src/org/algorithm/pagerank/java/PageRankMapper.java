package org.algorithm.pagerank.java;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * PageRank��Mapper�࣬����ʵ�ִ������ļ�����������վ��������Ϣ
 * 
 * @author lk
 * 
 */
public class PageRankMapper extends Mapper<Text, Text, Text, Text> {
	private static final Log log = LogFactory.getLog(PageRankMapper.class);

	public static float factor = 0.85f;// ��������

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// ��ȡ��������ֵ��Ĭ��0.85
		factor = context.getConfiguration().getFloat("mapred.pagerank.factor",
				0.85f);
	}

	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		log.info(key.toString() + ";" + value.toString());
		// �����ļ���ʽΪ��A b,c,d,...
		//�����жϸ������Ƿ���ڳ����������ڳ�����ֱ������
		if (value.toString().length() == 0) {
			return;
		}
		// ��keyΪĿ����վ��valueΪA���еĳ���������','�ָ�
		String[] outLinks = value.toString().split(",");
		// �ָ�key����ȡkey��rankֵ����','�ָ�
		String[] link = key.toString().split(",");
		float rank = factor;
		if (link.length > 1) {
			rank = Float.parseFloat(link[1]);// ����rankֵ����ȡ�ã�����������ΪĬ��ֵ����������
		}
		int outLinkLen = outLinks.length;// A�ĳ�������
		// ����A���еĳ����������ʽ��key-A�ĸ�������(b,c,d...);value-[A+','+rank+','+outLinkLen]
		// �õ�ÿ�����ӵ�����������PRֵ�Լ����ӵ������ӵ����ӵĳ�����
		for (String s : outLinks) {
			context.write(new Text(s), new Text(link[0] + ";" + rank + ";"
					+ outLinkLen));
		}
		// ����Ҫ���A�����г�����Ϣ���Ա������һ��mapreduce����Ĵ���
		context.write(new Text(link[0]), value);
	}
}
