package org.algorithm.pagerank.java;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
	private static final Log log = LogFactory.getLog(PageRankReducer.class);

	public static float factor = 0.85f;// ��������

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// ��ȡ��������ֵ��Ĭ��0.85
		factor = context.getConfiguration().getFloat("mapred.pagerank.factor",
				0.85f);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		log.info(key.toString());
		float rank = 1 - factor;// PageRankֵ
		String[] str;
		Text outLinks = new Text();// ��¼�����ӵ����г�����Ϣ
		// ���ϵ�����λkey�������������ӵ�page,rank,countֵ���Լ�key�����г�����Ϣ
		for (Text t : values) {
			// ������Ϣ��';'�ָ������Ϣ��','�ָ�Դ�����
			str = t.toString().split(";");
			if (str.length == 3) {
				// ����key��rankֵ=(1-d)+d*key������rankֵ/�������
				rank += Float.parseFloat(str[1]) / Integer.parseInt(str[2])
						* factor;
			} else {
				outLinks.set(t.toString());
			}
		}
		context.write(new Text(key.toString() + "," + rank), outLinks);
	}
}
