package org.algorithm.matrix.java;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixReducer extends
		Reducer<MatrixPair, IntWritable, Matrix, IntWritable> {
	public static final Log log = LogFactory.getLog(MatrixReducer.class);

	@Override
	/**
	 * ���ڷ���Ƚ���������Ƚ����Ĺ��ͣ�ʹ�õ�reduce��һ�����ܼ򵥡�
	 * ��������ֻ��Ҫ��value���б�����ÿ��˫���ͽ�ǰ��һ��ֵͬ��ֵ������ˣ������ͼ��ɡ�
	 * ��ͨ�����key��m1���󣬼��ɻ�����ľ���ڵ���Ϣ��
	 * Ȼ��ʹ�������Զ����MatrixRecordWriter�����д���ļ��С�
	 * ��Ϊ����������ʱ�Ѱ�����c���к��н�������ʹ�����ǿ���ֱ�Ӱ�˳��д������ļ���
	 * ��ͬʱ���Ա�֤��������˳�򲻱䣬������һ�������ǲ�֪����λ��У��������Զ�����һ��MatrixRecordWriter����
	 * ������ɻ��в�����
	 */
	protected void reduce(MatrixPair key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int v = 0;
		int temp = 0;
		int index = 0;
		for (IntWritable i : values) {
			if (index % 2 == 1) {
				v += temp * i.get();
			} else {
				temp = i.get();
			}
			index++;
		}
		context.write(key.getM1(), new IntWritable(v));
	}

}
