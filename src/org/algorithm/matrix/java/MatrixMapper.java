package org.algorithm.matrix.java;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MatrixMapper extends
		Mapper<LongWritable, Text, MatrixPair, IntWritable> {
	public static final Log log = LogFactory.getLog(MatrixMapper.class);
	public Matrix[][] cc = null;// ���ڴ�Ž������(c[n][n])

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		/**
		 * ��һ����������ʵ���������еĽ���������(c[n][n])�� ���ڸ�a,b����ڵ�������Ϊreduce������
		 */
		// ����Ĵ�С�����������Ĵ�С
		int size = context.getConfiguration().getInt("mapred.matrix.size", 0);
		cc = new Matrix[size][size];
		for (int i = 0; i < size; i++) {
			for (int j = 0; j < size; j++) {
				cc[i][j] = new Matrix(MatrixType.RESULT, i + 1, j + 1);// c(ij)
			}
		}
	}

	/**
	 * �����key�����壬��value���ļ��е�һ�С� ����ÿ����ֵ��'\t'�ָ�ҵ�һ������Ϊ�к�(��1��ʼ)��
	 * ������ÿһ�еľ���㡣��value��'\t'�ָ�õ��к��Լ�����ֵ�� �ݴ˹����������a[i][j]����b[x][y]��
	 * �����key��һ�Ծ��������ɵĶ����������ĵ�һ���������m1�� �������(c)�����ڶ�����������ǳ�ʼ����(a/b)��
	 * ��������˼���ǣ�c[i][j]=sum(a[i][x]*b[x][j])(x=1->n)��
	 * �������ǽ�����ĳ��c[i][j]ֵ������a[i][x]��b[x][j]ͬc[i][j]�ŵ�һ����Ϊkey���䵽reduce��
	 * Ȼ���ڷ���ʱ����ֻ����key�ĵ�һ���������(m1)�����з��飬
	 * ʹ��������������ĳһ��c[i][j]ֵ������a[i][x]��b[x][j]�ŵ�һ����reduce����
	 * �������ǿ�����reduce��һ�α����м����c
	 * [i][j]�����ֵ������ʹ�õ�һ������Ƚ���(MatrixPairGroupComparator)��
	 * ������Ȼ���ǿ��Խ�����c[i][j]ֵ��������Ҫ�ľ���ڵ�ȫ����õ��ˣ����Ǽ�����Ի��ǱȽ��鷳�ġ�������ǻ���Ҫ����
	 * ͨ������ļ��㹫ʽ�����ǵ�֪a[i][x]*b[x][j]��a����=b���У�������ǿ���������ʱ����a���к�b���н�������
	 * �����ó�������������a[i][1],b[1][j],a[i][2],b[2][j],a[i][3],b[3][j]��
	 * ����������������ֻ��Ҫ��ǰ��һ�����Ϻ���һ��ֵ��ͼ��ɵõ�c[i][j]��ֵ��
	 * ��Ҫʵ����������������Ҫ�Զ���������������������MatrixPair�е�compareTo�����У�
	 * ��reduce����sortʱ���Զ����ø÷����������� ���Ͼ������˼·��
	 */
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		int flag;
		FileSplit sp = (FileSplit) context.getInputSplit();
		String s = sp.getPath().getName();
		// ������Ҫ������a�����b����
		// ���ֹ����Ƿ�Ƭ�ļ����ļ���������'1'��˵����a���󣬷�����b����
		// ��������ļ�Ҳ��Ҫ��һ�����򣬼�a������ļ��������1����b���󲻿ɰ���1
		if (s.contains("1")) {
			flag = MatrixType.LEFT;//a
		} else {
			flag = MatrixType.RIGHT;//b
		}

		String line = value.toString();
		String[] ms = line.split("\t");
		int i = Integer.parseInt(ms[0]);// �к�,ÿ�еĵ�һ��ֵ
		for (int j = 1; j < ms.length; j++) {// �к�
			int v = 0;// ֵ
			try {
				v = Integer.parseInt(ms[j]);
			} catch (NumberFormatException e) {
				e.printStackTrace();
				System.exit(1);
			}
			Matrix aa = new Matrix(flag, i, j);// a(ij)�����һ���������
			for (int x = 1; x < ms.length; x++) {
				MatrixPair p = null;
				if (flag == MatrixType.LEFT) {//�����ж���a����b�ԺͲ�ͬ��c����
					p = new MatrixPair(cc[i - 1][x - 1], aa);
				} else {
					p = new MatrixPair(cc[x - 1][j - 1], aa);
				}
				log.info(p + "\t" + v);
				context.write(p, new IntWritable(v));
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
		cc = null;
	}
}
