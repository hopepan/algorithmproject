package org.algorithm.matrix.java;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MatrixRecordWriter extends RecordWriter<Matrix, IntWritable> {
	public static final Log log = LogFactory.getLog(MatrixRecordWriter.class);

	protected DataOutputStream out;
	private static final String utf8 = "UTF-8";
	private static final byte[] newline;// ���з�
	private final byte[] separator = "\t".getBytes();// �ָ���
	private Matrix last = null;// ������һ������ڵ���Ϣ���ж��Ƿ���Ҫ����

	static {
		try {
			newline = "\n".getBytes(utf8);
		} catch (UnsupportedEncodingException uee) {
			throw new IllegalArgumentException("can't find " + utf8
					+ " encoding");
		}
	}

	public MatrixRecordWriter(DataOutputStream out) {
		this.out = out;
	}

	/**
	 * �÷���������˳��д������ļ���ͬʱȷ���ܹ���ʱ�Ļ��С�
	 * �������������ÿ�ε��ø÷���ʱ���������key��Ȼ�����´ε��ø÷���ʱ�ж�key�����뱣���key�����Ƿ���ͬ��
	 * ��ͬ���С���������'\t'�ָ
	 * ������һ���ر���Ҫע�⣺����Ӧ�ÿ��Ժܼ򵥵�ʹ��last=key����ɱ���key�ķ���������ʵ�鷢�֣����ʹ������
	 * ��������key�������´ε��ø÷���ʱlastֵ�Զ�����˴���Ĳ���key����ʹ����ÿ����last=key��
	 * �����뵽�Ľ����������keyʹ�õ������ô��ݣ������ǵ���last=key��ʱ��ͽ�last������ָ��key������ÿ��key�ı�֮��
	 * lastҲ�������ı��ˣ�ʹ��ÿ��last���к����ǵ���key���кţ��޷�ʵ�ֻ��С�
	 * ���ս���취�Ǹ���key����������ֵ��newһ��Matrix������Ϊlast����֮��ÿ�ε��ø÷���ʱֻ�ǽ�last�������������
	 * ֵ����key��ֵ���и��²�����ͬʱ�÷���Ҳ��Լ�˲����ڴ��ʹ�ã���new�˺ܶ����
	 */
	public synchronized void write(Matrix key, IntWritable value)
			throws IOException {
		if (null != last) {
			if (key.getScript1().get() == last.getScript1().get()) {// �б�һ����������
				out.write(separator);
			} else {// ����
				out.write(newline);
			}
		} else {
			try {
				last = (Matrix) key.clone();//�÷�������key��ֵ��newһ��Matrix����
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
		}
		last.update(key);//����key��ֵ����last�����ֵ
		out.write(String.valueOf(value.get()).getBytes());
	}

	public synchronized void close(TaskAttemptContext context)
			throws IOException {
		out.close();
	}
}
