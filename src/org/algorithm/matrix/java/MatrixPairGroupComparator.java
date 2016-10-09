package org.algorithm.matrix.java;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;

/**
 * �ж�����MatrixPair�����Ƿ�����ͬһ���飬����reduce����ķ���
 * 
 * @author lk
 * 
 */
public class MatrixPairGroupComparator implements RawComparator<MatrixPair> {
	private MatrixPair key1;
	private MatrixPair key2;
	private DataInputBuffer buffer;

	public MatrixPairGroupComparator() {
		super();
		this.key1 = new MatrixPair();
		this.key2 = new MatrixPair();
		this.buffer = new DataInputBuffer();
	}

	@Override
	/**
	 * ֻ��MatrixPair�ĵ�һ��Matrix���жԱ�
	 */
	public int compare(MatrixPair o1, MatrixPair o2) {
		return o1.getM1().compareTo(o2.getM1());
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		try {
			buffer.reset(b1, s1, l1); // parse key1
			key1.readFields(buffer);
			buffer.reset(b2, s2, l2); // parse key2
			key2.readFields(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return compare(key1, key2);
	}
}
