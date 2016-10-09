package org.algorithm.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class FileUtil {

	public static void main(String[] args) throws IOException {
		File f = new File(System.getProperty("user.dir")
				+ "\files\full_2.txt");
		if (!f.exists()) {
			f.createNewFile();
		}
		writeMatrix(f, 1000, 1000);
	}

	// ʵ����һ���ļ���д��һ��n1*n2�ľ���ע��ÿ�е�һ��ֵ���кţ�������Ǿ���ֵ
	public static void writeMatrix(File f, int n1, int n2) {
		Random ran = new Random();
		FileWriter fw = null;
		try {
			fw = new FileWriter(f);
			for (int i = 0; i < n1; i++) {
				fw.write(String.valueOf(i + 1));
				fw.write('\t');
				for (int j = 0; j < n2; j++) {
					fw.write(String.valueOf(ran.nextInt(1000) + 1));//ֵ��1-1000֮��������
					if (j != n2 - 1) {
						fw.write('\t');
					}
				}
				if (i != n1 - 1) {
					fw.write('\n');
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
