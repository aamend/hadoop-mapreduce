package com.aamend.hadoop.mapreduce.inputsplit.format;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class FileFilter extends Configured implements PathFilter {

	Configuration conf;
	FileSystem fs;

	@Override
	public boolean accept(Path path) {
		
		try {
			if(fs.isDirectory(path)){
				return true;
			}
		} catch (IOException e1) {
			e1.printStackTrace();
			return false;
		}
		
		if (conf.get("file.mtime") != null) {
			int mTime = 0;
			String strMtime = conf.get("file.mtime");
			mTime = Integer.valueOf(strMtime.substring(1, strMtime.length()));
			try {
				FileStatus file = fs.getFileStatus(path);
				long now = System.currentTimeMillis() / (1000 * 3600 * 24);
				long time = file.getModificationTime() / (1000 * 3600 * 24);
				long lastModifTime = now - time;
				boolean accept;
				if (strMtime.charAt(0) == '-') {
					accept = mTime < lastModifTime ? true : false;
					System.out.println("File " + path.toString() + " modified "
							+ lastModifTime + " days ago, is " + mTime
							+ " lower ? "+accept);
				} else {
					accept = mTime > lastModifTime ? true : false;
					System.out.println("File " + path.toString() + " modified "
							+ lastModifTime + " days ago, is " + mTime
							+ " greater ? "+accept);
				}
				
				return accept;

			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}

		} else {
			return true;
		}

	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		if (conf != null) {
			try {
				fs = FileSystem.get(conf);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
