package com.tencent.angel.spark.examples.cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

public class DataParse {

    public static void appendHDFS(String file, String field, String auc) {
        System.out.println("auc is saved");
        Configuration conf = new Configuration();
        conf.setBoolean("dfs.support.append", true);
        String content = field + ":" + auc + ",";
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(file), conf);
            InputStream in = new BufferedInputStream(new ByteArrayInputStream(content.getBytes()));
            OutputStream out = fs.append(new Path(file));
            IOUtils.copyBytes(in, out, 4096, true);
            fs.close();
        }  catch (IOException e) {
            e.printStackTrace();
        }
    }
}
