package com.ruozedata.bigdata.hadoop.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class RuozedataRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream ruozeOut;
    FSDataOutputStream pkOut;
    FileSystem fs;

    public RuozedataRecordWriter(TaskAttemptContext context) {

        try {
            //父类里没有Configuration
            fs = FileSystem.get(context.getConfiguration());
            ruozeOut = fs.create(new Path("out/ruoze.log"));
            pkOut = fs.create(new Path("out/pk.log"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {

        if(key.toString().contains("ruoze")){
            ruozeOut.write(key.toString().getBytes());
        }else {
            pkOut.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        IOUtils.closeStream(ruozeOut);
        IOUtils.closeStream(pkOut);
    }
}
