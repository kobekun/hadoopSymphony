package com.ruozedata.bigdata.hadoop.mapreduce.inputformat;

import com.ruozedata.bigdata.hadoop.mapreduce.wc.WCMapper;
import com.ruozedata.bigdata.hadoop.mapreduce.wc.WCReducer;
import com.ruozedata.bigdata.hadoop.util.DeleteDir;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;


public class NLineInputFormatDriver {

    public static void main(String[] args) throws Exception {

        String input = "data/ruozedata2";
        String output = "out";
//       1、 获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        File outputPath = new File(output);
        DeleteDir.deleteLocal(outputPath);

//        2、本job对应要执行的主类是哪个
        job.setJarByClass(NLineInputFormatDriver.class);

//        3、设置map和reduce
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

//        4、设置map的key和value的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

//        5、设置reduce的key和value的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //分片数，说明4行记录一个分片，一个分片对应一个maptask数量
        NLineInputFormat.setNumLinesPerSplit(job,4);
        job.setInputFormatClass(NLineInputFormat.class);

//        6、设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

//        7、提交作业
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0:1);

    }
}
