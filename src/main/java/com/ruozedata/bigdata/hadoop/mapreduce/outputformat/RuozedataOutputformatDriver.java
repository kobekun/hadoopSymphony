package com.ruozedata.bigdata.hadoop.mapreduce.outputformat;

import com.ruozedata.bigdata.hadoop.util.DeleteDir;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;


public class RuozedataOutputformatDriver {

    public static void main(String[] args) throws Exception {

        String input = "data/click.log";
        String output = "out";
//       1、 获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

//        2、如果输出路径存在，需要删除
        File outputPath = new File(output);
        DeleteDir.deleteLocal(outputPath);

//        3、本job对应要执行的主类是哪个
        job.setJarByClass(RuozedataOutputformatDriver.class);

//        4、设置map和reduce
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

//        5、设置map的key和value的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

//        6、设置reduce的key和value的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置输出格式
        job.setOutputFormatClass(RuozedataOutputFormat.class);
//        7、设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

//        8、提交作业
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0:1);
    }

    public static class MyMapper extends Mapper<LongWritable,Text,Text, NullWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            context.write(value,NullWritable.get());
        }
    }

    public static class MyReducer extends Reducer<Text, NullWritable,Text, NullWritable>{

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            for(NullWritable value: values){
                context.write(new Text(key.toString()+"\n"),NullWritable.get());
            }
        }
    }
}
