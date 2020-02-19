package com.ruozedata.bigdata.hadoop.mapreduce.inputformat;

import com.ruozedata.bigdata.hadoop.util.DeleteDir;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;

/**
 * MySQLReadDriverV2这个版本   生产上推荐的
 *     extends Configured implements Tool
 *     把我们的mysql jar加载到hadoop能访问的到的路径  *****
 */
public class MysqlReadDriverV2 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        int run = ToolRunner.run(configuration, new MysqlReadDriverV2(), args);

        System.exit(run);
    }

    @Override
    public int run(String[] args) throws Exception {

        String output = "mysqlReadOut";

        Configuration configuration = super.getConf();
        //1、获取job对象
        DBConfiguration.configureDB(configuration,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://ruozedata001:3306/sqoop",
                "root",
                "kobekun");

        Job job = Job.getInstance(configuration);

        File outputPath = new File(output);
        DeleteDir.deleteLocal(outputPath);

        //2、本job要执行的主类是哪个
        job.setJarByClass(MysqlReadDriver.class);

        //3、设置mapper和reducer
        job.setMapperClass(MysqlReadDriver.MyMapper.class);

        //4、设置map阶段输出数据的类型
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(DeptWritable.class);

        //5、设置输入输出路径
        String[] fields = {"deptno","dname","loc"};
        DBInputFormat.setInput(job,DeptWritable.class,"dept",null,null,fields);

        FileOutputFormat.setOutputPath(job,new Path(output));

        //6、提交作业
        boolean result = job.waitForCompletion(true);
        return 1;
    }

    public static class MyMapper extends Mapper<LongWritable,DeptWritable,NullWritable,DeptWritable> {
        @Override
        protected void map(LongWritable key, DeptWritable value, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(),value);
        }
    }
}
