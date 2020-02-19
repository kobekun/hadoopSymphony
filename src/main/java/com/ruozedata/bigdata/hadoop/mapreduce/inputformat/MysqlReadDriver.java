package com.ruozedata.bigdata.hadoop.mapreduce.inputformat;

import com.ruozedata.bigdata.hadoop.util.DeleteDir;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * MySQLReadDriver
 *     一定是可以在本地运行的
 *     是不能在服务器运行的！！！
 *
 *     本地：mysql驱动
 *     但是：我们打包，由于使用的是瘦包，所以我们打的包是不包含mysql驱动的
 *     不允许你们打胖包
 *     ==> mysql驱动传到服务器上去
 */
public class MysqlReadDriver {

    public static void main(String[] args) throws Exception {

        String output = "mysqlReadOut";

        //1、获取job对象
        Configuration configuration = new Configuration();
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
        job.setMapperClass(MyMapper.class);

        //4、设置map阶段输出数据的类型
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(DeptWritable.class);

        //5、设置输入输出路径
        String[] fields = {"deptno","dname","loc"};
        DBInputFormat.setInput(job,DeptWritable.class,"dept",null,null,fields);

        FileOutputFormat.setOutputPath(job,new Path(output));

        //6、提交作业
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }

    public static class MyMapper extends Mapper<LongWritable,DeptWritable,NullWritable,DeptWritable> {
        @Override
        protected void map(LongWritable key, DeptWritable value, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(),value);
        }
    }
}
