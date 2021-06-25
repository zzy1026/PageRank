package org.apache.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeMap;
import java.util.StringTokenizer;

public class TopN {

    public static final int K = 10;

    public static class MyDoubleWritable extends DoubleWritable {

        public MyDoubleWritable() {
        }

        public MyDoubleWritable(double value) {
            super(value);
        }

        public int compareTo(DoubleWritable o) {
            return -super.compareTo(o);  
        }
    }


    public static class MyMapper extends Mapper<LongWritable, Text, MyDoubleWritable, Text> {
        private String id;
        private double pr; 

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer str = new StringTokenizer(value.toString());
            id =str.nextToken();
            pr = Double.parseDouble(str.nextToken());
            context.write(new MyDoubleWritable(pr), new Text(id));
        }
    }

    public static class MyReducer extends Reducer<MyDoubleWritable, Text, Text, MyDoubleWritable> {

        int num = 0;

        protected void reduce(MyDoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text text : values) {
                if (num < K) {
                    context.write(text, key);
                }
                num++;
            }
        }
    }

    public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
        System.err.println("Usage: page <in> <out>");
        System.exit(2);
    }
    Job job = new Job(conf, "topn");

    job.setJarByClass(TopN.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setMapOutputKeyClass(MyDoubleWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MyDoubleWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
