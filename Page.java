package org.apache.hadoop.examples;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.StringTokenizer;


import org.apache.hadoop.io.NullWritable;

public class Page {

    public static class PrMapper
        extends Mapper<Object, Text, Text, Text>{
      
    private String id1;
    private String id2;
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    StringTokenizer str = new StringTokenizer(value.toString());
    id1 =str.nextToken();
    id2 =str.nextToken();
    context.write(new Text(id1), new Text(id2));
    context.write(new Text(id2), new Text(" "));
    }
}

public static class PrReducer
        extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
    String lianjie = "";
    double v=0.00009194556;
    public void reduce(Text key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException {
      String lianjie = "";
      for(Text val:values) lianjie += val.toString()+" ";
      result.set(v+"\t"+lianjie);
      context.write(key,result);
    }
}

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
        System.err.println("Usage: page <in> <out>");
        System.exit(2);
    }
    Job job = new Job(conf, "page");
    job.setJarByClass(Page.class);
    job.setMapperClass(PrMapper.class);
    job.setReducerClass(PrReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}