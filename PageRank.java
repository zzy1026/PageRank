package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.io.NullWritable;

public class PageRank {

    public static class lxnmapper extends Mapper<Object,Text,Text,Text>{        
        private String id;
        private double pr;       
        private int count;
        private double average_pr;       
        public void map(Object key,Text value,Context context)
            throws IOException,InterruptedException{            
        // input value(example):  <FromNodeId    PageRank value   All ToNodeIds>£¨1     0.0005    2 3 4 5£©    
            StringTokenizer str = new StringTokenizer(value.toString());
            id =str.nextToken();     //FromNodeId
            pr = Double.parseDouble(str.nextToken());    //PageRank value
            count = str.countTokens();  //the count of ToNodeIds
            average_pr = pr/count;
            String linkids ="&";
            if(count==0){    //If node is dead-ends
               average_pr = pr/10876;
               for(int i=1;i<10877;i++){
               String s=String.valueOf(i); 
               context.write(new Text(s),new Text("@"+average_pr));  //The first type of output key_value pair
               }
               linkids +=" ";
            }
            else while(str.hasMoreTokens()){
                String linkid = str.nextToken();
                context.write(new Text(linkid),new Text("@"+average_pr));  //The first type of output key_value pair
                linkids +=" "+ linkid;
            }       
            context.write(new Text(id), new Text(linkids));  //The second type of output key_value pair
        }       
    }

    
    public static class lxnreduce extends Reducer<Text,Text,Text,Text>{     
        public void reduce(Text key,Iterable<Text> values,Context context)
            throws IOException,InterruptedException{            
            String lianjie = "";
            double pr = 0;
            
            for(Text val:values){
                if(val.toString().substring(0,1).equals("@")){
                    pr += Double.parseDouble(val.toString().substring(1));
                }
                else if(val.toString().substring(0,1).equals("&")){
                    lianjie += val.toString().substring(1);
                }
            }

            pr = 0.8*pr + 0.2/10876;          //PageRank equation
            String result = pr+lianjie;
            context.write(key, new Text(result));        //output key_value pair:     FromNodeId    PageRank value   All ToNodeIds
        }
    }

    public static void main(String[] args) throws Exception{
      Configuration conf = new Configuration();
      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      if (otherArgs.length != 2) {
    	  System.err.println("Usage: PageRank <in> <out>");
    	  System.exit(2);
      }
      String pathIn1=otherArgs[0];
      String pathOut=otherArgs[1];
        for(int i=1;i<21;i++){
        Job job = new Job(conf,"page rank");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(lxnmapper.class);
        job.setReducerClass(lxnreduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(pathIn1));
        FileOutputFormat.setOutputPath(job, new Path(pathOut));
        pathIn1 = pathOut;
        pathOut = pathOut+i;
        job.waitForCompletion(true);
        }
    }
}