/*
 *
 */

package pk.wordcount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import pk.wordcount.mapper.WordCountMapper;
import pk.wordcount.reducer.WordCountReducer;

/**
 * @author Prasad Khode
 */
public class WordCount extends Configured implements Tool {
    public int run(String[] arg0) throws Exception {
        String inputPath = getConf().get("input_path");
        String outputPath = getConf().get("output_path");

        System.out.println("input = [" + inputPath + "]");
        System.out.println("output = [" + outputPath + "]");

        Job job = new Job(getConf());
        job.setJarByClass(WordCount.class);
        job.setJobName("wordcount");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

}
