/*
 *
 */

package pk.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Prasad Khode
 */
public class App {
    public static void main(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];

        Configuration configuration = new Configuration();
        configuration.set("input_path", inputPath);
        configuration.set("output_path", outputPath);

        int res = ToolRunner.run(configuration, new WordCount(), args);
        System.exit(res);
    }
}
