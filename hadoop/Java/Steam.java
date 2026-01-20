import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Steam {

    public static class GameMapper extends Mapper<Object, Text, Text, Text> {
        private Text gameKey = new Text();
        private Text voteType = new Text();

        private String[] dlcKeywords = {"DLC", "Soundtrack", "OST", "Pack", "Bundle", "Expansion", "Pass", "Artbook", "Content"};

        private String getCategory(String name) {
            String upperName = name.toUpperCase();
            for (String keyword : dlcKeywords) {
                if (upperName.contains(keyword.toUpperCase())) {
                    return "DLC";
                }
            }
            return "GAME";
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(","); 
            
            if (parts.length >= 2) {
                String rawName = parts[0].trim();
                
                String cleanName = rawName.replaceAll("\"", "");
                
                if(cleanName.length() < 2) return;

                String category = getCategory(cleanName);
                String vote = parts[1].trim().toLowerCase();
                gameKey.set(category + "::" + cleanName);
                
                if (vote.equals("true") || vote.equals("1")) {
                    voteType.set("POS");
                    context.write(gameKey, voteType);
                } else if (vote.equals("false") || vote.equals("0")) {
                    voteType.set("NEG");
                    context.write(gameKey, voteType);
                }
            }
        }
    }

    public static class GameReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sumPos = 0;
            int sumNeg = 0;

            for (Text val : values) {
                if (val.toString().equals("POS")) {
                    sumPos++;
                } else {
                    sumNeg++;
                }
            }
            result.set(sumPos + "\t" + sumNeg);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Steam");
        job.setJarByClass(Steam.class);
        job.setMapperClass(GameMapper.class);
        job.setReducerClass(GameReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}