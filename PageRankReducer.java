import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import javax.naming.Context;

public class PageRankReducer
        extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double pr_val = 0.0;
        String t = "";
        for(Text value: values) {
            String line = value.toString();

            if(!line.contains(",")) {
                t = key + " " + line;
                continue;
            }
            String[] splits = line.split(",");
            double pr = Double.parseDouble(splits[1]);
            pr_val += pr;
        }
        context.write(NullWritable.get(), new Text(t + " " + pr_val + ""));
    }
}
