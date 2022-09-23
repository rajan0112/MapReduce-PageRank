import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce. Mapper;
import javax.naming.Context;

public class PageRankMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] splits = line.split(" ");
        String source = splits[0];
        int connections = splits.length - 2;
        double start_pr = Double.parseDouble(splits[splits.length - 1]);
        double end_pr = start_pr / connections;
        StringBuilder Nodes = new StringBuilder();
        for(int i = 1; i < splits.length - 1; i++) {
            String end_val = source + "," + String.valueOf(end_pr);
            System.out.println(splits[i] + " " + end_val);
            Nodes.append(splits[i] + " ");
            context.write(new Text(splits[i]), new Text(end_val));
        }
        System.out.println(Nodes.toString());
        context.write(new Text(source), new Text(Nodes.toString().trim()));
    }
}

