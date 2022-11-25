
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import static javax.json.JsonValue.ValueType.STRING;
import java.io.StringReader;

/**
 * ScanWordsMapper.java.
 */
public class ScanWordsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    /**
     * Parses a json file into a map of expendedUrls to the value 1.
     * @param key                       this is the position of line in the file or the offset of the line
     * @param value                     this is a line from the file
     * @param output                    this is a map from the expendedUrls (including duplicates) to the value 1
     * @throws IOException              throws possible IOException
     * @throws InterruptedException     throws possible InterruptedException
     */
    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

        String line = value.toString();
        JsonReader reader = Json.createReader(new StringReader(line));
        JsonObject tweetObject = reader.readObject();
        JsonObject entities = tweetObject.getJsonObject("entities");

        if (entities != null) {
            JsonArray urls = entities.getJsonArray("urls");
            if (urls != null) {
                for (int i = 0; i < urls.size(); i++) {
                    JsonObject items = urls.getJsonObject(i);
                    JsonValue jsonValue = items.get("expanded_url");
                    if (jsonValue.getValueType() == STRING && jsonValue != null) {
                        String expendedUrls = jsonValue.toString();
                        output.write(new Text(expendedUrls), new LongWritable(1));
                    }
                }
            }
        }
    }
}
