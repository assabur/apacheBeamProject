import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;

public class wordcount {

    public static void main (String [] args)
    {
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        //read from text file
       PCollection <String> lines = pipeline.apply("read from file", TextIO.read().from("src/main/resources/input.txt"));
        //split each line
        PCollection <List <String >> wordsPerline =  lines.apply(MapElements.via(new SimpleFunction<String, List<String>>() {
            @Override
            public List<String> apply(String input) {
                return Arrays.asList(input.split(" "));
            }
        }));
        PCollection words = wordsPerline.apply(Flatten.iterables());
        //count the number of times each word occured
       PCollection <KV<String,Long>> wordcount = (PCollection<KV<String, Long>>) words.apply(Count.perElement());

       wordcount
               .apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                   @Override
                   public String apply(KV<String, Long> input) {
                       return String.format("%s => %s", input.getKey(), input.getValue());
                   }
               }))
               .apply(TextIO.write().to("src/main/resources/output.txt"));

        pipeline.run().waitUntilFinish();
    }
}
