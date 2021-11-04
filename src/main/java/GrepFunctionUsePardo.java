import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class GrepFunctionUsePardo {
    public static void main (String [] args)
    {
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);
        String input = "src/main/resources/input.txt" ;
        String output = "src/main/resources/grepOutput.txt" ;
        final String searchTerm = "private" ;
        pipeline.apply("GetFile", TextIO.read().from(input))
        .apply("Grep", ParDo.of(new DoFn<String, String>() {
        @ProcessElement
            public void processElement (ProcessContext context) throws Exception
        {
            String line = context.element();
            if (line.contains(searchTerm))
            {
                context.output(line);
            }

        }

        })).apply(TextIO.write().to(output).withSuffix(".txt").withoutSharding());

        pipeline.run().waitUntilFinish();

    }
}
