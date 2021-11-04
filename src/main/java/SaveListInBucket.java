import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;

import java.util.Arrays;
import java.util.List;

public class SaveListInBucket {


    public static void main (String [] args)
    {
        DataflowPipelineOptions pipelineOptions =
                PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
        pipelineOptions.setJobName("savelistinbucket");
        pipelineOptions.setProject("swift-stack-330507");
        pipelineOptions.setRegion("australia-southeast1");
        pipelineOptions.setRunner(DataflowRunner.class);
        pipelineOptions.setGcpTempLocation("gs://trainning_002//tmp");

        Pipeline pipeline = Pipeline.create (pipelineOptions) ;

        final List <String> input = Arrays.asList("first","second","third","last");

        pipeline.apply(Create.of(input)).apply(TextIO.write().to("gs://trainning_002//bucket").withSuffix(".txt")) ;

        pipeline.run().waitUntilFinish();
    }
}
