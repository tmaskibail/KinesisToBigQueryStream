package com.tmaksibail;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.kinesis.KinesisIO;
import org.apache.beam.sdk.io.kinesis.KinesisRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class KinesisToBigQueryStream {
    private static final String KINESIS_STREAM = "test-stream";
    private static final String ACCESS_KEY = "xxxx";
    private static final Regions REGION = Regions.EU_WEST_1;
    private static final String SECRET_KEY = "yyyyy";
    private static final String DESTINATION_TABLE = "PROJECT:DATASET.TABLE";

    private static final Logger LOG = LoggerFactory.getLogger(KinesisToBigQueryStream.class);

    public static void main(String[] args) {
        PipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);
        runPipeline(options);
    }

    static void runPipeline(PipelineOptions options) {

        LOG.debug("Initialised the pipeline");
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("id").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("name").setType("STRING"));
        TableSchema schema = new TableSchema().setFields(fields);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("KinesisRead", KinesisIO.read()
                .withStreamName(KINESIS_STREAM)
                .withInitialPositionInStream(InitialPositionInStream.LATEST)
                .withAWSClientsProvider(ACCESS_KEY, SECRET_KEY, REGION))

                .apply("convertRow", ParDo.of(new RecordMapper()))

                .apply("Apply records to a fixed window", Window.into(FixedWindows.of(Duration.standardSeconds(20))))

                .apply("Write records to BigQuery", BigQueryIO.writeTableRows()
                        .to(DESTINATION_TABLE)
                        .withSchema(schema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run().waitUntilFinish();
    }

    static class RecordMapper extends DoFn<KinesisRecord, TableRow> {
        @ProcessElement
        public void processElement(@Element KinesisRecord record, OutputReceiver<TableRow> outputReceiver) {

            // Dummy code to trigger auto scale.
            long sleepTime = 100 * 1000000L;
            long startTime = System.nanoTime();
            while ((System.nanoTime() - startTime) < sleepTime) {
            }

            TableRow row = new TableRow();
            row.set("id", record.getUniqueId());
            row.set("name", new String(record.getData().array(), StandardCharsets.UTF_8));
            outputReceiver.output(row);
        }
    }

}
