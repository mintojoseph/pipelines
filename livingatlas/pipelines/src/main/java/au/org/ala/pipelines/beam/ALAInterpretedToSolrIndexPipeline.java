package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

import au.org.ala.pipelines.options.ALASolrPipelineOptions;
import au.org.ala.pipelines.transforms.ALAAttributionTransform;
import au.org.ala.pipelines.transforms.ALASolrDocumentTransform;
import au.org.ala.pipelines.transforms.ALATaxonomyTransform;
import au.org.ala.pipelines.transforms.ALAUUIDTransform;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import java.io.FileNotFoundException;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.solr.SolrIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.solr.common.SolrInputDocument;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.ingest.utils.MetricsHandler;
import org.gbif.pipelines.io.avro.ALAAttributionRecord;
import org.gbif.pipelines.io.avro.ALATaxonRecord;
import org.gbif.pipelines.io.avro.ALAUUIDRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationFeatureRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
import org.gbif.pipelines.transforms.specific.LocationFeatureTransform;
import org.slf4j.MDC;

/**
 * ALA Beam pipeline for creating a SOLR index. This pipeline uses the HTTP SOLR api to index
 * records..
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAInterpretedToSolrIndexPipeline {

  public static void main(String[] args) throws FileNotFoundException {
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "index");
    ALASolrPipelineOptions options =
        PipelinesOptionsFactory.create(ALASolrPipelineOptions.class, combinedArgs);
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
  }

  public static void run(ALASolrPipelineOptions options) {

    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", StepType.INTERPRETED_TO_INDEX.name());

    log.info("Adding step 1: Options");
    UnaryOperator<String> pathFn =
        t -> FsUtils.buildPathInterpretUsingTargetPath(options, t, "*" + AVRO_EXTENSION);
    UnaryOperator<String> identifiersPathFn =
        t -> ALAFsUtils.buildPathIdentifiersUsingTargetPath(options, t, "*" + AVRO_EXTENSION);
    UnaryOperator<String> samplingPathFn =
        t -> ALAFsUtils.buildPathSamplingUsingTargetPath(options, t, "*" + AVRO_EXTENSION);

    Pipeline p = Pipeline.create(options);

    log.info("Adding step 2: Creating transformations");
    // Core
    BasicTransform basicTransform = BasicTransform.builder().create();
    MetadataTransform metadataTransform = MetadataTransform.builder().create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.create();
    TaxonomyTransform taxonomyTransform = TaxonomyTransform.builder().create();

    // ALA specific
    ALAUUIDTransform alaUuidTransform = ALAUUIDTransform.create();
    ALATaxonomyTransform alaTaxonomyTransform = ALATaxonomyTransform.builder().create();
    LocationFeatureTransform locationFeatureTransform = LocationFeatureTransform.builder().create();
    LocationTransform locationTransform = LocationTransform.builder().create();
    ALAAttributionTransform alaAttributionTransform = ALAAttributionTransform.builder().create();

    log.info("Adding step 3: Creating beam pipeline");
    PCollectionView<MetadataRecord> metadataView =
        p.apply("Read Metadata", metadataTransform.read(pathFn))
            .apply("Convert to view", View.asSingleton());

    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        p.apply("Read Verbatim", verbatimTransform.read(pathFn))
            .apply("Map Verbatim to KV", verbatimTransform.toKv());

    PCollection<KV<String, BasicRecord>> basicCollection =
        p.apply("Read Basic", basicTransform.read(pathFn))
            .apply("Map Basic to KV", basicTransform.toKv());

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        p.apply("Read Temporal", temporalTransform.read(pathFn))
            .apply("Map Temporal to KV", temporalTransform.toKv());

    PCollection<KV<String, LocationRecord>> locationCollection =
        p.apply("Read Location", locationTransform.read(pathFn))
            .apply("Map Location to KV", locationTransform.toKv());

    PCollection<KV<String, TaxonRecord>> taxonCollection =
        p.apply(
                "Read Taxon",
                options.getIncludeGbifTaxonomy()
                    ? taxonomyTransform.read(pathFn)
                    : Create.empty(TypeDescriptor.of(TaxonRecord.class)))
            .apply("Map Taxon to KV", taxonomyTransform.toKv());

    // ALA Specific
    PCollection<KV<String, ALAUUIDRecord>> alaUUidCollection =
        p.apply("Read UUID", alaUuidTransform.read(identifiersPathFn))
            .apply("Map UUID to KV", alaUuidTransform.toKv());

    PCollection<KV<String, ALATaxonRecord>> alaTaxonCollection =
        p.apply("Read ALA Taxon", alaTaxonomyTransform.read(pathFn))
            .apply("Map ALA Taxon to KV", alaTaxonomyTransform.toKv());

    PCollection<KV<String, ALAAttributionRecord>> alaAttributionCollection =
        p.apply("Read attribution", alaAttributionTransform.read(pathFn))
            .apply("Map attribution to KV", alaAttributionTransform.toKv());

    PCollection<KV<String, LocationFeatureRecord>> locationFeatureCollection =
        p.apply(
                "Read Sampling",
                options.getIncludeSampling()
                    ? locationFeatureTransform.read(samplingPathFn)
                    : Create.empty(TypeDescriptor.of(LocationFeatureRecord.class)))
            .apply("Map Sampling to KV", locationFeatureTransform.toKv());

    log.info("Adding step 3: Converting into a json object");
    ParDo.SingleOutput<KV<String, CoGbkResult>, SolrInputDocument> alaSolrDoFn =
        ALASolrDocumentTransform.builder()
            .datasetId(options.getDatasetId())
            .metadataView(metadataView)
            .urTag(alaUuidTransform.getTag())
            .erTag(verbatimTransform.getTag())
            .brTag(basicTransform.getTag())
            .trTag(temporalTransform.getTag())
            .lrTag(locationTransform.getTag())
            .txrTag(taxonomyTransform.getTag())
            .asrTag(locationFeatureTransform.getTag())
            .atxrTag(alaTaxonomyTransform.getTag())
            .aarTag(alaAttributionTransform.getTag())
            .build()
            .converter();

    PCollection<SolrInputDocument> solrInputDocumentPCollection =
        KeyedPCollectionTuple
            // Core
            .of(basicTransform.getTag(), basicCollection)
            .and(temporalTransform.getTag(), temporalCollection)
            .and(locationTransform.getTag(), locationCollection)
            // Raw
            .and(verbatimTransform.getTag(), verbatimCollection)
            // ALA Specific
            .and(alaUuidTransform.getTag(), alaUUidCollection)
            .and(alaTaxonomyTransform.getTag(), alaTaxonCollection)
            .and(alaAttributionTransform.getTag(), alaAttributionCollection)
            .and(locationFeatureTransform.getTag(), locationFeatureCollection)
            .and(taxonomyTransform.getTag(), taxonCollection)
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging to Solr doc", alaSolrDoFn);

    log.info("Adding step 4: SOLR indexing");
    SolrIO.ConnectionConfiguration conn =
        SolrIO.ConnectionConfiguration.create(options.getZkHost());

    solrInputDocumentPCollection.apply(
        SolrIO.write()
            .to(options.getSolrCollection())
            .withConnectionConfiguration(conn)
            .withMaxBatchSize(options.getSolrBatchSize()));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    log.info("Pipeline has been finished");
  }
}
