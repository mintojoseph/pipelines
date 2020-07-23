package au.org.ala.pipelines.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;

import au.org.ala.pipelines.converters.ALASolrDocumentConverter;
import java.io.Serializable;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.solr.common.SolrInputDocument;
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

/**
 * A SOLR transform that aims to provide a index that is backwards compatible with ALA's
 * biocache-service.
 */
@Slf4j
@Builder
public class ALASolrDocumentTransform implements Serializable {

  private static final long serialVersionUID = -3879254305014174944L;

  // Core
  @NonNull private final TupleTag<ExtendedRecord> erTag;
  @NonNull private final TupleTag<BasicRecord> brTag;
  @NonNull private final TupleTag<TemporalRecord> trTag;
  @NonNull private final TupleTag<LocationRecord> lrTag;

  private final TupleTag<TaxonRecord> txrTag;
  @NonNull private final TupleTag<ALATaxonRecord> atxrTag;

  private final TupleTag<LocationFeatureRecord> asrTag;

  private final TupleTag<ALAAttributionRecord> aarTag;
  @NonNull private final TupleTag<ALAUUIDRecord> urTag;

  @NonNull private final PCollectionView<MetadataRecord> metadataView;

  String datasetId;

  public ParDo.SingleOutput<KV<String, CoGbkResult>, SolrInputDocument> converter() {

    DoFn<KV<String, CoGbkResult>, SolrInputDocument> fn =
        new DoFn<KV<String, CoGbkResult>, SolrInputDocument>() {

          private final Counter counter =
              Metrics.counter(ALASolrDocumentTransform.class, AVRO_TO_JSON_COUNT);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            // Core
            MetadataRecord mdr = c.sideInput(metadataView);
            ExtendedRecord er = v.getOnly(erTag, ExtendedRecord.newBuilder().setId(k).build());
            BasicRecord br = v.getOnly(brTag, BasicRecord.newBuilder().setId(k).build());
            TemporalRecord tr = v.getOnly(trTag, TemporalRecord.newBuilder().setId(k).build());
            LocationRecord lr = v.getOnly(lrTag, LocationRecord.newBuilder().setId(k).build());
            TaxonRecord txr = v.getOnly(txrTag, TaxonRecord.newBuilder().setId(k).build());

            // ALA specific
            ALAUUIDRecord ur = v.getOnly(urTag);
            ALATaxonRecord atxr = v.getOnly(atxrTag, ALATaxonRecord.newBuilder().setId(k).build());
            ALAAttributionRecord aar =
                v.getOnly(aarTag, ALAAttributionRecord.newBuilder().setId(k).build());
            LocationFeatureRecord asr =
                v.getOnly(asrTag, LocationFeatureRecord.newBuilder().setId(k).build());

            SolrInputDocument doc =
                ALASolrDocumentConverter.builder()
                    .metadataRecord(mdr)
                    .basicRecord(br)
                    .temporalRecord(tr)
                    .locationRecord(lr)
                    .taxonRecord(txr)
                    .alaTaxonRecord(atxr)
                    .extendedRecord(er)
                    .alaAttributionRecord(aar)
                    .locationFeatureRecord(asr)
                    .alauuidRecord(ur)
                    .build()
                    .createSolrDocument();

            c.output(doc);
            counter.inc();
          }
        };

    return ParDo.of(fn).withSideInputs(metadataView);
  }
}
