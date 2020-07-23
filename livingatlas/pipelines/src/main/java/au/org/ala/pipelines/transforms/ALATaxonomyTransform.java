package au.org.ala.pipelines.transforms;

import static au.org.ala.pipelines.common.ALARecordTypes.ALA_TAXONOMY;

import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.names.ws.api.NameSearch;
import au.org.ala.names.ws.api.NameUsageMatch;
import au.org.ala.pipelines.interpreters.ALATaxonomyInterpreter;
import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter;
import org.gbif.pipelines.io.avro.ALATaxonRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.transforms.SerializableConsumer;
import org.gbif.pipelines.transforms.SerializableSupplier;
import org.gbif.pipelines.transforms.Transform;

/**
 * ALA taxonomy transform for adding ALA taxonomy to interpreted occurrence data.
 *
 * <p>Beam level transformations for the DWC Taxon, reads an avro, writes an avro, maps from value
 * to keyValue and transforms form {@link ExtendedRecord} to {@link TaxonRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link TaxonRecord} using {@link ExtendedRecord} as
 * a source and {@link TaxonomyInterpreter} as interpretation steps
 *
 * @see <a href="https://dwc.tdwg.org/terms/#taxon</a>
 */
@Slf4j
public class ALATaxonomyTransform extends Transform<ExtendedRecord, ALATaxonRecord> {

  private final String datasetId;
  private KeyValueStore<NameSearch, NameUsageMatch> nameMatchStore;
  private SerializableSupplier<KeyValueStore<NameSearch, NameUsageMatch>> nameMatchStoreSupplier;
  private KeyValueStore<String, ALACollectoryMetadata> dataResourceStore;
  private SerializableSupplier<KeyValueStore<String, ALACollectoryMetadata>>
      dataResourceStoreSupplier;

  @Builder(buildMethodName = "create")
  private ALATaxonomyTransform(
      String datasetId,
      SerializableSupplier<KeyValueStore<NameSearch, NameUsageMatch>> nameMatchStoreSupplier,
      SerializableSupplier<KeyValueStore<String, ALACollectoryMetadata>>
          dataResourceStoreSupplier) {
    super(
        ALATaxonRecord.class,
        ALA_TAXONOMY,
        ALATaxonomyTransform.class.getName(),
        "alaTaxonRecordsCount");
    this.datasetId = datasetId;
    this.nameMatchStoreSupplier = nameMatchStoreSupplier;
    this.dataResourceStoreSupplier = dataResourceStoreSupplier;
  }

  /** Maps {@link ALATaxonRecord} to key value, where key is {@link TaxonRecord#getId} */
  public MapElements<ALATaxonRecord, KV<String, ALATaxonRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, ALATaxonRecord>>() {})
        .via((ALATaxonRecord tr) -> KV.of(tr.getId(), tr));
  }

  public ALATaxonomyTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (nameMatchStore == null && nameMatchStoreSupplier != null) {
      log.info("Initialize NameUsageMatchKvStore");
      nameMatchStore = nameMatchStoreSupplier.get();
    }
    if (dataResourceStore == null && dataResourceStoreSupplier != null) {
      log.info("Initialize CollectoryKvStore");
      dataResourceStore = dataResourceStoreSupplier.get();
    }
  }

  @Override
  public Optional<ALATaxonRecord> convert(ExtendedRecord source) {
    ALACollectoryMetadata dataResource = dataResourceStore.get(datasetId);
    return Interpretation.from(source)
        .to(er -> ALATaxonRecord.newBuilder().setId(er.getId()).build())
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(ALATaxonomyInterpreter.alaTaxonomyInterpreter(dataResource, nameMatchStore))
        // the id is null when there is an error in the interpretation. In these
        // cases we do not write the taxonRecord because it is totally empty.
        .get();
  }
}
