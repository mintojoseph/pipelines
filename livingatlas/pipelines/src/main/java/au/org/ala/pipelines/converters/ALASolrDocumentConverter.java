package au.org.ala.pipelines.converters;

import static org.apache.avro.Schema.Type.UNION;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.SolrInputDocument;
import org.gbif.pipelines.core.utils.TemporalUtils;
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
import org.jetbrains.annotations.NotNull;

/** Create a SOLR document using the supplied records. */
@Slf4j
@Builder
public class ALASolrDocumentConverter {

  private static final Set<String> SKIP_KEYS = new HashSet<>();

  static {
    SKIP_KEYS.add("id");
    SKIP_KEYS.add("created");
    SKIP_KEYS.add("text");
    SKIP_KEYS.add("name");
    SKIP_KEYS.add("coreRowType");
    SKIP_KEYS.add("coreTerms");
    SKIP_KEYS.add("extensions");
    SKIP_KEYS.add("usage");
    SKIP_KEYS.add("classification");
    SKIP_KEYS.add("issues");
    SKIP_KEYS.add("eventDate");
    SKIP_KEYS.add("hasCoordinate");
    SKIP_KEYS.add("hasGeospatialIssue");
    SKIP_KEYS.add("gbifId");
    SKIP_KEYS.add("crawlId");
    SKIP_KEYS.add("networkKeys");
    SKIP_KEYS.add("protocol");
    SKIP_KEYS.add("issues");
    SKIP_KEYS.add("machineTags"); // TODO review content
  }

  private final MetadataRecord metadataRecord;
  private final BasicRecord basicRecord;
  private final TemporalRecord temporalRecord;
  private final LocationRecord locationRecord;
  private final TaxonRecord taxonRecord;
  private final ALATaxonRecord alaTaxonRecord;
  private final ExtendedRecord extendedRecord;
  private final ALAAttributionRecord alaAttributionRecord;
  private final LocationFeatureRecord locationFeatureRecord;
  private final ALAUUIDRecord alauuidRecord;

  @NotNull
  public SolrInputDocument createSolrDocument() {

    SolrInputDocument doc = new SolrInputDocument();

    fillALAUUIDRecord(doc);
    fillMetadataRecord(doc);
    fillBasicRecord(doc);
    fillTemporalRecord(doc);
    fillLocationRecord(doc);
    fillTaxonomyRecord(doc);
    fillALATaxonomy(doc);
    fillExtendedRecord(doc);
    fillALAAttributionRecord(doc);
    fillLocationFeatureRecord(doc);

    doc.setField("first_loaded_date", new Date());

    return doc;
  }

  private void fillALAUUIDRecord(SolrInputDocument doc) {
    doc.setField("id", alauuidRecord.getUuid());
  }

  private void fillLocationFeatureRecord(SolrInputDocument doc) {
    if (locationFeatureRecord != null) {
      locationFeatureRecord.getItems().entrySet().stream()
          .filter(sample -> !StringUtils.isEmpty(sample.getValue()))
          .forEach(
              sample -> {
                if (sample.getKey().startsWith("el")) {
                  doc.setField(sample.getKey(), Double.valueOf(sample.getValue()));
                } else {
                  doc.setField(sample.getKey(), sample.getValue());
                }
              });
    }
  }

  private void fillALAAttributionRecord(SolrInputDocument doc) {
    // Add legacy collectory fields
    if (alaAttributionRecord != null) {
      addIfNotEmpty(doc, "license", alaAttributionRecord.getLicenseType());
      // for backwards compatibility
      addIfNotEmpty(doc, "raw_dataResourceUid", alaAttributionRecord.getDataResourceUid());
      addIfNotEmpty(doc, "dataResourceUid", alaAttributionRecord.getDataResourceUid());
      addIfNotEmpty(doc, "dataResourceName", alaAttributionRecord.getDataResourceName());
      addIfNotEmpty(doc, "dataProviderUid", alaAttributionRecord.getDataProviderUid());
      addIfNotEmpty(doc, "dataProviderName", alaAttributionRecord.getDataProviderName());
      addIfNotEmpty(doc, "institutionUid", alaAttributionRecord.getInstitutionUid());
      addIfNotEmpty(doc, "collectionUid", alaAttributionRecord.getCollectionUid());
      addIfNotEmpty(doc, "institutionName", alaAttributionRecord.getInstitutionName());
      addIfNotEmpty(doc, "collectionName", alaAttributionRecord.getCollectionName());
    }
  }

  /** Verbatim (Raw) data */
  private void fillExtendedRecord(SolrInputDocument doc) {
    addToDoc(extendedRecord, doc);

    extendedRecord
        .getCoreTerms()
        .forEach(
            (key, value) -> {
              if (key.startsWith("http")) {
                key = key.substring(key.lastIndexOf("/") + 1);
              }
              doc.setField("raw_" + key, value);
            });
  }

  private void fillALATaxonomy(SolrInputDocument doc) {
    // ALA taxonomy & species groups - backwards compatible for EYA
    if (alaTaxonRecord.getTaxonConceptID() != null) {
      List<Field> fields = alaTaxonRecord.getSchema().getFields();
      for (Field field : fields) {
        Object value = alaTaxonRecord.get(field.name());
        if (value != null
            && !field.name().equals("speciesGroup")
            && !field.name().equals("speciesSubgroup")
            && !SKIP_KEYS.contains(field.name())) {
          if (field.name().equalsIgnoreCase("issues")) {
            doc.setField("assertions", value);
          } else {
            if (value instanceof Integer) {
              doc.setField(field.name(), value);
            } else {
              doc.setField(field.name(), value.toString());
            }
          }
        }
      }

      // required for EYA
      doc.setField(
          "names_and_lsid",
          String.join(
              "|",
              alaTaxonRecord.getScientificName(),
              alaTaxonRecord.getTaxonConceptID(),
              alaTaxonRecord.getVernacularName(),
              alaTaxonRecord.getKingdom(),
              alaTaxonRecord.getFamily())); // is set to IGNORE in headerAttributes

      doc.setField(
          "common_name_and_lsid",
          String.join(
              "|",
              alaTaxonRecord.getVernacularName(),
              alaTaxonRecord.getScientificName(),
              alaTaxonRecord.getTaxonConceptID(),
              alaTaxonRecord.getVernacularName(),
              alaTaxonRecord.getKingdom(),
              alaTaxonRecord.getFamily())); // is set to IGNORE in headerAttributes

      // legacy fields referenced in biocache-service code
      doc.setField("taxon_name", alaTaxonRecord.getScientificName());
      doc.setField("lsid", alaTaxonRecord.getTaxonConceptID());
      doc.setField("rank", alaTaxonRecord.getRank());
      doc.setField("rank_id", alaTaxonRecord.getRankID());

      Optional.ofNullable(alaTaxonRecord.getVernacularName())
          .ifPresent(x -> doc.setField("common_name", x));
      alaTaxonRecord.getSpeciesGroup().forEach(s -> doc.setField("species_group", s));
      alaTaxonRecord.getSpeciesSubgroup().forEach(s -> doc.setField("species_subgroup", s));
    }
  }

  private void fillMetadataRecord(SolrInputDocument doc) {
    addToDoc(metadataRecord, doc);
    addToDoc(metadataRecord, doc);
    metadataRecord.getIssues().getIssueList().forEach(issue -> doc.setField("assertions", issue));
  }

  private void fillBasicRecord(SolrInputDocument doc) {
    addToDoc(basicRecord, doc);
    basicRecord.getIssues().getIssueList().forEach(issue -> doc.setField("assertions", issue));
  }

  private void fillTemporalRecord(SolrInputDocument doc) {
    addToDoc(temporalRecord, doc);
    // add event date
    try {
      if (temporalRecord.getEventDate() != null && temporalRecord.getEventDate().getGte() != null) {
        doc.setField(
            "eventDateSingle",
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm")
                .parse(temporalRecord.getEventDate().getGte()));
      } else {
        TemporalUtils.getTemporal(
                temporalRecord.getYear(), temporalRecord.getMonth(), temporalRecord.getDay())
            .ifPresent(x -> doc.setField("eventDateSingle", x));
      }
    } catch (ParseException e) {
      log.error(
          "Unparseable date produced by downstream interpretation "
              + temporalRecord.getEventDate().getGte());
    }
    temporalRecord.getIssues().getIssueList().forEach(issue -> doc.setField("assertions", issue));
  }

  private void fillLocationRecord(SolrInputDocument doc) {
    addToDoc(locationRecord, doc);
    if (locationRecord.getDecimalLatitude() != null
        && locationRecord.getDecimalLongitude() != null) {
      addGeo(doc, locationRecord.getDecimalLatitude(), locationRecord.getDecimalLongitude());
    }
    doc.setField("geospatial_kosher", locationRecord.getHasCoordinate());
    locationRecord.getIssues().getIssueList().forEach(issue -> doc.setField("assertions", issue));
  }

  private void fillTaxonomyRecord(SolrInputDocument doc) {
    // GBIF taxonomy - add if available
    if (taxonRecord != null) {
      // add the classification
      taxonRecord
          .getClassification()
          .forEach(
              entry -> {
                String rank = entry.getRank().toString().toLowerCase();
                doc.setField("gbif_s_" + rank + "_id", entry.getKey());
                doc.setField("gbif_s_" + rank, entry.getName());
              });

      doc.setField("gbif_s_rank", taxonRecord.getAcceptedUsage().getRank().toString());
      doc.setField("gbif_s_scientificName", taxonRecord.getAcceptedUsage().getName());
      // legacy fields reference directly in biocache-service code

      taxonRecord.getIssues().getIssueList().forEach(issue -> doc.setField("assertions", issue));
    }
  }

  static void addIfNotEmpty(SolrInputDocument doc, String fieldName, String value) {
    if (StringUtils.isNotEmpty(value)) {
      doc.setField(fieldName, value);
    }
  }

  static void addGeo(SolrInputDocument doc, double lat, double lon) {
    String latlon = "";
    // ensure that the lat longs are in the required range before
    if (lat <= 90 && lat >= -90d && lon <= 180 && lon >= -180d) {
      // https://lucene.apache.org/solr/guide/7_0/spatial-search.html#indexing-points
      // required format for indexing geodetic points in SOLR
      latlon = lat + "," + lon;
    }

    // is set to IGNORE in headerAttributes
    doc.addField("lat_long", latlon);
    // is set to IGNORE in headerAttributes
    doc.addField("point-1", getLatLongString(lat, lon, "#"));
    // is set to IGNORE in headerAttributes
    doc.addField("point-0.1", getLatLongString(lat, lon, "#.#"));
    // is set to IGNORE in headerAttributes
    doc.addField("point-0.01", getLatLongString(lat, lon, "#.##"));
    // is set to IGNORE in headerAttributes
    doc.addField("point-0.02", getLatLongStringStep(lat, lon, "#.##", 0.02));
    // is set to IGNORE in headerAttributes
    doc.addField("point-0.001", getLatLongString(lat, lon, "#.###"));
    // is set to IGNORE in headerAttributes
    doc.addField("point-0.0001", getLatLongString(lat, lon, "#.####"));
  }

  static String getLatLongStringStep(Double lat, Double lon, String format, Double step) {
    DecimalFormat df = new DecimalFormat(format);
    // By some "strange" decision the default rounding model is HALF_EVEN
    df.setRoundingMode(java.math.RoundingMode.HALF_UP);
    return df.format(Math.round(lat / step) * step)
        + ","
        + df.format(Math.round(lon / step) * step);
  }

  /** Returns a lat,long string expression formatted to the supplied Double format */
  static String getLatLongString(Double lat, Double lon, String format) {
    DecimalFormat df = new DecimalFormat(format);
    // By some "strange" decision the default rounding model is HALF_EVEN
    df.setRoundingMode(java.math.RoundingMode.HALF_UP);
    return df.format(lat) + "," + df.format(lon);
  }

  static void addToDoc(SpecificRecordBase record, SolrInputDocument doc) {

    record.getSchema().getFields().stream()
        .filter(n -> !SKIP_KEYS.contains(n.name()))
        .forEach(
            f ->
                Optional.ofNullable(record.get(f.pos()))
                    .ifPresent(
                        r -> {
                          Schema schema = f.schema();
                          Optional<Schema.Type> type =
                              schema.getType() == UNION
                                  ? schema.getTypes().stream()
                                      .filter(t -> t.getType() != Schema.Type.NULL)
                                      .findFirst()
                                      .map(Schema::getType)
                                  : Optional.of(schema.getType());

                          type.ifPresent(
                              t -> {
                                switch (t) {
                                  case BOOLEAN:
                                    doc.setField(f.name(), r);
                                    break;
                                  case FLOAT:
                                    doc.setField(f.name(), r);
                                    break;
                                  case DOUBLE:
                                    doc.setField(f.name(), r);
                                    break;
                                  case INT:
                                    doc.setField(f.name(), r);
                                    break;
                                  case LONG:
                                    doc.setField(f.name(), r);
                                    break;
                                  default:
                                    doc.setField(f.name(), r.toString());
                                    break;
                                }
                              });
                        }));
  }
}
