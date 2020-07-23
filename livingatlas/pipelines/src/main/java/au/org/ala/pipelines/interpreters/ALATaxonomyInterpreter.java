package au.org.ala.pipelines.interpreters;

import java.util.ArrayList;
import java.util.Map;
import java.util.function.BiConsumer;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.io.avro.ALAMatchType;
import org.gbif.pipelines.io.avro.ALATaxonRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.parsers.utils.ModelUtils;

import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.names.ws.api.NameSearch;
import au.org.ala.names.ws.api.NameUsageMatch;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_FUZZY;
import static org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_HIGHERRANK;
import static org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_NONE;
import static org.gbif.pipelines.parsers.utils.ModelUtils.addIssue;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALATaxonomyInterpreter {

  /**
   * Interprets a utils from the taxonomic fields specified in the {@link ExtendedRecord} received.
   */
  public static BiConsumer<ExtendedRecord, ALATaxonRecord> alaTaxonomyInterpreter(
      ALACollectoryMetadata dataResource, KeyValueStore<NameSearch, NameUsageMatch> kvStore) {
    return (er, atr) -> {
      atr.setId(er.getId());

      if (kvStore == null) {
        return;
      }

      Map<String, String> defaults = dataResource.getDefaultDarwinCoreValues();
      String genus = extractValue(er, DwcTerm.genus, defaults);
      if (genus == null) genus = extractValue(er, GbifTerm.genericName, defaults);
      NameSearch matchRequest =
          NameSearch.builder()
              .kingdom(extractValue(er, DwcTerm.kingdom, defaults))
              .phylum(extractValue(er, DwcTerm.phylum, defaults))
              .clazz(extractValue(er, DwcTerm.class_, defaults))
              .order(extractValue(er, DwcTerm.order, defaults))
              .family(extractValue(er, DwcTerm.family, defaults))
              .genus(genus)
              .scientificName(extractValue(er, DwcTerm.scientificName, defaults))
              .rank(extractValue(er, DwcTerm.taxonRank, defaults))
              .verbatimTaxonRank(extractValue(er, DwcTerm.verbatimTaxonRank, defaults))
              .specificEpithet(extractValue(er, DwcTerm.specificEpithet, defaults))
              .infraspecificEpithet(extractValue(er, DwcTerm.infraspecificEpithet, defaults))
              .scientificNameAuthorship(
                  extractValue(er, DwcTerm.scientificNameAuthorship, defaults))
              .vernacularName(extractValue(er, DwcTerm.vernacularName, defaults))
              .build();

      NameUsageMatch usageMatch = kvStore.get(matchRequest);
      if (usageMatch != null && usageMatch.isSuccess()) {
        // Do a straight property-property copy to catch complications with field names
        atr.setTaxonConceptID(usageMatch.getTaxonConceptID());
        atr.setScientificName(usageMatch.getScientificName());
        atr.setScientificNameAuthorship(usageMatch.getScientificNameAuthorship());
        atr.setRank(usageMatch.getRank());
        atr.setRankID(usageMatch.getRankID());
        atr.setLft(usageMatch.getLft());
        atr.setRgt(usageMatch.getRgt());
        atr.setMatchType(usageMatch.getMatchType());
        atr.setNameType(usageMatch.getNameType());
        // Ignore synonym type
        atr.setKingdom(usageMatch.getKingdom());
        atr.setKingdomID(usageMatch.getKingdomID());
        atr.setPhylum(usageMatch.getPhylum());
        atr.setPhylumID(usageMatch.getPhylumID());
        atr.setClasss(usageMatch.getClasss());
        atr.setClassID(usageMatch.getClassID());
        atr.setOrder(usageMatch.getOrder());
        atr.setOrderID(usageMatch.getOrderID());
        atr.setFamily(usageMatch.getFamily());
        atr.setFamilyID(usageMatch.getFamilyID());
        atr.setGenus(usageMatch.getGenus());
        atr.setGenusID(usageMatch.getGenusID());
        atr.setSpecies(usageMatch.getSpecies());
        atr.setSpeciesID(usageMatch.getSpeciesID());
        atr.setVernacularName(usageMatch.getVernacularName());
        atr.setSpeciesGroup(
            usageMatch.getSpeciesGroup() == null
                ? new ArrayList<>()
                : usageMatch.getSpeciesGroup());
        atr.setSpeciesSubgroup(
            usageMatch.getSpeciesSubgroup() == null
                ? new ArrayList<>()
                : usageMatch.getSpeciesSubgroup());
        // Issues can happen for match/nomatch
      } else {
        // happens when we get an empty response from the WS
        addIssue(atr, TAXON_MATCH_NONE);
      }


      if (usageMatch != null) {
        if (usageMatch.getIssues() != null) {
          for (String issue : usageMatch.getIssues()) addIssue(atr, issue);
        }
        // Additional issue flags
        String matchType = usageMatch.getMatchType();
        if (ALAMatchType.fuzzyMatch.name().equalsIgnoreCase(matchType))
          addIssue(atr, TAXON_MATCH_FUZZY);
        if (ALAMatchType.higherMatch.name().equalsIgnoreCase(matchType))
          addIssue(atr, TAXON_MATCH_HIGHERRANK);
      }
    };
  }

  /**
   * Extract a value from a record, with a potential default value
   *
   * @param er The extened record
   * @param term The term to look up
   * @param defaults Any defaults that apply to this value
   * @return The resulting value, or null for not found
   */
  private static String extractValue(ExtendedRecord er, Term term, Map<String, String> defaults) {
    String value = ModelUtils.extractValue(er, term);
    if (value == null && defaults != null && !defaults.isEmpty()) {
      value = defaults.get(term.simpleName());
    }
    return value;
  }
}
