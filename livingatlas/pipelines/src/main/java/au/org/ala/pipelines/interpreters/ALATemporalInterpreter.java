package au.org.ala.pipelines.interpreters;

import static org.gbif.pipelines.parsers.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.parsers.utils.ModelUtils.extractValue;
import static org.gbif.pipelines.parsers.utils.ModelUtils.hasValue;

import au.org.ala.pipelines.parser.DateParser;
import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.core.ParseResult.CONFIDENCE;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.common.parsers.date.TemporalParser;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.interpreters.core.DefaultTemporalInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

/**
 * LivingAustralia temporal interpret.
 *
 * <p>It supports DD/MM/YYYY formats
 */
public class ALATemporalInterpreter {

  // Support DMY format
  private static TemporalParser dmyParser = DateParser.getInstance();

  /** Inherit from GBIF interpretTemporal method. Add extra assertions */
  public static void interpretTemporal(ExtendedRecord er, TemporalRecord tr) {
    DefaultTemporalInterpreter.getInstance(dmyParser).interpretTemporal(er, tr);

    checkRecordDateQuality(er, tr);
    checkDateIdentified(tr);
    checkGeoreferencedDate(er, tr);
  }

  /**
   * Code copied from GBIF. Add an extra assertion
   *
   * <p>Raise Missing_COLLECTION_DATE ASSERTION
   */
  private static void checkRecordDateQuality(ExtendedRecord er, TemporalRecord tr) {
    final String year = extractValue(er, DwcTerm.year);
    final String month = extractValue(er, DwcTerm.month);
    final String day = extractValue(er, DwcTerm.day);
    final String dateString = extractValue(er, DwcTerm.eventDate);
    boolean atomizedDateProvided =
        StringUtils.isNotBlank(year)
            || StringUtils.isNotBlank(month)
            || StringUtils.isNotBlank(day);
    boolean dateStringProvided = StringUtils.isNotBlank(dateString);

    if (!atomizedDateProvided && !dateStringProvided) {
      addIssue(tr, ALAOccurrenceIssue.MISSING_COLLECTION_DATE.name());
    }

    if (tr.getDay() != null && tr.getDay() == 1) {
      addIssue(tr, ALAOccurrenceIssue.FIRST_OF_MONTH.name());
    }
    if (tr.getMonth() != null && tr.getMonth() == 1) {
      addIssue(tr, ALAOccurrenceIssue.FIRST_OF_YEAR.name());
    }
    if (tr.getYear() != null && tr.getYear() % 100 == 0) {
      addIssue(tr, ALAOccurrenceIssue.FIRST_OF_CENTURY.name());
    }
  }

  private static void checkDateIdentified(TemporalRecord tr) {
    if (tr.getEventDate() != null && StringUtils.isNotBlank(tr.getDateIdentified())) {
      ParseResult<TemporalAccessor> parsedIdentifiedResult =
          dmyParser.parse(tr.getDateIdentified());
      ParseResult<TemporalAccessor> parsedEventDateResult =
          dmyParser.parse(tr.getEventDate().getGte());

      if (parsedEventDateResult.isSuccessful() && parsedIdentifiedResult.isSuccessful()) {
        if (TemporalAccessorUtils.toDate(parsedEventDateResult.getPayload())
            .after(TemporalAccessorUtils.toDate(parsedIdentifiedResult.getPayload()))) {
          addIssue(tr, ALAOccurrenceIssue.ID_PRE_OCCURRENCE.name());
        }
      }
    }
  }

  /**
   * Require TemporalInterpreter.interpretTemporal has been called. georeferenced date is in
   * LocationRecord, but GEOREFERENCE_POST_OCCURRENCE is raised in TemporalRecord. Because,
   * geoReferencedDate needs to compare with eventDate, but due to the complexity of paring
   * eventDate, We put this check in TemporalInterpreter
   */
  private static void checkGeoreferencedDate(ExtendedRecord er, TemporalRecord tr) {

    if (tr.getEventDate() != null && hasValue(er, DwcTerm.georeferencedDate)) {
      ParseResult<TemporalAccessor> parsedGeoreferencedResult =
          dmyParser.parse(extractValue(er, DwcTerm.georeferencedDate));
      ParseResult<TemporalAccessor> parsedEventDateResult =
          dmyParser.parse(tr.getEventDate().getGte());

      if (parsedEventDateResult.isSuccessful() && parsedGeoreferencedResult.isSuccessful()) {
        if (TemporalAccessorUtils.toDate(parsedEventDateResult.getPayload())
            .before(TemporalAccessorUtils.toDate(parsedGeoreferencedResult.getPayload()))) {
          addIssue(tr, ALAOccurrenceIssue.GEOREFERENCE_POST_OCCURRENCE.name());
        }
      }
    }
  }

  /**
   * interprete geoReferenceDate, and assign it to LocationRecord
   *
   * @param er
   * @param lr
   */
  public static void interpretGeoreferencedDate(ExtendedRecord er, LocationRecord lr) {
    if (hasValue(er, DwcTerm.georeferencedDate)) {
      OccurrenceParseResult<TemporalAccessor> result =
          new OccurrenceParseResult<>(dmyParser.parse(extractValue(er, DwcTerm.georeferencedDate)));
      // check year makes sense
      if (result.isSuccessful()) {
        Optional.ofNullable(
                TemporalAccessorUtils.toEarliestLocalDateTime(result.getPayload(), false))
            .map(LocalDateTime::toString)
            .ifPresent(lr::setGeoreferencedDate);
        if (!DefaultTemporalInterpreter.isValidDate(result.getPayload(), true)) {
          lr.getIssues().getIssueList().add(ALAOccurrenceIssue.GEOREFERENCED_DATE_UNLIKELY.name());
        }
        if (result.getConfidence() == CONFIDENCE.POSSIBLE) {
          lr.getIssues().getIssueList().add(ALAOccurrenceIssue.GEOREFERENCED_DATE_AMBIGUOUS.name());
        }
      }
    } else {
      addIssue(lr, ALAOccurrenceIssue.MISSING_GEOREFERENCE_DATE.name());
    }
  }
}
