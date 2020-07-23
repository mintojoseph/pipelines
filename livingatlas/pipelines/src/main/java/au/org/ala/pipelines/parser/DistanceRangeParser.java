package au.org.ala.pipelines.parser;

import java.util.UnknownFormatConversionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * This is a port of
 * https://github.com/AtlasOfLivingAustralia/biocache-store/blob/master/src/main/scala/au/org/ala/biocache/parser/DistanceRangeParser.scala
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DistanceRangeParser {

  static String singleNumber = "(-?[0-9]{1,})";
  static String decimalNumber = "(-?[0-9]{1,}[.]{1}[0-9]{1,})";
  static String range =
      "(-?[0-9.]{1,})([km|kilometres|kilometers|m|metres|meters|ft|feet|f]{0,})-([0-9.]{1,})([km|kilometres|kilometers|m|metres|meters|ft|feet|f]{0,})";
  static String greaterOrLessThan =
      "(\\>|\\<)(-?[0-9.]{1,})([km|kilometres|kilometers|m|metres|meters|ft|feet|f]{0,})";
  static String singleNumberMetres = "(-?[0-9]{1,})(m|metres|meters)";
  static String singleNumberKilometres = "(-?[0-9]{1,})(km|kilometres|kilometers)";
  static String singleNumberFeet = "(-?[0-9]{1,})(ft|feet|f)";

  /**
   * Handle these formats: 2000 1km-10km 100m-1000m >10km >100m 100-1000 m
   *
   * @return the value in metres and the original units
   */
  public static double parse(String value) {
    String normalised =
        value.replaceAll("\\[", "").replaceAll(",", "").replaceAll("]", "").toLowerCase().trim();

    if (normalised.matches(singleNumber + "|" + decimalNumber)) {
      return Double.parseDouble(normalised);
    }

    // Sequence of pattern match does matter
    Matcher glMatcher = Pattern.compile(greaterOrLessThan).matcher(normalised);
    if (glMatcher.find()) {
      String numberStr = glMatcher.group(2);
      String uom = glMatcher.group(3);
      return convertUOM(Double.parseDouble(numberStr), uom);
    }

    // range check
    Matcher rangeMatcher = Pattern.compile(range).matcher(normalised);
    if (rangeMatcher.find()) {
      String numberStr = rangeMatcher.group(3);
      String uom = rangeMatcher.group(4);
      return convertUOM(Double.parseDouble(numberStr), uom);
    }

    // single number metres
    Matcher smMatcher = Pattern.compile(singleNumberMetres).matcher(normalised);
    if (smMatcher.find()) {
      String numberStr = smMatcher.group(1);
      return Double.parseDouble(numberStr);
    }
    // single number feet
    Matcher sfMatcher = Pattern.compile(singleNumberFeet).matcher(normalised);
    if (sfMatcher.find()) {
      String numberStr = sfMatcher.group(1);
      return convertUOM(Double.parseDouble(numberStr), "ft");
    }

    // single number km
    Matcher skmMatcher = Pattern.compile(singleNumberKilometres).matcher(normalised);
    if (skmMatcher.find()) {
      String numberStr = skmMatcher.group(1);
      return convertUOM(Double.parseDouble(numberStr), "km");
    }

    throw new UnknownFormatConversionException("Uncertainty: " + value + " cannot be parsed!");
  }

  private static double convertUOM(double value, String uom) {
    switch (uom.toLowerCase()) {
      case "m":
      case "meters":
      case "metres":
      case "":
        return value;
      case "ft":
      case "feet":
      case "f":
        return value * 0.3048d;
      case "km":
      case "kilometers":
      case "kilometres":
        return value * 1000d;
      default:
        log.error("{} is not recognised UOM", uom);
        throw new UnknownFormatConversionException(uom + " is not recognised UOM");
    }
  }
}
