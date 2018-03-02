package org.gbif.pipelines.mapper;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.function.Function;

public class LocationMapper {

  private LocationMapper() {
    //Can't have an instance
  }

  public static Location map(ExtendedRecord record) {
    Function<DwcTerm, String> getValue = dwcterm -> record.getCoreTerms().get(dwcterm.qualifiedName());
    return Location.newBuilder()
      .setOccurrenceID(record.getId())
      .setLocationID(getValue.apply(DwcTerm.locationID))
      .setHigherGeographyID(getValue.apply(DwcTerm.higherGeographyID))
      .setHigherGeography(getValue.apply(DwcTerm.higherGeography))
      .setWaterBody(getValue.apply(DwcTerm.waterBody))
      .setIslandGroup(getValue.apply(DwcTerm.islandGroup))
      .setIsland(getValue.apply(DwcTerm.island))
      .setStateProvince(getValue.apply(DwcTerm.stateProvince))
      .setCounty(getValue.apply(DwcTerm.county))
      .setMunicipality(getValue.apply(DwcTerm.municipality))
      .setLocality(getValue.apply(DwcTerm.locality))
      .setVerbatimLocality(getValue.apply(DwcTerm.verbatimLocality))
      .setMinimumElevationInMeters(getValue.apply(DwcTerm.minimumElevationInMeters))
      .setMaximumElevationInMeters(getValue.apply(DwcTerm.maximumElevationInMeters))
      .setVerbatimElevation(getValue.apply(DwcTerm.verbatimElevation))
      .setMaximumDepthInMeters(getValue.apply(DwcTerm.maximumDepthInMeters))
      .setMinimumDepthInMeters(getValue.apply(DwcTerm.minimumDepthInMeters))
      .setLocationAccordingTo(getValue.apply(DwcTerm.locationAccordingTo))
      .setLocationRemarks(getValue.apply(DwcTerm.locationRemarks))
      .setDecimalLatitude(getValue.apply(DwcTerm.decimalLatitude))
      .setDecimalLongitude(getValue.apply(DwcTerm.decimalLongitude))
      .setGeodeticDatum(getValue.apply(DwcTerm.geodeticDatum))
      .setCoordinateUncertaintyInMeters(getValue.apply(DwcTerm.coordinateUncertaintyInMeters))
      .setCoordinatePrecision(getValue.apply(DwcTerm.coordinatePrecision))
      .setPointRadiusSpatialFit(getValue.apply(DwcTerm.pointRadiusSpatialFit))
      .setVerbatimCoordinates(getValue.apply(DwcTerm.verbatimCoordinates))
      .setVerbatimLatitude(getValue.apply(DwcTerm.verbatimLatitude))
      .setVerbatimLongitude(getValue.apply(DwcTerm.verbatimLongitude))
      .setVerbatimCoordinateSystem(getValue.apply(DwcTerm.verbatimCoordinateSystem))
      .setVerbatimSRS(getValue.apply(DwcTerm.verbatimSRS))
      .setFootprintWKT(getValue.apply(DwcTerm.footprintWKT))
      .setFootprintSRS(getValue.apply(DwcTerm.footprintSRS))
      .setFootprintSpatialFit(getValue.apply(DwcTerm.footprintSpatialFit))
      .setGeoreferencedBy(getValue.apply(DwcTerm.georeferencedBy))
      .setGeoreferencedDate(getValue.apply(DwcTerm.georeferencedDate))
      .setGeoreferenceProtocol(getValue.apply(DwcTerm.georeferenceProtocol))
      .setGeoreferenceSources(getValue.apply(DwcTerm.georeferenceSources))
      .setGeoreferenceVerificationStatus(getValue.apply(DwcTerm.georeferenceVerificationStatus))
      .setGeoreferenceRemarks(getValue.apply(DwcTerm.georeferenceRemarks))
      .setInstitutionID(getValue.apply(DwcTerm.institutionID))
      .setCollectionID(getValue.apply(DwcTerm.collectionID))
      .setDatasetID(getValue.apply(DwcTerm.datasetID))
      .setInstitutionCode(getValue.apply(DwcTerm.institutionCode))
      .setCollectionCode(getValue.apply(DwcTerm.collectionCode))
      .setDatasetName(getValue.apply(DwcTerm.datasetName))
      .setOwnerInstitutionCode(getValue.apply(DwcTerm.ownerInstitutionCode))
      .setDynamicProperties(getValue.apply(DwcTerm.dynamicProperties))
      .setInformationWithheld(getValue.apply(DwcTerm.informationWithheld))
      .setDataGeneralizations(getValue.apply(DwcTerm.dataGeneralizations))
      .build();
  }

}