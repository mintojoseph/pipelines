package org.gbif.pipelines.core.parsers.location.parser;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.io.avro.GadmFeatures;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.geocode.Location;

import java.util.Objects;
import java.util.Optional;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GadmParser {

  public static Optional<GadmFeatures> parseGadm(
      LocationRecord lr, KeyValueStore<LatLng, GeocodeResponse> kvStore) {
    Objects.requireNonNull(lr, "LocationRecord is required");
    Objects.requireNonNull(kvStore, "GeocodeService kvStore is required");

    // Take parsed values
    LatLng latLng = new LatLng(lr.getDecimalLatitude(), lr.getDecimalLongitude());

    // Use these to retrieve the GADM areas.
    // Check parameters
    if (latLng.getLatitude() == null || latLng.getLongitude() == null) {
      throw new IllegalArgumentException("Empty coordinates");
    }

    // Match to GADM administrative regions
    return getGadmFromCoordinates(latLng, kvStore);
  }

  private static Optional<GadmFeatures> getGadmFromCoordinates(
      LatLng latLng, KeyValueStore<LatLng, GeocodeResponse> kvStore) {
    if (latLng.isValid()) {
      GeocodeResponse geocodeResponse = kvStore.get(latLng);
      if (geocodeResponse != null && !geocodeResponse.getLocations().isEmpty()) {
        GadmFeatures gf = GadmFeatures.newBuilder().build();
        geocodeResponse.getLocations().forEach(l -> acceptGadm(l, gf));
        return Optional.of(gf);
      }
    }
    return Optional.empty();
  }

  private static void acceptGadm(Location l, GadmFeatures g) {
    if (l.getType() != null) {
      switch (l.getType()) {
        case "GADM0":
          if (g.getLevel0Gid() == null) {
            g.setLevel0Gid(l.getId());
            g.setLevel0Name(l.getName());
          }
          return;
        case "GADM1":
          if (g.getLevel1Gid() == null) {
            g.setLevel1Gid(l.getId());
            g.setLevel1Name(l.getName());
          }
          return;
        case "GADM2":
          if (g.getLevel2Gid() == null) {
            g.setLevel2Gid(l.getId());
            g.setLevel2Name(l.getName());
          }
          return;
        case "GADM3":
          if (g.getLevel3Gid() == null) {
            g.setLevel3Gid(l.getId());
            g.setLevel3Name(l.getName());
          }
          return;
        default:
      }
    }
  }
}
