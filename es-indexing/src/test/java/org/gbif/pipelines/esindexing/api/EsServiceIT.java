package org.gbif.pipelines.esindexing.api;

import org.gbif.pipelines.esindexing.EsIntegrationTest;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import org.elasticsearch.client.Response;
import org.junit.After;
import org.junit.Test;

import static org.gbif.pipelines.esindexing.common.EsConstants.MAPPINGS_FIELD;
import static org.gbif.pipelines.esindexing.common.SettingsType.INDEXING;
import static org.gbif.pipelines.esindexing.common.SettingsType.SEARCH;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link EsService}.
 */
public class EsServiceIT extends EsIntegrationTest {

  private static final String ALIAS_TEST = "alias";

  @After
  public void cleanIndexes() {
    deleteAllIndexes();
  }

  @Test
  public void createIndexWithSettingsAndMappingsTest() {
    String idx = EsService.createIndex(esServer.getEsClient(), "idx-settings", INDEXING, Paths.get(TEST_MAPPINGS_PATH));

    // check that the index was created as expected
    Response response = assertCreatedIndex(idx);

    // check settings
    assertIndexingSettings(response, idx);

    // check mappings
    JsonNode mappings = getMappingsFromIndex(idx).path(idx).path(MAPPINGS_FIELD);
    assertTrue(mappings.has("doc"));
    assertTrue(mappings.path("doc").path("properties").has("test"));
    assertEquals("text", mappings.path("doc").path("properties").path("test").get("type").asText());
  }

  @Test
  public void createAndUpdateIndexWithSettingsTest() {
    String idx = EsService.createIndex(esServer.getEsClient(), "idx-settings", INDEXING);

    // check that the index was created as expected
    Response response = assertCreatedIndex(idx);

    // check settings
    assertIndexingSettings(response, idx);

    EsService.updateIndexSettings(esServer.getEsClient(), idx, SEARCH);

    // check that the index was updated as expected
    response = assertCreatedIndex(idx);

    // check settings
    assertSearchSettings(response, idx);
  }

  @Test(expected = IllegalStateException.class)
  public void updateMissingIndex() {
    EsService.updateIndexSettings(esServer.getEsClient(), "fake-index", INDEXING);
  }

  @Test(expected = IllegalStateException.class)
  public void createWrongIndexTest() {
    EsService.createIndex(esServer.getEsClient(), "UPPERCASE", INDEXING);
  }

  @Test
  public void getIndexesByAliasAndSwapIndexTest() {
    // create some indexes to test
    String idx1 = EsService.createIndex(esServer.getEsClient(), "idx1", INDEXING);
    String idx2 = EsService.createIndex(esServer.getEsClient(), "idx2", INDEXING);
    String idx3 = EsService.createIndex(esServer.getEsClient(), "idx3", INDEXING);
    Set<String> initialIndexes = new HashSet<>(Arrays.asList(idx1, idx2, idx3));

    // there shouldn't be indexes before we start
    Set<String> indexes = EsService.getIndexesByAliasAndIndexPattern(esServer.getEsClient(), "idx*", ALIAS_TEST);
    assertEquals(0, indexes.size());

    // add them to the same alias
    addIndexToAlias(ALIAS_TEST, initialIndexes);

    // get the indexes of the alias
    indexes = EsService.getIndexesByAliasAndIndexPattern(esServer.getEsClient(), "idx*", ALIAS_TEST);

    // assert conditions
    assertEquals(3, indexes.size());
    assertTrue(indexes.containsAll(initialIndexes));

    // create a new index and swap it to the alias
    String idx4 = EsService.createIndex(esServer.getEsClient(), "idx4", INDEXING);
    EsService.swapIndexes(esServer.getEsClient(), ALIAS_TEST, Collections.singleton(idx4), initialIndexes);
    assertSwapResults(idx4, "idx*", ALIAS_TEST, initialIndexes);

    // repeat previous step with a new index
    String idx5 = EsService.createIndex(esServer.getEsClient(), "idx5", INDEXING);
    EsService.swapIndexes(esServer.getEsClient(), ALIAS_TEST, Collections.singleton(idx5), Collections.singleton(idx4));
    assertSwapResults(idx5, "idx*", ALIAS_TEST, initialIndexes);
  }

  @Test
  public void getIndexesFromMissingAlias() {
    Set<String> idx = EsService.getIndexesByAliasAndIndexPattern(esServer.getEsClient(), "idx*", "fake-alias");
    assertTrue(idx.isEmpty());
  }

  @Test
  public void swapEmptyAliasTest() {
    String idx1 = EsService.createIndex(esServer.getEsClient(), "idx1", INDEXING);
    EsService.swapIndexes(esServer.getEsClient(), ALIAS_TEST, Collections.singleton(idx1), Collections.emptySet());
    assertSwapResults(idx1, "idx*", ALIAS_TEST, Collections.emptySet());
  }

  @Test(expected = IllegalStateException.class)
  public void swapMissingIndexTest() {
    EsService.swapIndexes(esServer.getEsClient(),
                          "fake-alias",
                          Collections.singleton("fake-index"),
                          Collections.emptySet());
  }

}
