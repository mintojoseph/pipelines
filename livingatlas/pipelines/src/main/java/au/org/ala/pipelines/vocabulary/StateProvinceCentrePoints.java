package au.org.ala.pipelines.vocabulary;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.LocationInfoConfig;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.Strings;

/** Load centres of stateProvince from resources */
@Slf4j
public class StateProvinceCentrePoints {

  private static String classpathFile = "/stateProvinceCentrePoints.txt";
  private static CentrePoints cp;

  public static CentrePoints getInstance(LocationInfoConfig config) throws FileNotFoundException {
    if (cp == null) {
      InputStream is;
      if (config != null) {
        String externalFilePath = config.getStateProvinceCentrePointsFile();
        if (Strings.isNullOrEmpty(externalFilePath)) {
          is = CentrePoints.class.getResourceAsStream(classpathFile);
        } else {
          File externalFile = new File(externalFilePath);
          is = new FileInputStream(externalFile);
        }
      } else {
        is = CentrePoints.class.getResourceAsStream(classpathFile);
      }
      cp = CentrePoints.getInstance(is);
      log.info("We found {} state centres", cp.size());
    }
    return cp;
  }

  public static void main(String[] args){
    ALAPipelinesConfig alaConfig = new ALAPipelinesConfig();
    alaConfig.setLocationInfoConfig(new LocationInfoConfig());

    for(int i=0; i< 100; i++){
      Thread newThread = new Thread(() -> {
        try {
          //assertEquals(6, StateProvinceCentrePoints.getInstance(alaConfig.getLocationInfoConfig()).size()) ;
          System.out.println(StateProvinceCentrePoints.getInstance(alaConfig.getLocationInfoConfig()).size());
        }catch(Exception e){
          e.printStackTrace();
        }
      });
      newThread.start();
    }


  }
}
