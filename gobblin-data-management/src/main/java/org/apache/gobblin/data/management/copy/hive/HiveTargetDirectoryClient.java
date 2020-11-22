package org.apache.gobblin.data.management.copy.hive;

import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.fs.Path;


public class HiveTargetDirectoryClient {

  public HiveTargetDirectoryClient(Properties properties){}

  public Path getOrCreateTargetPath(Path path) throws IOException {
    return path;
  }

}
