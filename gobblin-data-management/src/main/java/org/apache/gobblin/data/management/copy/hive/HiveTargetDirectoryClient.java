package org.apache.gobblin.data.management.copy.hive;

import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.fs.Path;


/**
 * Determines a target path for {@link HiveCopyEntityHelper}, and can be extended to ensure that target paths exist before copying files
 * An example would be to create target directories on cloud environments before initiating the copy
 * By default, returns the input path (do nothing)
 */
public class HiveTargetDirectoryClient {

  public HiveTargetDirectoryClient(Properties properties){}

  /**
   * Creates a target directory path if it does not exist, otherwise fetch its path
   * @param path initial target path
   * @return the path after creating the target directory
   * @throws IOException if creating or getting the path fails
   */
  public Path getOrCreateTargetPath(Path path) throws IOException {
    return path;
  }
}

