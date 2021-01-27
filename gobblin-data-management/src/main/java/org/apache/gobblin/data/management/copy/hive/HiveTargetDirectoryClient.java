package org.apache.gobblin.data.management.copy.hive;

import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.fs.Path;


/**
 * Determines and manages target paths for {@link HiveCopyEntityHelper}
 * An example implementation would create target directories on cloud environments before initiating the copy
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

  /**
   * Deletes references to the path from the directory client
   * @param path path on target directory
   * @throws IOException if deleting the path fails
   */
  public void deletePath(Path path) throws IOException {
    return;
  }
}

