/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.data.management.copy.hive;

import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.fs.Path;


/**
 * Ensure that target paths which are sharded will exist before copying files
 * An example would be to create target directories on cloud environments before initiating the copy
 * By default, returns the input path (do nothing)
 */
public interface ShardDirectoryClient {

  /**
   * Creates a target directory path if it does not exist, otherwise fetch its path
   * @param path initial target path
   * @return the path after creating the target directory
   * @throws IOException if creating or getting the path fails
   */
   Path getOrCreateTargetPath(final Path path) throws IOException;

   void close();
}

