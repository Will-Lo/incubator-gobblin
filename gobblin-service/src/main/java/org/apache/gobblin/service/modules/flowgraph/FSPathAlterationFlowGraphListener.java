package org.apache.gobblin.service.modules.flowgraph;

import com.google.common.base.Optional;
import java.net.URI;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.modules.template_catalog.FSFlowTemplateCatalog;
import org.apache.gobblin.util.filesystem.PathAlterationListener;
import org.apache.gobblin.util.filesystem.PathAlterationObserver;
import org.apache.hadoop.fs.Path;

@Slf4j
public class FSPathAlterationFlowGraphListener extends BaseFlowGraphListener implements PathAlterationListener {

  public FSPathAlterationFlowGraphListener(Optional<? extends FSFlowTemplateCatalog> flowTemplateCatalog,
      FlowGraph graph, Map<URI, TopologySpec> topologySpecMap, String baseDirectory, String flowGraphFolderName,
      String javaPropsExtentions, String hoconFileExtensions) {
    super(flowTemplateCatalog, graph, topologySpecMap, baseDirectory, flowGraphFolderName, javaPropsExtentions, hoconFileExtensions);

  }
  public void onStart(final PathAlterationObserver observer) {

  }

  public void onFileCreate(final Path path) {
    log.info("Detected flowgraph add at {}", path.toString());
    if (checkFileLevelRelativeToRoot(path, NODE_FILE_DEPTH)) {
      addDataNode(path.toString());
    } else if (checkFileLevelRelativeToRoot(path, EDGE_FILE_DEPTH)) {
      addFlowEdge(path.toString());
    }
  }

  public void onFileChange(final Path path) {
    log.info("Detected flowgraph change at {}", path.toString());
    onFileCreate(path);
  }

  public void onStop(final PathAlterationObserver observer) {
  }

  public void onDirectoryCreate(final Path directory) {
  }

  public void onDirectoryChange(final Path directory) {
  }

  public void onDirectoryDelete(final Path directory) {
  }

  public void onFileDelete(final Path path) {
    log.info("Detected flowgraph delete at {}", path.toString());
    if (checkFileLevelRelativeToRoot(path, NODE_FILE_DEPTH)) {
      removeDataNode(path.toString());
    } else if (checkFileLevelRelativeToRoot(path, EDGE_FILE_DEPTH)) {
      removeFlowEdge(path.toString());
    }
  }

}
