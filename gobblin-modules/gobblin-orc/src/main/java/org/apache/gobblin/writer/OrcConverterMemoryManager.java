package org.apache.gobblin.writer;

import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.ListColumnVector;
import org.apache.orc.storage.ql.exec.vector.MapColumnVector;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.UnionColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;


/**
 * A helper class to calculate the size of array buffers in a {@link VectorizedRowBatch}.
 * This estimate is mainly based on the maximum size of each variable length column, which can be resized
 * Since the resizing algorithm for each column can balloon, this can affect likelihood of OOM
 */
public class OrcConverterMemoryManager {

  private VectorizedRowBatch rowBatch;
  long converterBufferTotalSize;

  // TODO: Consider moving the resize algorithm from the converter to this class
  OrcConverterMemoryManager(VectorizedRowBatch rowBatch) {
    this.rowBatch = rowBatch;
  }

   // TODO: consider performing this calculation live whenever a resize is done
   private void calculateResizeSpaceOfArrayBuffers() {
    ColumnVector[] cols = this.rowBatch.cols;
    for (int i = 0; i < cols.length; i++) {
      calculateSizeOfColHelper(cols[i]);
    }
  }

  public void calculateSizeOfColHelper(ColumnVector col) {
    if (col instanceof ListColumnVector) {
      ListColumnVector listColumnVector = (ListColumnVector) col;
      converterBufferTotalSize += listColumnVector.child.isNull.length;
      calculateSizeOfColHelper(listColumnVector.child);
    } else if (col instanceof MapColumnVector) {
      MapColumnVector mapColumnVector = (MapColumnVector) col;
      converterBufferTotalSize += mapColumnVector.keys.isNull.length + mapColumnVector.values.isNull.length;
      calculateSizeOfColHelper(mapColumnVector.keys);
      calculateSizeOfColHelper(mapColumnVector.values);
    } else if (col instanceof StructColumnVector) {
      StructColumnVector structColumnVector = (StructColumnVector) col;
      for (int j = 0; j < structColumnVector.fields.length; j++) {
        calculateSizeOfColHelper(structColumnVector.fields[j]);
      }
    } else if (col instanceof UnionColumnVector) {
      UnionColumnVector unionColumnVector = (UnionColumnVector) col;
      for (int j = 0; j < unionColumnVector.fields.length; j++) {
        calculateSizeOfColHelper(unionColumnVector.fields[j]);
      }
    }
  }

  public long getConverterBufferTotalSize() {
    this.calculateResizeSpaceOfArrayBuffers();
    return converterBufferTotalSize;
  }

}
