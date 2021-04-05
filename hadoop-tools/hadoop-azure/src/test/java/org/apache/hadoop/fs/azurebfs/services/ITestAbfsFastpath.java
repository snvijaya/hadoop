/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class ITestAbfsFastpath
    extends org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest {

  protected static final int HUNDRED = 100;
  java.util.List<String> filesToUnregister = new java.util.ArrayList<String>();

  @Override
  public void setup() throws Exception {
    loadConfiguredFileSystem();
    super.setup();
  }

  @org.junit.After
  public void tearDown() throws Exception {
    super.teardown();
    java.util.Iterator<String> itr = filesToUnregister.iterator();
    while(itr.hasNext()) {
      org.apache.hadoop.fs.azurebfs.utils.AbfsTestUtils.unregisterMockFastpathAppend(itr.next());
    }
  }

  public ITestAbfsFastpath() throws Exception {
  }

  @org.junit.Test
  public void teste2e() throws Exception {
    int fileSize = ONE_MB;
    final org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem fs = getFileSystem();
    fs.getAbfsStore().disableMockedSoPath();
      String fileName = methodName.getMethodName();
      byte[] fileContent = getRandomBytesArray(fileSize);
      org.apache.hadoop.fs.Path testFilePath = createFileWithContent(fs, fileName, fileContent);
    org.apache.hadoop.fs.FSDataInputStream iStream = fs.open(testFilePath);
    try {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();
      iStream = new org.apache.hadoop.fs.FSDataInputStream(abfsInputStream);
      int length = fileSize;
        byte[] buffer = new byte[length];
        int bytesRead = iStream.read(buffer, 0, length);
        System.out.println("Total bytes read = " + bytesRead);
        System.out.println("Data read [first 10 bytes]= " + (new String(buffer)).substring(0, 10));
    } finally {
      iStream.close();
    }

    }

//  protected org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem getFileSystem(boolean readSmallFilesCompletely)
//      throws java.io.IOException {
//    final org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem fs = getFileSystem();
//    getAbfsStore(fs).getAbfsConfiguration()
//        .setReadSmallFilesCompletely(readSmallFilesCompletely);
//    return fs;
//  }
//
//  private org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem getFileSystem(boolean optimizeFooterRead,
//      boolean readSmallFileCompletely, int fileSize) throws java.io.IOException {
//    final org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem fs = getFileSystem();
//    getAbfsStore(fs).getAbfsConfiguration()
//        .setOptimizeFooterRead(optimizeFooterRead);
//    if (fileSize <= getAbfsStore(fs).getAbfsConfiguration()
//        .getReadBufferSize()) {
//      getAbfsStore(fs).getAbfsConfiguration()
//          .setReadSmallFilesCompletely(readSmallFileCompletely);
//    }
//    return fs;
//  }

  protected byte[] getRandomBytesArray(int length) {
    final byte[] b = new byte[length];
    new java.util.Random().nextBytes(b);
    return b;
  }

  protected org.apache.hadoop.fs.Path createFileWithContent(org.apache.hadoop.fs.FileSystem fs, String fileName,
      byte[] fileContent) throws java.io.IOException {
    org.apache.hadoop.fs.Path testFilePath = path(fileName);
    try (org.apache.hadoop.fs.FSDataOutputStream oStream = fs.create(testFilePath)) {
      oStream.write(fileContent);
      oStream.flush();
    }
    org.apache.hadoop.fs.azurebfs.utils.AbfsTestUtils.registerMockFastpathAppend(
        fileContent.length, testFilePath.getName(), fileContent, 0, fileContent.length);

    filesToUnregister.add(testFilePath.getName());
    return testFilePath;
  }
}
