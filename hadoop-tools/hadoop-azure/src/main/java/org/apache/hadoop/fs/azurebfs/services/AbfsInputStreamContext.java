/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

/**
 * Class to hold extra input stream configs.
 */
public class AbfsInputStreamContext extends AbfsStreamContext {

  private int readBufferSize;

  private int readAheadQueueDepth;

  private boolean tolerateOobAppends;

  private boolean alwaysReadBufferSize;

  private int readAheadBlockSize;

  private int readAheadBufferCount;

  private boolean enableReadAheadForRandomRead;

  private int readAheadQueueDepthForRandomRead;

  private AbfsInputStreamStatistics streamStatistics;

  public AbfsInputStreamContext(final long sasTokenRenewPeriodForStreamsInSeconds) {
    super(sasTokenRenewPeriodForStreamsInSeconds);
  }

  public AbfsInputStreamContext withReadBufferSize(final int readBufferSize) {
    this.readBufferSize = readBufferSize;
    return this;
  }

  public AbfsInputStreamContext withReadAheadQueueDepth(
          final int readAheadQueueDepth) {
    this.readAheadQueueDepth = (readAheadQueueDepth >= 0)
            ? readAheadQueueDepth
            : Runtime.getRuntime().availableProcessors();
    return this;
  }

  public AbfsInputStreamContext withTolerateOobAppends(
          final boolean tolerateOobAppends) {
    this.tolerateOobAppends = tolerateOobAppends;
    return this;
  }

  public AbfsInputStreamContext withStreamStatistics(
      final AbfsInputStreamStatistics streamStatistics) {
    this.streamStatistics = streamStatistics;
    return this;
  }

  public AbfsInputStreamContext withShouldReadBufferSizeAlways(
      final boolean alwaysReadBufferSize) {
    this.alwaysReadBufferSize = alwaysReadBufferSize;
    return this;
  }

  public AbfsInputStreamContext withReadAheadBlockSize(
      final int readAheadBlockSize) {
    this.readAheadBlockSize = readAheadBlockSize;
    return this;
  }

  public AbfsInputStreamContext withReadAheadBufferCount(
      final int readAheadBufferCount) {
    this.readAheadBufferCount = readAheadBufferCount;
    return this;
  }

  public AbfsInputStreamContext withIsReadAheadEnabledForRandomRead(
      final boolean enableReadAheadForRandomRead) {
    this.enableReadAheadForRandomRead = enableReadAheadForRandomRead;
    return this;
  }

  public AbfsInputStreamContext withReadAheadQueueDepthForRandomRead(
      final int readAheadQueueDepthForRandomRead) {
    this.readAheadQueueDepthForRandomRead = readAheadQueueDepthForRandomRead;
    return this;
  }

  public AbfsInputStreamContext build() {
    // Validation of parameters to be done here.
    return this;
  }

  public int getReadBufferSize() {
    return readBufferSize;
  }

  public int getReadAheadQueueDepth() {
    return readAheadQueueDepth;
  }

  public boolean isTolerateOobAppends() {
    return tolerateOobAppends;
  }

  public AbfsInputStreamStatistics getStreamStatistics() {
    return streamStatistics;
  }

  public boolean shouldReadBufferSizeAlways() {
    return alwaysReadBufferSize;
  }

  public int getReadAheadBlockSize() {
    return readAheadBlockSize;
  }

  public int getReadAheadBufferCount() {
    return readAheadBufferCount;
  }

  public boolean isReadAheadEnabledForRandomRead() {
    return enableReadAheadForRandomRead;
  }

  public int getReadAheadQueueDepthForRandomRead() {
    return readAheadQueueDepthForRandomRead;
  }

}
