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

import java.io.IOException;
import java.net.URL;
import java.util.List;

import com.microsoft.fastpath.FastpathConnection;
import com.microsoft.fastpath.requestParameters.AccessTokenType;
import com.microsoft.fastpath.requestParameters.FastpathCloseRequestParams;
import com.microsoft.fastpath.requestParameters.FastpathOpenRequestParams;
import com.microsoft.fastpath.requestParameters.FastpathReadRequestParams;
import com.microsoft.fastpath.responseProviders.FastpathOpenResponse;
import com.microsoft.fastpath.responseProviders.FastpathReadResponse;
import com.microsoft.fastpath.responseProviders.FastpathCloseResponse;
import com.microsoft.fastpath.responseProviders.FastpathResponse;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DEFAULT_TIMEOUT;
import static org.apache.hadoop.fs.azurebfs.services.AuthType.OAuth;

import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;

/**
 * Represents a Fastpath operation.
 */

// TODO - debugging - success + failure
public class AbfsFastpathConnection extends AbfsHttpOperation {

  String fastpathFileHandle;
  // should be configurable ?
  int defaultTimeout = Integer.valueOf(DEFAULT_TIMEOUT);
  FastpathResponse response = null;

  public String getFastpathFileHandle() {
    return fastpathFileHandle;
  }

  public AbfsFastpathConnection(final AbfsRestOperationType opType,
      final URL url,
      final String method,
      final AuthType authType,
      final String authToken,
      List<AbfsHttpHeader> requestHeaders,
      final String fastpathFileHandle) throws IOException {
    super(opType, url, method, authType, authToken, requestHeaders);
    this.authType = authType;
    this.authToken = authToken;
    this.fastpathFileHandle = fastpathFileHandle;
    this.requestHeaders = requestHeaders;
  }

  public String getResponseHeader(String httpHeader) {
    return response.getResponseHeaders().get(httpHeader);
  }

  public java.util.Map<String, java.util.List<String>> getRequestHeaders() {
      java.util.Map<String, java.util.List<String>> headers = new java.util.HashMap<String, java.util.List<String>>();
      for(AbfsHttpHeader abfsHeader : this.requestHeaders) {
        headers.put(abfsHeader.getName(),
            java.util.Collections.singletonList(abfsHeader.getValue()));
      }

      headers.put(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID, java.util.Collections.singletonList(this.clientRequestId));

      return headers;
  }

  /**
   * Gets and processes the HTTP response.
   *
   * @param buffer a buffer to hold the response entity body
   * @param offset an offset in the buffer where the data will being.
   * @param length the number of bytes to be written to the buffer.
   *
   * @throws IOException if an error occurs.
   */
  public void processResponse(byte[] buffer, final int offset, final int length) throws IOException {
    processFastpathReadRequest(buffer, offset, length);
  }

  private void processFastpathReadRequest(final byte[] buffer, final int buffOffset, final int length) throws IOException {
    switch (this.opType) {
    case FastpathOpen:
      long startTime = System.nanoTime();
      processFastpathOpenResponse();
      this.recvResponseTimeMs = elapsedTimeMs(startTime);
      break;
    case FastpathRead:
      startTime = System.nanoTime();
      processFastpathReadResponse(buffer, buffOffset, length);
      this.recvResponseTimeMs = elapsedTimeMs(startTime);
    case FastpathClose:
      startTime = System.nanoTime();
      processFastpathCloseResponse();
      this.recvResponseTimeMs = elapsedTimeMs(startTime);
    default:
      throw new IOException("Invalid state");
    }
  }

  private void setStatusFromFastpathResponse(FastpathResponse response) {
    this.response = response;
    this.statusCode = response.getHttpStatus();
    this.statusDescription = String.valueOf(response.getHttpStatus());
    this.storageErrorCode = String.valueOf(response.getStoreErrorCode());
    this.storageErrorMessage = response.getStoreErrorDescription();
  }

  private AccessTokenType getAccessTokenType(AuthType authType)
      throws java.io.IOException {
    if (authType == OAuth) {
      return AccessTokenType.AadBearer;
    }

    throw new java.io.IOException("Unsupported authType for Fastpath connection");
  }

  private void processFastpathOpenResponse() throws java.io.IOException {
    FastpathOpenRequestParams openRequestParams = new FastpathOpenRequestParams(
        url,
        getAccessTokenType(authType),
        authToken,
        getRequestHeaders(),
        defaultTimeout);
    FastpathConnection conn = new FastpathConnection();
    FastpathOpenResponse openResponse = conn.open(openRequestParams);
    setStatusFromFastpathResponse(openResponse);
    if (openResponse.isSuccessResponse()) {
      System.out.println(openResponse.getClientRequestId());
      System.out.println(openResponse.getRequestId());
      System.out.println(openResponse.getHandleKey());
      this.fastpathFileHandle = openResponse.getHandleKey();
    }
  }

  private void processFastpathReadResponse(final byte[] buffer,
      final int buffOffset, final int length) throws IOException {
    FastpathReadRequestParams readRequestParams
        = new FastpathReadRequestParams(url, getAccessTokenType(authType),
        authToken, getRequestHeaders(),
        defaultTimeout, buffOffset, fastpathFileHandle);
    FastpathConnection conn = new FastpathConnection();
    FastpathReadResponse readResponse = conn.read(readRequestParams, buffer);
    setStatusFromFastpathResponse(readResponse);
    if (readResponse.isSuccessResponse()) {
      this.bytesReceived = readResponse.getBytesRead();
      System.out.println(readResponse.getClientRequestId());
      System.out.println(readResponse.getRequestId());
      System.out.println("AbfsHttpOperation:: buffer = " + new String(buffer));
    }
  }

  private void processFastpathCloseResponse() throws java.io.IOException {
    FastpathCloseRequestParams closeRequestParams
        = new FastpathCloseRequestParams(url, getAccessTokenType(authType),
        authToken, getRequestHeaders(), defaultTimeout, fastpathFileHandle);
    FastpathConnection conn = new FastpathConnection();
    FastpathCloseResponse closeResponse = conn.close(closeRequestParams);
    setStatusFromFastpathResponse(closeResponse);
  }
}
