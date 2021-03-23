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

//todo remove
import com.microsoft.fastpath.AbfsDriverMockFastpathConnection;
import com.microsoft.fastpath.FastpathConnection;
import com.microsoft.fastpath.exceptions.FastpathException;
import com.microsoft.fastpath.requestParameters.AccessTokenType;
import com.microsoft.fastpath.requestParameters.FastpathCloseRequestParams;
import com.microsoft.fastpath.requestParameters.FastpathOpenRequestParams;
import com.microsoft.fastpath.requestParameters.FastpathReadRequestParams;
import com.microsoft.fastpath.responseProviders.FastpathCloseResponse;
import com.microsoft.fastpath.responseProviders.FastpathOpenResponse;
import com.microsoft.fastpath.responseProviders.FastpathReadResponse;
import com.microsoft.fastpath.responseProviders.FastpathResponse;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DEFAULT_TIMEOUT;
import static org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType.FastpathErrRead404;
import static org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType.FastpathErrRead500;
import static org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType.FastpathErrRead503;
import static org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType.FastpathErrOpen404;
import static org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType.FastpathErrOpen500;
import static org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType.FastpathErrClose500;
import static org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType.FastpathOpenNonMock;
import static org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType.FastpathReadNonMock;
import static org.apache.hadoop.fs.azurebfs.services.AuthType.OAuth;

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
    java.util.Map<String, java.util.List<String>> headers
        = new java.util.HashMap<String, java.util.List<String>>();
    for (AbfsHttpHeader abfsHeader : this.requestHeaders) {
      headers.put(abfsHeader.getName(),
          java.util.Collections.singletonList(abfsHeader.getValue()));
    }

    headers.put(
        org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID,
        java.util.Collections.singletonList(this.clientRequestId));

    return headers;
  }

  //public java.util.Map<String, java.util.List<String>> getRequestHeaders() {
  public String getRequestHeader(String header) {
    String value ="";
    for (AbfsHttpHeader abfsHeader : this.requestHeaders) {
      if (abfsHeader.getName().equals(header)) {
        value = abfsHeader.getValue();
        break;
      }
    }

    return value;
//      java.util.Map<String, java.util.List<String>> headers = new java.util.HashMap<String, java.util.List<String>>();
//      for(AbfsHttpHeader abfsHeader : this.requestHeaders) {
//        headers.put(abfsHeader.getName(),
//            java.util.Collections.singletonList(abfsHeader.getValue()));
//      }
//
//      headers.put(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID, java.util.Collections.singletonList(this.clientRequestId));
//
//      return headers;
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
     switch (this.opType) {
    case FastpathOpen:
     case FastpathErrOpen404:
     case FastpathErrOpen500:
     case FastpathOpenNonMock:
      long startTime = System.nanoTime();
      processFastpathOpenResponse();
      this.recvResponseTimeMs = elapsedTimeMs(startTime);
      break;
    case FastpathRead:
     case FastpathErrRead404:
     case FastpathErrRead503:
     case FastpathErrRead500:
     case FastpathReadNonMock:
      startTime = System.nanoTime();
      processFastpathReadResponse(buffer, offset, length);
      this.recvResponseTimeMs = elapsedTimeMs(startTime);
      break;
    case FastpathClose:
     case FastpathErrClose500:
     case FastpathCloseNonMock:
      startTime = System.nanoTime();
      processFastpathCloseResponse();
      this.recvResponseTimeMs = elapsedTimeMs(startTime);
      break;
    default:
      throw new FastpathException("Invalid state");
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
      throws FastpathException {
    if (authType == OAuth) {
      return AccessTokenType.AadBearer;
    }

    throw new FastpathException("Unsupported authType for Fastpath connection");
  }

  private void processFastpathOpenResponse() throws FastpathException {
    FastpathOpenRequestParams openRequestParams = new FastpathOpenRequestParams(
        url,
        getAccessTokenType(authType),
        authToken,
        getRequestHeaders(),
        defaultTimeout);
    //FastpathConnection conn = new FastpathConnection();
    AbfsDriverMockFastpathConnection conn = new AbfsDriverMockFastpathConnection();
    FastpathOpenResponse openResponse;

    if (opType == FastpathErrOpen404) {
      openResponse = conn.errOpen(openRequestParams, 404);
    } else if (opType == FastpathErrOpen500) {
      openResponse = conn.errOpen(openRequestParams, 500);
    } else if (opType == FastpathOpenNonMock) {
      FastpathConnection conna = new com.microsoft.fastpath.FastpathConnection();
      openResponse = conna.open(openRequestParams);
    } else {
      openResponse = conn.open(openRequestParams);
    }
    setStatusFromFastpathResponse(openResponse);
    if (openResponse.isSuccessResponse()) {
      this.fastpathFileHandle = openResponse.getFastpathFileHandle();
      System.out.println("Fast path open successful - handle received = " + this.fastpathFileHandle);
    }
  }

  private void processFastpathReadResponse(final byte[] buffer,
      final int buffOffset, final int length) throws FastpathException {
    FastpathReadRequestParams readRequestParams
        = new FastpathReadRequestParams(url, getAccessTokenType(authType),
        authToken, getRequestHeaders(),
        defaultTimeout, buffOffset, fastpathFileHandle);
    //FastpathConnection conn = new FastpathConnection();
    AbfsDriverMockFastpathConnection conn = new AbfsDriverMockFastpathConnection();
    FastpathReadResponse readResponse;
    if (opType == FastpathErrRead404) {
      readResponse = conn.errRead(readRequestParams, buffer, 404);
    } else if (opType == FastpathErrRead500) {
      readResponse = conn.errRead(readRequestParams, buffer, 500);
    } else if (opType == FastpathErrRead503) {
      readResponse = conn.errRead(readRequestParams, buffer, 503);
    } else if (opType == FastpathReadNonMock) {
      FastpathConnection conna = new com.microsoft.fastpath.FastpathConnection();
      readResponse = conna.read(readRequestParams, buffer);
    } else {
      readResponse = conn.read(readRequestParams, buffer);
    }

    setStatusFromFastpathResponse(readResponse);
    if (readResponse.isSuccessResponse()) {
      this.bytesReceived = readResponse.getBytesRead();
      System.out.println("Fast path open successful - bytes received = " + this.bytesReceived);
    }
  }

  private void processFastpathCloseResponse() throws FastpathException {
    FastpathCloseRequestParams closeRequestParams
        = new FastpathCloseRequestParams(url, getAccessTokenType(authType),
        authToken, getRequestHeaders(), defaultTimeout, fastpathFileHandle);
    //FastpathConnection conn = new FastpathConnection();
    AbfsDriverMockFastpathConnection conn = new AbfsDriverMockFastpathConnection();
    FastpathCloseResponse closeResponse;
    if (opType == FastpathErrClose500) {
      closeResponse = conn.errClose(closeRequestParams, 500);
    } else if (opType == FastpathOpenNonMock) {
      FastpathConnection conna = new com.microsoft.fastpath.FastpathConnection();
      closeResponse = conna.close(closeRequestParams);
    }else {
      closeResponse = conn.close(closeRequestParams);
    }

    setStatusFromFastpathResponse(closeResponse);

    if (closeResponse.isSuccessResponse()) {
      System.out.println("Fast path close successful");
    }
  }

  public static void registerAppend(String path, byte[] data, int offset, int length) {
    com.microsoft.fastpath.AbfsDriverMockFastpathConnection.registerAppend(path, data, offset, length);
  }

  public static void registerAppend(int fileSize, String path, byte[] data, int offset, int length) {
    com.microsoft.fastpath.AbfsDriverMockFastpathConnection.registerAppend(fileSize, path, data, offset, length);
  }
  public static void unregisterAppend(String path) {
    com.microsoft.fastpath.AbfsDriverMockFastpathConnection.unregisterAppendPath(path);
  }

}
