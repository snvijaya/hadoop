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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.UUID;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsPerfLoggable;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;

public class AbfsHttpConnection extends AbfsHttpOperation {

  private HttpURLConnection connection;
  protected ListResultSchema listResultSchema = null;

  public  HttpURLConnection getConnection() {
    return connection;
  }

  public ListResultSchema getListResultSchema() {
    return listResultSchema;
  }

  public String getResponseHeader(String httpHeader) {
    return connection.getHeaderField(httpHeader);
  }

  public AbfsHttpConnection(final URL url, final String method, List<AbfsHttpHeader> requestHeaders) throws IOException {
    super(url, method, requestHeaders);
    init(url, method, requestHeaders);
  }

  /**
   * Initializes a new HTTP request and opens the connection.
   *
   * @param url The full URL including query string parameters.
   * @param method The HTTP method (PUT, PATCH, POST, GET, HEAD, or DELETE).
   * @param requestHeaders The HTTP request headers.READ_TIMEOUT
   *
   * @throws IOException if an error occurs.
   */
  public void init(final URL url, final String method, List<AbfsHttpHeader> requestHeaders)
      throws IOException {
    this.connection = openConnection();
    if (this.connection instanceof HttpsURLConnection) {
      HttpsURLConnection secureConn = (HttpsURLConnection) this.connection;
      SSLSocketFactory sslSocketFactory = DelegatingSSLSocketFactory.getDefaultFactory();
      if (sslSocketFactory != null) {
        secureConn.setSSLSocketFactory(sslSocketFactory);
      }
    }

    this.connection.setConnectTimeout(CONNECT_TIMEOUT);
    this.connection.setReadTimeout(READ_TIMEOUT);

    this.connection.setRequestMethod(method);

    for (AbfsHttpHeader header : requestHeaders) {
      this.connection.setRequestProperty(header.getName(), header.getValue());
    }

    this.connection.setRequestProperty(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID, clientRequestId);
  }

  public java.util.Map<String, java.util.List<String>> getRequestHeaders() {
  return getConnection().getRequestProperties();
  }

  public String getRequestHeader(String header) {
      return getConnection().getRequestProperty(header);
  }

  /**
   * Sends the HTTP request.  Note that HttpUrlConnection requires that an
   * empty buffer be sent in order to set the "Content-Length: 0" header, which
   * is required by our endpoint.
   *
   * @param buffer the request entity body.
   * @param offset an offset into the buffer where the data beings.
   * @param length the length of the data in the buffer.
   *
   * @throws IOException if an error occurs.
   */
  public void sendRequest(byte[] buffer, int offset, int length) throws IOException {
    this.connection.setDoOutput(true);
    this.connection.setFixedLengthStreamingMode(length);
    if (buffer == null) {
      // An empty buffer is sent to set the "Content-Length: 0" header, which
      // is required by our endpoint.
      buffer = new byte[]{};
      offset = 0;
      length = 0;
    }

    // send the request body

    long startTime = 0;
    if (this.isTraceEnabled) {
      startTime = System.nanoTime();
    }
    try (OutputStream outputStream = this.connection.getOutputStream()) {
      // update bytes sent before they are sent so we may observe
      // attempted sends as well as successful sends via the
      // accompanying statusCode
      this.bytesSent = length;
      outputStream.write(buffer, offset, length);
    } finally {
      if (this.isTraceEnabled) {
        this.sendRequestTimeMs = elapsedTimeMs(startTime);
      }
    }
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
    // get the response
    long startTime = 0;
    if (this.isTraceEnabled) {
      startTime = System.nanoTime();
    }

    this.statusCode = this.connection.getResponseCode();

    if (this.isTraceEnabled) {
      this.recvResponseTimeMs = elapsedTimeMs(startTime);
    }

    this.statusDescription = this.connection.getResponseMessage();

    this.requestId = this.connection.getHeaderField(HttpHeaderConfigurations.X_MS_REQUEST_ID);
    if (this.requestId == null) {
      this.requestId = AbfsHttpConstants.EMPTY_STRING;
    }
    // dump the headers
    AbfsIoUtils.dumpHeadersToDebugLog("Response Headers",
        connection.getHeaderFields());

    if (AbfsHttpConstants.HTTP_METHOD_HEAD.equals(this.method)) {
      // If it is HEAD, and it is ERROR
      return;
    }

    if (this.isTraceEnabled) {
      startTime = System.nanoTime();
    }

    if (statusCode >= HttpURLConnection.HTTP_BAD_REQUEST) {
      processStorageErrorResponse();
      if (this.isTraceEnabled) {
        this.recvResponseTimeMs += elapsedTimeMs(startTime);
      }
      this.bytesReceived = this.connection.getHeaderFieldLong(HttpHeaderConfigurations.CONTENT_LENGTH, 0);
    } else {
      // consume the input stream to release resources
      int totalBytesRead = 0;

      try (InputStream stream = this.connection.getInputStream()) {
        if (isNullInputStream(stream)) {
          return;
        }
        boolean endOfStream = false;

        // this is a list operation and need to retrieve the data
        // need a better solution
        if (AbfsHttpConstants.HTTP_METHOD_GET.equals(this.method) && buffer == null) {
          parseListFilesResponse(stream);
        } else {
          if (buffer != null) {
            while (totalBytesRead < length) {
              int bytesRead = stream.read(buffer, offset + totalBytesRead, length - totalBytesRead);
              if (bytesRead == -1) {
                endOfStream = true;
                break;
              }
              totalBytesRead += bytesRead;
            }
          }
          if (!endOfStream && stream.read() != -1) {
            // read and discard
            int bytesRead = 0;
            byte[] b = new byte[CLEAN_UP_BUFFER_SIZE];
            while ((bytesRead = stream.read(b)) >= 0) {
              totalBytesRead += bytesRead;
            }
          }
        }
      } catch (IOException ex) {
        LOG.error("UnexpectedError: ", ex);
        throw ex;
      } finally {
        if (this.isTraceEnabled) {
          this.recvResponseTimeMs += elapsedTimeMs(startTime);
        }
        this.bytesReceived = totalBytesRead;
      }
    }
  }

  /**
   * Open the HTTP connection.
   *
   * @throws IOException if an error occurs.
   */
  private HttpURLConnection openConnection() throws IOException {
    if (!isTraceEnabled) {
      return (HttpURLConnection) url.openConnection();
    }
    long start = System.nanoTime();
    try {
      return (HttpURLConnection) url.openConnection();
    } finally {
      connectionTimeMs = elapsedTimeMs(start);
    }
  }

  /**
   * When the request fails, this function is used to parse the responseAbfsHttpClient.LOG.debug("ExpectedError: ", ex);
   * and extract the storageErrorCode and storageErrorMessage.  Any errors
   * encountered while attempting to process the error response are logged,
   * but otherwise ignored.
   *
   * For storage errors, the response body *usually* has the following format:
   *
   * {
   *   "error":
   *   {
   *     "code": "string",
   *     "message": "string"
   *   }
   * }
   *
   */
  private void processStorageErrorResponse() {
    try (InputStream stream = connection.getErrorStream()) {
      if (stream == null) {
        return;
      }
      JsonFactory jf = new JsonFactory();
      try (JsonParser jp = jf.createJsonParser(stream)) {
        String fieldName, fieldValue;
        jp.nextToken();  // START_OBJECT - {
        jp.nextToken();  // FIELD_NAME - "error":
        jp.nextToken();  // START_OBJECT - {
        jp.nextToken();
        while (jp.hasCurrentToken()) {
          if (jp.getCurrentToken() == JsonToken.FIELD_NAME) {
            fieldName = jp.getCurrentName();
            jp.nextToken();
            fieldValue = jp.getText();
            switch (fieldName) {
            case "code":
              storageErrorCode = fieldValue;
              break;
            case "message":
              storageErrorMessage = fieldValue;
              break;
            case "ExpectedAppendPos":
              expectedAppendPos = fieldValue;
              break;
            default:
              break;
            }
          }
          jp.nextToken();
        }
      }
    } catch (IOException ex) {
      // Ignore errors that occur while attempting to parse the storage
      // error, since the response may have been handled by the HTTP driver
      // or for other reasons have an unexpected
      LOG.debug("ExpectedError: ", ex);
    }
  }

  /**
   * Parse the list file response
   *
   * @param stream InputStream contains the list results.
   * @throws IOException
   */
  private void parseListFilesResponse(final InputStream stream) throws IOException {
    if (stream == null) {
      return;
    }

    if (listResultSchema != null) {
      // already parse the response
      return;
    }

    try {
      final ObjectMapper objectMapper = new ObjectMapper();
      this.listResultSchema = objectMapper.readValue(stream, ListResultSchema.class);
    } catch (IOException ex) {
      LOG.error("Unable to deserialize list results", ex);
      throw ex;
    }
  }
}