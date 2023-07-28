/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.rpc;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;

import javax.annotation.Nullable;

/**
 * Returned by {@link ServiceClient#asyncRequest(RequestBuilder, HttpResponseHandler)} when a request has failed due
 * to an HTTP response.
 */
public class HttpResponseException extends RpcException
{
  private final StringFullResponseHolder responseHolder;

  public HttpResponseException(final StringFullResponseHolder responseHolder)
  {
    super(
        "Server error [%s]; %s",
        responseHolder.getStatus(),
        choppedBodyErrorMessage(responseHolder.getContent())
    );

    this.responseHolder = responseHolder;
  }

  public StringFullResponseHolder getResponse()
  {
    return responseHolder;
  }

  static String choppedBodyErrorMessage(@Nullable final String responseContent)
  {
    if (responseContent == null || responseContent.isEmpty()) {
      return "no body";
    } else if (responseContent.length() > 1000) {
      final String choppedMessage = StringUtils.chop(responseContent, 1000);
      return "first 1KB of body: " + choppedMessage;
    } else {
      return "body: " + responseContent;
    }
  }
}
