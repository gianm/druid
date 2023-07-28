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

import java.io.IOException;

/**
 * Returned by {@link ServiceClient#asyncRequest(RequestBuilder, HttpResponseHandler)} when a request has failed.
 */
public class RpcException extends IOException
{
  public RpcException(String formatText, Object... arguments)
  {
    super(StringUtils.nonStrictFormat(formatText, arguments));
  }

  public RpcException(Throwable cause, String formatText, Object... arguments)
  {
    super(StringUtils.nonStrictFormat(formatText, arguments), cause);
  }
}
