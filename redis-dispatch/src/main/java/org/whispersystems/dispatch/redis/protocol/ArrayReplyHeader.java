/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.dispatch.redis.protocol;

import java.io.IOException;

/**
 * 数组以*符号开头，后跟数组长度
 */
public class ArrayReplyHeader {

  private final int elementCount;

  public ArrayReplyHeader(String header) throws IOException {
    if (header == null || header.length() < 2 || header.charAt(0) != '*') {
      throw new IOException("Invalid array reply header: " + header);
    }

    try {
      this.elementCount = Integer.parseInt(header.substring(1));
    } catch (NumberFormatException e) {
      throw new IOException(e);
    }
  }

  public int getElementCount() {
    return elementCount;
  }
}
