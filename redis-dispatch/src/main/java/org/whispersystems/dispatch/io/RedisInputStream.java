/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.dispatch.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class RedisInputStream {

  /**
   * \r
   */
  private static final byte CR = 0x0D;
  /**
   * \n
   */
  private static final byte LF = 0x0A;

  private final InputStream inputStream;

  public RedisInputStream(InputStream inputStream) {
    this.inputStream = inputStream;
  }

  /**
   * 通过解析连续的两个字节是否分别是 \r\n 来读取一行
   * @return
   * @throws IOException
   */
  public String readLine() throws IOException {
    ByteArrayOutputStream boas = new ByteArrayOutputStream();

    boolean foundCr = false;

    while (true) {
      int character = inputStream.read();

      if (character == -1) {
        throw new IOException("Stream closed!");
      }

      boas.write(character);

      if      (foundCr && character == LF) break;
      else if (character == CR)            foundCr = true;
      else if (foundCr)                    foundCr = false;
    }

    byte[] data = boas.toByteArray();
    return new String(data, 0, data.length-2);
  }

  /**
   * 读取指定长度的字节
   * @param size
   * @return
   * @throws IOException
   */
  public byte[] readFully(int size) throws IOException {
    byte[] result    = new byte[size];
    int    offset    = 0;
    int    remaining = result.length;

    while (remaining > 0) {
      int read = inputStream.read(result, offset, remaining);

      if (read < 0) {
        throw new IOException("Stream closed!");
      }

      offset    += read;
      remaining -= read;
    }

    return result;
  }

  public void close() throws IOException {
    inputStream.close();
  }

}
