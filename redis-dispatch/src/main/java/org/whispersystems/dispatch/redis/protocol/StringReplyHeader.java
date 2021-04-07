package org.whispersystems.dispatch.redis.protocol;

import java.io.IOException;

/**
 * 多行字符串以$符号开头，后跟字符串长度
 */
public class StringReplyHeader {

  private final int stringLength;

  public StringReplyHeader(String header) throws IOException {
    if (header == null || header.length() < 2 || header.charAt(0) != '$') {
      throw new IOException("Invalid string reply header: " + header);
    }

    try {
      this.stringLength = Integer.parseInt(header.substring(1));
    } catch (NumberFormatException e) {
      throw new IOException(e);
    }
  }

  public int getStringLength() {
    return stringLength;
  }
}
