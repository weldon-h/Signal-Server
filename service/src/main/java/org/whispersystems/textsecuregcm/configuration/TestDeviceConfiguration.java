/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

public class TestDeviceConfiguration {

  @JsonProperty
  @NotEmpty
  private String number;

  @JsonProperty
  @NotNull
  private int code;

  public String getNumber() {
    return number;
  }

  public int getCode() {
    return code;
  }
}
