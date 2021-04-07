/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Size;

public class DeprecatedPin {

  @JsonProperty
  @NotEmpty
  @Size(min=4, max=20)
  private String pin;

  public DeprecatedPin() {}

  @VisibleForTesting
  public DeprecatedPin(String pin) {
    this.pin = pin;
  }

  public String getPin() {
    return pin;
  }

}
