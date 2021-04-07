/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Size;

public class RegistrationLock {

  @JsonProperty
  @Size(min=64, max=64)
  @NotEmpty
  private String registrationLock;

  public RegistrationLock() {}

  @VisibleForTesting
  public RegistrationLock(String registrationLock) {
    this.registrationLock = registrationLock;
  }

  public String getRegistrationLock() {
    return registrationLock;
  }

}
