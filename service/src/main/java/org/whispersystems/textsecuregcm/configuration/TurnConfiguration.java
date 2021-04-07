/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

public class TurnConfiguration {

  @JsonProperty
  @NotEmpty
  private String secret;

  @JsonProperty
  @NotNull
  private List<String> uris;

  public List<String> getUris() {
    return uris;
  }

  public String getSecret() {
    return secret;
  }
}
