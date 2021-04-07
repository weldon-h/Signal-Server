/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

public class InvalidDestinationException extends Exception {
  public InvalidDestinationException(String message) {
    super(message);
  }
}
