/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.sms;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Locale.LanguageRange;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.TwilioConfiguration;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.http.FormDataBodyPublisher;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.Base64;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.ExecutorUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.Util;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class TwilioSmsSender {
  private static final Logger logger = LoggerFactory.getLogger(TwilioSmsSender.class);

  private final MetricRegistry metricRegistry  = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Meter          smsMeter        = metricRegistry.meter(name(getClass(), "sms", "delivered"));
  private final Meter          voxMeter        = metricRegistry.meter(name(getClass(), "vox", "delivered"));
  private final Meter          priceMeter      = metricRegistry.meter(name(getClass(), "price"));

  private static final String FAILED_REQUEST_COUNTER_NAME = name(TwilioSmsSender.class, "failedRequest");
  private static final String STATUS_CODE_TAG_NAME = "statusCode";
  private static final String ERROR_CODE_TAG_NAME = "errorCode";

  private final String            accountId;
  private final String            accountToken;
  private final String            messagingServiceSid;
  private final String            nanpaMessagingServiceSid;
  private final String            localDomain;
  private final Random            random;
  private final String            androidNgVerificationText;
  private final String            android202001VerificationText;
  private final String            android202103VerificationText;
  private final String            iosVerificationText;
  private final String            genericVerificationText;

  private final FaultTolerantHttpClient httpClient;
  private final URI                     smsUri;
  private final URI                     voxUri;

  private final DynamicConfigurationManager dynamicConfigurationManager;

  @VisibleForTesting
  public TwilioSmsSender(String baseUri, TwilioConfiguration twilioConfiguration, DynamicConfigurationManager dynamicConfigurationManager) {
    Executor executor = ExecutorUtils.newFixedThreadBoundedQueueExecutor(10, 100);

    this.accountId                     = twilioConfiguration.getAccountId();
    this.accountToken                  = twilioConfiguration.getAccountToken();
    this.localDomain                   = twilioConfiguration.getLocalDomain();
    this.messagingServiceSid           = twilioConfiguration.getMessagingServiceSid();
    this.nanpaMessagingServiceSid      = twilioConfiguration.getNanpaMessagingServiceSid();
    this.random                        = new Random(System.currentTimeMillis());
    this.androidNgVerificationText     = twilioConfiguration.getAndroidNgVerificationText();
    this.android202001VerificationText = twilioConfiguration.getAndroid202001VerificationText();
    this.android202103VerificationText = twilioConfiguration.getAndroid202103VerificationText();
    this.iosVerificationText           = twilioConfiguration.getIosVerificationText();
    this.genericVerificationText       = twilioConfiguration.getGenericVerificationText();
    this.smsUri                        = URI.create(baseUri + "/2010-04-01/Accounts/" + accountId + "/Messages.json");
    this.voxUri                        = URI.create(baseUri + "/2010-04-01/Accounts/" + accountId + "/Calls.json"   );
    this.httpClient                    = FaultTolerantHttpClient.newBuilder()
                                                                .withCircuitBreaker(twilioConfiguration.getCircuitBreaker())
                                                                .withRetry(twilioConfiguration.getRetry())
                                                                .withVersion(HttpClient.Version.HTTP_2)
                                                                .withConnectTimeout(Duration.ofSeconds(10))
                                                                .withRedirect(HttpClient.Redirect.NEVER)
                                                                .withExecutor(executor)
                                                                .withName("twilio")
                                                                .build();
    this.dynamicConfigurationManager   = dynamicConfigurationManager;
  }

  public TwilioSmsSender(TwilioConfiguration twilioConfiguration, DynamicConfigurationManager dynamicConfigurationManager) {
      this("https://api.twilio.com", twilioConfiguration, dynamicConfigurationManager);
  }

  public CompletableFuture<Boolean> deliverSmsVerification(String destination, Optional<String> clientType, String verificationCode) {
    Map<String, String> requestParameters = new HashMap<>();
    requestParameters.put("To", destination);
    requestParameters.put("MessagingServiceSid", "1".equals(Util.getCountryCode(destination)) ? nanpaMessagingServiceSid : messagingServiceSid);
    requestParameters.put("Body", String.format(Locale.US, getBodyFormatString(destination, clientType.orElse(null)), verificationCode));

    HttpRequest request = HttpRequest.newBuilder()
                                     .uri(smsUri)
                                     .POST(FormDataBodyPublisher.of(requestParameters))
                                     .header("Content-Type", "application/x-www-form-urlencoded")
                                     .header("Authorization", "Basic " + Base64.encodeBytes((accountId + ":" + accountToken).getBytes(StandardCharsets.UTF_8)))
                                     .build();

    smsMeter.mark();

    return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                     .thenApply(this::parseResponse).handle(this::processResponse);
  }

  private String getBodyFormatString(@Nonnull String destination, @Nullable String clientType) {
    final String result;
    if ("ios".equals(clientType)) {
      result = iosVerificationText;
    } else if ("android-ng".equals(clientType)) {
      result = androidNgVerificationText;
    } else if ("android-2020-01".equals(clientType)) {
      result = android202001VerificationText;
    } else if ("android-2021-03".equals(clientType)) {
      result = android202103VerificationText;
    } else {
      result = genericVerificationText;
    }
    if (destination.startsWith("+86")) {  // is China
      return result + "\u2008";
      // Twilio recommends adding this character to the end of strings delivered to China because some carriers in
      // China are blocking GSM-7 encoding and this will force Twilio to send using UCS-2 instead.
    } else {
      return result;
    }
  }

  public CompletableFuture<Boolean> deliverVoxVerification(String destination, String verificationCode, List<LanguageRange> languageRanges) {
    String url = "https://" + localDomain + "/v1/voice/description/" + verificationCode;

    final String languageQueryParams = languageRanges.stream()
        .map(range -> Locale.forLanguageTag(range.getRange()))
        .map(locale -> {
          if (StringUtils.isNotBlank(locale.getCountry())) {
            return locale.getLanguage().toLowerCase() + "-" + locale.getCountry().toUpperCase();
          } else {
            return locale.getLanguage().toLowerCase();
          }
        })
        .map(languageTag -> "l=" + languageTag)
        .collect(Collectors.joining("&"));

    if (StringUtils.isNotBlank(languageQueryParams)) {
      url += "?" + languageQueryParams;
    }

    Map<String, String> requestParameters = new HashMap<>();
    requestParameters.put("Url", url);
    requestParameters.put("To", destination);
    requestParameters.put("From", getRandom(random, dynamicConfigurationManager.getConfiguration().getTwilioConfiguration().getNumbers()));

    HttpRequest request = HttpRequest.newBuilder()
                                     .uri(voxUri)
                                     .POST(FormDataBodyPublisher.of(requestParameters))
                                     .header("Content-Type", "application/x-www-form-urlencoded")
                                     .header("Authorization", "Basic " + Base64.encodeBytes((accountId + ":" + accountToken).getBytes()))
                                     .build();

    voxMeter.mark();

    return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                     .thenApply(this::parseResponse)
                     .handle(this::processResponse);
  }

  private String getRandom(Random random, List<String> elements) {
    return elements.get(random.nextInt(elements.size()));
  }

  private boolean processResponse(TwilioResponse response, Throwable throwable) {
    if (response != null && response.isSuccess()) {
      priceMeter.mark((long) (response.successResponse.price * 1000));
      return true;
    } else if (response != null && response.isFailure()) {
      logger.debug("Twilio request failed: " + response.failureResponse.status + "(code " + response.failureResponse.code + "), " + response.failureResponse.message);
      Metrics.counter(FAILED_REQUEST_COUNTER_NAME, STATUS_CODE_TAG_NAME, String.valueOf(response.failureResponse.status), ERROR_CODE_TAG_NAME, String.valueOf(response.failureResponse.code)).increment();
      return false;
    } else if (throwable != null) {
      logger.info("Twilio request failed", throwable);
      return false;
    } else {
      logger.warn("No response or throwable!");
      return false;
    }
  }

  private TwilioResponse parseResponse(HttpResponse<String> response) {
    ObjectMapper mapper = SystemMapper.getMapper();

    if (response.statusCode() >= 200 && response.statusCode() < 300) {
      if ("application/json".equals(response.headers().firstValue("Content-Type").orElse(null))) {
        return new TwilioResponse(TwilioResponse.TwilioSuccessResponse.fromBody(mapper, response.body()));
      } else {
        return new TwilioResponse(new TwilioResponse.TwilioSuccessResponse());
      }
    }

    if ("application/json".equals(response.headers().firstValue("Content-Type").orElse(null))) {
      return new TwilioResponse(TwilioResponse.TwilioFailureResponse.fromBody(mapper, response.body()));
    } else {
      return new TwilioResponse(new TwilioResponse.TwilioFailureResponse());
    }
  }

  public static class TwilioResponse {

    private TwilioSuccessResponse successResponse;
    private TwilioFailureResponse failureResponse;

    TwilioResponse(TwilioSuccessResponse successResponse) {
      this.successResponse = successResponse;
    }

    TwilioResponse(TwilioFailureResponse failureResponse) {
      this.failureResponse = failureResponse;
    }

    boolean isSuccess() {
      return successResponse != null;
    }

    boolean isFailure() {
      return failureResponse != null;
    }

    private static class TwilioSuccessResponse {
      @JsonProperty
      private double price;

      static TwilioSuccessResponse fromBody(ObjectMapper mapper, String body) {
        try {
          return mapper.readValue(body, TwilioSuccessResponse.class);
        } catch (IOException e) {
          logger.warn("Error parsing twilio success response: " + e);
          return new TwilioSuccessResponse();
        }
      }
    }

    private static class TwilioFailureResponse {
      @JsonProperty
      private int status;

      @JsonProperty
      private String message;

      @JsonProperty
      private int code;

      static TwilioFailureResponse fromBody(ObjectMapper mapper, String body) {
        try {
          return mapper.readValue(body, TwilioFailureResponse.class);
        } catch (IOException e) {
          logger.warn("Error parsing twilio success response: " + e);
          return new TwilioFailureResponse();
        }
      }
    }
  }
}
