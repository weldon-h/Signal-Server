/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.controllers;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.tests.util.JsonHelpers.asJson;
import static org.whispersystems.textsecuregcm.tests.util.JsonHelpers.jsonFixture;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit.ResourceTestRule;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.auth.AmbiguousIdentifier;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAccount;
import org.whispersystems.textsecuregcm.auth.OptionalAccess;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicMessageRateConfiguration;
import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.entities.IncomingMessageList;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.MismatchedDevices;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntityList;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.entities.StaleDevices;
import org.whispersystems.textsecuregcm.limits.CardinalityRateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;
import org.whispersystems.textsecuregcm.util.Base64;

@RunWith(JUnitParamsRunner.class)
public class MessageControllerTest {

  private static final String SINGLE_DEVICE_RECIPIENT = "+14151111111";
  private static final UUID   SINGLE_DEVICE_UUID      = UUID.randomUUID();

  private static final String MULTI_DEVICE_RECIPIENT  = "+14152222222";
  private static final UUID   MULTI_DEVICE_UUID       = UUID.randomUUID();

  private static final String INTERNATIONAL_RECIPIENT = "+61123456789";
  private static final UUID   INTERNATIONAL_UUID      = UUID.randomUUID();

  @SuppressWarnings("unchecked")
  private final RedisAdvancedClusterCommands<String, String> redisCommands  = mock(RedisAdvancedClusterCommands.class);

  private final MessageSender               messageSender               = mock(MessageSender.class);
  private final ReceiptSender               receiptSender               = mock(ReceiptSender.class);
  private final AccountsManager             accountsManager             = mock(AccountsManager.class);
  private final MessagesManager             messagesManager             = mock(MessagesManager.class);
  private final RateLimiters                rateLimiters                = mock(RateLimiters.class);
  private final RateLimiter                 rateLimiter                 = mock(RateLimiter.class);
  private final CardinalityRateLimiter      unsealedSenderLimiter       = mock(CardinalityRateLimiter.class);
  private final ApnFallbackManager          apnFallbackManager          = mock(ApnFallbackManager.class);
  private final DynamicConfigurationManager dynamicConfigurationManager = mock(DynamicConfigurationManager.class);
  private final FaultTolerantRedisCluster   metricsCluster              = RedisClusterHelper.buildMockRedisCluster(redisCommands);
  private final ScheduledExecutorService    receiptExecutor             = mock(ScheduledExecutorService.class);

  private final ObjectMapper mapper = new ObjectMapper();

  @Rule
  public final ResourceTestRule resources = ResourceTestRule.builder()
                                                            .addProvider(AuthHelper.getAuthFilter())
                                                            .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(ImmutableSet.of(Account.class, DisabledPermittedAccount.class)))
                                                            .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
                                                            .addResource(new MessageController(rateLimiters, messageSender, receiptSender, accountsManager,
                                                                                               messagesManager, apnFallbackManager, dynamicConfigurationManager, metricsCluster, receiptExecutor))
                                                            .build();


  @Before
  public void setup() throws Exception {
    Set<Device> singleDeviceList = new HashSet<Device>() {{
      add(new Device(1, null, "foo", "bar",
          "isgcm", null, null, false, 111, new SignedPreKey(333, "baz", "boop"), System.currentTimeMillis(), System.currentTimeMillis(), "Test", 0, new Device.DeviceCapabilities(true, false, false, true, true, false)));
    }};

    Set<Device> multiDeviceList = new HashSet<Device>() {{
      add(new Device(1, null, "foo", "bar",
          "isgcm", null, null, false, 222, new SignedPreKey(111, "foo", "bar"), System.currentTimeMillis(), System.currentTimeMillis(), "Test", 0, new Device.DeviceCapabilities(true, false, false, true, false, false)));
      add(new Device(2, null, "foo", "bar",
          "isgcm", null, null, false, 333, new SignedPreKey(222, "oof", "rab"), System.currentTimeMillis(), System.currentTimeMillis(), "Test", 0, new Device.DeviceCapabilities(true, false, false, true, false, false)));
      add(new Device(3, null, "foo", "bar",
          "isgcm", null, null, false, 444, null, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(31), System.currentTimeMillis(), "Test", 0, new Device.DeviceCapabilities(false, false, false, false, false, false)));
    }};

    Account singleDeviceAccount  = new Account(SINGLE_DEVICE_RECIPIENT, SINGLE_DEVICE_UUID, singleDeviceList, "1234".getBytes());
    Account multiDeviceAccount   = new Account(MULTI_DEVICE_RECIPIENT, MULTI_DEVICE_UUID, multiDeviceList, "1234".getBytes());
    Account internationalAccount = new Account(INTERNATIONAL_RECIPIENT, INTERNATIONAL_UUID, singleDeviceList, "1234".getBytes());

    when(accountsManager.get(eq(SINGLE_DEVICE_RECIPIENT))).thenReturn(Optional.of(singleDeviceAccount));
    when(accountsManager.get(argThat((ArgumentMatcher<AmbiguousIdentifier>) identifier -> identifier != null && identifier.hasNumber() && identifier.getNumber().equals(SINGLE_DEVICE_RECIPIENT)))).thenReturn(Optional.of(singleDeviceAccount));
    when(accountsManager.get(eq(MULTI_DEVICE_RECIPIENT))).thenReturn(Optional.of(multiDeviceAccount));
    when(accountsManager.get(argThat((ArgumentMatcher<AmbiguousIdentifier>) identifier -> identifier != null && identifier.hasNumber() && identifier.getNumber().equals(MULTI_DEVICE_RECIPIENT)))).thenReturn(Optional.of(multiDeviceAccount));
    when(accountsManager.get(INTERNATIONAL_RECIPIENT)).thenReturn(Optional.of(internationalAccount));
    when(accountsManager.get(argThat((ArgumentMatcher<AmbiguousIdentifier>) identifier -> identifier != null && identifier.hasNumber() && identifier.getNumber().equals(INTERNATIONAL_RECIPIENT)))).thenReturn(Optional.of(internationalAccount));

    when(rateLimiters.getMessagesLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getUnsealedSenderLimiter()).thenReturn(unsealedSenderLimiter);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(new DynamicConfiguration());

    when(receiptExecutor.schedule(any(Runnable.class), anyLong(), any())).thenAnswer(
        (Answer<ScheduledFuture<?>>) invocation -> {
          invocation.getArgument(0, Runnable.class).run();
          return mock(ScheduledFuture.class);
        });
  }

  @Test
  public synchronized void testSendFromDisabledAccount() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/messages/%s", SINGLE_DEVICE_RECIPIENT))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_NUMBER, AuthHelper.DISABLED_PASSWORD))
                 .put(Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_single_device.json"), IncomingMessageList.class),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat("Unauthorized response", response.getStatus(), is(equalTo(401)));
  }

  @Test
  public synchronized void testSingleDeviceCurrent() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/messages/%s", SINGLE_DEVICE_RECIPIENT))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_single_device.json"), IncomingMessageList.class),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response", response.getStatus(), is(equalTo(200)));

    ArgumentCaptor<Envelope> captor = ArgumentCaptor.forClass(Envelope.class);
    verify(messageSender, times(1)).sendMessage(any(Account.class), any(Device.class), captor.capture(), eq(false));

    assertTrue(captor.getValue().hasSource());
    assertTrue(captor.getValue().hasSourceDevice());
  }

  @Test
  public synchronized void testInternationalUnsealedSenderFromRateLimitedHost() throws Exception {
    final String senderHost = "10.0.0.1";

    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    final DynamicMessageRateConfiguration messageRateConfiguration = mock(DynamicMessageRateConfiguration.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getMessageRateConfiguration()).thenReturn(messageRateConfiguration);
    when(messageRateConfiguration.getRateLimitedCountryCodes()).thenReturn(Set.of("1"));
    when(messageRateConfiguration.getRateLimitedHosts()).thenReturn(Set.of(senderHost));
    when(messageRateConfiguration.getResponseDelay()).thenReturn(Duration.ofMillis(1));
    when(messageRateConfiguration.getResponseDelayJitter()).thenReturn(Duration.ofMillis(1));
    when(messageRateConfiguration.getReceiptDelay()).thenReturn(Duration.ofMillis(1));
    when(messageRateConfiguration.getReceiptDelayJitter()).thenReturn(Duration.ofMillis(1));
    when(messageRateConfiguration.getReceiptProbability()).thenReturn(1.0);

    when(redisCommands.evalsha(any(), any(), any(), any())).thenReturn(List.of(1L, 1L));

    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", INTERNATIONAL_RECIPIENT))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
            .header("X-Forwarded-For", senderHost)
            .put(Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_single_device.json"), IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response", response.getStatus(), is(equalTo(200)));

    verify(messageSender, never()).sendMessage(any(), any(), any(), anyBoolean());
    verify(receiptSender).sendReceipt(any(), eq(AuthHelper.VALID_NUMBER), anyLong());
  }

  @Test
  public synchronized void testSingleDeviceCurrentUnidentified() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/messages/%s", SINGLE_DEVICE_RECIPIENT))
                 .request()
                 .header(OptionalAccess.UNIDENTIFIED, Base64.encodeBytes("1234".getBytes()))
                 .put(Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_single_device.json"), IncomingMessageList.class),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response", response.getStatus(), is(equalTo(200)));

    ArgumentCaptor<Envelope> captor = ArgumentCaptor.forClass(Envelope.class);
    verify(messageSender, times(1)).sendMessage(any(Account.class), any(Device.class), captor.capture(), eq(false));

    assertFalse(captor.getValue().hasSource());
    assertFalse(captor.getValue().hasSourceDevice());
  }


  @Test
  public synchronized void testSendBadAuth() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/messages/%s", SINGLE_DEVICE_RECIPIENT))
                 .request()
                 .put(Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_single_device.json"), IncomingMessageList.class),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response", response.getStatus(), is(equalTo(401)));
  }

  @Test
  public synchronized void testMultiDeviceMissing() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/messages/%s", MULTI_DEVICE_RECIPIENT))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_single_device.json"), IncomingMessageList.class),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response Code", response.getStatus(), is(equalTo(409)));

    assertThat("Good Response Body",
               asJson(response.readEntity(MismatchedDevices.class)),
               is(equalTo(jsonFixture("fixtures/missing_device_response.json"))));

    verifyNoMoreInteractions(messageSender);
  }

  @Test
  public synchronized void testMultiDeviceExtra() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/messages/%s", MULTI_DEVICE_RECIPIENT))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_extra_device.json"), IncomingMessageList.class),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response Code", response.getStatus(), is(equalTo(409)));

    assertThat("Good Response Body",
               asJson(response.readEntity(MismatchedDevices.class)),
               is(equalTo(jsonFixture("fixtures/missing_device_response2.json"))));

    verifyNoMoreInteractions(messageSender);
  }

  @Test
  public synchronized void testMultiDevice() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/messages/%s", MULTI_DEVICE_RECIPIENT))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_multi_device.json"), IncomingMessageList.class),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response Code", response.getStatus(), is(equalTo(200)));

    verify(messageSender, times(2)).sendMessage(any(Account.class), any(Device.class), any(Envelope.class), eq(false));
  }

  @Test
  public synchronized void testRegistrationIdMismatch() throws Exception {
    Response response =
        resources.getJerseyTest().target(String.format("/v1/messages/%s", MULTI_DEVICE_RECIPIENT))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(mapper.readValue(jsonFixture("fixtures/current_message_registration_id.json"), IncomingMessageList.class),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response Code", response.getStatus(), is(equalTo(410)));

    assertThat("Good Response Body",
               asJson(response.readEntity(StaleDevices.class)),
               is(equalTo(jsonFixture("fixtures/mismatched_registration_id.json"))));

    verifyNoMoreInteractions(messageSender);

  }

  @Test
  public synchronized void testGetMessages() throws Exception {

    final long timestampOne = 313377;
    final long timestampTwo = 313388;

    final UUID messageGuidOne = UUID.randomUUID();
    final UUID sourceUuid     = UUID.randomUUID();

    List<OutgoingMessageEntity> messages = new LinkedList<>() {{
      add(new OutgoingMessageEntity(1L, false, messageGuidOne, Envelope.Type.CIPHERTEXT_VALUE, null, timestampOne, "+14152222222", sourceUuid, 2, "hi there".getBytes(), null, 0));
      add(new OutgoingMessageEntity(2L, false, null, Envelope.Type.RECEIPT_VALUE, null, timestampTwo, "+14152222222", sourceUuid, 2, null, null, 0));
    }};

    OutgoingMessageEntityList messagesList = new OutgoingMessageEntityList(messages, false);

    when(messagesManager.getMessagesForDevice(eq(AuthHelper.VALID_UUID), eq(1L), anyString(), anyBoolean())).thenReturn(messagesList);

    OutgoingMessageEntityList response =
        resources.getJerseyTest().target("/v1/messages/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID.toString(), AuthHelper.VALID_PASSWORD))
                 .accept(MediaType.APPLICATION_JSON_TYPE)
                 .get(OutgoingMessageEntityList.class);


    assertEquals(response.getMessages().size(), 2);

    assertEquals(response.getMessages().get(0).getId(), 0);
    assertEquals(response.getMessages().get(1).getId(), 0);

    assertEquals(response.getMessages().get(0).getTimestamp(), timestampOne);
    assertEquals(response.getMessages().get(1).getTimestamp(), timestampTwo);

    assertEquals(response.getMessages().get(0).getGuid(), messageGuidOne);
    assertNull(response.getMessages().get(1).getGuid());

    assertEquals(response.getMessages().get(0).getSourceUuid(), sourceUuid);
    assertEquals(response.getMessages().get(1).getSourceUuid(), sourceUuid);
  }

  @Test
  public synchronized void testGetMessagesBadAuth() throws Exception {
    final long timestampOne = 313377;
    final long timestampTwo = 313388;

    List<OutgoingMessageEntity> messages = new LinkedList<OutgoingMessageEntity>() {{
      add(new OutgoingMessageEntity(1L, false, UUID.randomUUID(), Envelope.Type.CIPHERTEXT_VALUE, null, timestampOne, "+14152222222", UUID.randomUUID(), 2, "hi there".getBytes(), null, 0));
      add(new OutgoingMessageEntity(2L, false, UUID.randomUUID(), Envelope.Type.RECEIPT_VALUE, null, timestampTwo, "+14152222222", UUID.randomUUID(), 2, null, null, 0));
    }};

    OutgoingMessageEntityList messagesList = new OutgoingMessageEntityList(messages, false);

    when(messagesManager.getMessagesForDevice(eq(AuthHelper.VALID_UUID), eq(1L), anyString(), anyBoolean())).thenReturn(messagesList);

    Response response =
        resources.getJerseyTest().target("/v1/messages/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID.toString(), AuthHelper.INVALID_PASSWORD))
                 .accept(MediaType.APPLICATION_JSON_TYPE)
                 .get();

    assertThat("Unauthorized response", response.getStatus(), is(equalTo(401)));
  }

  @Test
  public synchronized void testDeleteMessages() throws Exception {
    long timestamp = System.currentTimeMillis();

    UUID sourceUuid = UUID.randomUUID();

    when(messagesManager.delete(AuthHelper.VALID_UUID, 1, "+14152222222", 31337))
        .thenReturn(Optional.of(new OutgoingMessageEntity(31337L, true, null,
                                                          Envelope.Type.CIPHERTEXT_VALUE,
                                                          null, timestamp,
                                                          "+14152222222", sourceUuid, 1, "hi".getBytes(), null, 0)));

    when(messagesManager.delete(AuthHelper.VALID_UUID, 1, "+14152222222", 31338))
        .thenReturn(Optional.of(new OutgoingMessageEntity(31337L, true, null,
                                                          Envelope.Type.RECEIPT_VALUE,
                                                          null, System.currentTimeMillis(),
                                                          "+14152222222", sourceUuid, 1, null, null, 0)));


    when(messagesManager.delete(AuthHelper.VALID_UUID, 1, "+14152222222", 31339))
        .thenReturn(Optional.empty());

    Response response = resources.getJerseyTest()
                                 .target(String.format("/v1/messages/%s/%d", "+14152222222", 31337))
                                 .request()
                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                 .delete();

    assertThat("Good Response Code", response.getStatus(), is(equalTo(204)));
    verify(receiptSender).sendReceipt(any(Account.class), eq("+14152222222"), eq(timestamp));

    response = resources.getJerseyTest()
                        .target(String.format("/v1/messages/%s/%d", "+14152222222", 31338))
                        .request()
                        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID.toString(), AuthHelper.VALID_PASSWORD))
                        .delete();

    assertThat("Good Response Code", response.getStatus(), is(equalTo(204)));
    verifyNoMoreInteractions(receiptSender);

    response = resources.getJerseyTest()
                        .target(String.format("/v1/messages/%s/%d", "+14152222222", 31339))
                        .request()
                        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                        .delete();

    assertThat("Good Response Code", response.getStatus(), is(equalTo(204)));
    verifyNoMoreInteractions(receiptSender);

  }

  @Test
  @Parameters(method = "argumentsForTestOnlineMessage")
  public void testOnlineMessage(final String fixture, final boolean expectedOnline) throws Exception {

    final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/messages/%s", SINGLE_DEVICE_RECIPIENT))
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(mapper.readValue(jsonFixture(fixture), IncomingMessageList.class),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat("Good Response", response.getStatus(), is(equalTo(200)));

    verify(messageSender, times(1)).sendMessage(any(Account.class), any(Device.class), any(Envelope.class), eq(expectedOnline));
  }

  private static Object argumentsForTestOnlineMessage() {
    return new Object[] {
        new Object[] { "fixtures/current_message_single_device.json", false }, // default to `false` when absent
        new Object[] { "fixtures/online_message_true.json", true },
        new Object[] { "fixtures/online_message_false.json", false },
        // iOS versions prior to 5.5.0.7 send `online` on  IncomingMessageList.message, rather on the top-level entity.
        // This causes some odd client behaviors, such as persisted typing indicators, so we have a temporary
        // server-side adaptation.
        new Object[] { "fixtures/online_message_true_nested_property.json", true },
        new Object[] { "fixtures/online_message_false_nested_property.json", false },
    };
  }

}
