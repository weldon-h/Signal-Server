/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm;

import static com.codahale.metrics.MetricRegistry.name;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.jdbi3.strategies.DefaultNameStrategy;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.dropwizard.Application;
import io.dropwizard.auth.AuthFilter;
import io.dropwizard.auth.PolymorphicAuthDynamicFeature;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.auth.basic.BasicCredentialAuthFilter;
import io.dropwizard.auth.basic.BasicCredentials;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.db.PooledDataSourceFactory;
import io.dropwizard.jdbi3.JdbiFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.lettuce.core.resource.ClientResources;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.wavefront.WavefrontConfig;
import io.micrometer.wavefront.WavefrontMeterRegistry;
import java.net.http.HttpClient;
import java.security.Security;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import javax.servlet.ServletRegistration;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.jdbi.v3.core.Jdbi;
import org.signal.zkgroup.ServerSecretParams;
import org.signal.zkgroup.auth.ServerZkAuthOperations;
import org.signal.zkgroup.profiles.ServerZkProfileOperations;
import org.whispersystems.dispatch.DispatchManager;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.CertificateGenerator;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAccount;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.configuration.DirectoryServerConfiguration;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.controllers.AttachmentControllerV1;
import org.whispersystems.textsecuregcm.controllers.AttachmentControllerV2;
import org.whispersystems.textsecuregcm.controllers.AttachmentControllerV3;
import org.whispersystems.textsecuregcm.controllers.CertificateController;
import org.whispersystems.textsecuregcm.controllers.DeviceController;
import org.whispersystems.textsecuregcm.controllers.DirectoryController;
import org.whispersystems.textsecuregcm.controllers.KeepAliveController;
import org.whispersystems.textsecuregcm.controllers.KeysController;
import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.controllers.PaymentsController;
import org.whispersystems.textsecuregcm.controllers.ProfileController;
import org.whispersystems.textsecuregcm.controllers.ProvisioningController;
import org.whispersystems.textsecuregcm.controllers.RemoteConfigController;
import org.whispersystems.textsecuregcm.controllers.SecureBackupController;
import org.whispersystems.textsecuregcm.controllers.SecureStorageController;
import org.whispersystems.textsecuregcm.controllers.StickerController;
import org.whispersystems.textsecuregcm.controllers.VoiceVerificationController;
import org.whispersystems.textsecuregcm.currency.CurrencyConversionManager;
import org.whispersystems.textsecuregcm.currency.FixerClient;
import org.whispersystems.textsecuregcm.currency.FtxClient;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.filters.RemoteDeprecationFilter;
import org.whispersystems.textsecuregcm.filters.TimestampResponseFilter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.liquibase.NameableMigrationsBundle;
import org.whispersystems.textsecuregcm.mappers.DeviceLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.IOExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.InvalidWebsocketAddressExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RetryLaterExceptionMapper;
import org.whispersystems.textsecuregcm.metrics.BufferPoolGauges;
import org.whispersystems.textsecuregcm.metrics.CpuUsageGauge;
import org.whispersystems.textsecuregcm.metrics.FileDescriptorGauge;
import org.whispersystems.textsecuregcm.metrics.FreeMemoryGauge;
import org.whispersystems.textsecuregcm.metrics.GarbageCollectionGauges;
import org.whispersystems.textsecuregcm.metrics.MaxFileDescriptorGauge;
import org.whispersystems.textsecuregcm.metrics.MetricsApplicationEventListener;
import org.whispersystems.textsecuregcm.metrics.NetworkReceivedGauge;
import org.whispersystems.textsecuregcm.metrics.NetworkSentGauge;
import org.whispersystems.textsecuregcm.metrics.NstatCounters;
import org.whispersystems.textsecuregcm.metrics.OperatingSystemMemoryGauge;
import org.whispersystems.textsecuregcm.metrics.PushLatencyManager;
import org.whispersystems.textsecuregcm.metrics.TrafficSource;
import org.whispersystems.textsecuregcm.providers.RedisClientFactory;
import org.whispersystems.textsecuregcm.providers.RedisClusterHealthCheck;
import org.whispersystems.textsecuregcm.push.APNSender;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.push.GCMSender;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.ProvisioningManager;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.recaptcha.RecaptchaClient;
import org.whispersystems.textsecuregcm.redis.ConnectionEventLogger;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.s3.PolicySigner;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;
import org.whispersystems.textsecuregcm.securebackup.SecureBackupClient;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.sms.SmsSender;
import org.whispersystems.textsecuregcm.sms.TwilioSmsSender;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.storage.AbusiveHostRules;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountCleaner;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawler;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerCache;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerListener;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ActiveUserCounter;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciler;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciliationClient;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.FaultTolerantDatabase;
import org.whispersystems.textsecuregcm.storage.KeysDynamoDb;
import org.whispersystems.textsecuregcm.storage.MessagePersister;
import org.whispersystems.textsecuregcm.storage.MessagesCache;
import org.whispersystems.textsecuregcm.storage.MessagesDynamoDb;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.PendingAccounts;
import org.whispersystems.textsecuregcm.storage.PendingAccountsManager;
import org.whispersystems.textsecuregcm.storage.PendingDevices;
import org.whispersystems.textsecuregcm.storage.PendingDevicesManager;
import org.whispersystems.textsecuregcm.storage.Profiles;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.PubSubManager;
import org.whispersystems.textsecuregcm.storage.PushFeedbackProcessor;
import org.whispersystems.textsecuregcm.storage.RegistrationLockVersionCounter;
import org.whispersystems.textsecuregcm.storage.RemoteConfigs;
import org.whispersystems.textsecuregcm.storage.RemoteConfigsManager;
import org.whispersystems.textsecuregcm.storage.ReservedUsernames;
import org.whispersystems.textsecuregcm.storage.Usernames;
import org.whispersystems.textsecuregcm.storage.UsernamesManager;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.websocket.AuthenticatedConnectListener;
import org.whispersystems.textsecuregcm.websocket.DeadLetterHandler;
import org.whispersystems.textsecuregcm.websocket.ProvisioningConnectListener;
import org.whispersystems.textsecuregcm.websocket.WebSocketAccountAuthenticator;
import org.whispersystems.textsecuregcm.workers.CertificateCommand;
import org.whispersystems.textsecuregcm.workers.DeleteUserCommand;
import org.whispersystems.textsecuregcm.workers.GetRedisCommandStatsCommand;
import org.whispersystems.textsecuregcm.workers.GetRedisSlowlogCommand;
import org.whispersystems.textsecuregcm.workers.SetCrawlerAccelerationTask;
import org.whispersystems.textsecuregcm.workers.SetRequestLoggingEnabledTask;
import org.whispersystems.textsecuregcm.workers.VacuumCommand;
import org.whispersystems.textsecuregcm.workers.ZkParamsCommand;
import org.whispersystems.websocket.WebSocketResourceProviderFactory;
import org.whispersystems.websocket.setup.WebSocketEnvironment;

public class WhisperServerService extends Application<WhisperServerConfiguration> {

  static {
    Security.addProvider(new BouncyCastleProvider());
  }

  @Override
  public void initialize(Bootstrap<WhisperServerConfiguration> bootstrap) {
    bootstrap.addCommand(new VacuumCommand());
    bootstrap.addCommand(new DeleteUserCommand());
    bootstrap.addCommand(new CertificateCommand());
    bootstrap.addCommand(new ZkParamsCommand());
    bootstrap.addCommand(new GetRedisSlowlogCommand());
    bootstrap.addCommand(new GetRedisCommandStatsCommand());

    bootstrap.addBundle(new NameableMigrationsBundle<WhisperServerConfiguration>("accountdb", "accountsdb.xml") {
      @Override
      public DataSourceFactory getDataSourceFactory(WhisperServerConfiguration configuration) {
        return configuration.getAccountsDatabaseConfiguration();
      }
    });


    bootstrap.addBundle(new NameableMigrationsBundle<WhisperServerConfiguration>("abusedb", "abusedb.xml") {
      @Override
      public PooledDataSourceFactory getDataSourceFactory(WhisperServerConfiguration configuration) {
        return configuration.getAbuseDatabaseConfiguration();
      }
    });
  }

  @Override
  public String getName() {
    return "whisper-server";
  }

  @Override
  public void run(WhisperServerConfiguration config, Environment environment)
      throws Exception
  {
    SharedMetricRegistries.add(Constants.METRICS_NAME, environment.metrics());

    final WavefrontConfig wavefrontConfig = new WavefrontConfig() {
      @Override
      public String get(final String key) {
        return null;
      }

      @Override
      public String uri() {
        return config.getMicrometerConfiguration().getUri();
      }

      @Override
      public int batchSize() {
        return config.getMicrometerConfiguration().getBatchSize();
      }
    };

    Metrics.addRegistry(new WavefrontMeterRegistry(wavefrontConfig, Clock.SYSTEM) {
      @Override
      protected DistributionStatisticConfig defaultHistogramConfig() {
        return DistributionStatisticConfig.builder()
                .percentiles(.75, .95, .99, .999)
                .build()
                .merge(super.defaultHistogramConfig());
      }
    });

    environment.getObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    environment.getObjectMapper().setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
    environment.getObjectMapper().setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

    JdbiFactory jdbiFactory = new JdbiFactory(DefaultNameStrategy.CHECK_EMPTY);
    Jdbi        accountJdbi = jdbiFactory.build(environment, config.getAccountsDatabaseConfiguration(), "accountdb");
    Jdbi        abuseJdbi   = jdbiFactory.build(environment, config.getAbuseDatabaseConfiguration(), "abusedb"  );

    FaultTolerantDatabase accountDatabase = new FaultTolerantDatabase("accounts_database", accountJdbi, config.getAccountsDatabaseConfiguration().getCircuitBreakerConfiguration());
    FaultTolerantDatabase abuseDatabase   = new FaultTolerantDatabase("abuse_database", abuseJdbi, config.getAbuseDatabaseConfiguration().getCircuitBreakerConfiguration());

    AmazonDynamoDBClientBuilder messageDynamoDbClientBuilder = AmazonDynamoDBClientBuilder
            .standard()
            .withRegion(config.getMessageDynamoDbConfiguration().getRegion())
            .withClientConfiguration(new ClientConfiguration().withClientExecutionTimeout(((int) config.getMessageDynamoDbConfiguration().getClientExecutionTimeout().toMillis()))
                                                              .withRequestTimeout((int) config.getMessageDynamoDbConfiguration().getClientRequestTimeout().toMillis()))
            .withCredentials(InstanceProfileCredentialsProvider.getInstance());

    AmazonDynamoDBClientBuilder keysDynamoDbClientBuilder = AmazonDynamoDBClientBuilder
            .standard()
            .withRegion(config.getKeysDynamoDbConfiguration().getRegion())
            .withClientConfiguration(new ClientConfiguration().withClientExecutionTimeout(((int) config.getKeysDynamoDbConfiguration().getClientExecutionTimeout().toMillis()))
                                                              .withRequestTimeout((int) config.getKeysDynamoDbConfiguration().getClientRequestTimeout().toMillis()))
            .withCredentials(InstanceProfileCredentialsProvider.getInstance());

    DynamoDB messageDynamoDb = new DynamoDB(messageDynamoDbClientBuilder.build());
    DynamoDB preKeyDynamoDb = new DynamoDB(keysDynamoDbClientBuilder.build());

    Accounts          accounts          = new Accounts(accountDatabase);
    PendingAccounts   pendingAccounts   = new PendingAccounts(accountDatabase);
    PendingDevices    pendingDevices    = new PendingDevices (accountDatabase);
    Usernames         usernames         = new Usernames(accountDatabase);
    ReservedUsernames reservedUsernames = new ReservedUsernames(accountDatabase);
    Profiles          profiles          = new Profiles(accountDatabase);
    KeysDynamoDb      keysDynamoDb      = new KeysDynamoDb(preKeyDynamoDb, config.getKeysDynamoDbConfiguration().getTableName());
    MessagesDynamoDb  messagesDynamoDb  = new MessagesDynamoDb(messageDynamoDb, config.getMessageDynamoDbConfiguration().getTableName(), config.getMessageDynamoDbConfiguration().getTimeToLive());
    AbusiveHostRules  abusiveHostRules  = new AbusiveHostRules(abuseDatabase);
    RemoteConfigs     remoteConfigs     = new RemoteConfigs(accountDatabase);

    RedisClientFactory  pubSubClientFactory = new RedisClientFactory("pubsub_cache", config.getPubsubCacheConfiguration().getUrl(), config.getPubsubCacheConfiguration().getReplicaUrls(), config.getPubsubCacheConfiguration().getCircuitBreakerConfiguration());
    ReplicatedJedisPool pubsubClient        = pubSubClientFactory.getRedisClientPool();

    ClientResources generalCacheClientResources       = ClientResources.builder().build();
    ClientResources messageCacheClientResources       = ClientResources.builder().build();
    ClientResources presenceClientResources           = ClientResources.builder().build();
    ClientResources metricsCacheClientResources       = ClientResources.builder().build();
    ClientResources pushSchedulerCacheClientResources = ClientResources.builder().ioThreadPoolSize(4).build();

    ConnectionEventLogger.logConnectionEvents(generalCacheClientResources);
    ConnectionEventLogger.logConnectionEvents(messageCacheClientResources);
    ConnectionEventLogger.logConnectionEvents(presenceClientResources);
    ConnectionEventLogger.logConnectionEvents(metricsCacheClientResources);

    FaultTolerantRedisCluster cacheCluster             = new FaultTolerantRedisCluster("main_cache_cluster", config.getCacheClusterConfiguration(), generalCacheClientResources);
    FaultTolerantRedisCluster messagesCluster          = new FaultTolerantRedisCluster("messages_cluster", config.getMessageCacheConfiguration().getRedisClusterConfiguration(), messageCacheClientResources);
    FaultTolerantRedisCluster clientPresenceCluster    = new FaultTolerantRedisCluster("client_presence_cluster", config.getClientPresenceClusterConfiguration(), presenceClientResources);
    FaultTolerantRedisCluster metricsCluster           = new FaultTolerantRedisCluster("metrics_cluster", config.getMetricsClusterConfiguration(), metricsCacheClientResources);
    FaultTolerantRedisCluster pushSchedulerCluster     = new FaultTolerantRedisCluster("push_scheduler", config.getPushSchedulerCluster(), pushSchedulerCacheClientResources);

    BlockingQueue<Runnable> keyspaceNotificationDispatchQueue = new ArrayBlockingQueue<>(10_000);
    Metrics.gaugeCollectionSize(name(getClass(), "keyspaceNotificationDispatchQueueSize"), Collections.emptyList(), keyspaceNotificationDispatchQueue);

    ScheduledExecutorService recurringJobExecutor                 = environment.lifecycle().scheduledExecutorService(name(getClass(), "recurringJob-%d")).threads(2).build();
    ScheduledExecutorService declinedMessageReceiptExecutor       = environment.lifecycle().scheduledExecutorService(name(getClass(), "declined-receipt-%d")).threads(2).build();
    ScheduledExecutorService retrySchedulingExecutor              = environment.lifecycle().scheduledExecutorService(name(getClass(), "retry-%d")).threads(2).build();
    ExecutorService          keyspaceNotificationDispatchExecutor = environment.lifecycle().executorService(name(getClass(), "keyspaceNotification-%d")).maxThreads(16).workQueue(keyspaceNotificationDispatchQueue).build();
    ExecutorService          apnSenderExecutor                    = environment.lifecycle().executorService(name(getClass(), "apnSender-%d")).maxThreads(1).minThreads(1).build();
    ExecutorService          gcmSenderExecutor                    = environment.lifecycle().executorService(name(getClass(), "gcmSender-%d")).maxThreads(1).minThreads(1).build();
    ExecutorService          backupServiceExecutor                = environment.lifecycle().executorService(name(getClass(), "backupService-%d")).maxThreads(8).minThreads(1).build();
    ExecutorService          storageServiceExecutor               = environment.lifecycle().executorService(name(getClass(), "storageService-%d")).maxThreads(8).minThreads(1).build();

    ExternalServiceCredentialGenerator directoryCredentialsGenerator = new ExternalServiceCredentialGenerator(config.getDirectoryConfiguration().getDirectoryClientConfiguration().getUserAuthenticationTokenSharedSecret(),
            config.getDirectoryConfiguration().getDirectoryClientConfiguration().getUserAuthenticationTokenUserIdSecret(),
            true);

    DynamicConfigurationManager dynamicConfigurationManager = new DynamicConfigurationManager(config.getAppConfig().getApplication(), config.getAppConfig().getEnvironment(), config.getAppConfig().getConfigurationName());
    dynamicConfigurationManager.start();

    ExperimentEnrollmentManager experimentEnrollmentManager = new ExperimentEnrollmentManager(dynamicConfigurationManager);

    ExternalServiceCredentialGenerator storageCredentialsGenerator   = new ExternalServiceCredentialGenerator(config.getSecureStorageServiceConfiguration().getUserAuthenticationTokenSharedSecret(), new byte[0], false);
    ExternalServiceCredentialGenerator backupCredentialsGenerator    = new ExternalServiceCredentialGenerator(config.getSecureBackupServiceConfiguration().getUserAuthenticationTokenSharedSecret(), new byte[0], false);
    ExternalServiceCredentialGenerator paymentsCredentialsGenerator  = new ExternalServiceCredentialGenerator(config.getPaymentsServiceConfiguration().getUserAuthenticationTokenSharedSecret(), new byte[0], false);

    SecureBackupClient         secureBackupClient         = new SecureBackupClient(backupCredentialsGenerator, backupServiceExecutor, config.getSecureBackupServiceConfiguration());
    SecureStorageClient        secureStorageClient        = new SecureStorageClient(storageCredentialsGenerator, storageServiceExecutor, config.getSecureStorageServiceConfiguration());
    ClientPresenceManager      clientPresenceManager      = new ClientPresenceManager(clientPresenceCluster, recurringJobExecutor, keyspaceNotificationDispatchExecutor);
    DirectoryQueue             directoryQueue             = new DirectoryQueue(config.getDirectoryConfiguration().getSqsConfiguration());
    PendingAccountsManager     pendingAccountsManager     = new PendingAccountsManager(pendingAccounts, cacheCluster);
    PendingDevicesManager      pendingDevicesManager      = new PendingDevicesManager(pendingDevices, cacheCluster);
    UsernamesManager           usernamesManager           = new UsernamesManager(usernames, reservedUsernames, cacheCluster);
    ProfilesManager            profilesManager            = new ProfilesManager(profiles, cacheCluster);
    MessagesCache              messagesCache              = new MessagesCache(messagesCluster, messagesCluster, keyspaceNotificationDispatchExecutor);
    PushLatencyManager         pushLatencyManager         = new PushLatencyManager(metricsCluster);
    MessagesManager            messagesManager            = new MessagesManager(messagesDynamoDb, messagesCache, pushLatencyManager);
    AccountsManager            accountsManager            = new AccountsManager(accounts, cacheCluster, directoryQueue, keysDynamoDb, messagesManager, usernamesManager, profilesManager, secureStorageClient, secureBackupClient);
    RemoteConfigsManager       remoteConfigsManager       = new RemoteConfigsManager(remoteConfigs);
    DeadLetterHandler          deadLetterHandler          = new DeadLetterHandler(accountsManager, messagesManager);
    DispatchManager            dispatchManager            = new DispatchManager(pubSubClientFactory, Optional.of(deadLetterHandler));
    PubSubManager              pubSubManager              = new PubSubManager(pubsubClient, dispatchManager);
    APNSender                  apnSender                  = new APNSender(apnSenderExecutor, accountsManager, config.getApnConfiguration());
    GCMSender                  gcmSender                  = new GCMSender(gcmSenderExecutor, accountsManager, config.getGcmConfiguration().getApiKey());
    RateLimiters               rateLimiters               = new RateLimiters(config.getLimitsConfiguration(), dynamicConfigurationManager, cacheCluster);
    ProvisioningManager        provisioningManager        = new ProvisioningManager(pubSubManager);

    AccountAuthenticator                  accountAuthenticator                  = new AccountAuthenticator(accountsManager);
    DisabledPermittedAccountAuthenticator disabledPermittedAccountAuthenticator = new DisabledPermittedAccountAuthenticator(accountsManager);

    ApnFallbackManager       apnFallbackManager = new ApnFallbackManager(pushSchedulerCluster, apnSender, accountsManager);
    TwilioSmsSender          twilioSmsSender    = new TwilioSmsSender(config.getTwilioConfiguration(), dynamicConfigurationManager);
    SmsSender                smsSender          = new SmsSender(twilioSmsSender);
    MessageSender            messageSender      = new MessageSender(apnFallbackManager, clientPresenceManager, messagesManager, gcmSender, apnSender, pushLatencyManager);
    ReceiptSender            receiptSender      = new ReceiptSender(accountsManager, messageSender);
    TurnTokenGenerator       turnTokenGenerator = new TurnTokenGenerator(config.getTurnConfiguration());
    RecaptchaClient          recaptchaClient    = new RecaptchaClient(config.getRecaptchaConfiguration().getSecret());

    MessagePersister messagePersister = new MessagePersister(messagesCache, messagesManager, accountsManager, dynamicConfigurationManager, Duration.ofMinutes(config.getMessageCacheConfiguration().getPersistDelayMinutes()));

    final List<AccountDatabaseCrawlerListener> accountDatabaseCrawlerListeners = new ArrayList<>();
    accountDatabaseCrawlerListeners.add(new PushFeedbackProcessor(accountsManager, directoryQueue));
    accountDatabaseCrawlerListeners.add(new ActiveUserCounter(config.getMetricsFactory(), cacheCluster));
    for (DirectoryServerConfiguration directoryServerConfiguration : config.getDirectoryConfiguration().getDirectoryServerConfiguration()) {
      final DirectoryReconciliationClient directoryReconciliationClient = new DirectoryReconciliationClient(directoryServerConfiguration);
      final DirectoryReconciler directoryReconciler = new DirectoryReconciler(directoryServerConfiguration.getReplicationName(), directoryReconciliationClient);
      accountDatabaseCrawlerListeners.add(directoryReconciler);
    }
    accountDatabaseCrawlerListeners.add(new AccountCleaner(accountsManager));
    accountDatabaseCrawlerListeners.add(new RegistrationLockVersionCounter(metricsCluster, config.getMetricsFactory()));

    HttpClient                currencyClient  = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).connectTimeout(Duration.ofSeconds(10)).build();
    FixerClient               fixerClient     = new FixerClient(currencyClient, config.getPaymentsServiceConfiguration().getFixerApiKey());
    FtxClient                 ftxClient       = new FtxClient(currencyClient);
    CurrencyConversionManager currencyManager = new CurrencyConversionManager(fixerClient, ftxClient, config.getPaymentsServiceConfiguration().getPaymentCurrencies());

    AccountDatabaseCrawlerCache accountDatabaseCrawlerCache = new AccountDatabaseCrawlerCache(cacheCluster);
    AccountDatabaseCrawler      accountDatabaseCrawler      = new AccountDatabaseCrawler(accountsManager, accountDatabaseCrawlerCache, accountDatabaseCrawlerListeners, config.getAccountDatabaseCrawlerConfiguration().getChunkSize(), config.getAccountDatabaseCrawlerConfiguration().getChunkIntervalMs());

    apnSender.setApnFallbackManager(apnFallbackManager);
    environment.lifecycle().manage(apnFallbackManager);
    environment.lifecycle().manage(pubSubManager);
    environment.lifecycle().manage(messageSender);
    environment.lifecycle().manage(accountDatabaseCrawler);
    environment.lifecycle().manage(remoteConfigsManager);
    environment.lifecycle().manage(messagesCache);
    environment.lifecycle().manage(messagePersister);
    environment.lifecycle().manage(clientPresenceManager);
    environment.lifecycle().manage(currencyManager);

    AWSCredentials         credentials               = new BasicAWSCredentials(config.getCdnConfiguration().getAccessKey(), config.getCdnConfiguration().getAccessSecret());
    AWSCredentialsProvider credentialsProvider       = new AWSStaticCredentialsProvider(credentials);
    AmazonS3               cdnS3Client               = AmazonS3Client.builder().withCredentials(credentialsProvider).withRegion(config.getCdnConfiguration().getRegion()).build();
    PostPolicyGenerator    profileCdnPolicyGenerator = new PostPolicyGenerator(config.getCdnConfiguration().getRegion(), config.getCdnConfiguration().getBucket(), config.getCdnConfiguration().getAccessKey());
    PolicySigner           profileCdnPolicySigner    = new PolicySigner(config.getCdnConfiguration().getAccessSecret(), config.getCdnConfiguration().getRegion());

    ServerSecretParams        zkSecretParams         = new ServerSecretParams(config.getZkConfig().getServerSecret());
    ServerZkProfileOperations zkProfileOperations    = new ServerZkProfileOperations(zkSecretParams);
    ServerZkAuthOperations    zkAuthOperations       = new ServerZkAuthOperations(zkSecretParams);
    boolean                   isZkEnabled            = config.getZkConfig().isEnabled();

    AttachmentControllerV1 attachmentControllerV1    = new AttachmentControllerV1(rateLimiters, config.getAwsAttachmentsConfiguration().getAccessKey(), config.getAwsAttachmentsConfiguration().getAccessSecret(), config.getAwsAttachmentsConfiguration().getBucket());
    AttachmentControllerV2 attachmentControllerV2    = new AttachmentControllerV2(rateLimiters, config.getAwsAttachmentsConfiguration().getAccessKey(), config.getAwsAttachmentsConfiguration().getAccessSecret(), config.getAwsAttachmentsConfiguration().getRegion(), config.getAwsAttachmentsConfiguration().getBucket());
    AttachmentControllerV3 attachmentControllerV3    = new AttachmentControllerV3(rateLimiters, config.getGcpAttachmentsConfiguration().getDomain(), config.getGcpAttachmentsConfiguration().getEmail(), config.getGcpAttachmentsConfiguration().getMaxSizeInBytes(), config.getGcpAttachmentsConfiguration().getPathPrefix(), config.getGcpAttachmentsConfiguration().getRsaSigningKey());
    KeysController         keysController            = new KeysController(rateLimiters, keysDynamoDb, accountsManager, directoryQueue);
    MessageController      messageController         = new MessageController(rateLimiters, messageSender, receiptSender, accountsManager, messagesManager, apnFallbackManager, dynamicConfigurationManager, metricsCluster, declinedMessageReceiptExecutor);
    ProfileController      profileController         = new ProfileController(rateLimiters, accountsManager, profilesManager, usernamesManager, dynamicConfigurationManager, cdnS3Client, profileCdnPolicyGenerator, profileCdnPolicySigner, config.getCdnConfiguration().getBucket(), zkProfileOperations, isZkEnabled);
    StickerController      stickerController         = new StickerController(rateLimiters, config.getCdnConfiguration().getAccessKey(), config.getCdnConfiguration().getAccessSecret(), config.getCdnConfiguration().getRegion(), config.getCdnConfiguration().getBucket());
    RemoteConfigController remoteConfigController    = new RemoteConfigController(remoteConfigsManager, config.getRemoteConfigConfiguration().getAuthorizedTokens(), config.getRemoteConfigConfiguration().getGlobalConfig());

    AuthFilter<BasicCredentials, Account>                  accountAuthFilter                  = new BasicCredentialAuthFilter.Builder<Account>().setAuthenticator(accountAuthenticator).buildAuthFilter                                  ();
    AuthFilter<BasicCredentials, DisabledPermittedAccount> disabledPermittedAccountAuthFilter = new BasicCredentialAuthFilter.Builder<DisabledPermittedAccount>().setAuthenticator(disabledPermittedAccountAuthenticator).buildAuthFilter();

    environment.servlets().addFilter("RemoteDeprecationFilter", new RemoteDeprecationFilter(dynamicConfigurationManager))
        .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), false, "/*");

    environment.jersey().register(new MetricsApplicationEventListener(TrafficSource.HTTP));

    environment.jersey().register(new PolymorphicAuthDynamicFeature<>(ImmutableMap.of(Account.class, accountAuthFilter,
                                                                                      DisabledPermittedAccount.class, disabledPermittedAccountAuthFilter)));
    environment.jersey().register(new PolymorphicAuthValueFactoryProvider.Binder<>(ImmutableSet.of(Account.class, DisabledPermittedAccount.class)));

    environment.jersey().register(new TimestampResponseFilter());

    environment.jersey().register(new AccountController(pendingAccountsManager, accountsManager, usernamesManager, abusiveHostRules, rateLimiters, smsSender, directoryQueue, messagesManager, dynamicConfigurationManager, turnTokenGenerator, config.getTestDevices(), recaptchaClient, gcmSender, apnSender, backupCredentialsGenerator));
    environment.jersey().register(new DeviceController(pendingDevicesManager, accountsManager, messagesManager, directoryQueue, rateLimiters, config.getMaxDevices()));
    environment.jersey().register(new DirectoryController(directoryCredentialsGenerator));
    environment.jersey().register(new ProvisioningController(rateLimiters, provisioningManager));
    environment.jersey().register(new CertificateController(new CertificateGenerator(config.getDeliveryCertificate().getCertificate(), config.getDeliveryCertificate().getPrivateKey(), config.getDeliveryCertificate().getExpiresDays()), zkAuthOperations, isZkEnabled));
    environment.jersey().register(new VoiceVerificationController(config.getVoiceVerificationConfiguration().getUrl(), config.getVoiceVerificationConfiguration().getLocales()));
    environment.jersey().register(new SecureStorageController(storageCredentialsGenerator));
    environment.jersey().register(new SecureBackupController(backupCredentialsGenerator));
    environment.jersey().register(new PaymentsController(currencyManager, paymentsCredentialsGenerator));
    environment.jersey().register(attachmentControllerV1);
    environment.jersey().register(attachmentControllerV2);
    environment.jersey().register(attachmentControllerV3);
    environment.jersey().register(keysController);
    environment.jersey().register(messageController);
    environment.jersey().register(profileController);
    environment.jersey().register(stickerController);
    environment.jersey().register(remoteConfigController);

    ///
    WebSocketEnvironment<Account> webSocketEnvironment = new WebSocketEnvironment<>(environment, config.getWebSocketConfiguration(), 90000);
    webSocketEnvironment.setAuthenticator(new WebSocketAccountAuthenticator(accountAuthenticator));
    webSocketEnvironment.setConnectListener(new AuthenticatedConnectListener(receiptSender, messagesManager, messageSender, apnFallbackManager, clientPresenceManager, retrySchedulingExecutor));
    webSocketEnvironment.jersey().register(new MetricsApplicationEventListener(TrafficSource.WEBSOCKET));
    webSocketEnvironment.jersey().register(new KeepAliveController(clientPresenceManager));
    webSocketEnvironment.jersey().register(messageController);
    webSocketEnvironment.jersey().register(profileController);
    webSocketEnvironment.jersey().register(attachmentControllerV1);
    webSocketEnvironment.jersey().register(attachmentControllerV2);
    webSocketEnvironment.jersey().register(attachmentControllerV3);
    webSocketEnvironment.jersey().register(remoteConfigController);

    WebSocketEnvironment<Account> provisioningEnvironment = new WebSocketEnvironment<>(environment, webSocketEnvironment.getRequestLog(), 60000);
    provisioningEnvironment.setConnectListener(new ProvisioningConnectListener(pubSubManager));
    provisioningEnvironment.jersey().register(new MetricsApplicationEventListener(TrafficSource.WEBSOCKET));
    provisioningEnvironment.jersey().register(new KeepAliveController(clientPresenceManager));

    registerCorsFilter(environment);
    registerExceptionMappers(environment, webSocketEnvironment, provisioningEnvironment);

    WebSocketResourceProviderFactory<Account> webSocketServlet    = new WebSocketResourceProviderFactory<>(webSocketEnvironment, Account.class);
    WebSocketResourceProviderFactory<Account> provisioningServlet = new WebSocketResourceProviderFactory<>(provisioningEnvironment, Account.class);

    ServletRegistration.Dynamic websocket    = environment.servlets().addServlet("WebSocket", webSocketServlet      );
    ServletRegistration.Dynamic provisioning = environment.servlets().addServlet("Provisioning", provisioningServlet);

    websocket.addMapping("/v1/websocket/");
    websocket.setAsyncSupported(true);

    provisioning.addMapping("/v1/websocket/provisioning/");
    provisioning.setAsyncSupported(true);

    environment.admin().addTask(new SetRequestLoggingEnabledTask());
    environment.admin().addTask(new SetCrawlerAccelerationTask(accountDatabaseCrawlerCache));

///

    environment.healthChecks().register("cacheCluster", new RedisClusterHealthCheck(cacheCluster));

    environment.metrics().register(name(CpuUsageGauge.class, "cpu"), new CpuUsageGauge(3, TimeUnit.SECONDS));
    environment.metrics().register(name(FreeMemoryGauge.class, "free_memory"), new FreeMemoryGauge());
    environment.metrics().register(name(NetworkSentGauge.class, "bytes_sent"), new NetworkSentGauge());
    environment.metrics().register(name(NetworkReceivedGauge.class, "bytes_received"), new NetworkReceivedGauge());
    environment.metrics().register(name(FileDescriptorGauge.class, "fd_count"), new FileDescriptorGauge());
    environment.metrics().register(name(MaxFileDescriptorGauge.class, "max_fd_count"), new MaxFileDescriptorGauge());
    environment.metrics().register(name(OperatingSystemMemoryGauge.class, "buffers"), new OperatingSystemMemoryGauge("Buffers"));
    environment.metrics().register(name(OperatingSystemMemoryGauge.class, "cached"), new OperatingSystemMemoryGauge("Cached"));

    BufferPoolGauges.registerMetrics();
    GarbageCollectionGauges.registerMetrics();

    new NstatCounters().registerMetrics(recurringJobExecutor, wavefrontConfig.step());
  }

  private void registerExceptionMappers(Environment environment, WebSocketEnvironment<Account> webSocketEnvironment, WebSocketEnvironment<Account> provisioningEnvironment) {
    environment.jersey().register(new IOExceptionMapper());
    environment.jersey().register(new RateLimitExceededExceptionMapper());
    environment.jersey().register(new InvalidWebsocketAddressExceptionMapper());
    environment.jersey().register(new DeviceLimitExceededExceptionMapper());
    environment.jersey().register(new RetryLaterExceptionMapper());

    webSocketEnvironment.jersey().register(new IOExceptionMapper());
    webSocketEnvironment.jersey().register(new RateLimitExceededExceptionMapper());
    webSocketEnvironment.jersey().register(new InvalidWebsocketAddressExceptionMapper());
    webSocketEnvironment.jersey().register(new DeviceLimitExceededExceptionMapper());
    webSocketEnvironment.jersey().register(new RetryLaterExceptionMapper());

    provisioningEnvironment.jersey().register(new IOExceptionMapper());
    provisioningEnvironment.jersey().register(new RateLimitExceededExceptionMapper());
    provisioningEnvironment.jersey().register(new InvalidWebsocketAddressExceptionMapper());
    provisioningEnvironment.jersey().register(new DeviceLimitExceededExceptionMapper());
    provisioningEnvironment.jersey().register(new RetryLaterExceptionMapper());
  }

  private void registerCorsFilter(Environment environment) {
    FilterRegistration.Dynamic filter = environment.servlets().addFilter("CORS", CrossOriginFilter.class);
    filter.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");
    filter.setInitParameter("allowedOrigins", "*");
    filter.setInitParameter("allowedHeaders", "Content-Type,Authorization,X-Requested-With,Content-Length,Accept,Origin,X-Signal-Agent");
    filter.setInitParameter("allowedMethods", "GET,PUT,POST,DELETE,OPTIONS");
    filter.setInitParameter("preflightMaxAge", "5184000");
    filter.setInitParameter("allowCredentials", "true");
  }

  public static void main(String[] args) throws Exception {
    new WhisperServerService().run(args);
  }
}
