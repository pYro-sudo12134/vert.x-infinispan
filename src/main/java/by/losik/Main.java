package by.losik;

import by.losik.config.AppConfig;
import by.losik.config.MetricsConfig;
import by.losik.config.VertxConfig;
import by.losik.config.VolumeManagerConfig;
import by.losik.verticle.FileProcessorVerticle;
import by.losik.verticle.HttpVerticle;
import by.losik.verticle.MigrationVerticle;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.ThreadingModel;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.EventBusOptions;
import io.vertx.core.file.FileSystemOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Starting File Storage Service");

        String clusterName = System.getenv().getOrDefault("CLUSTER_NAME", "vertx-cluster");
        log.debug("Cluster name: {}", clusterName);

        JsonObject config = new JsonObject()
                .put("clustered", Boolean.parseBoolean(
                        System.getenv().getOrDefault("CLUSTERED", "false")
                ))
                .put("http.port", Integer.parseInt(
                        System.getenv().getOrDefault("HTTP_SERVER_PORT", "8080")))
                .put("metrics.port", Integer.parseInt(
                        System.getenv().getOrDefault("METRICS_PORT", "8079")))
                .put("nfs.path",
                        System.getenv().getOrDefault("NFS_MOUNT_PATH", "/mnt/nfs"))
                .put("file.upload.timeout", Long.parseLong(
                        System.getenv().getOrDefault("FILE_UPLOAD_TIMEOUT_MS", "30000")))
                .put("cors.enabled", Boolean.parseBoolean(
                        System.getenv().getOrDefault("CORS_ENABLED", "true")))
                .put("cors.allowed.origins",
                        System.getenv().getOrDefault("CORS_ALLOWED_ORIGINS", "*"))
                .put("cors.allowed.methods",
                        System.getenv().getOrDefault("CORS_ALLOWED_METHODS", "GET,POST,PUT,DELETE,OPTIONS"))
                .put("cors.allowed.headers",
                        System.getenv().getOrDefault("CORS_ALLOWED_HEADERS", "Content-Type,Authorization"))
                .put("temp.file.path",
                        System.getenv().getOrDefault("TEMP_FILE_PATH", "/tmp/file-uploads"))
                .put("max.page.size", Integer.parseInt(
                        System.getenv().getOrDefault("MAX_PAGE_SIZE", "1000")))
                .put("default.page.size", Integer.parseInt(
                        System.getenv().getOrDefault("DEFAULT_PAGE_SIZE", "100")))
                .put("cluster.name", clusterName)
                .put("node.id",
                        System.getenv().getOrDefault("NODE_ID", "node-" + System.currentTimeMillis()))
                .put("volume.strategy",
                        System.getenv().getOrDefault("VOLUME_STRATEGY", "hash"));

        log.info("Configuration loaded: Clustered={} HTTP Port={}, NFS Path={}, Upload Timeout={}ms, CORS Enabled={}",
                config.getBoolean("clustered"),
                config.getInteger("http.port"),
                config.getString("nfs.path"),
                config.getLong("file.upload.timeout"),
                config.getBoolean("cors.enabled"));

        System.setProperty("vertx.infinispan.config", "cluster.xml");

        AppConfig.load(config);

        PrometheusMeterRegistry prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        new JvmGcMetrics().bindTo(prometheusRegistry);
        new JvmMemoryMetrics().bindTo(prometheusRegistry);
        new JvmThreadMetrics().bindTo(prometheusRegistry);
        new ProcessorMetrics().bindTo(prometheusRegistry);

        MicrometerMetricsOptions metricsOptions = new MicrometerMetricsOptions()
                .setPrometheusOptions(new VertxPrometheusOptions()
                        .setEnabled(true)
                        .setStartEmbeddedServer(true)
                        .setPublishQuantiles(true))
                .setEnabled(true);

        BackendRegistries.setupBackend(metricsOptions, prometheusRegistry);

        VertxOptions vertxOptions = new VertxOptions()
                .setFileSystemOptions(new FileSystemOptions())
                .setMetricsOptions(metricsOptions)
                .setEventBusOptions(new EventBusOptions());

        VertxConfig.initVertx(vertxOptions, AppConfig.isClustered());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered, closing Vert.x instance");

            CountDownLatch latch = new CountDownLatch(1);
            Vertx vertx = VertxConfig.vertx();
            long startTime = System.currentTimeMillis();

            vertx.close().onComplete(ar -> {
                long duration = System.currentTimeMillis() - startTime;
                if (ar.succeeded()) {
                    log.info("Vert.x instance closed gracefully in {} ms", duration);
                } else {
                    log.error("Failed to close Vert.x instance after {} ms", duration, ar.cause());
                }
                latch.countDown();
            });

            try {
                if (!latch.await(30, TimeUnit.SECONDS)) {
                    log.warn("Vert.x instance did not close within 30 seconds, forcing shutdown");
                }
            } catch (InterruptedException e) {
                log.warn("Shutdown interrupted", e);
                Thread.currentThread().interrupt();
            }

            log.info("Shutdown hook finished");
        }));

        VolumeManagerConfig.init(VertxConfig.vertx(), AppConfig.nodeId());

        int fsVerticles = Integer.parseInt(System.getenv().getOrDefault("FS_VERTICLES", "2"));
        log.info("Deploying {} FileProcessorVerticle instance(s)", fsVerticles);
        VertxConfig.vertx().deployVerticle(FileProcessorVerticle.class,
                        new DeploymentOptions().setInstances(fsVerticles)
                                .setHa(true).setThreadingModel(ThreadingModel.WORKER))
                .onSuccess(deploymentId -> log.info("FileProcessorVerticle deployed successfully with ID: {}", deploymentId))
                .onFailure(err -> log.error("Failed to deploy FileProcessorVerticle: {}", err.getMessage(), err));

        int httpVerticles = Integer.parseInt(System.getenv().getOrDefault("HTTP_VERTICLES", "1"));
        log.info("Deploying {} HttpVerticle instance(s)", httpVerticles);
        VertxConfig.vertx().deployVerticle(HttpVerticle.class,
                        new DeploymentOptions().setInstances(httpVerticles)
                                .setHa(true))
                .onSuccess(deploymentId -> log.info("HttpVerticle deployed successfully with ID: {}", deploymentId))
                .onFailure(err -> log.error("Failed to deploy HttpVerticle: {}", err.getMessage(), err));

        int migrationVerticles = Integer.parseInt(System.getenv().getOrDefault("MIGRATION_VERTICLES", "2"));
        log.info("Deploying {} MigrationVerticle instance(s)", migrationVerticles);

        VertxConfig.vertx().deployVerticle(MigrationVerticle.class,
                        new DeploymentOptions()
                                .setInstances(migrationVerticles)
                                .setHa(true).setThreadingModel(ThreadingModel.WORKER))
                .onSuccess(deploymentId -> log.info("MigrationVerticle deployed successfully with ID: {}", deploymentId))
                .onFailure(err -> log.error("Failed to deploy MigrationVerticle: {}", err.getMessage(), err));

        MetricsConfig.setupMetricsServer();

        log.info("File Storage Service started successfully");
    }
}