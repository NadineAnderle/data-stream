package com.example;

import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.model.HealthData;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

public class HealthDataPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(HealthDataPipeline.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final OutputTag<String> FAILED_RECORDS = new OutputTag<String>("failed-records") {
    };

    private static final String DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "kafka:9093";
    private static final String DEFAULT_POSTGRES_HOST = "postgres";
    private static final String DEFAULT_POSTGRES_PORT = "5432";
    private static final String DEFAULT_POSTGRES_DB = "health_db";
    private static final String DEFAULT_POSTGRES_USER = "health_user";
    private static final String DEFAULT_POSTGRES_PASSWORD = "health_password";

    public static void main(String[] args) {
        LOG.info("Iniciando Health Data Pipeline");

        String kafkaBootstrapServers = getArgOrDefault(args, "kafka.bootstrap.servers",
                DEFAULT_KAFKA_BOOTSTRAP_SERVERS);
        String postgresHost = getArgOrDefault(args, "postgres.host", DEFAULT_POSTGRES_HOST);
        String postgresPort = getArgOrDefault(args, "postgres.port", DEFAULT_POSTGRES_PORT);
        String postgresDb = getArgOrDefault(args, "postgres.db", DEFAULT_POSTGRES_DB);
        String postgresUser = getArgOrDefault(args, "postgres.user", DEFAULT_POSTGRES_USER);
        String postgresPassword = getArgOrDefault(args, "postgres.password", DEFAULT_POSTGRES_PASSWORD);

        String postgresUrl = String.format("jdbc:postgresql://%s:%s/%s", postgresHost, postgresPort, postgresDb);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Forçar paralelismo global para 2
        env.setParallelism(2);

                // Configurações de checkpoint
        env.enableCheckpointing(60000); // 60 segundos
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(300000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        // Estratégia de restart mais resiliente
        env.setRestartStrategy(
            RestartStrategies.failureRateRestart(
                3, // máximo de falhas por intervalo
                Time.minutes(5), // intervalo de medição
                Time.seconds(10) // delay entre tentativas
            )
        );


        try {
            Properties kafkaProps = configureKafkaProperties(kafkaBootstrapServers);
            KafkaSource<String> source = createKafkaSource(kafkaProps);
            LOG.info("Kafka configurado em {}", kafkaBootstrapServers);

            // Source com paralelismo explícito
            DataStream<String> kafkaStream = env
                    .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                    .setParallelism(2)
                    .name("Kafka-Consumer");

            // Processor com paralelismo e validação
            SingleOutputStreamOperator<HealthData> healthDataStream = kafkaStream
                    .map(message -> {
                        LOG.debug("Mensagem recebida: {}", message);
                        return message;
                    })
                    .keyBy(message -> {
                        try {
                            if (message == null) {
                                LOG.error("Mensagem recebida é null");
                                return "error-key";
                            }

                            String trimmedMessage = message.trim();
                            if (trimmedMessage.isEmpty()) {
                                LOG.error("Mensagem recebida está vazia");
                                return "error-key";
                            }

                            LOG.debug("Tentando fazer parse do JSON: {}", trimmedMessage);
                            JsonNode node = objectMapper.readTree(trimmedMessage);

                            if (!node.has("patient_id")) {
                                LOG.error("JSON não contém campo patientId: {}", trimmedMessage);
                                return "error-key";
                            }

                            JsonNode patientIdNode = node.get("patient_id");
                            if (patientIdNode == null || patientIdNode.isNull()) {
                                LOG.error("Campo patientId é null no JSON: {}", trimmedMessage);
                                return "error-key";
                            }

                            String patientId = patientIdNode.asText();
                            if (patientId == null || patientId.trim().isEmpty()) {
                                LOG.error("PatientId está vazio no JSON: {}", trimmedMessage);
                                return "error-key";
                            }

                            LOG.debug("PatientId extraído com sucesso: {}", patientId);
                            return patientId;

                        } catch (Exception e) {
                            LOG.error("Erro ao processar mensagem: '{}'. Erro: {}",
                                    message,
                                    e.getMessage());
                            return "error-key";
                        }
                    })
                    .process(new DeduplicationProcessor())
                    .setParallelism(2)
                    .name("Message-Processor");

            // DLQ com paralelismo
            healthDataStream
                    .getSideOutput(FAILED_RECORDS)
                    .addSink(new FlinkKafkaProducer<>(
                            "health-data-dlq",
                            new SimpleStringSchema(),
                            kafkaProps))
                    .setParallelism(2)
                    .name("Failed-Records-Sink");

            // PostgreSQL sink com paralelismo
            configurePostgreSQLSink(healthDataStream, postgresUrl, postgresUser, postgresPassword);

            env.execute("Health Data Pipeline");

        } catch (Exception e) {
            LOG.error("Erro fatal no pipeline: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    private static String getArgOrDefault(String[] args, String key, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals("--" + key)) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }

    private static class DeduplicationProcessor
            extends KeyedProcessFunction<String, String, HealthData> {

        private ValueState<Long> lastProcessedTimestamp;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("last-processed-timestamp", Long.class);
            lastProcessedTimestamp = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(
                String value,
                Context ctx,
                Collector<HealthData> out) throws Exception {

            try {
                // Verifica se é uma mensagem com erro
                if ("error-key".equals(ctx.getCurrentKey())) {
                    LOG.warn("Mensagem com erro detectada, enviando para DLQ: {}", value);
                    ctx.output(FAILED_RECORDS, value);
                    return;
                }

                LOG.debug("Iniciando processamento da mensagem: {}", value);
                HealthData data = objectMapper.readValue(value, HealthData.class);

                validateHealthData(data);

                Long lastTimestamp = lastProcessedTimestamp.value();
                if (lastTimestamp == null || !lastTimestamp.equals(data.getTimestamp())) {
                    lastProcessedTimestamp.update(data.getTimestamp());
                    out.collect(data);
                    LOG.info("Thread {} - Mensagem processada: Paciente {} - Timestamp {}",
                            Thread.currentThread().getName(),
                            data.getPatientId(),
                            data.getTimestamp());
                } else {
                    LOG.warn("Thread {} - Mensagem duplicada detectada para Paciente {} - Timestamp {}. Ignorando.",
                            Thread.currentThread().getName(),
                            data.getPatientId(),
                            data.getTimestamp());
                }
            } catch (Exception e) {
                LOG.error("Thread {} - Erro no processamento: {}. Mensagem: {}",
                        Thread.currentThread().getName(),
                        e.getMessage(),
                        value);
                ctx.output(FAILED_RECORDS, value);
            }
        }
    }

    private static void validateHealthData(HealthData data) {
        if (data.getPatientId() == null || data.getPatientId().trim().isEmpty()) {
            throw new IllegalArgumentException("PatientId é obrigatório");
        }
        if (data.getHeartRate() <= 0) {
            throw new IllegalArgumentException("HeartRate deve ser maior que zero");
        }
        if (data.getTemperature() <= 0) {
            throw new IllegalArgumentException("Temperature deve ser maior que zero");
        }
        if (data.getOxygenSaturation() <= 0) {
            throw new IllegalArgumentException("OxygenSaturation deve ser maior que zero");
        }
        if (data.getTimestamp() <= 0) {
            throw new IllegalArgumentException("Timestamp inválido");
        }
    }

    private static void configurePostgreSQLSink(
            SingleOutputStreamOperator<HealthData> healthDataStream,
            String jdbcUrl, String username, String password) {

        healthDataStream
                .filter(data -> data != null)
                .addSink(
                        JdbcSink.sink(
                                "INSERT INTO health_records (patient_id, heart_rate, temperature, oxygen_saturation, timestamp, large_data) "
                                        +
                                        "VALUES (?, ?, ?, ?, ?, ?) " +
                                        "ON CONFLICT (patient_id, timestamp) DO UPDATE SET " +
                                        "heart_rate = EXCLUDED.heart_rate, " +
                                        "temperature = EXCLUDED.temperature, " +
                                        "oxygen_saturation = EXCLUDED.oxygen_saturation, " +
                                        "large_data = EXCLUDED.large_data",
                                (statement, healthData) -> {
                                    statement.setString(1, healthData.getPatientId());
                                    statement.setDouble(2, healthData.getHeartRate());
                                    statement.setDouble(3, healthData.getTemperature());
                                    statement.setDouble(4, healthData.getOxygenSaturation());
                                    statement.setTimestamp(5, new java.sql.Timestamp(healthData.getTimestamp()));
                                    statement.setString(6, healthData.getLargeData());
                                    LOG.info("Thread {} - Persistindo dados do paciente: {}",
                                            Thread.currentThread().getName(),
                                            healthData.getPatientId());
                                },
                                JdbcExecutionOptions.builder()
                                        .withBatchSize(1)
                                        .withBatchIntervalMs(200)
                                        .withMaxRetries(3)
                                        .build(),
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl(jdbcUrl)
                                        .withDriverName("org.postgresql.Driver")
                                        .withUsername(username)
                                        .withPassword(password)
                                        .build()))
                .setParallelism(2)
                .name("PostgreSQL-Sink");
    }

    private static KafkaSource<String> createKafkaSource(Properties kafkaProps) {
        return KafkaSource.<String>builder()
                .setProperties(kafkaProps)
                .setTopics("health-data")
                .setGroupId("health-data-consumer") // Definição explícita do group.id
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "30000")
                .setProperty("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RoundRobinAssignor")
                // Configurações adicionais para garantir visibilidade do grupo
                .setProperty("enable.auto.commit", "false")
                .setProperty("auto.offset.reset", "earliest")
                .setProperty("allow.auto.create.topics", "false")
                .build();
    }

    private static Properties configureKafkaProperties(String bootstrapServers) {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", bootstrapServers);

        // Configuração explícita do group.id nas properties
        kafkaProps.setProperty("group.id", "health-data-consumer");

        // Configs para garantir que o grupo seja registrado
        kafkaProps.setProperty("enable.auto.commit", "false");
        kafkaProps.setProperty("auto.offset.reset", "earliest");
        kafkaProps.setProperty("session.timeout.ms", "45000");
        kafkaProps.setProperty("heartbeat.interval.ms", "15000");
        kafkaProps.setProperty("max.poll.interval.ms", "300000");

        // Reduzir timeouts para debug
        kafkaProps.setProperty("request.timeout.ms", "30000");
        kafkaProps.setProperty("max.poll.records", "500");

        return kafkaProps;
    }

}