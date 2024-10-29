package com.example;

import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.model.HealthData;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;


public class HealthDataPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(HealthDataPipeline.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    // Tag para mensagens com erro (serão redirecionadas para DLQ)
    private static final OutputTag<String> FAILED_RECORDS = 
        new OutputTag<String>("failed-records"){};

    // Default connection values
    private static final String DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "kafka:9093";
    private static final String DEFAULT_POSTGRES_HOST = "postgres";
    private static final String DEFAULT_POSTGRES_PORT = "5432";
    private static final String DEFAULT_POSTGRES_DB = "health_db";
    private static final String DEFAULT_POSTGRES_USER = "health_user";
    private static final String DEFAULT_POSTGRES_PASSWORD = "health_password";

    public static void main(String[] args) {
        LOG.info("Iniciando Health Data Pipeline");
        
        // Parse connection arguments
        String kafkaBootstrapServers = getArgOrDefault(args, "kafka.bootstrap.servers", DEFAULT_KAFKA_BOOTSTRAP_SERVERS);
        String postgresHost = getArgOrDefault(args, "postgres.host", DEFAULT_POSTGRES_HOST);
        String postgresPort = getArgOrDefault(args, "postgres.port", DEFAULT_POSTGRES_PORT);
        String postgresDb = getArgOrDefault(args, "postgres.db", DEFAULT_POSTGRES_DB);
        String postgresUser = getArgOrDefault(args, "postgres.user", DEFAULT_POSTGRES_USER);
        String postgresPassword = getArgOrDefault(args, "postgres.password", DEFAULT_POSTGRES_PASSWORD);

        String postgresUrl = String.format("jdbc:postgresql://%s:%s/%s", postgresHost, postgresPort, postgresDb);
        
        // Criar ambiente de execução do Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configuração de checkpointing para garantir processamento exactly-once
        configureCheckpointing(env);
        
        try {
            // Configurar fonte Kafka com propriedades otimizadas
            Properties kafkaProps = configureKafkaProperties(kafkaBootstrapServers);
            KafkaSource<String> source = createKafkaSource(kafkaProps);
            LOG.info("Kafka configurado em {}", kafkaBootstrapServers);

            // Criar stream de dados do Kafka
            DataStream<String> kafkaStream = env.fromSource(
                source, 
                WatermarkStrategy.noWatermarks(), 
                "Kafka Source"
            );
            
            // Processar mensagens com deduplicação baseada em patientId e timestamp
            SingleOutputStreamOperator<HealthData> healthDataStream = kafkaStream
                .keyBy(message -> {
                    try {
                        JsonNode node = objectMapper.readTree(message);
                        return node.get("patientId").asText() + "_" + node.get("timestamp").asText();
                    } catch (Exception e) {
                        return "error-key";
                    }
                })
                .process(new DeduplicationProcessor())
                .name("Message-Processor");

            // Configurar DLQ (Dead Letter Queue) para mensagens com erro
            configureDLQ(healthDataStream, kafkaBootstrapServers);

            // Configurar sink do PostgreSQL com retry
            configurePostgreSQLSink(healthDataStream, postgresUrl, postgresUser, postgresPassword);

            LOG.info("Pipeline configurado e pronto para execução");
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

    /**
     * Configura checkpointing do Flink para garantir processamento exactly-once
     */
    private static void configureCheckpointing(StreamExecutionEnvironment env) {
        env.enableCheckpointing(30000); // Checkpoint a cada 30 segundos
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints");
        LOG.info("Checkpoint storage configurado em: file:///tmp/flink-checkpoints");
        
        // Estratégia de restart em caso de falhas
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
    }

    /**
     * Configura propriedades do Kafka
     */
    private static Properties configureKafkaProperties(String bootstrapServers) {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", bootstrapServers);
        kafkaProps.setProperty("group.id", "health-data-consumer");
        kafkaProps.setProperty("enable.auto.commit", "false");
        
        // Timeouts e retry
        kafkaProps.setProperty("session.timeout.ms", "30000");
        kafkaProps.setProperty("heartbeat.interval.ms", "10000");
        kafkaProps.setProperty("max.poll.interval.ms", "300000");
        kafkaProps.setProperty("transaction.timeout.ms", "900000");
        kafkaProps.setProperty("request.timeout.ms", "30000");
        kafkaProps.setProperty("retries", "3");
        kafkaProps.setProperty("retry.backoff.ms", "1000");
        
        // Configurações de consumo
        kafkaProps.setProperty("max.partition.fetch.bytes", "1048576");
        kafkaProps.setProperty("fetch.max.wait.ms", "500");
        kafkaProps.setProperty("max.poll.records", "500");
        
        return kafkaProps;
    }

    /**
     * Cria fonte Kafka para consumo de dados
     */
    private static KafkaSource<String> createKafkaSource(Properties kafkaProps) {
        return KafkaSource.<String>builder()
                .setProperties(kafkaProps)
                .setTopics("health-data")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    /**
     * Configura DLQ para mensagens com erro
     */
    private static void configureDLQ(SingleOutputStreamOperator<HealthData> healthDataStream, String bootstrapServers) {
        healthDataStream.getSideOutput(FAILED_RECORDS)
            .sinkTo(
                org.apache.flink.connector.kafka.sink.KafkaSink.<String>builder()
                    .setBootstrapServers(bootstrapServers)
                    .setRecordSerializer(
                        org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.builder()
                            .setTopic("health-data-dlq")
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build()
                    )
                    .build()
            )
            .name("Failed-Records-Sink");
    }

    /**
     * Configura sink do PostgreSQL com retry
     */
    private static void configurePostgreSQLSink(SingleOutputStreamOperator<HealthData> healthDataStream, 
                                              String jdbcUrl, String username, String password) {
        healthDataStream
            .filter(data -> data != null)
            .addSink(
                JdbcSink.sink(
                    "INSERT INTO health_records (patient_id, heart_rate, temperature, oxygen_saturation, timestamp) " +
                    "VALUES (?, ?, ?, ?, ?) " +
                    "ON CONFLICT (patient_id, timestamp) DO UPDATE SET " +
                    "heart_rate = EXCLUDED.heart_rate, " +
                    "temperature = EXCLUDED.temperature, " +
                    "oxygen_saturation = EXCLUDED.oxygen_saturation",
                    (statement, healthData) -> {
                        statement.setString(1, healthData.getPatientId());
                        statement.setDouble(2, healthData.getHeartRate());
                        statement.setDouble(3, healthData.getTemperature());
                        statement.setDouble(4, healthData.getOxygenSaturation());
                        statement.setTimestamp(5, new java.sql.Timestamp(healthData.getTimestamp()));
                        LOG.info("Persistindo dados do paciente: {}", healthData.getPatientId());
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
                        .build()
                )
            ).name("PostgreSQL-Sink");
    }

    /**
     * Processor para deduplicação de mensagens usando Flink State
     */
    private static class DeduplicationProcessor 
            extends KeyedProcessFunction<String, String, HealthData> {
        
        private ValueState<Long> lastProcessedTimestamp;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> descriptor = 
                new ValueStateDescriptor<>("last-processed-timestamp", Long.class);
            lastProcessedTimestamp = getRuntimeContext().getState(descriptor);
        }
        
        @Override
        public void processElement(
            String value, 
            Context ctx, 
            Collector<HealthData> out) throws Exception {
            
            try {
                HealthData data = objectMapper.readValue(value, HealthData.class);
                validateHealthData(data);
                
                Long lastTimestamp = lastProcessedTimestamp.value();
                if (lastTimestamp == null || lastTimestamp != data.getTimestamp()) {
                    lastProcessedTimestamp.update(data.getTimestamp());
                    out.collect(data);
                    LOG.info("Mensagem processada: Paciente {} - Timestamp {}", 
                            data.getPatientId(), data.getTimestamp());
                } else {
                    LOG.warn("ALERTA: Mensagem duplicada detectada para Paciente {} - Timestamp {}. A mensagem será ignorada.", 
                            data.getPatientId(), data.getTimestamp());
                }
            } catch (Exception e) {
                LOG.error("Erro no processamento: {}", e.getMessage());
                ctx.output(FAILED_RECORDS, value);
            }
        }
    }

    /**
     * Valida os dados vitais recebidos
     */
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
}
