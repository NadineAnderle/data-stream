flowchart TD
    subgraph KafkaCluster
        K[Kafka Broker]
        subgraph Topic
            P1[Partition 1]
            P2[Partition 2]
        end
    end

    subgraph FlinkCluster
        JM[JobManager]
        TM1[TaskManager 1]
        TM2[TaskManager 2]
    end

    subgraph PostgreSQL
        DB[(health_db)]
    end

    P1[Python Producer] -->|Generate Data| K
    P2[Duplicate Producer] -->|Generate Data| K

    K -->|Messages| Topic

    P1 -->|Consume| TM1
    P2 -->|Consume| TM2

    JM -->|Distribute Tasks| TM1
    JM -->|Distribute Tasks| TM2

    TM1 -->|Process Stream| DB
    TM2 -->|Process Stream| DB

    JM -->|Checkpointing & Recovery| TM1
    JM -->|Checkpointing & Recovery| TM2

    style KafkaCluster fill:#f9f,stroke:#333,stroke-width:2px
    style FlinkCluster fill:#bbf,stroke:#333,stroke-width:2px
    style PostgreSQL fill:#bfb,stroke:#333,stroke-width:2px
