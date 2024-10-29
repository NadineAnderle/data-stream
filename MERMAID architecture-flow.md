flowchart TD
    P1[Python Producer] -->|Generate Data| K[Kafka Broker]
    P2[Duplicate Producer] -->|Generate Data| K
    
    K -->|Messages| F[Flink Processor]
    Z[ZooKeeper] -->|Manage| K
    
    F -->|Process Stream| D{Deduplication Check}
    D -->|Duplicate| R[Reject]
    D -->|Unique| V{Validate Data}
    
    V -->|Invalid| DLQ[Dead Letter Queue]
    V -->|Valid| PG[(PostgreSQL)]
    
    KD[Kafdrop] -->|Monitor| K
    
    style P1 fill:#ff9,stroke:#000
    style P2 fill:#ff9,stroke:#333
    style K fill:#f96,stroke:#333
    style Z fill:#f96,stroke:#333
    style F fill:#58f,stroke:#333
    style D fill:#58f,stroke:#333
    style V fill:#58f,stroke:#333
    style PG fill:#5b5,stroke:#333
    style DLQ fill:#f96,stroke:#333
    style KD fill:#f5f,stroke:#333
    style R fill:#f55,stroke:#333
