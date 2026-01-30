# log-events-consumer

Service for consuming qubic log event messages from a kafka message broker.

## Build

`go build` in the module root directory will create the executable.

## Run tests

`go test -p 1 -tags ci ./...` will run all unit tests.

## Prerequisites

The application needs kafka and elastic to be installed. The kafka topics and elastic index need to be created before starting the application.

## Configuration

```Bash
Usage: log-events-consumer [options...] [arguments...]

OPTIONS
      --broker-bootstrap-servers  <string>,[string...]  (default: localhost:9092)          
      --broker-consume-topic      <string>              (default: qubic-log-events-data)   
      --broker-consumer-group     <string>              (default: qubic-elastic)           
      --elastic-addresses         <string>,[string...]  (default: https://localhost:9200)  
      --elastic-certificate       <string>              (default: http_ca.crt)             
      --elastic-index-name        <string>              (default: qubic-log-events-alias)  
      --elastic-max-retries       <int>                 (default: 15)                      
      --elastic-password          <string>                                                 
      --elastic-username          <string>              (default: qubic-ingestion)         
  -h, --help                                                                               display this help message
      --metrics-namespace         <string>              (default: qubic_kafka)             
      --metrics-port              <int>                 (default: 9999)                    

ENVIRONMENT
  QUBIC_LOG_EVENTS_CONSUMER_BROKER_BOOTSTRAP_SERVERS  <string>,[string...]  (default: localhost:9092)          
  QUBIC_LOG_EVENTS_CONSUMER_BROKER_CONSUME_TOPIC      <string>              (default: qubic-log-events-data)   
  QUBIC_LOG_EVENTS_CONSUMER_BROKER_CONSUMER_GROUP     <string>              (default: qubic-elastic)           
  QUBIC_LOG_EVENTS_CONSUMER_ELASTIC_ADDRESSES         <string>,[string...]  (default: https://localhost:9200)  
  QUBIC_LOG_EVENTS_CONSUMER_ELASTIC_CERTIFICATE       <string>              (default: http_ca.crt)             
  QUBIC_LOG_EVENTS_CONSUMER_ELASTIC_INDEX_NAME        <string>              (default: qubic-log-events-alias)  
  QUBIC_LOG_EVENTS_CONSUMER_ELASTIC_MAX_RETRIES       <int>                 (default: 15)                      
  QUBIC_LOG_EVENTS_CONSUMER_ELASTIC_PASSWORD          <string>                                                 
  QUBIC_LOG_EVENTS_CONSUMER_ELASTIC_USERNAME          <string>              (default: qubic-ingestion)         
  QUBIC_LOG_EVENTS_CONSUMER_METRICS_NAMESPACE         <string>              (default: qubic_kafka)             
  QUBIC_LOG_EVENTS_CONSUMER_METRICS_PORT              <int>                 (default: 9999)  
```