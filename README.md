# trino-pubsub-event-listener

## Overview

Trino Pub/Sub event listener is a plugin for [Trino](https://trino.io/) that allows you to send query events to Google Cloud Pub/Sub.

``` mermaid
graph LR
    A(Trino) -- Query events --> B((Pub/Sub))
    B --> C(HTTP)
    B --> D(BigQuery)
    B --> E(Apache Avro / GCS)
```


## Usage

To use this plugin you need to copy the distribution package to the Trino plugin (`<path_to_trino>/plugin/pubsub-event-listener/`) directory and configure the plugin.

### Configuration

Create `<path_to_trino>/etc/pubsub-event-listener.properties` with the following required parameters, e.g.:

```properties
event-listener.name=pubsub
pubsub-event-listener.log-created=false
pubsub-event-listener.log-completed=true
pubsub-event-listener.log-split=false
pubsub-event-listener.project-id=<gcp-project-id>
pubsub-event-listener.topic-id=<topic-id>
pubsub-event-listener.message-format=<proto | json>
pubsub-event-listener.credentials-file=<path> # optional
```

## Development

### Prerequisites

- Java 17
- Gradle
- `protoc` (Protobuf compiler)

### Build

```bash
./gradlew build
```

### Distribution package

```bash
./gradlew distTar
```
