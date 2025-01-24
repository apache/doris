# Penguin

Penguin is a high-performance, real-time, distributed SQL database for big data analytics, based on Apache Doris. It is designed to support both low-latency queries and high throughput data ingestion.

## Features

- **High Performance**: Optimized for both high throughput and low latency.
- **Real-Time Analytics**: Supports real-time data ingestion and query.
- **Distributed**: Scales horizontally to handle large datasets.
- **SQL Compatibility**: Supports SQL for easy integration with existing tools and workflows.
- **Integration**: Compatible with various data sources and sinks.

## Getting Started

### Prerequisites

- Java Development Kit (JDK) 8 or higher
- Maven 3.6.0 or higher
- Docker (optional, for running in a container)

### Installation

1. **Clone the repository**
    ```bash
    git clone https://github.com/pingvinkowalski/Penguin.git
    cd penguin
    ```

2. **Build the project**
    ```bash
    mvn clean install
    ```

3. **Run the application**
    ```bash
    java -jar target/penguin-1.0.0.jar
    ```

### Docker

To run Penguin using Docker, follow these steps:

1. **Build the Docker image**
    ```bash
    docker build -t penguin:latest .
    ```

2. **Run the Docker container**
    ```bash
    docker run -d -p 8080:8080 penguin:latest
    ```

## Usage

### Connecting to Penguin

You can connect to Penguin using any SQL client. The default port is `8080`.

Example using `curl`:
```bash
curl -X POST "http://localhost:8080/query" -d 'SELECT * FROM your_table;'# Penguin

Penguin is a high-performance, real-time, distributed SQL database for big data analytics, based on Apache Doris. It is designed to support both low-latency queries and high throughput data ingestion.

## Features

- **High Performance**: Optimized for both high throughput and low latency.
- **Real-Time Analytics**: Supports real-time data ingestion and query.
- **Distributed**: Scales horizontally to handle large datasets.
- **SQL Compatibility**: Supports SQL for easy integration with existing tools and workflows.
- **Integration**: Compatible with various data sources and sinks.

## Getting Started

### Prerequisites

- Java Development Kit (JDK) 8 or higher
- Maven 3.6.0 or higher
- Docker (optional, for running in a container)

### Installation

1. **Clone the repository**
    ```bash
    git clone https://github.com/your-username/penguin.git
    cd penguin
    ```

2. **Build the project**
    ```bash
    mvn clean install
    ```

3. **Run the application**
    ```bash
    java -jar target/penguin-1.0.0.jar
    ```

### Docker

To run Penguin using Docker, follow these steps:

1. **Build the Docker image**
    ```bash
    docker build -t penguin:latest .
    ```

2. **Run the Docker container**
    ```bash
    docker run -d -p 8080:8080 penguin:latest
    ```

## Usage

### Connecting to Penguin

You can connect to Penguin using any SQL client. The default port is `8080`.

Example using `curl`:
```bash
curl -X POST "http://localhost:8080/query" -d 'SELECT * FROM your_table;'
