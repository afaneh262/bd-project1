# Crypto Prediction Dashboard

This project provides a cryptocurrency prediction dashboard that collects, processes, and visualizes real-time and historical data from Binance. The system is built using Node.js, Kafka, MongoDB, Spark, and Docker.

## Project Structure

- `binance-collector/`: Collects trades data from Binance and sends it to Kafka.
- `spark-app/`: Processes the data using Apache Spark and stores the results in MongoDB.
- `visualization/`: Provides a web interface to visualize the metrics.

## Prerequisites

- Docker
- Docker Compose

## Running the Project

1. Clone the repository:
    ```sh
    git clone https://github.com/afaneh262/bd-project1
    cd bd-project1
    ```

2. Start the Docker containers:
    ```sh
    docker-compose up --build
    ```

3. Open the following links in your browser:

    - **Kafka UI**: [http://localhost:8090](http://localhost:8090)
    - **Mongo Express**: [http://localhost:8081](http://localhost:8081)
    - **Visualization Dashboard**: [http://localhost:3010](http://localhost:3010)

## Services

- **Zookeeper**: Manages Kafka brokers.
- **Kafka**: Message broker for streaming data.
- **Kafka UI**: Web interface to monitor Kafka topics.
- **MongoDB**: Database to store processed data.
- **Mongo Express**: Web interface to manage MongoDB.
- **Visualization**: Web server to visualize predictions.
- **Spark Master**: Spark cluster master node.
- **Spark Worker**: Spark cluster worker node.
- **Binance Collector**: Collects data from Binance and sends it to Kafka.
- **Spark App**: Processes data from Kafka and stores results in MongoDB.

## License

This project is licensed under the MIT License.