FROM sbtscala/scala-sbt:eclipse-temurin-17.0.4_1.7.1_2.13.8

WORKDIR /app
COPY . .
RUN sbt clean assembly

CMD ["spark-submit", "--class", "com.crypto.prediction.Main", "target/scala-2.12/crypto-prediction-assembly-1.0.jar"]
