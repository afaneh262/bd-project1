const Binance = require("binance-api-node").default;
const { Kafka } = require("kafkajs");
require("dotenv").config();

// Kafka configuration
const kafka = new Kafka({
  clientId: "binance-collector",
  brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS || "localhost:9092"],
});

const producer = kafka.producer();

// Binance configuration
const client = Binance();

// Symbols to track
const SYMBOLS = [
  "BTCUSDT",
  "ETHUSDT",
  "BNBUSDT",
  "ADAUSDT",
  "XRPUSDT",
  "DOGEUSDT",
  "DOTUSDT",
  "UNIUSDT",
  "LINKUSDT",
  "LTCUSDT",
  "BCHUSDT",
  "SOLUSDT",
  "MATICUSDT",
  "XLMUSDT",
  "ETCUSDT",
  "THETAUSDT",
  "VETUSDT",
  "TRXUSDT",
  "EOSUSDT",
  "FILUSDT",
  "AAVEUSDT",
  "XTZUSDT",
  "ATOMUSDT",
  "NEOUSDT",
  "ALGOUSDT",
  "KSMUSDT",
  "MKRUSDT",
  "COMPUSDT",
  "CROUSDT",
  "FTTUSDT",
  "ICPUSDT",
  "AVAXUSDT",
  "BTTUSDT",
  "CAKEUSDT"
];

async function setupKafkaTopics() {
  const admin = kafka.admin();
  await admin.connect();

  const tradesTopics = SYMBOLS.map((symbol) => ({
    topic: `trades_${symbol.toLowerCase()}`,
    numPartitions: 1,
    replicationFactor: 1,
  }));

  const topics = [...tradesTopics];

  await admin.createTopics({ topics });
  await admin.disconnect();
}

async function startDataCollection() {
  try {
    // Connect to Kafka
    await producer.connect();
    console.log("Connected to Kafka");

    // Collec trade data
    SYMBOLS.forEach((symbol) => {
      client.ws.trades(symbol, async (trade) => {
        const message = {
          tradeTime: trade.tradeTime,
          symbol,
          price: trade.price,
          quantity: trade.quantity,
          isBuyerMaker: trade.isBuyerMaker,
          maker: trade.maker,
          tradeId: trade.tradeId,
        };

        try {
          await producer.send({
            topic: `trades_${symbol.toLowerCase()}`,
            messages: [
              {
                value: JSON.stringify(message),
              },
            ],
          });
          console.log(
            `Sent trade data to Kafka for ${symbol}, tradeId: ${trade.tradeId}`
          );
        } catch (error) {
          console.error(
            `Error sending trade data to Kafka for ${symbol}:`,
            error
          );
        }
      });

      console.log(`Started collecting trade data for ${symbol}`);
    });
  } catch (error) {
    console.error("Error in data collection:", error);
    process.exit(1);
  }
}

// Error handling and graceful shutdown
process.on("SIGTERM", async () => {
  console.log("Received SIGTERM. Shutting down gracefully...");
  await producer.disconnect();
  process.exit(0);
});

process.on("unhandledRejection", (error) => {
  console.error("Unhandled Promise rejection:", error);
});

// Start the application
async function main() {
  try {
    await setupKafkaTopics();
    await startDataCollection();
  } catch (error) {
    console.error("Failed to start application:", error);
    process.exit(1);
  }
}

main();
