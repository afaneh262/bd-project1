// server.js
const express = require("express");
const { MongoClient } = require("mongodb");
const { Server } = require("ws");
const http = require("http");
const path = require("path");

const app = express();
const server = http.createServer(app);
const wss = new Server({ server });
const mongoUri = "mongodb://root:example@localhost:27017";
const dbName = "crypto_analysis";

// Serve static files from public directory
app.use(express.static("public"));
app.use("/js", express.static("js")); // For our JavaScript modules

// MongoDB connection and change stream handling
async function startMongoConnection() {
  const client = await MongoClient.connect(mongoUri);
  const db = client.db(dbName);

  // Watch for changes in collections
  const collections = ["trade_volume", "price_trends", "volatility", "vwap"];
  collections.forEach((collection) => {
    const changeStream = db.collection(collection).watch();
    changeStream.on("change", (change) => {
      wss.clients.forEach((client) => {
        client.send(
          JSON.stringify({
            type: collection,
            data: change.fullDocument,
          })
        );
      });
    });
  });

  return db;
}

// API Routes
async function setupRoutes(db) {
  app.get("/api/symbols", async (req, res) => {
    const symbols = await db.collection("trade_volume").distinct("symbol");
    res.json(symbols);
  });

  app.get("/api/data/:symbol/:metric/:interval", async (req, res) => {
    const { symbol, metric, interval } = req.params;
    const intervalMs = getIntervalInMs(interval);

    const pipeline = [
      { $match: { symbol: symbol } },
      {
        $group: {
          _id: {
            $toDate: {
              $subtract: [
                { $toDate: "$start_time" },
                { $mod: [{ $toDate: "$start_time" }, intervalMs] },
              ],
            },
          },
          avgValue: { $avg: getMetricField(metric) },
          maxValue: { $max: getMetricField(metric) },
          minValue: { $min: getMetricField(metric) },
        },
      },
      { $sort: { _id: 1 } },
      { $limit: 100 },
    ];

    const data = await db
      .collection(getCollectionName(metric))
      .aggregate(pipeline)
      .toArray();

    res.json(data);
  });

  // Serve index.html for all other routes
  app.get("*", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "index.html"));
  });
}

// Helper functions
function getIntervalInMs(interval) {
  const intervals = {
    "1m": 60 * 1000,
    "5m": 5 * 60 * 1000,
    "30m": 30 * 60 * 1000,
    "1h": 60 * 60 * 1000,
  };
  return intervals[interval] || intervals["1m"];
}

function getMetricField(metric) {
  const metricFields = {
    volume: "$total_trade_volume",
    price: "$rolling_avg_price",
    volatility: "$price_volatility",
    vwap: "$vwap",
  };
  return metricFields[metric] || metricFields["volume"];
}

function getCollectionName(metric) {
  const collections = {
    volume: "trade_volume",
    price: "price_trends",
    volatility: "volatility",
    vwap: "vwap",
  };
  return collections[metric] || collections["volume"];
}

// Start server
async function startServer() {
  try {
    const db = await startMongoConnection();
    await setupRoutes(db);

    const PORT = process.env.PORT || 3010;
    server.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
    });
  } catch (error) {
    console.error("Failed to start server:", error);
    process.exit(1);
  }
}

startServer();
