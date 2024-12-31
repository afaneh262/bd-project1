// server.js
const express = require("express");
const { MongoClient } = require("mongodb");
const http = require("http");
const path = require("path");

const app = express();
const server = http.createServer(app);
const mongoUri = "mongodb://root:example@localhost:27017";
const dbName = "crypto_analysis";

// Serve static files from public directory
app.use(express.static("public"));
app.use("/js", express.static("js"));

let db;

// MongoDB connection
async function connectToMongo() {
  try {
    const client = await MongoClient.connect(mongoUri);
    db = client.db(dbName);
    console.log("Connected to MongoDB");
  } catch (error) {
    console.error("Failed to connect to MongoDB:", error);
    process.exit(1);
  }
}

// API Routes
async function setupRoutes() {
  // Get available symbols
  app.get("/api/symbols", async (req, res) => {
    try {
      const symbols = await db.collection("trade_volume").distinct("symbol");
      res.json(symbols);
    } catch (error) {
      res.status(500).json({ error: "Failed to fetch symbols" });
    }
  });

  // Get latest data for a specific interval
  app.get("/api/data/:symbol/:metric/:interval", async (req, res) => {
    try {
      const { symbol, metric, interval } = req.params;
      const intervalMs = getIntervalInMs(interval);

      const pipeline = [
        { $match: { symbol: symbol } },
        {
          $group: {
            _id: {
              $subtract: ["$timestamp", { $mod: ["$timestamp", intervalMs] }],
            },
            avgValue: { $avg: getMetricField(metric) },
            maxValue: { $max: getMetricField(metric) },
            minValue: { $min: getMetricField(metric) },
          },
        },
        { $sort: { _id: -1 } }, // Sort by newest first
        { $limit: 100 },
      ];

      const data = await db
        .collection(getCollectionName(metric))
        .aggregate(pipeline)
        .toArray();

      res.json(data.reverse());
    } catch (error) {
      console.log("error", error);
      res.status(500).json({ error: "Failed to fetch data" });
    }
  });

  // Get latest timestamp for a metric
  app.get("/api/latest/:metric", async (req, res) => {
    try {
      const { metric } = req.params;
      const latest = await db
        .collection(getCollectionName(metric))
        .find({})
        .sort({ timestamp: -1 })
        .limit(1)
        .toArray();

      res.json({ timestamp: latest[0]?.timestamp || null });
    } catch (error) {
      res.status(500).json({ error: "Failed to fetch latest timestamp" });
    }
  });

  app.get("/api/changes/:interval", async (req, res) => {
    try {
      const { interval } = req.params;
      const intervalMs = getIntervalInMs(interval);

      // Get current time rounded down to the nearest minute
      const now = new Date().getTime();

      const timePeriod = {
        $match: {
          timestamp: {
            $gte: now - intervalMs * 2,
            $lte: now,
          },
        },
      };

      console.log('timePeriod', timePeriod);

      // Pipeline for price changes
      const pricePipeline = [
        timePeriod,
        {
          $sort: { timestamp: 1 },
        },
        {
          $group: {
            _id: "$symbol",
            earliestPrice: {
              $first: "$rolling_avg_price",
            },
            latestPrice: {
              $last: "$rolling_avg_price",
            },
          },
        },
      ];

      // Pipeline for volume changes
      const volumePipeline = [
        timePeriod,
        {
          $sort: { timestamp: 1 },
        },
        {
          $group: {
            _id: "$symbol",
            earliestVolume: {
              $first: "$total_trade_volume",
            },
            latestVolume: {
              $last: "$total_trade_volume",
            },
          },
        },
      ];

      // Execute both pipelines
      const [priceData, volumeData] = await Promise.all([
        db.collection("price_trends").aggregate(pricePipeline).toArray(),
        db.collection("trade_volume").aggregate(volumePipeline).toArray(),
      ]);

      console.log('priceData, volumeData', priceData, volumeData);

      const volumeMap = new Map(volumeData.map((item) => [item._id, item]));

      // Combine and calculate percentage changes
      const changes = priceData.map((priceItem) => {
        const volumeItem = volumeMap.get(priceItem._id);

        return {
          symbol: priceItem._id,
          priceChange: priceItem.earliestPrice
            ? ((priceItem.latestPrice - priceItem.earliestPrice) /
                priceItem.earliestPrice) *
              100
            : null,
          volumeChange:
            volumeItem && volumeItem.earliestVolume
              ? ((volumeItem.latestVolume - volumeItem.earliestVolume) /
                  volumeItem.earliestVolume) *
                100
              : null,
        };
      });

      res.json(changes);
    } catch (error) {
      console.error("Error fetching changes:", error);
      res.status(500).json({ error: "Failed to fetch changes" });
    }
  });

  app.get("/changes.html", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "changes.html"));
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
    await connectToMongo();
    await setupRoutes();

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
