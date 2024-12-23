const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const mongoose = require('mongoose');
const path = require('path');
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const io = socketIo(server);

// MongoDB connection
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://mongodb:27017/crypto';
mongoose.connect(MONGODB_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true
});

// Define prediction schema
const predictionSchema = new mongoose.Schema({
  symbol: String,
  timestamp: Number,
  weighted_mid_price: Number,
  prediction: Number
});

const Prediction = mongoose.model('Prediction', predictionSchema);

// Serve static files
app.use(express.static(path.join(__dirname, 'public')));

// REST API endpoints
app.get('/api/predictions/:symbol', async (req, res) => {
  try {
    const { symbol } = req.params;
    const predictions = await Prediction.find({ symbol })
      .sort({ timestamp: -1 })
      .limit(100);
    res.json(predictions);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// WebSocket connection handling
io.on('connection', (socket) => {
  console.log('Client connected');

  socket.on('subscribe', async (symbol) => {
    // Set up change stream for real-time updates
    const changeStream = Prediction.watch([
      { $match: { 'fullDocument.symbol': symbol } }
    ]);

    changeStream.on('change', (change) => {
      if (change.operationType === 'insert') {
        socket.emit('prediction', change.fullDocument);
      }
    });

    socket.on('disconnect', () => {
      changeStream.close();
      console.log('Client disconnected');
    });
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});