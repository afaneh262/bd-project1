const socket = io();

let currentSymbol = "BTCUSDT";
let priceData = {
  x: [],
  close: [],
  high: [],
  low: [],
  open: [],
  type: "candlestick",
  name: "Price",
};

let predictionData = {
  x: [],
  y: [],
  type: "scatter",
  mode: "lines",
  name: "Prediction",
  line: { color: "rgb(219, 64, 82)" },
};

// Initialize charts
function initCharts() {
  Plotly.newPlot("priceChart", [priceData], {
    title: `${currentSymbol} Price and Prediction`,
    xaxis: { title: "Time" },
    yaxis: { title: "Price" },
  });

  Plotly.newPlot(
    "predictionAccuracy",
    [
      {
        x: [],
        y: [],
        type: "scatter",
        mode: "lines",
        name: "Prediction Error %",
      },
    ],
    {
      title: "Prediction Accuracy",
      xaxis: { title: "Time" },
      yaxis: { title: "Error %" },
    }
  );
}
function updateChartsWithNewPrediction(prediction) {
  const timestamp = new Date(prediction.timestamp);

  // Update price data with new values
  priceData.x.push(timestamp);
  priceData.open.push(prediction.weighted_mid_price);
  priceData.high.push(prediction.weighted_mid_price * 1.001);
  priceData.low.push(prediction.weighted_mid_price * 0.999);
  priceData.close.push(prediction.prediction);

  // Update prediction data
  predictionData.x.push(timestamp);
  predictionData.y.push(prediction.prediction);

  // Keep only last 100 data points for performance
  const maxDataPoints = 100;
  if (priceData.x.length > maxDataPoints) {
      priceData.x = priceData.x.slice(-maxDataPoints);
      priceData.open = priceData.open.slice(-maxDataPoints);
      priceData.high = priceData.high.slice(-maxDataPoints);
      priceData.low = priceData.low.slice(-maxDataPoints);
      priceData.close = priceData.close.slice(-maxDataPoints);
      predictionData.x = predictionData.x.slice(-maxDataPoints);
      predictionData.y = predictionData.y.slice(-maxDataPoints);
  }

  // Update price chart
  Plotly.update('priceChart', {
      x: [priceData.x],
      open: [priceData.open],
      high: [priceData.high],
      low: [priceData.low],
      close: [priceData.close]
  }, {
      title: `${currentSymbol} Price and Prediction`
  });

  // Calculate and update prediction accuracy
  const error = Math.abs((prediction.prediction - prediction.weighted_mid_price) / prediction.weighted_mid_price * 100);
  
  Plotly.extendTraces('predictionAccuracy', {
      x: [[timestamp]],
      y: [[error]]
  }, [0]);

  // Keep prediction accuracy chart to last 100 points
  const traceLength = Plotly.getData('predictionAccuracy')[0].x.length;
  if (traceLength > maxDataPoints) {
      Plotly.relayout('predictionAccuracy', {
          xaxis: {
              range: [
                  timestamp - (maxDataPoints * 1000), // Convert to milliseconds
                  timestamp
              ]
          }
      });
  }
}

function updateChartsWithHistoricalData(data) {
  // Reset data arrays
  priceData.x = [];
  priceData.open = [];
  priceData.high = [];
  priceData.low = [];
  priceData.close = [];
  predictionData.x = [];
  predictionData.y = [];

  // Process historical data
  data.forEach((record) => {
    const timestamp = new Date(record.timestamp);

    // Update price data
    priceData.x.push(timestamp);
    priceData.open.push(record.weighted_mid_price);
    priceData.close.push(record.prediction);
    // Calculate high and low with some spread for visualization
    priceData.high.push(record.weighted_mid_price * 1.001);
    priceData.low.push(record.weighted_mid_price * 0.999);

    // Update prediction data
    predictionData.x.push(timestamp);
    predictionData.y.push(record.prediction);
  });

  // Update price chart
  Plotly.update(
    "priceChart",
    {
      x: [priceData.x],
      open: [priceData.open],
      high: [priceData.high],
      low: [priceData.low],
      close: [priceData.close],
    },
    {
      title: `${currentSymbol} Price and Prediction`,
    }
  );

  // Calculate and update prediction accuracy
  const accuracyData = data.map((record) => ({
    timestamp: new Date(record.timestamp),
    error: Math.abs(
      ((record.prediction - record.weighted_mid_price) /
        record.weighted_mid_price) *
        100
    ),
  }));

  Plotly.update(
    "predictionAccuracy",
    {
      x: [accuracyData.map((d) => d.timestamp)],
      y: [accuracyData.map((d) => d.error)],
    },
    {
      title: "Prediction Accuracy",
    }
  );
}

// Load historical data
async function loadHistoricalData() {
  try {
    const response = await fetch(`/api/predictions/${currentSymbol}`);
    const data = await response.json();

    // Update charts with historical data
    updateChartsWithHistoricalData(data);
  } catch (error) {
    console.error("Error loading historical data:", error);
  }
}

// Handle symbol change
document.getElementById("symbolSelect").addEventListener("change", (e) => {
  currentSymbol = e.target.value;
  socket.emit("subscribe", currentSymbol);
  loadHistoricalData();
});

// Subscribe to real-time updates
socket.on("prediction", (prediction) => {
  updateChartsWithNewPrediction(prediction);
});

// Initialize
initCharts();
loadHistoricalData();
socket.emit("subscribe", currentSymbol);
