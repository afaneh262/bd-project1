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
