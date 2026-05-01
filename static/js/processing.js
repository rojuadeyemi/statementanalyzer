let steps = [
    "Starting analysis...",
    "Reading statement...",
    "Analyzing transactions...",
    "Generating risk indicators...",
    "Completed"
];

// Map each status to a progress percentage
let progressMap = {
    "Starting analysis...": 10,
    "Reading statement...": 30,
    "Analyzing transactions...": 60,
    "Generating risk indicators...": 90,
    "Completed": 100,
    "Error": 0
};

function updateProgress(status) {
    const progressBar = document.getElementById("progressBar");
    const percent = progressMap[status] || 0;
    progressBar.style.width = percent + "%";
    progressBar.innerText = percent + "%";
}

function checkStatus() {
    fetch(`/status/${jobId}`)
        .then(response => response.json())
        .then(data => {
            const statusText = data.status || "Processing...";
            document.getElementById("status").innerText = statusText;
            updateProgress(statusText);

            if (statusText === "Completed") {
                window.location.href = `/result/${jobId}`;
            } else if (statusText === "Error") {
                document.getElementById("status").innerText = "An error occurred: " + data.result;
                updateProgress("Error");
                clearInterval(statusChecker);
            }
        })
        .catch(err => {
            document.getElementById("status").innerText = "Connection error. Retrying...";
            console.error(err);
        });
}

// Poll every 1 second
let statusChecker = setInterval(checkStatus, 1000);