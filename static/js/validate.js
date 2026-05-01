// File validation
function validateFile() {

const fileInput = document.getElementById("fileInput");
const errorMessage = document.getElementById("error-message");

const file = fileInput.files[0];

errorMessage.textContent = "";

if (!file) {
errorMessage.textContent = "Please upload a file.";
return false;
}

const extension = file.name.split(".").pop().toLowerCase();

const validExtensions = ["pdf", "json", "txt"];

if (!validExtensions.includes(extension)) {

errorMessage.textContent =
"Only PDF, JSON or TXT files are allowed.";

return false;
}

return true;

}


// Start analysis
function startAnalysis() {

if (!validateFile()) {
return false;
}

document.getElementById("loading").style.display = "block";

const btn = document.getElementById("analyzeBtn");

btn.disabled = true;
btn.innerText = "Analyzing...";

return true;

}


// Drag and Drop support

const uploadArea = document.getElementById("upload-area");
const fileInput = document.getElementById("fileInput");
const fileName = document.getElementById("file-name");


uploadArea.addEventListener("dragover", function(e){

e.preventDefault();
uploadArea.classList.add("dragover");

});


uploadArea.addEventListener("dragleave", function(){

uploadArea.classList.remove("dragover");

});


uploadArea.addEventListener("drop", function(e){

e.preventDefault();

uploadArea.classList.remove("dragover");

fileInput.files = e.dataTransfer.files;

fileName.innerText = fileInput.files[0].name;

});


fileInput.addEventListener("change", function(){

if(fileInput.files.length > 0){
fileName.innerText = fileInput.files[0].name;
}

});