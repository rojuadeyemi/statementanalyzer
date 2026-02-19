function validateFile() {
    const fileInput = document.querySelector('input[type="file"]');
    const errorMessage = document.getElementById('error-message');
    const file = fileInput.files[0];

    errorMessage.textContent = '';

    if (!file) {
        errorMessage.textContent = "Please upload a file.";
        return false;
    }

    // Extract extension
    const extension = file.name.split('.').pop().toLowerCase();

    const validExtensions = ['pdf', 'json','txt'];

    if (!validExtensions.includes(extension)) {
        errorMessage.textContent = "Only pdf or JSON (.json) files are allowed.";
        return false;
    }

    return true;
}
