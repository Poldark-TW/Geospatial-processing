
var alreadyLogin = document.getElementById("already-login");

alreadyLogin.onclick = function (){
    alert("Already logged.");
}

document.getElementById('showLogButton').addEventListener('click', function(event) {
    event.preventDefault();
    fetch('/get_logs', {
        method: 'GET'
    })
    .then(response => response.json())

    .then(data => {
        document.getElementById('logDisplay').innerText = data.join('\n');
    })
    .catch(error => console.error('Error:', error));
});