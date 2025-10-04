fetch('/datos_litros_distribuidos')
    .then(response => response.json())
    .then(data => {
        document.getElementById('litros-distribuidos').innerText = `${data.litros_distribuidos} L`;
    })
    .catch(error => console.error('Error al obtener los datos:', error));