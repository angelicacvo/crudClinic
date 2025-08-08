const fs = require('fs');
const csv = require('csv-parser');
const mysql = require('mysql2/promise');
require('dotenv').config();

async function loadDoctorsFromCSV() {
    const connection = await mysql.createConnection({
        host: process.env.HOST,
        user: process.env.USER,
        password: process.env.PASSWORD,
        database: process.env.DATABASE,
        port: process.env.PORT
    });
    console.log('Conectado a la base de datos:', process.env.DATABASE);

    try {
        const doctors = [];

        // Usamos una promesa para manejar la lectura del CSV de forma síncrona
        await new Promise((resolve, reject) => {
            fs.createReadStream('doctors.csv')
                .pipe(csv())
                .on('data', (data) => {
                    doctors.push(data);
                })
                .on('end', () => {
                    console.log('Lectura del archivo CSV finalizada.');
                    resolve();
                })
                .on('error', (err) => {
                    reject(err);
                });
        });

        console.log(`Se encontraron ${doctors.length} doctores en el archivo.`);

        // Recorremos los datos leídos del CSV
        for (const doctor of doctors) {
            // Definimos la consulta SQL de forma más limpia en una sola línea
            const query = "INSERT IGNORE INTO doctors(name) VALUES (?)";

            // Validamos y limpiamos los valores antes de la inserción
            const name = doctor.name ? doctor.name.trim() : null;

            const values = [
                name
            ];

            // Verificamos si los valores principales no son nulos para evitar errores
            if (name) {
                try {
                    await connection.execute(query, values);
                    console.log(`doctore "${name}" insertado correctamente.`);
                } catch (err) {
                    console.error(`Error insertando al doctore "${name}":`, err);
                }
            } else {
                console.log('Fila omitida debido a datos incompletos.');
            }
        }

        console.log('Proceso de carga de doctores finalizado.');
    } catch (err) {
        console.error('Ocurrió un error:', err);
    } finally {
        await connection.end();
        console.log('Conexión a la base de datos cerrada.');
    }
}


loadDoctorsFromCSV();