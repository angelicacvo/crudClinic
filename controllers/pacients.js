const fs = require('fs');
const csv = require('csv-parser');
const mysql = require('mysql2/promise');
require('dotenv').config();

async function loadPacientsFromCSV() {
    const connection = await mysql.createConnection({
        host: process.env.HOST,
        user: process.env.USER,
        password: process.env.PASSWORD,
        database: process.env.DATABASE,
        port: process.env.PORT
    });
    console.log('Conectado a la base de datos:', process.env.DATABASE);

    try {
        const pacients = [];

        // Usamos una promesa para manejar la lectura del CSV de forma síncrona
        await new Promise((resolve, reject) => {
            fs.createReadStream('pacients.csv')
                .pipe(csv())
                .on('data', (data) => {
                    pacients.push(data);
                })
                .on('end', () => {
                    console.log('Lectura del archivo CSV finalizada.');
                    resolve();
                })
                .on('error', (err) => {
                    reject(err);
                });
        });

        console.log(`Se encontraron ${pacients.length} pacientes en el archivo.`);

        // Recorremos los datos leídos del CSV
        for (const pacient of pacients) {
            // Definimos la consulta SQL de forma más limpia en una sola línea
            const query = "INSERT IGNORE INTO pacients(name, email) VALUES (?, ?)";

            // Validamos y limpiamos los valores antes de la inserción
            const name = pacient.name ? pacient.name.trim() : null;
            const email = pacient.email ? pacient.email.trim() : null;

            const values = [
                name,
                email
            ];

            // Verificamos si los valores principales no son nulos para evitar errores
            if (name && email) {
                try {
                    await connection.execute(query, values);
                    console.log(`paciente "${name} ${email}" insertado correctamente.`);
                } catch (err) {
                    console.error(`Error insertando al paciente "${name} ${email}":`, err);
                }
            } else {
                console.log('Fila omitida debido a datos incompletos.');
            }
        }

        console.log('Proceso de carga de pacientes finalizado.');
    } catch (err) {
        console.error('Ocurrió un error:', err);
    } finally {
        await connection.end();
        console.log('Conexión a la base de datos cerrada.');
    }
}

loadPacientsFromCSV();