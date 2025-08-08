const fs = require('fs');
const csv = require('csv-parser');
const mysql = require('mysql2/promise');
require('dotenv').config();

async function loadSpecialitiesFromCSV() {
    const connection = await mysql.createConnection({
        host: process.env.HOST,
        user: process.env.USER,
        password: process.env.PASSWORD,
        database: process.env.DATABASE,
        port: process.env.PORT
    });
    console.log('Conectado a la base de datos:', process.env.DATABASE);

    try {
        const specialities = [];

        // Usamos una promesa para manejar la lectura del CSV de forma síncrona
        await new Promise((resolve, reject) => {
            fs.createReadStream('specialities.csv')
                .pipe(csv())
                .on('data', (data) => {
                    specialities.push(data);
                })
                .on('end', () => {
                    console.log('Lectura del archivo CSV finalizada.');
                    resolve();
                })
                .on('error', (err) => {
                    reject(err);
                });
        });

        console.log(`Se encontraron ${specialities.length} specialities en el archivo.`);

        // Recorremos los datos leídos del CSV
        for (const speciality of specialities) {
            // Definimos la consulta SQL de forma más limpia en una sola línea
            const query = "INSERT IGNORE INTO specialities(speciality) VALUES (?)";

            // Validamos y limpiamos los valores antes de la inserción
            const specialityValue = speciality.speciality ? speciality.speciality.trim() : null;

            const values = [
                specialityValue
            ];

            // Verificamos si los valores principales no son nulos para evitar errores
            if (specialityValue) {
                try {
                    await connection.execute(query, values);
                    console.log(`Especialidad "${speciality} insertado correctamente.`);
                } catch (err) {
                    console.error(`Error insertando ubicacion  "${name} ${email}":`, err);
                }
            } else {
                console.log('Fila omitida debido a datos incompletos.');
            }
        }

        console.log('Proceso de carga de especialidades finalizado.');
    } catch (err) {
        console.error('Ocurrió un error:', err);
    } finally {
        await connection.end();
        console.log('Conexión a la base de datos cerrada.');
    }
}

loadSpecialitiesFromCSV();