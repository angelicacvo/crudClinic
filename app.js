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

// loadPacientsFromCSV();

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

// loadDoctorsFromCSV();

async function loadLocationsFromCSV() {
    const connection = await mysql.createConnection({
        host: process.env.HOST,
        user: process.env.USER,
        password: process.env.PASSWORD,
        database: process.env.DATABASE,
        port: process.env.PORT
    });
    console.log('Conectado a la base de datos:', process.env.DATABASE);

    try {
        const locations = [];

        // Usamos una promesa para manejar la lectura del CSV de forma síncrona
        await new Promise((resolve, reject) => {
            fs.createReadStream('locations.csv')
                .pipe(csv())
                .on('data', (data) => {
                    locations.push(data);
                })
                .on('end', () => {
                    console.log('Lectura del archivo CSV finalizada.');
                    resolve();
                })
                .on('error', (err) => {
                    reject(err);
                });
        });

        console.log(`Se encontraron ${locations.length} locationes en el archivo.`);

        // Recorremos los datos leídos del CSV
        for (const location of locations) {
            // Definimos la consulta SQL de forma más limpia en una sola línea
            const query = "INSERT IGNORE INTO locations(location) VALUES (?)";

            // Validamos y limpiamos los valores antes de la inserción
            const locationValue  = location.location ? location.location.trim() : null;

            const values = [
                locationValue 
            ];

            // Verificamos si los valores principales no son nulos para evitar errores
            if (locationValue ) {
                try {
                    await connection.execute(query, values);
                    console.log(`Ubicacion "${location} insertado correctamente.`);
                } catch (err) {
                    console.error(`Error insertando ubicacion  "${name} ${email}":`, err);
                }
            } else {
                console.log('Fila omitida debido a datos incompletos.');
            }
        }

        console.log('Proceso de carga de ubicaciones finalizado.');
    } catch (err) {
        console.error('Ocurrió un error:', err);
    } finally {
        await connection.end();
        console.log('Conexión a la base de datos cerrada.');
    }
}

// loadLocationsFromCSV();

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
            const specialityValue  = speciality.speciality ? speciality.speciality.trim() : null;

            const values = [
                specialityValue 
            ];

            // Verificamos si los valores principales no son nulos para evitar errores
            if (specialityValue ) {
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




