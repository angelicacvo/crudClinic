const fs = require('fs');
const csv = require('csv-parser');
const mysql = require('mysql2/promise');
require('dotenv').config();

async function loadAppointmentsFromCSV() {
    const connection = await mysql.createConnection({
        host: process.env.HOST,
        user: process.env.USER,
        password: process.env.PASSWORD,
        database: process.env.DATABASE,
        port: process.env.PORT
    });
    console.log('Conectado a la base de datos:', process.env.DATABASE);

    try {
        const appointments = [];

        // Usamos una promesa para manejar la lectura del CSV de forma síncrona
        await new Promise((resolve, reject) => {
            fs.createReadStream('appointments.csv')
                .pipe(csv())
                .on('data', (data) => {
                    appointments.push(data);
                })
                .on('end', () => {
                    console.log('Lectura del archivo CSV finalizada.');
                    resolve();
                })
                .on('error', (err) => {
                    reject(err);
                });
        });

        console.log(`Se encontraron ${appointments.length} appointments en el archivo.`);

        // Recorremos los datos leídos del CSV
        for (const appointment of appointments) {
            // Definimos la consulta SQL de forma más limpia en una sola línea
            const query = "INSERT IGNORE INTO appointments(date, hour, reason, observations, payment_method, status, id_pacient, id_doctor, id_speciality, id_location) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            // Validamos y limpiamos los valores antes de la inserción
            const date = appointment.date ? appointment.date.trim() : null;
            const hour = appointment.hour ? appointment.hour.trim() : null;
            const reason = appointment.reason ? appointment.reason.trim() : null;
            const observations = appointment.observations ? appointment.observations.trim() : null;
            const payment_method = appointment.payment_method ? appointment.payment_method.trim() : null;
            const status = appointment.status ? appointment.status.trim() : null;
            const id_pacient = appointment.id_pacient ? appointment.id_pacient.trim() : null;
            const id_doctor = appointment.id_doctor ? appointment.id_doctor.trim() : null;
            const id_speciality = appointment.id_speciality ? appointment.id_speciality.trim() : null;
            const id_location = appointment.id_location ? appointment.id_location.trim() : null;

            const values = [
                date, hour, reason, observations, payment_method, status, id_pacient, id_doctor, id_speciality, id_location
            ];

            // Verificamos si los valores principales no son nulos para evitar errores
            if (date && hour && reason && payment_method && status) {
                try {
                    await connection.execute(query, values);
                   console.log(`Cita del ${date} a las ${hour} insertada correctamente.`);


                } catch (err) {
                    console.error(`Error insertando cita "${date} ${hour} ${reason} ${observations} ${payment_method} ${status} ${id_pacient} ${id_doctor} ${id_speciality} ${id_location}":`, err);
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

loadAppointmentsFromCSV();