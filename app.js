const fs = require('fs');
const csv = require('csv-parser');
const mysql = require('mysql2/promise');

async function loadPacientsFromCSV() {
  const connection = await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'password',
    database: 'sales_management',
    port: 3307
  });

  try {
    const products = [];

    // Usamos una promesa para manejar la lectura del CSV de forma síncrona
    await new Promise((resolve, reject) => {
      fs.createReadStream('products.csv')
        .pipe(csv())
        .on('data', (data) => {
          products.push(data);
        })
        .on('end', () => {
          console.log('Lectura del archivo CSV finalizada.');
          resolve();
        })
        .on('error', (err) => {
          reject(err);
        });
    });

    console.log(`Se encontraron ${products.length} productes en el archivo.`);

    // Recorremos los datos leídos del CSV
    for (const product of products) {
      // Definimos la consulta SQL de forma más limpia en una sola línea
      const query = "INSERT IGNORE INTO products(name, lastname, school, phone_number) VALUES (?, ?, ?, ?)";
      
      // Validamos y limpiamos los valores antes de la inserción
      const name = product.name ? product.name.trim() : null;
      const lastname = product.lastname ? product.lastname.trim() : null;
      const school = product.school ? product.school.trim() : null;
      const phoneNumber = product.phone_number ? parseInt(product.phone_number.trim(), 10) : null;
      
      const values = [
        name,
        lastname,
        school,
        phoneNumber
      ];
      
      // Verificamos si los valores principales no son nulos para evitar errores
      if (name && lastname) {
        try {
          await connection.execute(query, values);
          console.log(`producte "${name} ${lastname}" insertado correctamente.`);
        } catch (err) {
          console.error(`Error insertando al producte "${name} ${lastname}":`, err);
        }
      } else {
        console.log('Fila omitida debido a datos incompletos.');
      }
    }

    console.log('Proceso de carga de productes finalizado.');
  } catch (err) {
    console.error('Ocurrió un error:', err);
  } finally {
    await connection.end();
    console.log('Conexión a la base de datos cerrada.');
  }
}

loadproductsFromCSV();

//
async function loadProductsFromCSV() {
  const connection = await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'password',
    database: 'sales_management',
    port: 3307
  });

  try {
    const products = [];

    // Usamos una promesa para manejar la lectura del CSV de forma síncrona
    await new Promise((resolve, reject) => {
      fs.createReadStream('products.csv')
        .pipe(csv())
        .on('data', (data) => {
          products.push(data);
        })
        .on('end', () => {
          console.log('Lectura del archivo CSV finalizada.');
          resolve();
        })
        .on('error', (err) => {
          reject(err);
        });
    });

    console.log(`Se encontraron ${products.length} productes en el archivo.`);

    // Recorremos los datos leídos del CSV
    for (const product of products) {
      // Definimos la consulta SQL de forma más limpia en una sola línea
      const query = "INSERT IGNORE INTO products(name) VALUES (?)";
      
      // Validamos y limpiamos los valores antes de la inserción
      const name = product.name ? product.name.trim() : null;
           
      const values = [
        name
      ];
      
      // Verificamos si los valores principales no son nulos para evitar errores
      // if (name) {
      //   try {
      //     await connection.execute(query, values);
      //     console.log(`producte "${name}" insertado correctamente.`);
      //   } catch (err) {
      //     console.error(`Error insertando al producte "${name}":`, err);
      //   }
      // } else {
      //   console.log('Fila omitida debido a datos incompletos.');
      // }
    }

    console.log('Proceso de carga de productos finalizado.');
  } catch (err) {
    console.error('Ocurrió un error:', err);
  } finally {
    await connection.end();
    console.log('Conexión a la base de datos cerrada.');
  }
}

loadPacientsFromCSV();