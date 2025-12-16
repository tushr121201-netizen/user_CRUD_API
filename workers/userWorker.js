const { Worker } = require('bullmq');
const { pool } = require('../config/db');

const processBulkCreate = async (users) => {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        // Batch size for processing
        const batchSize = 100;

        for (let i = 0; i < users.length; i += batchSize) {
            const batch = users.slice(i, i + batchSize);

            // Construct bulk insert query
            // $1, $2, $3, $4, $5, $6...
            const values = [];
            const placeholders = [];

            batch.forEach((user, idx) => {
                const paramsOffset = idx * 3;
                placeholders.push(`($${paramsOffset + 1}, $${paramsOffset + 2}, $${paramsOffset + 3})`);
                values.push(user.name);
                values.push(user.email);
                values.push(user.age);
            });

            const query = `
                INSERT INTO users (name, email, age) 
                VALUES ${placeholders.join(', ')}
            `;

            await client.query(query, values);
        }

        await client.query('COMMIT');
        return { processed: users.length, status: 'completed' };
    } catch (err) {
        await client.query('ROLLBACK');
        throw err;
    } finally {
        client.release();
    }
};

const processBulkUpdate = async (users) => {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        // For updates, it's trickier to do one single query for different values.
        // We can use a common technique: UNNEST or multiple UPDATE statements.
        // For simplicity and compatibility, let's use a Transaction with individual updates 
        // but piped or just awaited in parallel/sequence.
        // Or better: update from a values list using a CTE (Common Table Expression).

        // Let's do batches of individual updates for simplicity, but grouped in transaction.
        // For 1000 users, 1000 queries in a transaction is okay for Postgres but a single query is better.
        // Let's try the CTE approach for better performance with 1000+ items.

        /*
        UPDATE users AS u
        SET 
            name = c.name,
            email = c.email,
            age = c.age
        FROM (VALUES 
            (1, 'Name1', 'email1', 20),
            (2, 'Name2', 'email2', 21)
        ) AS c(id, name, email, age) 
        WHERE c.id = u.id;
        */

        const batchSize = 100;

        for (let i = 0; i < users.length; i += batchSize) {
            const batch = users.slice(i, i + batchSize);

            const values = [];
            const valueStrings = [];

            batch.forEach((user, idx) => {
                // (id, name, email, age)
                const paramsOffset = idx * 4;
                valueStrings.push(`($${paramsOffset + 1}::integer, $${paramsOffset + 2}::text, $${paramsOffset + 3}::text, $${paramsOffset + 4}::integer)`);

                values.push(user.id);
                values.push(user.name);
                values.push(user.email);
                values.push(user.age);
            });

            const query = `
                UPDATE users AS u
                SET 
                    name = COALESCE(c.name, u.name),
                    email = COALESCE(c.email, u.email),
                    age = COALESCE(c.age, u.age)
                FROM (VALUES 
                    ${valueStrings.join(', ')}
                ) AS c(id, name, email, age) 
                WHERE c.id = u.id
            `;

            await client.query(query, values);
        }

        await client.query('COMMIT');
        return { processed: users.length, status: 'completed' };
    } catch (err) {
        await client.query('ROLLBACK');
        throw err;
    } finally {
        client.release();
    }
};

const initWorker = () => {
    const worker = new Worker('user-operations', async job => {
        console.log(`Processing job ${job.id} of type ${job.name}`);

        if (job.name === 'bulk-create') {
            return await processBulkCreate(job.data.users);
        } else if (job.name === 'bulk-update') {
            return await processBulkUpdate(job.data.users);
        }
    }, {
        connection: {
            url: process.env.REDIS_URL
        }
    });

    worker.on('completed', job => {
        console.log(`Job ${job.id} completed!`);
    });

    worker.on('failed', (job, err) => {
        console.error(`Job ${job.id} failed with ${err.message}`);
    });

    console.log('Worker initialized');
};

module.exports = { initWorker };
