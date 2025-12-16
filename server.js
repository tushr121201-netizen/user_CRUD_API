const express = require('express');
const cors = require('cors');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

app.get('/health', (req, res) => {
    res.json({ status: 'ok' });
});

const userRoutes = require('./routes/userRoutes');
app.use('/users', userRoutes);

const fs = require('fs');
const path = require('path');
const { pool } = require('./config/db');

const startServer = async () => {
    try {
        const initSql = fs.readFileSync(path.join(__dirname, 'db', 'init.sql'), 'utf8');
        await pool.query(initSql);
        console.log('Database initialized successfully');

        app.listen(port, () => {
            console.log(`Server running on port ${port}`);
        });
    } catch (err) {
        console.error('Failed to initialize database:', err);
        process.exit(1);
    }
};

startServer();
