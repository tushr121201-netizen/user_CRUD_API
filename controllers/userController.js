const db = require('../config/db');
const redisClient = require('../config/redis');

const CACHE_EXPIRATION = 3600; 
const createUser = async (req, res) => {
    try {
        const { name, email, age } = req.body;
        const result = await db.query(
            'INSERT INTO users (name, email, age) VALUES ($1, $2, $3) RETURNING *',
            [name, email, age]
        );
        const newUser = result.rows[0];


        res.status(201).json(newUser);
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Server error' });
    }
};

const getUserById = async (req, res) => {
    try {
        const { id } = req.params;
        const cacheKey = `user:${id}`;

        const cachedUser = await redisClient.get(cacheKey);
        if (cachedUser) {
            return res.json(JSON.parse(cachedUser));
        }

        const result = await db.query('SELECT * FROM users WHERE id = $1', [id]);
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'User not found' });
        }

        const user = result.rows[0];
        await redisClient.setEx(cacheKey, CACHE_EXPIRATION, JSON.stringify(user));

        res.json(user);
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Server error' });
    }
};

const updateUser = async (req, res) => {
    try {
        const { id } = req.params;
        const { name, email, age } = req.body;

        const result = await db.query(
            'UPDATE users SET name = COALESCE($1, name), email = COALESCE($2, email), age = COALESCE($3, age) WHERE id = $4 RETURNING *',
            [name, email, age, id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'User not found' });
        }

        const updatedUser = result.rows[0];
        const cacheKey = `user:${id}`;

        // Update cache
        await redisClient.setEx(cacheKey, CACHE_EXPIRATION, JSON.stringify(updatedUser));

        res.json(updatedUser);
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Server error' });
    }
};

const deleteUser = async (req, res) => {
    try {
        const { id } = req.params;
        const result = await db.query('DELETE FROM users WHERE id = $1 RETURNING *', [id]);

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'User not found' });
        }

        const cacheKey = `user:${id}`;
        await redisClient.del(cacheKey);

        res.json({ message: 'User deleted successfully' });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Server error' });
    }
};

const listUsers = async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 10;
        const offset = (page - 1) * limit;

        const countResult = await db.query('SELECT COUNT(*) FROM users');
        const total = parseInt(countResult.rows[0].count);

        const result = await db.query('SELECT * FROM users ORDER BY id LIMIT $1 OFFSET $2', [limit, offset]);

        res.json({
            users: result.rows,
            page,
            limit,
            total,
            totalPages: Math.ceil(total / limit)
        });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Server error' });
    }
};

module.exports = {
    createUser,
    getUserById,
    updateUser,
    deleteUser,
    listUsers
};
