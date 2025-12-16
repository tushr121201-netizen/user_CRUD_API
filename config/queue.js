const { Queue } = require('bullmq');
const redisClient = require('./redis');

// BullMQ needs a separate connection object or connection options, 
// it manages its own connections. We can pass the redis URL.
// Since we already have REDIS_URL in env, we use that.

const connection = {
    url: process.env.REDIS_URL
};

// If using a shared redis instance/url, the 'connection' object 
// for BullMQ can be just { url: ... } or { host, port }. 
// 'bullmq' handles the connection logic.

const userQueue = new Queue('user-operations', {
    connection: {
        url: process.env.REDIS_URL
    }
});

module.exports = {
    userQueue
};
