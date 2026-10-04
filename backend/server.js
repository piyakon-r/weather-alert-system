const express = require('express');
const cors = require('cors');
const amqp = require('amqplib');
const Redis = require('ioredis');
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json());

const redis = new Redis({
  host: process.env.REDIS_HOST || '127.0.0.1',
  retryStrategy: () => 2000
});

const pool = new Pool({
  host: process.env.DB_HOST || '127.0.0.1',
  user: 'user',
  password: 'secretpassword',
  database: 'weather_db'
});

let rabbitChannel = null;
async function connectRabbitMQ() {
  try {
    const conn = await amqp.connect(`amqp://${process.env.RABBITMQ_HOST || 'localhost'}`);
    rabbitChannel = await conn.createChannel();
    await rabbitChannel.assertQueue('weather_alerts', { durable: true });
    console.log('Connected to RabbitMQ');
  } catch (err) {
    console.error('RabbitMQ connection error, retrying in 5s...', err.message);
    setTimeout(connectRabbitMQ, 5000);
  }
}
connectRabbitMQ();

// Middleware ตรวจสอบสิทธิ์ (Auth)[cite: 1]
function authMiddleware(req, res, next) {
  const authHeader = req.headers['authorization'];
  if (authHeader === 'Bearer pico_secret_token_123') {
    return next();
  }
  return res.status(401).json({ error: 'Unauthorized: Invalid token' });
}

// 1. รับข้อมูลจาก Pi Pico (Node 1 หรือ Node 2)[cite: 1]
app.post('/api/sensors', authMiddleware, async (req, res) => {
  const { location_id, temperature, humidity, is_raining } = req.body;

  // Data Validation[cite: 3]
  if (!location_id || temperature === undefined || humidity === undefined || is_raining === undefined) {
    return res.status(400).json({ error: 'Invalid payload schema' });
  }

  const payload = {
    location_id,
    temperature: parseFloat(temperature),
    humidity: parseFloat(humidity),
    is_raining: Boolean(is_raining),
    updated_at: new Date().toISOString()
  };

  try {
    // เก็บสถานะล่าสุดลง Redis[cite: 1]
    await redis.set(`sensor:${location_id}:latest`, JSON.stringify(payload));

    // บันทึกลง PostgreSQL เป็น Log[cite: 1]
    await pool.query(
      'INSERT INTO sensor_logs (location_id, temperature, humidity, is_raining) VALUES ($1, $2, $3, $4)',
      [location_id, payload.temperature, payload.humidity, payload.is_raining]
    );

    // หากฝนตก ให้ส่ง Task เข้า RabbitMQ ให้ Worker ทำงานเบื้องหลัง[cite: 1]
    if (payload.is_raining && rabbitChannel) {
      const message = JSON.stringify({
        event: 'RAIN_ALERT',
        location: location_id,
        time: payload.updated_at
      });
      rabbitChannel.sendToQueue('weather_alerts', Buffer.from(message), { persistent: true });
    }

    res.status(200).json({ status: 'ok', data: payload });
  } catch (err) {
    console.error('Core API Error:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// 2. ส่งข้อมูลล่าสุดให้ Web Dashboard (มี Fallback ถ้า Redis ล่ม ให้ดึงจาก Postgres)[cite: 1, 3]
app.get('/api/sensors/:location_id/latest', async (req, res) => {
  const { location_id } = req.params;

  try {
    let cached = null;
    try {
      cached = await redis.get(`sensor:${location_id}:latest`);
    } catch (e) {
      console.warn('Redis down, falling back to PostgreSQL');
    }

    if (cached) {
      return res.json(JSON.parse(cached));
    }

    const result = await pool.query(
      'SELECT location_id, temperature, humidity, is_raining, recorded_at AS updated_at FROM sensor_logs WHERE location_id = $1 ORDER BY recorded_at DESC LIMIT 1',
      [location_id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Data not found for this location' });
    }

    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: 'Server error querying sensor data' });
  }
});

app.listen(8080, () => console.log('Core API listening on port 8080'));