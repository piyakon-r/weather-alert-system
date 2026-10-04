const amqp = require('amqplib');

async function start() {
  try {
    const conn = await amqp.connect(`amqp://${process.env.RABBITMQ_HOST || 'localhost'}`);
    const channel = await conn.createChannel();
    const queue = 'weather_alerts';

    await channel.assertQueue(queue, { durable: true });
    channel.prefetch(1);

    console.log('[Worker] พร้อมรับคำสั่งแจ้งเตือนจาก RabbitMQ...');

    channel.consume(queue, (msg) => {
      if (msg !== null) {
        const payload = JSON.parse(msg.content.toString());
        console.log(`[ALERT DISPATCHED 📢] ตรวจพบฝนตกที่: ${payload.location} (เวลา: ${payload.time})`);
        
        // จุดสำหรับต่อ API ภายนอก เช่น LINE Notify หรือ Discord Webhook
        
        channel.ack(msg); // ยืนยันว่างานเสร็จสิ้น เพื่อลบข้อความออกจากคิวอย่างปลอดภัย[cite: 2]
      }
    });
  } catch (err) {
    console.error('[Worker] Connection failed, retrying in 5s...', err.message);
    setTimeout(start, 5000);
  }
}

start();