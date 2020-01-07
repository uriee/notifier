const amqp = require('amqplib');
const createSubscriber  = require ('pg-listen'); 

const send = (queue,msg) => amqp.connect('amqp://localhost').then(function(conn) {
  return conn.createConfirmChannel().then(function(ch) {

    var ok = ch.assertQueue(queue, {durable: false});

    return ok.then(async function(_qok) {
      // NB: `sentToQueue` and `publish` both return a boolean
      // indicating whether it's OK to send again straight away, or
      // (when `false`) that you should wait for the event `'drain'`
      // to fire before writing again. We're just doing the one write,
      // so we'll ignore it.
      ch.sendToQueue(queue, Buffer.from(JSON.stringify(msg)));
      await ch.waitForConfirms();
      console.log(" [x] Sent '%s'", msg);
      return ch.close();
    });
  }).finally(function() { conn.close(); });
}).catch(console.warn);

const databaseURL = "postgres://admin:cbtadmin@192.9.200.102:5432/mymes";
//const databaseURL = "postgres://Administrator:Seabetee_admin@mymes.cwmyungyzkuu.us-east-2.rds.amazonaws.com:5432/demo";
// Accepts the same connection config object that the "pg" package would take
const subscriber = createSubscriber({ connectionString: databaseURL })

subscriber.notifications.on("notifications", (payload) => {
  // Payload as passed to subscriber.notify() (see below)
  console.log("Received notification :", payload)
  send(payload.username ,payload)
})

subscriber.events.on("error", (error) => {
  console.error("Fatal database connection error:", error)
  process.exit(1)
})
subscriber.events.on("reconnect", (x) => {
  console.log("reconnect:", x)
})


process.on("exit", () => {
  subscriber.close()
})
// connect to rabbitMQ


const connect = async ()=> {
    try {
      await subscriber.connect()
      await subscriber.listenTo("notifications")
    }catch(e){console.log(e)}
    console.log('Subscribed to DB')    
  }


connect() 



