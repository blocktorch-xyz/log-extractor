const LogExtractor = require('./log-extractor')
const express = require('express')

const app = express()
const port = process.env.PORT

require('events').EventEmitter.defaultMaxListeners = 100
const extractor = new LogExtractor()

app.get('/', (req, res) => {
  res.send('log collector');
});

app.listen(port, async () => {
  console.log(`Server listening on port ${port}`)
  await extractor.watch()
})
