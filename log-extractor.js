const axios = require('axios')
const fs = require('fs')
require('dotenv').config()
const { ethers } = require('ethers')
const rateLimit = require('axios-rate-limit')
const { Client } = require('@elastic/elasticsearch')


const elasticClient = new Client({
  cloud: {
    id: process.env.ELASTIC_CLOUD_ID,
  },
  auth: {
    username: process.env.ELASTIC_USER,
    password: process.env.ELASTIC_PASSWORD,
  },
})

elasticClient
  .info()
  .then((response) =>
    console.log(
      `started elastic client:\n ${JSON.stringify(response, null, 2)}`,
    ),
  )
  .catch((error) => console.error(`❌ ${error}`))

const http = rateLimit(axios.create(), {
  maxRequests: 1,
  perMilliseconds: 2000,
  maxRPS: 1,
})

const tatumHeaders = {
  'x-api-key': process.env.TATUM_API_KEY_2,
}
const provider = new ethers.providers.AlchemyProvider(
  'homestead',
  process.env.ALCHEMY_API_KEY,
)

const isContract = async (address) => {
  let addressCode = await provider.getCode(address)
  return addressCode !== '0x'
}

const fetchContractABI = async (contractAddress) => {
  http.getMaxRPS()
  console.log(`fethcing abi for ${contractAddress}`)
  try {
    const abiResponse = await http.get(
      `https://api.etherscan.io/api?module=contract&action=getabi&address=${contractAddress}&apikey=${process.env.ETHERSCAN_API_KEY}`,
    )
    return abiResponse.data.result
  } catch (error) {
    console.log(`❌ ${error}`)
  }
}

const fetchBlockFromTatum = async (currentBlockNum) => {
  try {
    const blockRes = await axios.get(
      `${process.env.TATUM_URL}/${currentBlockNum}`,
      tatumHeaders,
    )
    return blockRes.data
  } catch (error) {
    console.log(`❌ ${error}`)
  }
}

const fetchCurrentBlockNum = async () => {
  try {
    let config = {
      url: `${process.env.TATUM_URL}/current`,
      headers: tatumHeaders,
    }

    const currentBlockRes = await http.get(
      `${process.env.TATUM_URL}/current`,
      tatumHeaders,
    )
    return currentBlockRes.data
  } catch (error) {
    console.log(`❌ ${error}`)
  }
}

const ingestDecodedLog = async (log, txs) => {
  let indexName = 'decoded-evm-logs'
  await ingest(JSON.stringify(log), txs, indexName, log.id, ['decoded'])
}
const ingestNotParsedLog = async (log, txs) => {
  let indexName = 'not-parsed-evm-logs'
  await ingest(log, txs, indexName, log.id, ['not-decoded', 'not-parsed'])
}
const ingestNotDecodedLog = async (log, txs) => {
  let indexName = 'not-decoded-evm-logs'
  await ingest(log, txs, indexName, log.id, [
    'not-decoded',
    'abi-not-available',
  ])
}
const ingestEmptyLog = async (log, txs) => {
  let indexName = 'empty-evm-logs'
  let logId = Math.floor(Math.random() * 100)
  await ingest(log, txs, indexName, logId, ['empty-log'])
}

const ingest = async (log, txs, indexName, logId, additionalTags) => {
  let tags = ['ethereum', 'contract', ...additionalTags]

  const document = {
    index: indexName,
    body: {
      logsID: logId,
      logs: log,
      txs: txs,
      chain: 'ethereum',
      tags: tags,
      blockNumber: txs.blockNumber,
      timestamp: new Date(),
    },
  }
  try {
    console.log(`🏁 ingesting to index: ${indexName}`)
    await elasticClient.index(document)
    await elasticClient.indices.refresh({ index: indexName })
  } catch (error) {
    console.log(
      `❌ error ingesting: ${error} in index ${indexName} and log: ${log}`,
    )
  }
}

const transformAndLoad = async (block, contractAddresses, abis) => {
  await Promise.all(
    block.transactions.map(async (txs) => {
      // check if address is a contract address
      const isContractAddress = await isContract(txs.to)
      console.log(`${txs.to} is a contracts? ${isContractAddress}`)

      if (isContractAddress) {
        contractAddresses.push(txs.to)

        try {
          // get abis
          if (txs.logs.length > 0) {
            const abiRes = await fetchContractABI(txs.to)
            abis[txs.to] = abiRes
            if (abiRes.length > 0) {
              // TODO: missing rate limite handling
              // try to decode and ingest logs
              await Promise.all(
                txs.logs.map(async (log) => {
                  try {
                    if (abiRes === 'Contract source code not verified') {
                      await ingestNotDecodedLog(log, txs).catch(console.log)
                      console.log(
                        `✅ ingested not decoded txs: ${JSON.stringify(log)}`,
                      )
                    } else {
                      const iface = new ethers.utils.Interface(abiRes)
                      let parsedLog = iface.parseLog(log)
                      await ingestDecodedLog(parsedLog, txs).catch(console.log)
                      console.log(
                        `✅ ingested decoded txs: ${JSON.stringify(parsedLog)}`,
                      )
                    }
                  } catch (error) {
                    if (abiRes === 'Contract source code not verified') {
                      console.log(
                        `❌ [couldn't ingest not decoded log] error: ${error} // log: ${JSON.stringify(
                          log,
                        )}`,
                      )
                    } else {
                      console.log(
                        `⚠️ proceeding with ingestion without parsing because parsing/ingesting didn't work for the following log: ${JSON.stringify(
                          log,
                        )}`,
                      )
                      await ingestNotParsedLog(log, txs).catch(console.log)
                    }
                  }
                }),
              )
            } else {
              console.log(`------> ${abiRes.length}`)
              console.log(
                `⚠️ (type: ${typeof abiRes}) missed ${
                  txs.to
                } // etherscan abi API responded: ${abiRes}`,
              )
            }
          } else {
            await ingestEmptyLog({}, txs).catch(console.log)
            console.log(
              `✅ ingested txs without logs: ${JSON.stringify(txs.logs)}`,
            )
          }
        } catch (error) {
          console.log(`❌ ${error}`)
        }
      }
    }),
  )
}

class LogExtractor {
  async run() {
    try {
      const currentBlockNum = await fetchCurrentBlockNum()
      let block = await fetchBlockFromTatum(currentBlockNum)

      console.log(
        `current block is ${JSON.stringify(currentBlockNum, null, 2)}`,
      )

      let contractAddresses = []
      let abis = {}

      await transformAndLoad(block, contractAddresses, abis)
    } catch (error) {
      console.log(`❌ ${error}`)
    }
  }

  async watch() {
    try {
      const provider = new ethers.providers.AlchemyWebSocketProvider(
        'homestead',
        process.env.ALCHEMY_API_KEY,
      )

      provider.on('block', async (blockNum) => {
        console.log(`🔔 block ${blockNum} is mined`)
        setTimeout(async () => {
          let block = await fetchBlockFromTatum(blockNum)
          console.log(`✅ Fetched block ${block.transactions[0].blockNumber}`)
          let contractAddresses = []
          let abis = {}

          await transformAndLoad(block, contractAddresses, abis)
        }, 180 * 1000)
      })
    } catch (error) {
      console.log(`❌ ${error}`)
    }
  }
}

module.exports = LogExtractor
