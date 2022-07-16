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

const ingestDecodedLog = async (rawLog, decodedLog, txs) => {
  let indexName = 'decoded-evm-logs'
  await ingestEthEventLog(rawLog, decodedLog, txs, indexName, ['decoded'])
}
const ingestNotParsedLog = async (log, txs) => {
  let indexName = 'not-parsed-evm-logs'
  await ingestEthEventLog(log, {}, txs, indexName, ['not-decoded', 'not-parsed'])
}
const ingestNotDecodedLog = async (log, txs) => {
  let indexName = 'not-decoded-evm-logs'
  await ingestEthEventLog(log, {}, txs, indexName, [
    'not-decoded',
    'abi-not-available',
  ])
}
const ingestEmptyLog = async (log, txs) => {
  let indexName = 'empty-evm-logs'
  let logId = Math.floor(Math.random() * 100)
  await ingestEthEventLog(log, log, txs, indexName, ['empty-log'])
}

const ingestEthEventLog = async (rawlog, decodedLog, txs, indexName, additionalTags) => {
  let tags = [process.env.CHAIN, ...additionalTags]

  const document = {
    index: indexName,
    body: {
      type: "event",
      // TODO: needs to be fetched from etherscan
      contract: "unknown",
      from: txs.from,
      // abi is needed to decode the event name
      name: !decodedLog || Object.keys(decodedLog).length === 0 ? "unknown" : decodedLog.eventFragment.name,
      status: txs.status === true ? "success" : "fail",
      chain: process.env.CHAIN,
      tags: tags,
      blockNumber: txs.blockNumber,
      // TODO: propagate block timestamp
      timestamp: new Date(),
      metadata: decodedLog,
      rawData: rawlog,
      address: txs.to
    },
  }
  try {
    console.log(`🏁 ingesting event log to index: ${indexName}`)
    console.log(`🌱 Ingesting event log: ${JSON.stringify(document, null, 2)}`)
    await elasticClient.index(document)
    await elasticClient.indices.refresh({ index: indexName })
  } catch (error) {
    console.log(
      `❌ error ingesting eth event log: ${error} in index ${indexName} and log: ${rawlog}`,
    )
  }
}

const ingestEthTransaction = async (parsedTxs, rawTxs, additionalTags) => {
  let tags = [process.env.CHAIN, ...additionalTags]
  const decodedTransactionIndex = !parsedTxs || Object.keys(parsedTxs).length === 0 ? 'not-decoded-evm-transactions' : 'decoded-evm-transactions'

  const document = {
    index: decodedTransactionIndex,
    body: {

      type: "function",
      // TODO: needs to be fetched from etherscan
      contract: "unknown",
      from: rawTxs.from,
      // abi is needed to decode the event name
      name: !parsedTxs || Object.keys(parsedTxs).length === 0 ? "unknown" : parsedTxs.name,
      status: rawTxs.status === true ? "success" : "fail",
      chain: process.env.CHAIN,
      tags: tags,
      blockNumber: rawTxs.blockNumber,
      // TODO: propagate block timestamp
      timestamp: new Date(),
      metadata: parsedTxs,
      rawData: rawTxs,
      address: rawTxs.to
    }
  }

  try {
    console.log(`🏁 Ingesting transaction to index: ${decodedTransactionIndex}`)
    console.log(`🌈 Ingesting transaction: ${JSON.stringify(document, null, 2)}`)
    await elasticClient.index(document)
    await elasticClient.indices.refresh({ index: decodedTransactionIndex })
  } catch (error) {
    console.log(
      `❌ could't index a transaction in index ${decodedTransactionIndex}. error: ${error}. transaction: ${rawTxs}`,
    )
  }
}

const transformAndLoad = async (block, contractAddresses, abis) => {
  await Promise.all(
    block.transactions.map(async (txs) => {
      // check if address is a contract address
      const isContractAddress = await isContract(txs.to)
      // console.log(`${txs.to} is a contracts? ${isContractAddress}`)
      
      if (isContractAddress) {
        contractAddresses.push(txs.to)
        
        try {
          // get abis
          if (txs.logs && txs.logs.length > 0) {
            const abiRes = await fetchContractABI(txs.to).catch(console.log)
            abis[txs.to] = abiRes
            if (abiRes.length > 0) {
              const iface = new ethers.utils.Interface(abiRes)

              // try parsing transaction
              const parsedTransaction = iface.parseTransaction({data: txs.input, value: txs.value}) || {}

              await ingestEthTransaction(parsedTransaction, txs,["parsed-transaction"]).catch(console.log)

              // TODO: missing rate limit handling
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
                      let parsedLog = iface.parseLog(log)
                      await ingestDecodedLog(log, parsedLog, txs).catch(console.log)
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
              // ABI not available ingest raw transaction data
              console.log(`------> ${abiRes.length}`)
              console.log(
                `⚠️ (type: ${typeof abiRes}) missed ${
                  txs.to
                } // etherscan abi API responded: ${abiRes}`,
              )
            }
          } else {
            // transaction has no logs
            await ingestEmptyLog({}, txs).catch(console.log)
            console.log(
              `✅ ingested txs without logs: ${JSON.stringify(txs.logs)}`,
            )
          }
        } catch (error) {
          console.log(`❌ Ingestion failed with error ${error} || failed at transaction ${txs}`)
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
        process.env.RPC_NETWORK,
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
        }, process.env.TIMEOUT)
      })

      provider.on('error', async (error) => {
        console.log(`❌ stream emitted tge following error: ${error}`)
      })
    } catch (error) {
      console.log(`❌ ${error}`)
    }
  }
}

module.exports = LogExtractor
