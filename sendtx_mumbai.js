const Web3 = require('web3')
const fs = require('fs')
const AWS = require('aws-sdk')
var parquet = require('parquetjs-lite')
const moment = require('moment');
require("dotenv").config();

let rpc = process.env.PUBLIC_RPC_URL;
const provider = new Web3.providers.HttpProvider(rpc);
const web3 = new Web3(provider);

var PrevNonce = 0; 

async function makeParquetFile(data) {
  var schema = new parquet.ParquetSchema({
      executedAt:{type:'TIMESTAMP_MILLIS'},
      txhash:{type:'UTF8'},
      startTime:{type:'TIMESTAMP_MILLIS'},
      endTime:{type:'TIMESTAMP_MILLIS'},
      chainId:{type:'INT64'},
      latency:{type:'INT64'},
      error:{type:'UTF8'}
  })

  var d = new Date()
  //20220101_032921
  var datestring = moment().format('YYYYMMDD_HHmmss')

  var filename = `${datestring}.parquet`

  // create new ParquetWriter that writes to 'filename'
  var writer = await parquet.ParquetWriter.openFile(schema, filename);

  await writer.appendRow(data)

  writer.close()

  return filename;
}

async function uploadToS3(data){
  const s3 = new AWS.S3();
  const filename = await makeParquetFile(data)
  const param = {
    'Bucket':process.env.S3_BUCKET,
    'Key':filename,
    'Body':fs.createReadStream(filename),
    'ContentType':'application/octet-stream'
  }
  await s3.upload(param).promise()

  fs.unlinkSync(filename) 
}

async function sendTx(){
  var data = {
    executedAt: new Date().getTime(),
    txhash: '',
    startTime: 0,
    endTime: 0,
    chainId: 0,
    latency:0,
    error:'',
  }

  try{
    // Add your private key 
    const signer = web3.eth.accounts.privateKeyToAccount(
        process.env.SIGNER_PRIVATE_KEY
    );
    const balance = await web3.eth.getBalance(signer.address); //in wei  

    if(balance*(10**(-18)) < parseFloat(process.env.BALANCE_ALERT_CONDITION_IN_MATIC))
    { 
      console.log(`Current balance of ${addr} is less than ${process.env.BALANCE_ALERT_CONDITION_IN_MATIC} ! balance=${balance}`)
    }
    
    const latestNonce = await web3.eth.getTransactionCount(signer.address)
    if (latestNonce == PrevNonce) 
    {
      console.log(`Nonce ${latestNonce} = ${PrevNonce}`)
      return;
    }
    else{
      console.log(`Nonce ${latestNonce} != ${PrevNonce}`)
      PrevNonce = latestNonce
    }

    // Calculate maxPriorityFeePerGas based on Fee History 
    // https://web3js.readthedocs.io/en/v1.5.0/web3-eth.html#getfeehistory
    var maxPriorityFeePerGas; 
    await web3.eth.getFeeHistory(1, "latest", [25, 50, 75]).then((result)=>{
      maxPriorityFeePerGas = web3.utils.toHex(Number(result.reward[0][1]).toString()) //in wei 
    });

    //create value transfer transaction (EIP-1559) 
    const tx = {
      type: 2,
      from: signer.address,
      to:  signer.address,
      value: web3.utils.toHex(web3.utils.toWei("0", "ether")),
      gas: 8000000,
      maxPriorityFeePerGas,
    }

    //Sign to the transaction
    var RLPEncodedTx;
    await web3.eth.accounts.signTransaction(tx, process.env.SIGNER_PRIVATE_KEY)
    .then((result) => {
      RLPEncodedTx = result.rawTransaction // RLP encoded transaction & already HEX value
      data.txhash = result.transactionHash // the transaction hash of the RLP encoded transaction.
    });
 
    await web3.eth.net.getId().then((result)=>{
      data.chainId = result 
    })
    const start = new Date().getTime()
    data.startTime = start 
  
    await web3.eth
    .sendSignedTransaction(RLPEncodedTx) // Signed transaction data in HEX format 
    .on('receipt', function(receipt){
      data.txhash = receipt.transactionHash
      const end = new Date().getTime()
      data.endTime = end
      data.latency = end-start
      console.log(`${data.executedAt},${data.chainId},${data.txhash},${data.startTime},${data.endTime},${data.latency},${data.error}`) 
    })
  } catch(err){
    console.log("failed to execute.", err.toString())
    data.error = err.toString()
    console.log(`${data.executedAt},${data.chainId},${data.txhash},${data.startTime},${data.endTime},${data.latency},${data.error}`)
  }
  try{
    await uploadToS3(data)
  } catch(err){
    console.log('failed to s3.upload', err.toString())
  }
}

async function main(){
  const start = new Date().getTime()
  console.log(`starting tx latency measurement... start time = ${start}`)

  // run sendTx every SEND_TX_INTERVAL
  const interval = eval(process.env.SEND_TX_INTERVAL)
  setInterval(()=>{
    sendTx()
  }, interval)

}

main();