#![allow(unused)]

mod cli;
mod errors;

use bitcoin::script::{self, Instruction};
use bitcoin::{ecdsa, Amount, Block, BlockHash, EcdsaSighashType, Transaction, Txid};
use bitcoincore_rpc::{Auth, Client, RpcApi};
use clap::Parser as _;
use cli::Cli;
use errors::Error;
use miniscript::bitcoin::{amount, taproot, TapSighashType};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self};
use std::time::Duration;

fn u64_to_spin(step: u64) -> String {
    match step % 4 {
        0 => "-".to_string(),
        1 => "\\".to_string(),
        2 => "|".to_string(),
        3 => "/".to_string(),
        4 => "/".to_string(),
        _ => "?".to_string(),
    }
}

fn erase_line() {
    print!("\x1B[1A\x1B[K");
}

enum Request {
    /// Run on this block
    Run(u64 /* block */),
    /// Update chain height
    Height(u64),
}

enum Response {
    /// Runner is initialized
    Initialized(usize /* runner_id */),
    /// Runner is idle
    Idle(usize /* runner_id */),
    /// Runner start processing task
    // Started(usize /* runner_id */),
    /// Task finnished
    Finished(usize /* runner_id */, Vec<String>),
    /// Runner errored
    Error(String),
}

struct Pool {
    senders: BTreeMap<usize, Sender<Request>>,
    receiver: Receiver<Response>,
    runner_sender: Sender<Response>,
    running_tip: u64,
    client: Client,
    runners: usize,
    url: String,
    auth: Auth,
    chain_tip: u64,
}

impl Pool {
    fn new(url: String, auth: Auth, start_height: u64, runners: usize) -> Self {
        let client = Client::new(&url, auth.clone()).unwrap();
        let (runner_sender, receiver) = mpsc::channel();
        Self {
            senders: BTreeMap::new(),
            receiver,
            running_tip: start_height,
            client,
            runners,
            runner_sender,
            url,
            auth,
            chain_tip: start_height,
        }
    }

    fn init(&mut self) {
        let block_height = self.client.get_block_count().unwrap();
        self.chain_tip = block_height;
        for i in 0..self.runners {
            let sender = start_runner(
                i,
                self.runner_sender.clone(),
                self.url.clone(),
                self.auth.clone(),
                self.running_tip,
            );
            if let Ok(Response::Initialized(id)) = self.receiver.recv() {
                assert!(i == id);
            } else {
                panic!("wrong init Response for runner {}", i);
            }
            sender.send(Request::Height(block_height)).unwrap();
            self.senders.insert(i, sender);
        }
    }

    fn start(&mut self) {
        // start all runners on a different block
        for sender in self.senders.values_mut() {
            sender.send(Request::Run(self.running_tip)).unwrap();
            self.running_tip += 1;
        }

        loop {
            match self.receiver.recv().unwrap() {
                Response::Initialized(_) => unreachable!(),
                Response::Idle(_) => return,
                Response::Finished(id, items) => {
                    if self.running_tip < self.chain_tip {
                        self.senders
                            .get_mut(&id)
                            .unwrap()
                            .send(Request::Run(self.running_tip))
                            .unwrap();
                    }
                    for item in items {
                        println!("{}", item);
                    }
                    if self.running_tip >= self.chain_tip {
                        return;
                    } else {
                        self.running_tip += 1;
                    }
                }
                Response::Error(e) => {
                    println!("{}", e);
                }
            }
        }
    }
}

fn start_runner(
    runner_id: usize,
    sender: Sender<Response>,
    url: String,
    auth: Auth,
    chain_tip: u64,
) -> Sender<Request> {
    let (pool_sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let client = Client::new(&url, auth).unwrap();
        let mut runner = BlockRunner::new(client, chain_tip);
        runner.init().unwrap();
        sender.send(Response::Initialized(runner_id)).unwrap();
        loop {
            if let Ok(resp) = receiver.recv() {
                match resp {
                    Request::Run(block_height) => {
                        let block = runner.fetch_block(block_height).unwrap();
                        let res = process_block(block, block_height, &runner);
                        sender.send(Response::Finished(runner_id, res)).unwrap();
                    }
                    Request::Height(_) => {}
                }
            } else {
                return;
            }
        }
    });
    pool_sender
}

struct BlockRunner {
    rpc: Client,
    chain_height: u64,
    fetch_block_height: u64,
}

impl BlockRunner {
    fn new(rpc: Client, start_height: u64) -> Self {
        BlockRunner {
            rpc,
            chain_height: 0u64,
            fetch_block_height: start_height,
        }
    }

    fn fetch_height(&self) -> u64 {
        self.fetch_block_height
    }

    fn fetch_chain_height(&self) -> Result<u64, Error> {
        self.rpc.get_block_count().map_err(|_| Error::RPCCallFail)
    }

    fn init(&mut self) -> Result<(), Error> {
        let mut attempt = 0u64;
        println!("Check if first block received");
        while self.chain_height < self.fetch_block_height {
            attempt = attempt.wrapping_add(1);
            // erase_line();
            println!(
                "Waiting bitcoind to reach start block height ... {}",
                u64_to_spin(attempt)
            );
            match self.fetch_chain_height() {
                Ok(height) => self.chain_height = height,
                Err(e) => println!("Fail to fetch chain height: {}", e),
            }
            thread::sleep(Duration::from_millis(100));
        }
        Ok(())
    }

    fn fetch_block(&self, block_height: u64) -> Result<Block, Error> {
        match self.rpc.get_block_hash(block_height) {
            Ok(hash) => match self.rpc.get_by_id(&hash) {
                Ok(block) => Ok(block),
                Err(e) => {
                    println!("Fail to fetch block {}: {}", &hash, e);
                    Err(Error::GetBlockFail)
                }
            },
            Err(e) => {
                println!("Fail to fetch hash at height {}: {}", block_height, e);
                Err(Error::GetHashFail)
            }
        }
    }

    fn get_block_hash(&self, block_height: u64) -> Result<BlockHash, Error> {
        match self.rpc.get_block_hash(block_height) {
            Ok(h) => Ok(h),
            Err(e) => {
                println!("Fail to get block hash at height {}: {}", block_height, e);
                Err(Error::GetBlockHashFails)
            }
        }
    }

    fn fetch_tx(&self, txid: &Txid) -> Result<Transaction, Error> {
        match self.rpc.get_raw_transaction(txid, None) {
            Ok(t) => Ok(t),
            Err(e) => {
                println!("Fail to get transaction {} : {}", txid, e);
                Err(Error::GetRawTransactionFails)
            }
        }
    }

    fn next(&mut self) -> Block {
        while self.chain_height < self.fetch_block_height {
            // erase_line();
            // println!("sync, chain at height {} ...", self.chain_height);
            if let Ok(height) = self.fetch_chain_height() {
                self.chain_height = height;
            } else {
                thread::sleep(Duration::from_millis(100));
            }
        }
        loop {
            // erase_line();
            // println!("fetching block at height {}", self.fetch_block_height);
            match self.fetch_block(self.fetch_block_height) {
                Ok(block) => {
                    self.fetch_block_height += 1;
                    return block;
                }
                Err(e) => {
                    erase_line();
                    println!("Fail to fetch block: {}", e);
                    thread::sleep(Duration::from_millis(500));
                }
            }
        }
    }
}

fn main() {
    let cli = Cli::parse();
    let url = &cli.ip;
    let auth = match cli.auth() {
        Ok(auth) => auth,
        Err(e) => {
            println!("{}", e);
            return;
        }
    };

    let max_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let max_cores = if max_cores > 2 { max_cores - 1 } else { 1 };
    println!("Max cores: {}", max_cores);
    let mut pool = Pool::new(url.clone(), auth, cli.start(), max_cores);
    pool.init();
    pool.start();
}

fn instruction_to_sig(i: Result<Instruction, script::Error>) -> Option<ecdsa::Signature> {
    match i {
        Ok(i) => match i {
            bitcoin::script::Instruction::PushBytes(elmt) => {
                if elmt.len() >= 70 && elmt.len() <= 72 && elmt.as_bytes()[0] == 0x30 {
                    ecdsa::Signature::from_slice(elmt.as_bytes()).ok()
                } else {
                    None
                }
            }
            bitcoin::script::Instruction::Op(opcode) => None,
        },
        Err(_) => todo!(),
    }
}

fn ecdsa_sig(sig: ecdsa::Signature) -> bool {
    // if sig.hash_ty == EcdsaSighashType::AllPlusAnyoneCanPay
    // || sig.hash_ty == EcdsaSighashType::SinglePlusAnyoneCanPay
    // || sig.hash_ty == EcdsaSighashType::NonePlusAnyoneCanPay
    sig.hash_ty == EcdsaSighashType::NonePlusAnyoneCanPay
}

fn taproot_sig(sig: taproot::Signature) -> bool {
    // if tap_sig.sighash_type == TapSighashType::AllPlusAnyoneCanPay
    // || tap_sig.sighash_type == TapSighashType::SinglePlusAnyoneCanPay
    // || tap_sig.sighash_type == TapSighashType::NonePlusAnyoneCanPay
    sig.sighash_type == TapSighashType::NonePlusAnyoneCanPay
}

fn process_block(block: Block, height: u64, runner: &BlockRunner) -> Vec<String> {
    let mut output = Vec::<String>::new();
    let timestamp = block.header.time;
    let coinbase =
        Txid::from_str("0000000000000000000000000000000000000000000000000000000000000000").unwrap();

    let mut cb = true;
    for tx in block.txdata {
        if cb {
            cb = false;
            continue;
        }
        let mut inp_spendable = 0usize;
        let mut spendable_amount = Amount::ZERO;
        for inp in &tx.input {
            let mut sighashes = Vec::<bool>::new();

            let legacy = inp.witness.is_empty() && !inp.script_sig.is_empty();
            let wrapped = !inp.witness.is_empty() && !inp.script_sig.is_empty();
            let segwit = !inp.witness.is_empty() && inp.script_sig.is_empty();

            if legacy {
                for inst in inp.script_sig.instructions() {
                    if let Some(sig) = instruction_to_sig(inst) {
                        sighashes.push(ecdsa_sig(sig));
                    }
                }
            } else if wrapped || segwit {
                let segwit_v0 = inp.witness[0] == [0x00u8]; // V0
                let taproot = inp.witness[0] == [0x51u8]; // V1

                for elmt in inp.witness.into_iter() {
                    if (elmt.len() == 64 || elmt.len() == 65) {
                        if let Ok(sig) = taproot::Signature::from_slice(elmt) {
                            sighashes.push(taproot_sig(sig));
                        }
                    } else if (elmt.len() >= 70 && elmt.len() <= 72 && elmt[0] == 0x30) {
                        if let Ok(sig) = ecdsa::Signature::from_slice(elmt) {
                            sighashes.push(ecdsa_sig(sig));
                        }
                    }
                }
            }

            let spendable = sighashes.iter().all(|s| *s) && !sighashes.is_empty();
            if spendable {
                inp_spendable += 1;
                let outpoint = inp.previous_output;
                if outpoint.txid != coinbase {
                    let funding_tx = runner.fetch_tx(&outpoint.txid).unwrap();
                    spendable_amount += funding_tx.output[outpoint.vout as usize].value;
                }
            }
        }

        let total = tx.output.iter().fold(0.0, |a, b| a + b.value.to_btc());
        if inp_spendable > 0 {
            // all_acp_txs += 1;
            let line = format!(
                "{}:{}:{}:{}/{}:{}",
                height,
                timestamp,
                tx.txid(),
                inp_spendable,
                tx.input.len(),
                spendable_amount.to_btc()
            );
            output.push(line);
            // let _ = file.write_all(line.as_bytes());
        }
    }
    output
}
