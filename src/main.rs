mod cli;
mod errors;

use bitcoin::{ecdsa, Block, EcdsaSighashType};
use bitcoincore_rpc::{Client, RpcApi};
use clap::Parser as _;
use cli::Cli;
use errors::Error;
use miniscript::bitcoin::{taproot, TapSighashType};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::thread;
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
            thread::sleep(Duration::from_millis(400));
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

    let rpc = Client::new(url, auth).unwrap();
    let mut runner = BlockRunner::new(rpc, cli.start());
    runner.init().unwrap();

    let filename: PathBuf = "all_acp_txs.txt".into();
    let mut file = File::create(filename).unwrap();

    let mut all_acp_txs = 0usize;
    let mut can_erase = true;

    loop {
        let block = runner.next();
        let timestamp = block.header.time;
        if can_erase {
            erase_line();
        }
        println!(
            "processing block {}, {} possible all_acp tx found",
            runner.fetch_height(),
            all_acp_txs
        );
        can_erase = true;
        for tx in block.txdata {
            let mut all_acp_sigs = 0usize;
            let mut all_sigs = 0usize;
            for inp in &tx.input {
                for elmt in inp.witness.into_iter() {
                    if elmt.len() == 64 || elmt.len() == 65 {
                        // possibly taproot sig
                        if let Ok(tap_sig) = taproot::Signature::from_slice(elmt) {
                            all_sigs += 1;
                            // if tap_sig.sighash_type == TapSighashType::AllPlusAnyoneCanPay
                            // || tap_sig.sighash_type == TapSighashType::SinglePlusAnyoneCanPay
                            // || tap_sig.sighash_type == TapSighashType::NonePlusAnyoneCanPay
                            if tap_sig.sighash_type == TapSighashType::NonePlusAnyoneCanPay {
                                all_acp_sigs += 1;
                            }
                        }
                    } else if elmt.len() >= 70 && elmt.len() <= 72 && elmt[0] == 0x30 {
                        // possibly ecdsa sig
                        if let Ok(sig) = ecdsa::Signature::from_slice(elmt) {
                            all_sigs += 1;
                            // if sig.hash_ty == EcdsaSighashType::AllPlusAnyoneCanPay
                            // || sig.hash_ty == EcdsaSighashType::SinglePlusAnyoneCanPay
                            // || sig.hash_ty == EcdsaSighashType::NonePlusAnyoneCanPay
                            if sig.hash_ty == EcdsaSighashType::NonePlusAnyoneCanPay {
                                all_acp_sigs += 1;
                            }
                        }
                    }
                }
            }

            // if all_acp_sigs > 0 && all_acp_sigs == all_sigs {
            if all_acp_sigs > 0 {
                all_acp_txs += 1;
                let line = format!(
                    "{}: tx {} contains {} ALL | ACP signatures ({} inputs)",
                    timestamp,
                    tx.txid(),
                    all_acp_sigs,
                    tx.input.len()
                );
                if can_erase {
                    erase_line();
                }
                println!("{}", line);
                can_erase = false;
                let _ = file.write_all(line.as_bytes());
            }
        }
    }
}
