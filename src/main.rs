mod cli;
mod errors;

use bitcoin::script::{self, Instruction};
use bitcoin::{ecdsa, Amount, Block, BlockHash, EcdsaSighashType, Transaction, TxIn, Txid};
use bitcoincore_rpc::{Auth, Client, RpcApi};
use clap::Parser as _;
use cli::Cli;
use errors::Error;
use miniscript::bitcoin::{taproot, TapSighashType};
use std::collections::{BTreeMap, BTreeSet};
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

#[allow(unused)]
fn erase_line() {
    print!("\x1B[1A\x1B[K");
}

enum Request {
    /// Run on this block
    Run(u64 /* block */),
    /// Update chain height
    #[allow(dead_code)]
    Height(u64),
}

#[allow(dead_code)]
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

        let mut last_print_is_status = true;
        let mut count = 0usize;

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
                    if !items.is_empty() {
                        if last_print_is_status {
                            erase_line();
                        }
                        for item in &items {
                            println!("{}", item);
                            count += 1;
                        }
                        last_print_is_status = false;
                    }

                    if self.running_tip >= self.chain_tip {
                        return;
                    } else {
                        self.running_tip += 1;
                        if last_print_is_status {
                            erase_line();
                        }
                        println!(
                            "Processing block {}, {} inputs found",
                            self.running_tip, count
                        );
                        last_print_is_status = true;
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
                        let _ = sender.send(Response::Finished(runner_id, res));
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

    #[allow(dead_code)]
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

    #[allow(dead_code)]
    fn get_block_hash(&self, block_height: u64) -> Result<BlockHash, Error> {
        match self.rpc.get_block_hash(block_height) {
            Ok(h) => Ok(h),
            Err(e) => {
                println!("Fail to get block hash at height {}: {}", block_height, e);
                Err(Error::GetBlockHashFails)
            }
        }
    }

    #[allow(dead_code)]
    fn fetch_tx(&self, txid: &Txid) -> Result<Transaction, Error> {
        match self.rpc.get_raw_transaction(txid, None) {
            Ok(t) => Ok(t),
            Err(e) => {
                println!("Fail to get transaction {} : {}", txid, e);
                Err(Error::GetRawTransactionFails)
            }
        }
    }

    #[allow(dead_code)]
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
            bitcoin::script::Instruction::Op(_) => None,
        },
        Err(_) => todo!(),
    }
}

fn process_block(block: Block, height: u64, runner: &BlockRunner) -> Vec<String> {
    let mut output = Vec::<String>::new();
    let timestamp = block.header.time;
    #[allow(unused)]
    let coinbase =
        Txid::from_str("0000000000000000000000000000000000000000000000000000000000000000").unwrap();

    let mut cb = true;
    for tx in block.txdata {
        if cb {
            cb = false;
            continue;
        }

        // let mut inp_spendable = 0usize;
        // let mut spendable_amount = Amount::ZERO;

        let mut inputs = Vec::<usize>::new();
        for (index, inp) in tx.input.iter().enumerate() {
            let mut set = BTreeSet::new();
            set.insert(SigHash::AllPlusAnyoneCanPay);

            let sighashes = collect_sighashes(inp);
            let all_acp = !sighashes.is_empty() && all_sighash_in_set(sighashes, &set);

            if all_acp {
                inputs.push(index);
            }

            // if spendable {
            //     inp_spendable += 1;
            //     let outpoint = inp.previous_output;
            //     if outpoint.txid != coinbase {
            //         let funding_tx = runner.fetch_tx(&outpoint.txid).unwrap();
            //         spendable_amount += funding_tx.output[outpoint.vout as usize].value;
            //     }
            // }
        }

        let inputs_repr = if inputs.len() > 10 {
            "[.........]".to_string()
        } else {
            format!("{:?}", inputs)
        };
        if !inputs.is_empty() && tx.input.len() > 1 {
            // all_acp_txs += 1;
            let line = format!(
                "{}:{}:{}:{}/{}:{:?}",
                height,
                timestamp,
                tx.txid(),
                inputs.len(),
                tx.input.len(),
                inputs_repr,
            );
            output.push(line);
            // let _ = file.write_all(line.as_bytes());
        }
    }
    output
}

#[derive(Debug, PartialOrd, Ord, PartialEq, Eq)]
pub enum SigHash {
    All,
    None,
    Single,
    AllPlusAnyoneCanPay,
    NonePlusAnyoneCanPay,
    SinglePlusAnyoneCanPay,
}

impl From<EcdsaSighashType> for SigHash {
    fn from(value: EcdsaSighashType) -> Self {
        match value {
            EcdsaSighashType::All => Self::All,
            EcdsaSighashType::None => Self::None,
            EcdsaSighashType::Single => Self::Single,
            EcdsaSighashType::AllPlusAnyoneCanPay => Self::AllPlusAnyoneCanPay,
            EcdsaSighashType::NonePlusAnyoneCanPay => Self::NonePlusAnyoneCanPay,
            EcdsaSighashType::SinglePlusAnyoneCanPay => Self::SinglePlusAnyoneCanPay,
        }
    }
}

impl From<TapSighashType> for SigHash {
    fn from(value: TapSighashType) -> Self {
        match value {
            TapSighashType::All => Self::All,
            TapSighashType::None => Self::None,
            TapSighashType::Single => Self::Single,
            TapSighashType::AllPlusAnyoneCanPay => Self::AllPlusAnyoneCanPay,
            TapSighashType::NonePlusAnyoneCanPay => Self::NonePlusAnyoneCanPay,
            TapSighashType::SinglePlusAnyoneCanPay => Self::SinglePlusAnyoneCanPay,
            TapSighashType::Default => Self::All,
        }
    }
}

impl From<taproot::Signature> for SigHash {
    fn from(value: taproot::Signature) -> Self {
        value.sighash_type.into()
    }
}

impl From<ecdsa::Signature> for SigHash {
    fn from(value: ecdsa::Signature) -> Self {
        value.hash_ty.into()
    }
}

fn collect_sighashes(inp: &TxIn) -> Vec<SigHash> {
    let mut sighashes = Vec::<SigHash>::new();

    let legacy = inp.witness.is_empty() && !inp.script_sig.is_empty();
    let wrapped = !inp.witness.is_empty() && !inp.script_sig.is_empty();
    let segwit = !inp.witness.is_empty() && inp.script_sig.is_empty();

    if legacy {
        for inst in inp.script_sig.instructions() {
            if let Some(sig) = instruction_to_sig(inst) {
                sighashes.push(sig.into());
            }
        }
    } else if wrapped || segwit {
        for elmt in inp.witness.into_iter() {
            if elmt.len() == 64 || elmt.len() == 65 {
                if let Ok(sig) = taproot::Signature::from_slice(elmt) {
                    sighashes.push(sig.into());
                }
            } else if elmt.len() >= 70 && elmt.len() <= 72 && elmt[0] == 0x30 {
                if let Ok(sig) = ecdsa::Signature::from_slice(elmt) {
                    sighashes.push(sig.into());
                }
            }
        }
    }
    sighashes
}

fn all_sighash_in_set(vec: Vec<SigHash>, set: &BTreeSet<SigHash>) -> bool {
    for h in vec {
        if !set.contains(&h) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use bitcoin::{consensus, hex::test_hex_unwrap};

    use super::*;

    fn parse_tx(raw: &str) -> Transaction {
        let raw = test_hex_unwrap!(raw);
        let tx: Result<Transaction, _> = consensus::encode::deserialize(&raw);
        tx.unwrap()
    }

    #[test]
    fn all_hashes_are_none_acp() {
        let raw_tx = "02000000000105598985f6d122fb7d450ce34ed9de30fd054cd90fa2522e8f752e28ece1abbe4100000000171600144c67e771a34d7f741cabf70252aa9112550674d4ffffffffd2087d2f695a3bc7365f9c9ab7836c54ab03644648503625a7d62a3ed5d86d0700000000171600144c67e771a34d7f741cabf70252aa9112550674d4ffffffff4998347446d2b44f58d636a0cb65969879e7b3a1fe38d5a08e6d2e4bd14ceb000000000000ffffffff678a4d1d7c2669653b691807526a052f508170171d7308cba3e2f8c2d954fc9d03000000171600145e3e9ca907fc1a96f5c68718c0f88bbc20a040e9ffffffff678a4d1d7c2669653b691807526a052f508170171d7308cba3e2f8c2d954fc9d02000000171600145e3e9ca907fc1a96f5c68718c0f88bbc20a040e9ffffffff04a56600000000000017a914f714c8dfac3c6c1b5410e3f5cef0b5b91e1b01a787220200000000000022512081004a014b04c49319eba0e7afa1d6194af33b9fc259af3e3765bd5953aa25c2b33800000000000017a914458b3240570e9294987c0710228ed9cf9007011487102700000000000017a914458b3240570e9294987c0710228ed9cf900701148702483045022100e23c288db7dbec0a861b7af20635604d36624440aa2f585e7ff8a2fdb898dad402200615ff727ae23b3d686cafa61fb0ba66989cbbab077b1d49fda232a8dc3c18a9822103bc3c38c99b1bea90211bb7b64931552289feb9a44e4034be4ba68d02aaa13a5202483045022100d6f99975ca91690f7054783ae6c461eab00a63f0865294a906e362dfe8bc494b02201f34784181c828c6013a701f55e23ddc406c487e95bf1e2fe1ca9d1588f2cd18822103bc3c38c99b1bea90211bb7b64931552289feb9a44e4034be4ba68d02aaa13a5201417f7577af0ecf3ed3d9ad2fec2aecb255bf68fc0bb453a3396dfe20c349718748fe4e7ebeca76d1057c1088e502d036163a1bed0e19b11b2422694776d5298e76820247304402206076068e086c2ffd879c23de3aff615ae6449a082989f24f4f20c2a26f6b505e0220653e11abca95087001c452cb2d47eeb1c07e6f978d95f5409f86980a9ebc307a822103996cb69af6594575de17a144b33323ede6c54e6cb25d46ba47cbe39a92c1ac6a0247304402202e4bb5162d62ddc073a057bf76b8514f97e72b0cbcab8d04b7fb65d4911703ea022067731808cdb981751c24646176e8adca82d68f8b40701aa32654d1387a9d8a67822103996cb69af6594575de17a144b33323ede6c54e6cb25d46ba47cbe39a92c1ac6a00000000";

        let tx = parse_tx(raw_tx);
        let mut set = BTreeSet::new();
        set.insert(SigHash::NonePlusAnyoneCanPay);

        for inp in tx.input {
            let sighashes = collect_sighashes(&inp);
            assert!(all_sighash_in_set(sighashes, &set));
        }
    }
}
