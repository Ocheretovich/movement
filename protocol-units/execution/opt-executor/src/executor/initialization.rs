use super::Executor;
use crate::{bootstrap, Context, TransactionPipe};

use aptos_config::config::NodeConfig;
#[cfg(test)]
use aptos_crypto::ed25519::Ed25519PrivateKey;
use aptos_crypto::PrivateKey;
use aptos_executor::block_executor::BlockExecutor;
use aptos_mempool::MempoolClientRequest;
use aptos_types::transaction::SignedTransaction;
use maptos_execution_util::config::{chain::Config as ChainConfig, Config};

use anyhow::Context as _;
use futures::channel::mpsc as futures_mpsc;
use tokio::sync::mpsc;

#[cfg(test)]
use tempfile::TempDir;

use std::sync::{atomic::AtomicU64, Arc};

impl Executor {
	pub fn bootstrap(
		transaction_sender: mpsc::Sender<SignedTransaction>,
		chain_config: ChainConfig,
		node_config: NodeConfig,
	) -> Result<(Self, Context, TransactionPipe), anyhow::Error> {
		let (db, signer) = bootstrap::maybe_bootstrap_empty_db(
			chain_config.maptos_db_path.as_ref().context("No db path provided.")?,
			chain_config.maptos_chain_id.clone(),
			&chain_config.maptos_private_key.public_key(),
		)?;
		let executor = Self {
			block_executor: Arc::new(BlockExecutor::new(db.clone())),
			db,
			signer,
			transactions_in_flight: Arc::new(AtomicU64::new(0)),
		};
		let (context, transaction_pipe) =
			executor.background(transaction_sender, chain_config, node_config);
		Ok((executor, context, transaction_pipe))
	}

	pub fn try_from_config(
		transaction_sender: mpsc::Sender<SignedTransaction>,
		maptos_config: &Config,
	) -> Result<(Self, Context, TransactionPipe), anyhow::Error> {
		let mut node_config = NodeConfig::default();

		node_config.indexer.enabled = true;
		// indexer config
		node_config.indexer.postgres_uri =
			Some(maptos_config.indexer_processor.postgres_connection_string.clone());
		node_config.indexer.processor = Some("default_processor".to_string());
		node_config.indexer.check_chain_id = Some(false);
		node_config.indexer.skip_migrations = Some(false);
		node_config.indexer.fetch_tasks = Some(4);
		node_config.indexer.processor_tasks = Some(4);
		node_config.indexer.emit_every = Some(4);
		node_config.indexer.batch_size = Some(8);
		node_config.indexer.gap_lookback_versions = Some(4);

		node_config.indexer_grpc.enabled = true;

		// indexer_grpc config
		node_config.indexer_grpc.processor_batch_size = 4;
		node_config.indexer_grpc.processor_task_count = 4;
		node_config.indexer_grpc.output_batch_size = 4;
		node_config.indexer_grpc.address = format!(
			"{}:{}",
			maptos_config.indexer.maptos_indexer_grpc_listen_hostname,
			maptos_config.indexer.maptos_indexer_grpc_listen_port
		)
		.parse()?;
		node_config.indexer_grpc.use_data_service_interface = true;

		// indexer table info config
		node_config.indexer_table_info.enabled = true;
		node_config.storage.dir = "./.movement/maptos-storage".to_string().into();
		node_config.storage.set_data_dir(node_config.storage.dir.clone());

		Self::bootstrap(transaction_sender, maptos_config.chain.clone(), node_config)
	}

	#[cfg(test)]
	pub fn try_test_default(
		transaction_sender: mpsc::Sender<SignedTransaction>,
		private_key: Ed25519PrivateKey,
	) -> Result<(Self, Context, TransactionPipe, TempDir), anyhow::Error> {
		let tempdir = tempfile::tempdir()?;

		let mut maptos_config = Config::default();
		maptos_config.chain.maptos_private_key = private_key;

		// replace the db path with the temporary directory
		maptos_config.chain.maptos_db_path.replace(tempdir.path().to_path_buf());
		let (executor, context, transaction_pipe) =
			Self::try_from_config(transaction_sender, &maptos_config)?;
		Ok((executor, context, transaction_pipe, tempdir))
	}

	/// Creates instance of `Context` and the background `TransactionPipe`
	/// task to process transactions.
	pub fn background(
		&self,
		transaction_sender: mpsc::Sender<SignedTransaction>,
		chain_config: ChainConfig,
		node_config: NodeConfig,
	) -> (Context, TransactionPipe) {
		// use the default signer, block executor, and mempool
		let (mempool_client_sender, mempool_client_receiver) =
			futures_mpsc::channel::<MempoolClientRequest>(2 ^ 16); // allow 2^16 transactions before apply backpressure given theoretical maximum TPS of 170k
		let transaction_pipe = TransactionPipe::new(
			mempool_client_receiver,
			transaction_sender,
			&node_config,
			Arc::clone(&self.transactions_in_flight),
		);
		let cx = Context::new(self.db.clone(), mempool_client_sender, chain_config, node_config);
		(cx, transaction_pipe)
	}
}
