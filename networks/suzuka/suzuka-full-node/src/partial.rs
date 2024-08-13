use crate::{da_db::DaDB, tasks};
use m1_da_light_node_client::{
	blob_response, BatchWriteRequest, BlobWrite, LightNodeServiceClient,
	StreamReadFromHeightRequest,
};
use maptos_dof_execution::{
	v1::Executor, DynOptFinExecutor, ExecutableBlock, ExecutableTransactions, HashValue,
	SignatureVerifiedTransaction, SignedTransaction, Transaction,
};
use mcr_settlement_client::{McrSettlementClient, McrSettlementClientOperations};
use mcr_settlement_manager::CommitmentEventStream;
use mcr_settlement_manager::{McrSettlementManager, McrSettlementManagerOperations};
use movement_rest::MovementRest;
use movement_types::{Block, BlockCommitment, BlockCommitmentEvent, Commitment};

use anyhow::Context;
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use tokio::sync::mpsc;
use tokio::try_join;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, info_span, warn, Instrument};

use std::future::Future;
use std::sync::{atomic::AtomicU64, Arc};
use std::time::Duration;

pub struct SuzukaPartialNode<T> {
	executor: T,
	transaction_receiver: mpsc::Receiver<SignedTransaction>,
	light_node_client: LightNodeServiceClient<tonic::transport::Channel>,
	settlement_manager: McrSettlementManager,
	commitment_events: Option<CommitmentEventStream>,
	movement_rest: MovementRest,
	pub config: suzuka_config::Config,
	da_db: DaDB,
}

const LOGGING_UID: AtomicU64 = AtomicU64::new(0);

impl<T> SuzukaPartialNode<T>
where
	T: DynOptFinExecutor + Send + Sync,
{
	fn new<C>(
		executor: T,
		light_node_client: LightNodeServiceClient<tonic::transport::Channel>,
		settlement_client: C,
		movement_rest: MovementRest,
		da_db: DB,
	) -> (Self, impl Future<Output = Result<(), anyhow::Error>> + Send)
	where
		C: McrSettlementClientOperations + Send + 'static,
	{
		let (settlement_manager, commitment_events) =
			McrSettlementManager::new(settlement_client, &config.mcr);
		let (transaction_sender, transaction_receiver) = mpsc::channel(16);
		let bg_executor = executor.clone();
		(
			Self {
				executor,
				transaction_sender,
				transaction_receiver,
				light_node_client,
				settlement_manager,
				movement_rest,
				config: config.clone(),
				da_db: Arc::new(da_db),
			},
			read_commitment_events(commitment_events, bg_executor),
		)
	}

	fn bind_transaction_channel(&mut self) {
		self.executor.set_tx_channel(self.transaction_sender.clone());
	}

	pub fn bound<C>(
		executor: T,
		light_node_client: LightNodeServiceClient<tonic::transport::Channel>,
		settlement_client: C,
		movement_rest: MovementRest,
		config: &suzuka_config::Config,
		da_db: DB,
	) -> Result<(Self, impl Future<Output = Result<(), anyhow::Error>> + Send), anyhow::Error>
	where
		C: McrSettlementClientOperations + Send + 'static,
	{
		let (mut node, background_task) =
			Self::new(executor, light_node_client, settlement_client, movement_rest, config, da_db);
		node.bind_transaction_channel();
		Ok((node, background_task))
	}
}

impl<T> SuzukaPartialNode<T>
where
	T: DynOptFinExecutor + Send + 'static,
{
	// ! Currently this only implements opt.
	/// Runs the executor until crash or shutdown.
	pub async fn run(self) -> Result<(), anyhow::Error> {
		let exec_settle_task = tasks::exec_settle::Task::new(
			self.executor,
			self.settlement_manager,
			self.da_db,
			self.light_node_client.clone(),
			self.commitment_events,
		);
		let tx_ingress_task = tasks::tx_ingress::Task::new(
			self.transaction_receiver,
			self.light_node_client,
			// FIXME: why are struct member names so tautological?
			self.config.m1_da_light_node.m1_da_light_node_config,
		);
		let (res1, res2) = try_join!(
			tokio::spawn(async move { exec_settle_task.run().await }),
			tokio::spawn(async move { tx_ingress_task.run().await }),
		)?;
		res1.and(res2)
	}

	/// Runs the maptos rest api service until crash or shutdown.
	// TODO: move under run_services?
	#[allow(dead_code)]
	async fn run_movement_rest(&self) -> Result<(), anyhow::Error> {
		self.movement_rest.run_service().await?;
		Ok(())
	}
}

impl SuzukaPartialNode<Executor> {
	pub async fn try_from_config(
		config: suzuka_config::Config,
	) -> Result<(Self, impl Future<Output = Result<(), anyhow::Error>> + Send), anyhow::Error> {
		// todo: extract into getter
		let light_node_connection_hostname = config
			.m1_da_light_node
			.m1_da_light_node_config
			.m1_da_light_node_connection_hostname();

		// todo: extract into getter
		let light_node_connection_port = config
			.m1_da_light_node
			.m1_da_light_node_config
			.m1_da_light_node_connection_port();
		// todo: extract into getter
		debug!(
			"Connecting to light node at {}:{}",
			light_node_connection_hostname, light_node_connection_port
		);
		let light_node_client = LightNodeServiceClient::connect(format!(
			"http://{}:{}",
			light_node_connection_hostname, light_node_connection_port
		))
		.await
		.context("Failed to connect to light node")?;

		debug!("Creating the executor");
		let executor = Executor::try_from_config(tx, config.execution_config.maptos_config.clone())
			.context("Failed to create the inner executor")?;

		debug!("Creating the settlement client");
		let settlement_client = McrSettlementClient::build_with_config(config.mcr.clone())
			.await
			.context("Failed to build MCR settlement client with config")?;

		debug!("Creating the movement rest service");
		let movement_rest = MovementRest::try_from_env(Some(executor.executor.context.clone()))
			.context("Failed to create MovementRest")?;

		debug!("Creating the DA DB");
		let da_db = Self::create_or_get_da_db(&config)
			.await
			.context("Failed to create or get DA DB")?;

		Self::bound(executor, light_node_client, settlement_client, movement_rest, &config, da_db)
			.context(
			"Failed to bind the executor, light node client, settlement client, and movement rest",
		)
	}

	/// Runs the services until crash or shutdown.
	pub fn run_services(&self) -> impl Future<Output = Result<(), anyhow::Error>> + Send {
		let services = self.executor.services();
		services.run()
	}
}
