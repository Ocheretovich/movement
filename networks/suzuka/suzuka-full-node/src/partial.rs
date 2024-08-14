use crate::{da_db::DaDB, tasks};
use m1_da_light_node_client::{
	blob_response, BatchWriteRequest, BlobWrite, LightNodeServiceClient,
	StreamReadFromHeightRequest,
};
use maptos_dof_execution::MakeOptFinServices;
use maptos_dof_execution::{
	v1::Executor, DynOptFinExecutor, ExecutableBlock, ExecutableTransactions, HashValue,
	SignatureVerifiedTransaction, SignedTransaction, Transaction,
};
use mcr_settlement_client::{McrSettlementClient, McrSettlementClientOperations};
use mcr_settlement_manager::CommitmentEventStream;
use mcr_settlement_manager::{McrSettlementManager, McrSettlementManagerOperations};
use movement_rest::MovementRest;
use movement_types::{Block, BlockCommitment, BlockCommitmentEvent, Commitment};
use suzuka_config::Config;

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
	light_node_client: LightNodeServiceClient<tonic::transport::Channel>,
	settlement_manager: McrSettlementManager,
	commitment_events: Option<CommitmentEventStream>,
	movement_rest: MovementRest,
	config: Config,
	da_db: DaDB,
}

const LOGGING_UID: AtomicU64 = AtomicU64::new(0);

impl<T> SuzukaPartialNode<T>
where
	T: DynOptFinExecutor + Send + Sync,
{
	fn new(
		executor: T,
		light_node_client: LightNodeServiceClient<tonic::transport::Channel>,
		settlement_manager: McrSettlementManager,
		commitment_events: Option<CommitmentEventStream>,
		movement_rest: MovementRest,
		config: Config,
		da_db: DaDB,
	) -> Self {
		Self {
			executor,
			light_node_client,
			settlement_manager,
			commitment_events,
			movement_rest,
			config,
			da_db,
		}
	}

	fn bind_transaction_channel(&mut self) {
		self.executor.set_tx_channel(self.transaction_sender.clone());
	}

	pub fn bound<C>(
		executor: T,
		light_node_client: LightNodeServiceClient<tonic::transport::Channel>,
		settlement_client: C,
		movement_rest: MovementRest,
		config: &Config,
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
		let (transaction_sender, transaction_receiver) = mpsc::channel(16);
		let (context, exec_background) = self
			.executor
			.background(transaction_sender, &self.config.execution_config.maptos_config)?;
		let services = context.services();
		let exec_settle_task = tasks::exec_settle::Task::new(
			self.executor,
			self.settlement_manager,
			self.da_db,
			self.light_node_client.clone(),
			self.commitment_events,
		);
		let tx_ingress_task = tasks::tx_ingress::Task::new(
			transaction_receiver,
			self.light_node_client,
			// FIXME: why are struct member names so tautological?
			self.config.m1_da_light_node.m1_da_light_node_config,
		);
		let (res1, res2, res3, res4) = try_join!(
			tokio::spawn(async move { exec_settle_task.run().await }),
			tokio::spawn(async move { tx_ingress_task.run().await }),
			tokio::spawn(exec_background),
			tokio::spawn(services.run()),
		)?;
		res1.and(res2).and(res3).and(res4)
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
	pub async fn try_from_config(config: &Config) -> Result<Self, anyhow::Error> {
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
		let executor = Executor::try_from_config(&config.execution_config.maptos_config)
			.context("Failed to create the inner executor")?;

		debug!("Creating the settlement client");
		let settlement_client = McrSettlementClient::build_with_config(&config.mcr)
			.await
			.context("Failed to build MCR settlement client with config")?;
		let (settlement_manager, commitment_events) =
			McrSettlementManager::new(settlement_client, &config.mcr);
		let commitment_events =
			if config.mcr.should_settle() { Some(commitment_events) } else { None };

		debug!("Creating the movement rest service");
		let movement_rest = MovementRest::try_from_env(Some(executor.executor.context.clone()))
			.context("Failed to create MovementRest")?;

		debug!("Creating the DA DB");
		let da_db =
			DaDB::open(&config.da_db.da_db_path).context("Failed to create or get DA DB")?;

		Ok(Self {
			executor,
			light_node_client,
			settlement_manager,
			commitment_events,
			movement_rest,
			da_db,
		})
	}

	/// Runs the services until crash or shutdown.
	pub fn run_services(&self) -> impl Future<Output = Result<(), anyhow::Error>> + Send {
		let services = self.executor.services();
		services.run()
	}
}
