use aptos_config::config::NodeConfig;
use aptos_mempool::MempoolClientSender;
use aptos_storage_interface::DbReaderWriter;
use maptos_execution_util::config::chain::Config as ChainConfig;

/// Infrastructure shared by services using the storage and the mempool.
pub struct Context {
	pub(crate) db: DbReaderWriter,
	pub(crate) mempool_client_sender: MempoolClientSender,
	pub(crate) chain_config: ChainConfig,
	pub(crate) node_config: NodeConfig,
}

impl Context {
	pub(crate) fn new(
		db: DbReaderWriter,
		mempool_client_sender: MempoolClientSender,
		chain_config: ChainConfig,
		node_config: NodeConfig,
	) -> Self {
		Context { db, mempool_client_sender, chain_config, node_config }
	}
}
