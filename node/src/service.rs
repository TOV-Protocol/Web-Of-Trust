//! Service and ServiceFactory implementation. Specialized wrapper over substrate service.

use futures::FutureExt;
use node_runtime::{self, opaque::Block, RuntimeApi};
use sc_client_api::{Backend, BlockBackend};
use sc_consensus_grandpa::SharedVoterState;
use sc_consensus_manual_seal::{run_manual_seal, EngineCommand, ManualSealParams};
use sc_service::{error::Error as ServiceError, Configuration, TaskManager, WarpSyncParams};
use sc_telemetry::{Telemetry, TelemetryWorker};
use sc_transaction_pool_api::OffchainTransactionPoolFactory;
use sp_consensus_babe::inherents::InherentDataProvider;
use std::{sync::Arc, time::Duration};

pub(crate) type FullClient = sc_service::TFullClient<
	Block,
	RuntimeApi,
	sc_executor::WasmExecutor<sp_io::SubstrateHostFunctions>,
>;
type FullBackend = sc_service::TFullBackend<Block>;
type FullSelectChain = sc_consensus::LongestChain<FullBackend, Block>;

/// The minimum period of blocks on which justifications will be
/// imported and generated.
const GRANDPA_JUSTIFICATION_PERIOD: u32 = 512;

pub type Service = sc_service::PartialComponents<
	FullClient,
	FullBackend,
	FullSelectChain,
	sc_consensus::DefaultImportQueue<Block>,
	sc_transaction_pool::FullPool<Block, FullClient>,
	(
		sc_consensus_grandpa::GrandpaBlockImport<FullBackend, Block, FullClient, FullSelectChain>,
		sc_consensus_grandpa::LinkHalf<Block, FullClient, FullSelectChain>,
		Option<Telemetry>,
	),
>;

type FullGrandpaBlockImport<RuntimeApi, Executor> = sc_consensus_grandpa::GrandpaBlockImport<
	FullBackend,
	Block,
	FullClient<RuntimeApi, Executor>,
	FullSelectChain,
>;

#[allow(clippy::type_complexity)]
pub fn new_partial<RuntimeApi, Executor>(
	config: &Configuration,
	consensus_manual: bool,
) -> Result<
	sc_service::PartialComponents<
		FullClient<RuntimeApi, Executor>,
		FullBackend,
		FullSelectChain,
		sc_consensus::DefaultImportQueue<Block>,
		sc_transaction_pool::FullPool<Block, FullClient<RuntimeApi, Executor>>,
		(
			sc_consensus_babe::BabeBlockImport<
				Block,
				FullClient<RuntimeApi, Executor>,
				FullGrandpaBlockImport<RuntimeApi, Executor>,
			>,
			sc_consensus_babe::BabeLink<Block>,
			Option<sc_consensus_babe::BabeWorkerHandle<Block>>,
			sc_consensus_grandpa::LinkHalf<
				Block,
				FullClient<RuntimeApi, Executor>,
				FullSelectChain,
			>,
			Option<Telemetry>,
		),
	>,
	ServiceError,
>
where
	RuntimeApi: sp_api::ConstructRuntimeApi<Block, FullClient<RuntimeApi, Executor>>
		+ Send
		+ Sync
		+ 'static,
	RuntimeApi::RuntimeApi: RuntimeApiCollection,
	Executor: sc_executor::NativeExecutionDispatch + 'static,
	Executor: sc_executor::sp_wasm_interface::HostFunctions + 'static,
{
	let telemetry = config
		.telemetry_endpoints
		.clone()
		.filter(|x| !x.is_empty())
		.map(|endpoints| -> Result<_, sc_telemetry::Error> {
			let worker = TelemetryWorker::new(16)?;
			let telemetry = worker.handle().new_telemetry(endpoints);
			Ok((worker, telemetry))
		})
		.transpose()?;

	#[cfg(feature = "native")]
	let executor = sc_service::new_wasm_executor::<sp_io::SubstrateHostFunctions>(config);
	#[cfg(not(feature = "native"))]
	let executor = sc_service::new_wasm_executor(config);

	let (client, backend, keystore_container, task_manager) =
		sc_service::new_full_parts::<Block, RuntimeApi, _>(
			config,
			telemetry.as_ref().map(|(_, telemetry)| telemetry.handle()),
			executor,
		)?;
	let client = Arc::new(client);

	let telemetry = telemetry.map(|(worker, telemetry)| {
		task_manager.spawn_handle().spawn("telemetry", None, worker.run());
		telemetry
	});

	let select_chain = sc_consensus::LongestChain::new(backend.clone());

	let transaction_pool = sc_transaction_pool::BasicPool::new_full(
		config.transaction_pool.clone(),
		config.role.is_authority().into(),
		config.prometheus_registry(),
		task_manager.spawn_essential_handle(),
		client.clone(),
	);

	let client_ = client.clone();
	let (grandpa_block_import, grandpa_link) = sc_consensus_grandpa::block_import(
		client.clone(),
		GRANDPA_JUSTIFICATION_PERIOD,
		&(client_ as Arc<_>),
		select_chain.clone(),
		telemetry.as_ref().map(|x| x.handle()),
	)?;

	let justification_import = grandpa_block_import.clone();

	let (babe_block_import, babe_link) = sc_consensus_babe::block_import(
		sc_consensus_babe::configuration(&*client)?,
		grandpa_block_import,
		client.clone(),
	)?;

	let (import_queue, babe_worker_handle) = if consensus_manual {
		let import_queue = sc_consensus_manual_seal::import_queue(
			Box::new(babe_block_import.clone()),
			&task_manager.spawn_essential_handle(),
			config.prometheus_registry(),
		);
		(import_queue, None)
	} else {
		let slot_duration = babe_link.config().slot_duration();
		let (queue, handle) =
			sc_consensus_babe::import_queue(sc_consensus_babe::ImportQueueParams {
				link: babe_link.clone(),
				block_import: babe_block_import.clone(),
				justification_import: Some(Box::new(justification_import)),
				client: client.clone(),
				select_chain: select_chain.clone(),
				create_inherent_data_providers: move |_, ()| async move {
					let timestamp = sp_timestamp::InherentDataProvider::from_system_time();

					let slot = InherentDataProvider::from_timestamp_and_slot_duration(
						*timestamp,
						slot_duration,
					);

					Ok((slot, timestamp))
				},
				spawner: &task_manager.spawn_essential_handle(),
				registry: config.prometheus_registry(),
				telemetry: telemetry.as_ref().map(|x| x.handle()),
				offchain_tx_pool_factory:
					sc_transaction_pool_api::OffchainTransactionPoolFactory::new(
						transaction_pool.clone(),
					),
			})?;

		(queue, Some(handle))
	};

	Ok(sc_service::PartialComponents {
		client,
		backend,
		task_manager,
		import_queue,
		keystore_container,
		select_chain,
		transaction_pool,
		other: (babe_block_import, babe_link, babe_worker_handle, grandpa_link, telemetry),
	})
}

/// Builds a new service for a full client.
pub fn new_full<
	RuntimeApi,
	Executor,
	N: sc_network::NetworkBackend<Block, <Block as sp_runtime::traits::Block>::Hash>,
>(
	config: Configuration,
	sealing: crate::cli::Sealing,
) -> Result<TaskManager, ServiceError>
where
	RuntimeApi: sp_api::ConstructRuntimeApi<Block, FullClient<RuntimeApi, Executor>>
		+ Send
		+ Sync
		+ 'static,
	RuntimeApi::RuntimeApi: RuntimeApiCollection,
	Executor: sc_executor::NativeExecutionDispatch + 'static,
	Executor: sc_executor::sp_wasm_interface::HostFunctions + 'static,
{
	let sc_service::PartialComponents {
		client,
		backend,
		mut task_manager,
		import_queue,
		keystore_container,
		select_chain,
		transaction_pool,
		other: (block_import, babe_link, babe_worker_handle, grandpa_link, mut telemetry),
	} = new_partial::<RuntimeApi, Executor>(&config, sealing.is_manual_consensus())?;

	let grandpa_protocol_name = sc_consensus_grandpa::protocol_standard_name(
		&client.block_hash(0).ok().flatten().expect("Genesis block exists; qed"),
		&config.chain_spec,
	);

	let mut net_config = sc_network::config::FullNetworkConfiguration::<
		Block,
		<Block as sp_runtime::traits::Block>::Hash,
		N,
	>::new(&config.network);
	let metrics = N::register_notification_metrics(config.prometheus_registry());
	let peer_store_handle = net_config.peer_store_handle();

	let (grandpa_protocol_config, grandpa_notification_service) =
		sc_consensus_grandpa::grandpa_peers_set_config::<_, N>(
			grandpa_protocol_name.clone(),
			metrics.clone(),
			peer_store_handle,
		);
	net_config.add_notification_protocol(grandpa_protocol_config);

	let warp_sync = Arc::new(sc_consensus_grandpa::warp_proof::NetworkProvider::new(
		backend.clone(),
		grandpa_link.shared_authority_set().clone(),
		Vec::default(),
	));

	let (network, system_rpc_tx, tx_handler_controller, network_starter, sync_service) =
		sc_service::build_network(sc_service::BuildNetworkParams {
			config: &config,
			net_config,
			client: client.clone(),
			transaction_pool: transaction_pool.clone(),
			spawn_handle: task_manager.spawn_handle(),
			import_queue,
			block_announce_validator_builder: None,
			warp_sync_params: Some(WarpSyncParams::WithProvider(warp_sync)),
			block_relay: None,
			metrics,
		})?;

	let role = config.role.clone();
	let force_authoring = config.force_authoring;
	let backoff_authoring_blocks: Option<()> = None;
	let name = config.network.node_name.clone();
	let enable_grandpa = !config.disable_grandpa;
	let prometheus_registry = config.prometheus_registry().cloned();

	if config.offchain_worker.enabled {
		use futures::FutureExt;

		task_manager.spawn_handle().spawn(
			"offchain-workers-runner",
			"offchain-worker",
			sc_offchain::OffchainWorkers::new(sc_offchain::OffchainWorkerOptions {
				runtime_api_provider: client.clone(),
				is_validator: config.role.is_authority(),
				keystore: Some(keystore_container.keystore()),
				offchain_db: backend.offchain_storage(),
				transaction_pool: Some(
					sc_transaction_pool_api::OffchainTransactionPoolFactory::new(
						transaction_pool.clone(),
					),
				),
				network_provider: Arc::new(network.clone()),
				enable_http_requests: true,
				custom_extensions: |_| vec![],
			})
			.run(client.clone(), task_manager.spawn_handle())
			.boxed(),
		);
	}

	let rpc_extensions_builder = {
		let client = client.clone();
		let pool = transaction_pool.clone();
		let select_chain = select_chain;
		let chain_spec = config.chain_spec.cloned_box();
		let keystore = keystore_container.keystore().clone();
		let babe_deps = babe_worker_handle.map(|babe_worker_handle| crate::rpc::BabeDeps {
			babe_worker_handle,
			keystore: keystore.clone(),
		});

		Box::new(move |deny_unsafe, _| {
			let deps = crate::rpc::FullDeps {
				client: client.clone(),
				pool: pool.clone(),
				select_chain: select_chain.clone(),
				chain_spec: chain_spec.cloned_box(),
				deny_unsafe,
				babe: babe_deps.clone(),
				command_sink_opt: command_sink_opt.clone(),
			};

			crate::rpc::create_full(deps).map_err(Into::into)
		})
	};

	let _rpc_handlers = sc_service::spawn_tasks(sc_service::SpawnTasksParams {
		network: network.clone(),
		client: client.clone(),
		keystore: keystore_container.keystore(),
		task_manager: &mut task_manager,
		transaction_pool: transaction_pool.clone(),
		rpc_builder: rpc_extensions_builder,
		backend,
		system_rpc_tx,
		tx_handler_controller,
		sync_service: sync_service.clone(),
		config,
		telemetry: telemetry.as_mut(),
	})?;

	let mut command_sink_opt = None;
	if role.is_authority() {
		let distance_dir = config.base_path.config_dir(config.chain_spec.id()).join("distance");

		let proposer_factory = sc_basic_authorship::ProposerFactory::new(
			task_manager.spawn_handle(),
			client.clone(),
			transaction_pool.clone(),
			prometheus_registry.as_ref(),
			telemetry.as_ref().map(|x| x.handle()),
		);

		let keystore_ptr = keystore_container.keystore();
		let client = client.clone();

		if sealing.is_manual_consensus() {
			let commands_stream: Box<dyn Stream<Item = EngineCommand<H256>> + Send + Sync + Unpin> =
				match sealing {
					crate::cli::Sealing::Instant => {
						Box::new(
							// This bit cribbed from the implementation of instant seal.
							transaction_pool
								.pool()
								.validated_pool()
								.import_notification_stream()
								.map(|_| EngineCommand::SealNewBlock {
									create_empty: false,
									finalize: false,
									parent_hash: None,
									sender: None,
								}),
						)
					},
					crate::cli::Sealing::Manual => {
						let (sink, stream) = futures::channel::mpsc::channel(1000);
						// Keep a reference to the other end of the channel. It goes to the RPC.
						command_sink_opt = Some(sink);
						Box::new(stream)
					},
					crate::cli::Sealing::Interval(millis) => Box::new(StreamExt::map(
						Timer::interval(Duration::from_millis(millis)),
						|_| EngineCommand::SealNewBlock {
							create_empty: true,
							finalize: false,
							parent_hash: None,
							sender: None,
						},
					)),
					crate::cli::Sealing::Production => unreachable!(),
				};

			let babe_consensus_data_provider =
				sc_consensus_manual_seal::consensus::babe::BabeConsensusDataProvider::new(
					client.clone(),
					keystore_container.keystore(),
					babe_link.epoch_changes().clone(),
					vec![(
						sp_consensus_babe::AuthorityId::from(
							sp_keyring::sr25519::Keyring::Alice.public(),
						),
						1000,
					)],
				)
				.expect("failed to create BabeConsensusDataProvider");

			task_manager.spawn_essential_handle().spawn_blocking(
                "manual-seal",
                Some("block-authoring"),
                run_manual_seal(ManualSealParams {
                    block_import,
                    env: proposer_factory,
                    client: client.clone(),
                    pool: transaction_pool.clone(),
                    commands_stream,
                    select_chain: select_chain.clone(),
                    consensus_data_provider: Some(Box::new(babe_consensus_data_provider)),
                    create_inherent_data_providers: move |parent, _| {
                        let client = client.clone();
                        let distance_dir = distance_dir.clone();
                        let babe_owner_keys =
                            std::sync::Arc::new(sp_keystore::Keystore::sr25519_public_keys(
                                keystore_ptr.as_ref(),
                                sp_runtime::KeyTypeId(*b"babe"),
                            ));
                        async move {
                            let timestamp =
                                sc_consensus_manual_seal::consensus::timestamp::SlotTimestampProvider::new_babe(
                                    client.clone(),
                                )
                                .map_err(|err| format!("{:?}", err))?;
                            let babe = InherentDataProvider::new(
                                timestamp.slot(),
                            );
                            let distance =
                                dc_distance::create_distance_inherent_data_provider::<
                                    Block,
                                    FullClient<RuntimeApi, Executor>,
                                    FullBackend,
                                >(
                                    &*client, parent, distance_dir, &babe_owner_keys.clone()
                                )?;
                            Ok((timestamp, babe, distance))
                        }
                    },
                }),
            );
		} else {
			let slot_duration = babe_link.config().slot_duration();
			let babe_config = sc_consensus_babe::BabeParams {
				keystore: keystore_container.keystore(),
				client: client.clone(),
				select_chain: select_chain.clone(),
				block_import,
				env: proposer_factory,
				sync_oracle: sync_service.clone(),
				justification_sync_link: sync_service.clone(),
				create_inherent_data_providers: move |parent, ()| {
					// This closure is called during each block generation.

					let client = client.clone();
					let distance_dir = distance_dir.clone();
					let babe_owner_keys =
						std::sync::Arc::new(sp_keystore::Keystore::sr25519_public_keys(
							keystore_ptr.as_ref(),
							sp_runtime::KeyTypeId(*b"babe"),
						));

					async move {
						let timestamp = sp_timestamp::InherentDataProvider::from_system_time();

						let slot = InherentDataProvider::from_timestamp_and_slot_duration(
							*timestamp,
							slot_duration,
						);

						let storage_proof =
							sp_transaction_storage_proof::registration::new_data_provider(
								&*client, &parent,
							)?;

						let distance = dc_distance::create_distance_inherent_data_provider::<
							Block,
							FullClient<RuntimeApi, Executor>,
							FullBackend,
						>(&*client, parent, distance_dir, &babe_owner_keys.clone())?;

						Ok((slot, timestamp, storage_proof, distance))
					}
				},
				force_authoring,
				backoff_authoring_blocks,
				babe_link,
				block_proposal_slot_portion: sc_consensus_babe::SlotProportion::new(2f32 / 3f32),
				max_block_proposal_slot_portion: None,
				telemetry: telemetry.as_ref().map(|x| x.handle()),
			};
			let babe = sc_consensus_babe::start_babe(babe_config)?;

			// the BABE authoring task is considered essential, i.e. if it
			// fails we take down the service with it.
			task_manager.spawn_essential_handle().spawn_blocking(
				"babe-proposer",
				Some("block-authoring"),
				babe,
			);
		}
	}
	// if the node isn't actively participating in consensus then it doesn't
	// need a keystore, regardless of which protocol we use below.
	let keystore = if role.is_authority() { Some(keystore_container.keystore()) } else { None };

	let grandpa_config = sc_consensus_grandpa::Config {
		// FIXME #1578 make this available through chainspec
		gossip_duration: Duration::from_millis(333),
		justification_generation_period: GRANDPA_JUSTIFICATION_PERIOD,
		name: Some(name),
		observer_enabled: false,
		keystore,
		local_role: role,
		telemetry: telemetry.as_ref().map(|x| x.handle()),
		protocol_name: grandpa_protocol_name,
	};

	if enable_grandpa {
		// start the full GRANDPA voter
		// NOTE: non-authorities could run the GRANDPA observer protocol, but at
		// this point the full voter should provide better guarantees of block
		// and vote data availability than the observer. The observer has not
		// been tested extensively yet and having most nodes in a network run it
		// could lead to finality stalls.
		let grandpa_config = sc_consensus_grandpa::GrandpaParams {
			config: grandpa_config,
			link: grandpa_link,
			network,
			sync: Arc::new(sync_service),
			notification_service: grandpa_notification_service,
			voting_rule: sc_consensus_grandpa::VotingRulesBuilder::default().build(),
			prometheus_registry,
			shared_voter_state: SharedVoterState::empty(),
			telemetry: telemetry.as_ref().map(|x| x.handle()),
			offchain_tx_pool_factory: OffchainTransactionPoolFactory::new(transaction_pool),
		};

		// the GRANDPA voter task is considered infallible, i.e.
		// if it fails we take down the service with it.
		task_manager.spawn_essential_handle().spawn_blocking(
			"grandpa-voter",
			None,
			sc_consensus_grandpa::run_grandpa_voter(grandpa_config)?,
		);
	}

	network_starter.start_network();

	log::info!("***** Full Node has started! *****");

	Ok(task_manager)
}
