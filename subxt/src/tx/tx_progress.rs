// Copyright 2019-2023 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or GPL-3.0.
// see LICENSE for license details.

//! Types representing extrinsics/transactions that have been submitted to a node.

use std::collections::VecDeque;
use std::task::Poll;
use std::time::Duration;

use crate::{
    backend::{BlockRef, StreamOfResults, TransactionStatus as BackendTxStatus},
    client::OnlineClientT,
    config::Hasher,
    error::{DispatchError, Error, TransactionError},
    events::EventsClient,
    utils::strip_compact_prefix,
    Config,
};
use derive_where::derive_where;
use futures::{future::Either, FutureExt, Stream, StreamExt};

/// This struct represents a subscription to the progress of some transaction.
pub struct TxProgress<T: Config, C> {
    sub: Option<StreamOfResults<BackendTxStatus<T::Hash>>>,
    ext_hash: T::Hash,
    client: C,
}

impl<T: Config, C> std::fmt::Debug for TxProgress<T, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxProgress")
            .field("sub", &"<subscription>")
            .field("ext_hash", &self.ext_hash)
            .field("client", &"<client>")
            .finish()
    }
}

// The above type is not `Unpin` by default unless the generic param `T` is,
// so we manually make it clear that Unpin is actually fine regardless of `T`
// (we don't care if this moves around in memory while it's "pinned").
impl<T: Config, C> Unpin for TxProgress<T, C> {}

impl<T: Config, C> TxProgress<T, C> {
    /// Instantiate a new [`TxProgress`] from a custom subscription.
    pub fn new(
        sub: StreamOfResults<BackendTxStatus<T::Hash>>,
        client: C,
        ext_hash: T::Hash,
    ) -> Self {
        Self {
            sub: Some(sub),
            client,
            ext_hash,
        }
    }

    /// Return the hash of the extrinsic.
    pub fn extrinsic_hash(&self) -> T::Hash {
        self.ext_hash
    }
}

impl<T, C> TxProgress<T, C>
where
    T: Config,
    C: OnlineClientT<T>,
{
    /// Upper bound on how long [`Self::wait_for_finalized`] waits before returning an error
    /// (after trying on-chain fallback scans).
    const WAIT_FOR_FINALIZED_TIMEOUT: Duration = Duration::from_secs(240);
    /// How many recent finalized block hashes we remember for extrinsic fallback lookup.
    const RECENT_FINALIZED_CAP: usize = 128;

    /// Return the next transaction status when it's emitted. This just delegates to the
    /// [`futures::Stream`] implementation for [`TxProgress`], but allows you to
    /// avoid importing that trait if you don't otherwise need it.
    pub async fn next(&mut self) -> Option<Result<TxStatus<T, C>, Error>> {
        StreamExt::next(self).await
    }

    async fn block_contains_extrinsic(
        client: &C,
        ext_hash: T::Hash,
        block_hash: T::Hash,
    ) -> Result<bool, Error> {
        let Some(body) = client.backend().block_body(block_hash).await? else {
            return Ok(false);
        };
        Ok(body.iter().any(|ext| {
            let Ok((_, stripped)) = strip_compact_prefix(ext) else {
                return false;
            };
            T::Hasher::hash_of(&stripped) == ext_hash
        }))
    }

    fn push_recent_finalized(recent: &mut VecDeque<BlockRef<T::Hash>>, block_ref: BlockRef<T::Hash>) {
        recent.push_back(block_ref);
        while recent.len() > Self::RECENT_FINALIZED_CAP {
            recent.pop_front();
        }
    }

    async fn wait_finalized_timeout_fallback(
        client: &C,
        ext_hash: T::Hash,
        last_in_best: Option<BlockRef<T::Hash>>,
        recent_finalized: &VecDeque<BlockRef<T::Hash>>,
    ) -> Result<TxInBlock<T, C>, Error> {
        if let Some(br) = last_in_best {
            return Ok(TxInBlock::new(br, ext_hash, client.clone()));
        }
        for br in recent_finalized.iter().rev() {
            if Self::block_contains_extrinsic(client, ext_hash, br.hash()).await? {
                return Ok(TxInBlock::new(br.clone(), ext_hash, client.clone()));
            }
        }
        let br = client.backend().latest_finalized_block_ref().await?;
        if Self::block_contains_extrinsic(client, ext_hash, br.hash()).await? {
            return Ok(TxInBlock::new(br, ext_hash, client.clone()));
        }
        Err(Error::Other(
            "Timeout waiting for the transaction to be finalized".into(),
        ))
    }

    /// Wait for the transaction to be finalized, and return a [`TxInBlock`]
    /// instance when it is, or an error if there was a problem waiting for finalization.
    ///
    /// **Note:** consumes `self`. If you'd like to perform multiple actions as the state of the
    /// transaction progresses, use [`TxProgress::next()`] instead.
    ///
    /// **Note:** transaction statuses like `Invalid`/`Usurped`/`Dropped` indicate with some
    /// probability that the transaction will not make it into a block but there is no guarantee
    /// that this is true. In those cases the stream is closed however, so you currently have no way to find
    /// out if they finally made it into a block or not.
    ///
    /// **Note:** on some nodes or multi-validator networks the transaction watch stream may never
    /// deliver [`TxStatus::InFinalizedBlock`] even though the extrinsic is finalized. This method
    /// therefore multiplexes with finalized block headers, scans recent finalized blocks for the
    /// extrinsic hash, and enforces an overall timeout so the future does not hang indefinitely.
    pub async fn wait_for_finalized(mut self) -> Result<TxInBlock<T, C>, Error> {
        let ext_hash = self.ext_hash;
        let client = self.client.clone();
        let deadline = instant::Instant::now() + Self::WAIT_FOR_FINALIZED_TIMEOUT;
        let mut last_in_best: Option<BlockRef<T::Hash>> = None;
        let mut recent_finalized: VecDeque<BlockRef<T::Hash>> = VecDeque::new();
        let mut fin_sub = match client.backend().stream_finalized_block_headers().await {
            Ok(s) => Some(s),
            Err(_) => None,
        };

        loop {
            if instant::Instant::now() >= deadline {
                return Self::wait_finalized_timeout_fallback(
                    &client,
                    ext_hash,
                    last_in_best,
                    &recent_finalized,
                )
                .await;
            }

            let remaining = deadline.saturating_duration_since(instant::Instant::now());
            let tick = Duration::from_secs(1).min(remaining);

            let tx_next = self.next().boxed();
            let timer = futures_timer::Delay::new(tick).boxed();

            match futures::future::select(tx_next, timer).await {
                Either::Left((tx_status, _)) => match tx_status {
                    None => {
                        return Self::wait_finalized_timeout_fallback(
                            &client,
                            ext_hash,
                            last_in_best,
                            &recent_finalized,
                        )
                        .await;
                    }
                    Some(Err(e)) => return Err(e),
                    Some(Ok(status)) => match status {
                        TxStatus::InFinalizedBlock(s) => return Ok(s),
                        TxStatus::InBestBlock(s) => {
                            last_in_best = Some(BlockRef::from_hash(s.block_hash()));
                        }
                        TxStatus::Error { message } => {
                            return Err(TransactionError::Error(message).into());
                        }
                        TxStatus::Invalid { message } => {
                            return Err(TransactionError::Invalid(message).into());
                        }
                        TxStatus::Dropped { message } => {
                            return Err(TransactionError::Dropped(message).into());
                        }
                        _ => {}
                    },
                },
                Either::Right((_, _)) => {
                    // Timer tick: pull any ready finalized headers and scan bodies (watch stream can stall).
                    if let Some(ref mut fin) = fin_sub {
                        let drain_until = instant::Instant::now() + Duration::from_millis(100);
                        while instant::Instant::now() < drain_until {
                            let nf = fin.next().boxed();
                            let z = futures_timer::Delay::new(Duration::from_millis(2)).boxed();
                            match futures::future::select(nf, z).await {
                                Either::Left((fin_out, _)) => {
                                    if let Some(hdr_res) = fin_out {
                                        let (_header, block_ref) = hdr_res?;
                                        Self::push_recent_finalized(
                                            &mut recent_finalized,
                                            block_ref.clone(),
                                        );
                                        if Self::block_contains_extrinsic(
                                            &client,
                                            ext_hash,
                                            block_ref.hash(),
                                        )
                                        .await?
                                        {
                                            return Ok(TxInBlock::new(
                                                block_ref,
                                                ext_hash,
                                                client.clone(),
                                            ));
                                        }
                                    }
                                }
                                Either::Right((_, _)) => break,
                            }
                        }
                    }
                    for br in recent_finalized.iter().rev() {
                        if Self::block_contains_extrinsic(&client, ext_hash, br.hash()).await? {
                            return Ok(TxInBlock::new(br.clone(), ext_hash, client.clone()));
                        }
                    }
                }
            }
        }
    }

    /// Wait for the transaction to be finalized, and for the transaction events to indicate
    /// that the transaction was successful. Returns the events associated with the transaction,
    /// as well as a couple of other details (block hash and extrinsic hash).
    ///
    /// **Note:** consumes self. If you'd like to perform multiple actions as progress is made,
    /// use [`TxProgress::next()`] instead.
    ///
    /// **Note:** transaction statuses like `Invalid`/`Usurped`/`Dropped` indicate with some
    /// probability that the transaction will not make it into a block but there is no guarantee
    /// that this is true. In those cases the stream is closed however, so you currently have no way to find
    /// out if they finally made it into a block or not.
    pub async fn wait_for_finalized_success(
        self,
    ) -> Result<crate::blocks::ExtrinsicEvents<T>, Error> {
        let evs = self.wait_for_finalized().await?.wait_for_success().await?;
        Ok(evs)
    }
}

impl<T: Config, C: Clone> Stream for TxProgress<T, C> {
    type Item = Result<TxStatus<T, C>, Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let sub = match self.sub.as_mut() {
            Some(sub) => sub,
            None => return Poll::Ready(None),
        };

        sub.poll_next_unpin(cx).map_ok(|status| {
            match status {
                BackendTxStatus::Validated => TxStatus::Validated,
                BackendTxStatus::Broadcasted { num_peers } => TxStatus::Broadcasted { num_peers },
                BackendTxStatus::NoLongerInBestBlock => TxStatus::NoLongerInBestBlock,
                BackendTxStatus::InBestBlock { hash } => {
                    TxStatus::InBestBlock(TxInBlock::new(hash, self.ext_hash, self.client.clone()))
                }
                // These stream events mean that nothing further will be sent:
                BackendTxStatus::InFinalizedBlock { hash } => {
                    self.sub = None;
                    TxStatus::InFinalizedBlock(TxInBlock::new(
                        hash,
                        self.ext_hash,
                        self.client.clone(),
                    ))
                }
                BackendTxStatus::Error { message } => {
                    self.sub = None;
                    TxStatus::Error { message }
                }
                BackendTxStatus::Invalid { message } => {
                    self.sub = None;
                    TxStatus::Invalid { message }
                }
                BackendTxStatus::Dropped { message } => {
                    self.sub = None;
                    TxStatus::Dropped { message }
                }
            }
        })
    }
}

/// Possible transaction statuses returned from our [`TxProgress::next()`] call.
#[derive_where(Debug; C)]
pub enum TxStatus<T: Config, C> {
    /// Transaction is part of the future queue.
    Validated,
    /// The transaction has been broadcast to other nodes.
    Broadcasted {
        /// Number of peers it's been broadcast to.
        num_peers: u32,
    },
    /// Transaction is no longer in a best block.
    NoLongerInBestBlock,
    /// Transaction has been included in block with given hash.
    InBestBlock(TxInBlock<T, C>),
    /// Transaction has been finalized by a finality-gadget, e.g GRANDPA
    InFinalizedBlock(TxInBlock<T, C>),
    /// Something went wrong in the node.
    Error {
        /// Human readable message; what went wrong.
        message: String,
    },
    /// Transaction is invalid (bad nonce, signature etc).
    Invalid {
        /// Human readable message; why was it invalid.
        message: String,
    },
    /// The transaction was dropped.
    Dropped {
        /// Human readable message; why was it dropped.
        message: String,
    },
}

impl<T: Config, C> TxStatus<T, C> {
    /// A convenience method to return the finalized details. Returns
    /// [`None`] if the enum variant is not [`TxStatus::InFinalizedBlock`].
    pub fn as_finalized(&self) -> Option<&TxInBlock<T, C>> {
        match self {
            Self::InFinalizedBlock(val) => Some(val),
            _ => None,
        }
    }

    /// A convenience method to return the best block details. Returns
    /// [`None`] if the enum variant is not [`TxStatus::InBestBlock`].
    pub fn as_in_block(&self) -> Option<&TxInBlock<T, C>> {
        match self {
            Self::InBestBlock(val) => Some(val),
            _ => None,
        }
    }
}

/// This struct represents a transaction that has made it into a block.
#[derive_where(Debug; C)]
pub struct TxInBlock<T: Config, C> {
    block_ref: BlockRef<T::Hash>,
    ext_hash: T::Hash,
    client: C,
}

impl<T: Config, C> TxInBlock<T, C> {
    pub(crate) fn new(block_ref: BlockRef<T::Hash>, ext_hash: T::Hash, client: C) -> Self {
        Self {
            block_ref,
            ext_hash,
            client,
        }
    }

    /// Return the hash of the block that the transaction has made it into.
    pub fn block_hash(&self) -> T::Hash {
        self.block_ref.hash()
    }

    /// Return the hash of the extrinsic that was submitted.
    pub fn extrinsic_hash(&self) -> T::Hash {
        self.ext_hash
    }
}

impl<T: Config, C: OnlineClientT<T>> TxInBlock<T, C> {
    /// Fetch the events associated with this transaction. If the transaction
    /// was successful (ie no `ExtrinsicFailed`) events were found, then we return
    /// the events associated with it. If the transaction was not successful, or
    /// something else went wrong, we return an error.
    ///
    /// **Note:** If multiple `ExtrinsicFailed` errors are returned (for instance
    /// because a pallet chooses to emit one as an event, which is considered
    /// abnormal behaviour), it is not specified which of the errors is returned here.
    /// You can use [`TxInBlock::fetch_events`] instead if you'd like to
    /// work with multiple "error" events.
    ///
    /// **Note:** This has to download block details from the node and decode events
    /// from them.
    pub async fn wait_for_success(&self) -> Result<crate::blocks::ExtrinsicEvents<T>, Error> {
        let events = self.fetch_events().await?;

        // Try to find any errors; return the first one we encounter.
        for ev in events.iter() {
            let ev = ev?;
            if ev.pallet_name() == "System" && ev.variant_name() == "ExtrinsicFailed" {
                let dispatch_error =
                    DispatchError::decode_from(ev.field_bytes(), self.client.metadata())?;
                return Err(Error::Runtime(dispatch_error));
            }
        }

        Ok(events)
    }

    /// Fetch all of the events associated with this transaction. This succeeds whether
    /// the transaction was a success or not; it's up to you to handle the error and
    /// success events however you prefer.
    ///
    /// **Note:** This has to download block details from the node and decode events
    /// from them.
    pub async fn fetch_events(&self) -> Result<crate::blocks::ExtrinsicEvents<T>, Error> {
        let block_body = self
            .client
            .backend()
            .block_body(self.block_ref.hash())
            .await?
            .ok_or(Error::Transaction(TransactionError::BlockNotFound))?;

        let extrinsic_idx = block_body
            .iter()
            .position(|ext| {
                use crate::config::Hasher;
                let Ok((_, stripped)) = strip_compact_prefix(ext) else {
                    return false;
                };
                let hash = T::Hasher::hash_of(&stripped);
                hash == self.ext_hash
            })
            // If we successfully obtain the block hash we think contains our
            // extrinsic, the extrinsic should be in there somewhere..
            .ok_or(Error::Transaction(TransactionError::BlockNotFound))?;

        let events = EventsClient::new(self.client.clone())
            .at(self.block_ref.clone())
            .await?;

        Ok(crate::blocks::ExtrinsicEvents::new(
            self.ext_hash,
            extrinsic_idx as u32,
            events,
        ))
    }
}

#[cfg(test)]
mod test {
    use subxt_core::client::RuntimeVersion;

    use crate::{
        backend::{
            sealed::Sealed, Backend, BlockRef, StreamOf, StreamOfResults, StorageResponse,
            TransactionStatus,
        },
        client::{OfflineClientT, OnlineClientT},
        tx::TxProgress,
        Config, Error, SubstrateConfig,
    };
    use async_trait::async_trait;
    use futures::stream;

    type MockTxProgress = TxProgress<SubstrateConfig, MockClient>;
    type MockHash = <SubstrateConfig as Config>::Hash;
    type MockSubstrateTxStatus = TransactionStatus<MockHash>;

    #[derive(Debug)]
    struct UnitTestStubBackend;

    impl Sealed for UnitTestStubBackend {}

    fn empty_stream<T: Send + 'static>() -> StreamOfResults<T> {
        StreamOf::new(Box::pin(stream::empty()))
    }

    #[async_trait]
    impl Backend<SubstrateConfig> for UnitTestStubBackend {
        async fn storage_fetch_values(
            &self,
            _keys: Vec<Vec<u8>>,
            _at: MockHash,
        ) -> Result<StreamOfResults<StorageResponse>, Error> {
            Ok(empty_stream())
        }

        async fn storage_fetch_descendant_keys(
            &self,
            _key: Vec<u8>,
            _at: MockHash,
        ) -> Result<StreamOfResults<Vec<u8>>, Error> {
            Ok(empty_stream())
        }

        async fn storage_fetch_descendant_values(
            &self,
            _key: Vec<u8>,
            _at: MockHash,
        ) -> Result<StreamOfResults<StorageResponse>, Error> {
            Ok(empty_stream())
        }

        async fn genesis_hash(&self) -> Result<MockHash, Error> {
            Err(Error::Other("unit test stub backend".into()))
        }

        async fn block_header(
            &self,
            _at: MockHash,
        ) -> Result<Option<<SubstrateConfig as Config>::Header>, Error> {
            Ok(None)
        }

        async fn block_body(&self, _at: MockHash) -> Result<Option<Vec<Vec<u8>>>, Error> {
            Ok(None)
        }

        async fn latest_finalized_block_ref(&self) -> Result<BlockRef<MockHash>, Error> {
            Err(Error::Other("unit test stub backend".into()))
        }

        async fn current_runtime_version(
            &self,
        ) -> Result<subxt_core::client::RuntimeVersion, Error> {
            Err(Error::Other("unit test stub backend".into()))
        }

        async fn stream_runtime_version(
            &self,
        ) -> Result<StreamOfResults<subxt_core::client::RuntimeVersion>, Error> {
            Ok(empty_stream())
        }

        async fn stream_all_block_headers(
            &self,
        ) -> Result<
            StreamOfResults<(
                <SubstrateConfig as Config>::Header,
                BlockRef<MockHash>,
            )>,
            Error,
        > {
            Ok(empty_stream())
        }

        async fn stream_best_block_headers(
            &self,
        ) -> Result<
            StreamOfResults<(
                <SubstrateConfig as Config>::Header,
                BlockRef<MockHash>,
            )>,
            Error,
        > {
            Ok(empty_stream())
        }

        async fn stream_finalized_block_headers(
            &self,
        ) -> Result<
            StreamOfResults<(
                <SubstrateConfig as Config>::Header,
                BlockRef<MockHash>,
            )>,
            Error,
        > {
            Ok(empty_stream())
        }

        async fn submit_transaction(
            &self,
            _bytes: &[u8],
        ) -> Result<StreamOfResults<TransactionStatus<MockHash>>, Error> {
            Ok(empty_stream())
        }

        async fn call(
            &self,
            _method: &str,
            _call_parameters: Option<&[u8]>,
            _at: MockHash,
        ) -> Result<Vec<u8>, Error> {
            Err(Error::Other("unit test stub backend".into()))
        }
    }

    static STUB_BACKEND: UnitTestStubBackend = UnitTestStubBackend;

    /// a mock client to satisfy trait bounds in tests
    #[derive(Clone, Debug)]
    struct MockClient;

    impl OfflineClientT<SubstrateConfig> for MockClient {
        fn metadata(&self) -> crate::Metadata {
            unimplemented!("just a mock impl to satisfy trait bounds")
        }

        fn genesis_hash(&self) -> MockHash {
            unimplemented!("just a mock impl to satisfy trait bounds")
        }

        fn runtime_version(&self) -> RuntimeVersion {
            unimplemented!("just a mock impl to satisfy trait bounds")
        }

        fn client_state(&self) -> subxt_core::client::ClientState<SubstrateConfig> {
            unimplemented!("just a mock impl to satisfy trait bounds")
        }
    }

    impl OnlineClientT<SubstrateConfig> for MockClient {
        fn backend(&self) -> &dyn crate::backend::Backend<SubstrateConfig> {
            &STUB_BACKEND
        }
    }

    #[tokio::test]
    async fn wait_for_finalized_returns_err_when_error() {
        let tx_progress = mock_tx_progress(vec![
            MockSubstrateTxStatus::Broadcasted { num_peers: 2 },
            MockSubstrateTxStatus::Error {
                message: "err".into(),
            },
        ]);
        let finalized_result = tx_progress.wait_for_finalized().await;
        assert!(matches!(
            finalized_result,
            Err(Error::Transaction(crate::error::TransactionError::Error(e))) if e == "err"
        ));
    }

    #[tokio::test]
    async fn wait_for_finalized_returns_err_when_invalid() {
        let tx_progress = mock_tx_progress(vec![
            MockSubstrateTxStatus::Broadcasted { num_peers: 2 },
            MockSubstrateTxStatus::Invalid {
                message: "err".into(),
            },
        ]);
        let finalized_result = tx_progress.wait_for_finalized().await;
        assert!(matches!(
            finalized_result,
            Err(Error::Transaction(crate::error::TransactionError::Invalid(e))) if e == "err"
        ));
    }

    #[tokio::test]
    async fn wait_for_finalized_returns_err_when_dropped() {
        let tx_progress = mock_tx_progress(vec![
            MockSubstrateTxStatus::Broadcasted { num_peers: 2 },
            MockSubstrateTxStatus::Dropped {
                message: "err".into(),
            },
        ]);
        let finalized_result = tx_progress.wait_for_finalized().await;
        assert!(matches!(
            finalized_result,
            Err(Error::Transaction(crate::error::TransactionError::Dropped(e))) if e == "err"
        ));
    }

    fn mock_tx_progress(statuses: Vec<MockSubstrateTxStatus>) -> MockTxProgress {
        let sub = create_substrate_tx_status_subscription(statuses);
        TxProgress::new(sub, MockClient, Default::default())
    }

    fn create_substrate_tx_status_subscription(
        elements: Vec<MockSubstrateTxStatus>,
    ) -> StreamOfResults<MockSubstrateTxStatus> {
        let results = elements.into_iter().map(Ok);
        let stream = Box::pin(futures::stream::iter(results));
        let sub: StreamOfResults<MockSubstrateTxStatus> = StreamOfResults::new(stream);
        sub
    }
}
