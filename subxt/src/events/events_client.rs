// Copyright 2019-2023 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or GPL-3.0.
// see LICENSE for license details.

use crate::backend::{Backend, BackendExt, BlockRef};
use crate::{client::OnlineClientT, error::Error, events::Events, Config};
use derive_where::derive_where;
use polkadot_sdk::sp_crypto_hashing;
use std::future::Future;
use codec::Encode;
use subxt_core::config::Header;

/// A client for working with events.
#[derive_where(Clone; Client)]
pub struct EventsClient<T, Client> {
    client: Client,
    _marker: std::marker::PhantomData<T>,
}

impl<T, Client> EventsClient<T, Client> {
    /// Create a new [`EventsClient`].
    pub fn new(client: Client) -> Self {
        Self {
            client,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T, Client> EventsClient<T, Client>
where
    T: Config,
    Client: OnlineClientT<T>,
{
    /// Obtain events at some block hash.
    ///
    /// # Warning
    ///
    /// This call only supports blocks produced since the most recent
    /// runtime upgrade. You can attempt to retrieve events from older blocks,
    /// but may run into errors attempting to work with them.
    pub fn at(
        &self,
        block_ref: impl Into<BlockRef<T::Hash>>,
    ) -> impl Future<Output = Result<Events<T>, Error>> + Send + 'static {
        self.at_or_latest(Some(block_ref.into()))
    }

    /// Obtain events for the latest block.
    pub fn at_latest(&self) -> impl Future<Output = Result<Events<T>, Error>> + Send + 'static {
        self.at_or_latest(None)
    }

    /// Obtain events at some block hash.
    fn at_or_latest(
        &self,
        block_ref: Option<BlockRef<T::Hash>>,
    ) -> impl Future<Output = Result<Events<T>, Error>> + Send + 'static {
        // Clone and pass the client in like this so that we can explicitly
        // return a Future that's Send + 'static, rather than tied to &self.
        let client = self.client.clone();
        async move {
            // If a block ref isn't provided, we'll get the latest finalized block to use.
            let block_ref = match block_ref {
                Some(r) => r,
                None => client.backend().latest_finalized_block_ref().await?,
            };

            let event_bytes = get_event_bytes(client.backend(), block_ref.hash()).await?;
            Ok(Events::decode_from(event_bytes, client.metadata()))
        }
    }
}

// The storage key needed to access events.
fn system_events_key(height: u32) -> Vec<u8> {
    let mut a = sp_crypto_hashing::twox_128(b"System").to_vec();
    let mut b = sp_crypto_hashing::twox_128(b"EventsMap").to_vec();
    let mut map_key_hash = sp_crypto_hashing::blake2_128(&height.encode()).to_vec();
    let mut key = height.to_le_bytes().to_vec();
    let mut res = Vec::new();
    res.append(&mut a);
    res.append(&mut b);
    res.append(&mut map_key_hash);
    res.append(&mut key);
    res
}

// Get the event bytes from the provided client, at the provided block hash.
pub(crate) async fn get_event_bytes<T: Config>(
    backend: &dyn Backend<T>,
    block_hash: T::Hash,
) -> Result<Vec<u8>, Error> {
    let number = backend
        .block_header(block_hash)
        .await?
        .ok_or(Error::Unknown("Not find block header".as_bytes().to_vec()))?;
    Ok(backend
        .storage_fetch_value(system_events_key(number.number().into() as u32).to_vec(), block_hash)
        .await?
        .unwrap_or_default())
}

#[test]
fn test_event_map_key() {
    let key = system_events_key(2974263);
    println!("key: {:?}", hex::encode(&key));
}