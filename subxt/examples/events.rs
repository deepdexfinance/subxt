#![allow(missing_docs)]

use std::str::FromStr;
use primitive_types::H256;
use subxt::{OnlineClient, PolkadotConfig};

#[subxt::subxt(runtime_metadata_path = "../artifacts/deepx-node-metadata.scale")]
pub mod polkadot {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a client to use:
    let api = OnlineClient::<PolkadotConfig>::from_insecure_url("ws://127.0.0.1:9933").await?;

    // Get events for the latest block:
    let events = api.events().at(H256::from_str("0x855a654be8922ca60573d1d56e9ce517e07e9cfc61fb9ac1de9d02cda0636ca9").unwrap()).await?;

    // // We can dynamically decode events:
    // println!("Dynamic event details:");
    // for event in events.iter() {
    //     let event = event?;
    //
    //     let pallet = event.pallet_name();
    //     let variant = event.variant_name();
    //     let field_values = event.field_values()?;
    //
    //     println!("{pallet}::{variant}: {field_values}");
    // }

    // Or we can attempt to statically decode them into the root Event type:
    println!("Static event details:");
    for event in events.iter() {
        let event = event?;

        if let Ok(ev) = event.as_root_event::<polkadot::Event>() {
            println!("{ev:?}");
        } else {
            println!("<Cannot decode event>");
        }
    }

    // // Or we can look for specific events which match our statically defined ones:
    // let transfer_event = events.find_first::<polkadot::balances::events::Transfer>()?;
    // if let Some(ev) = transfer_event {
    //     println!("  - Balance transfer success: value: {:?}", ev.amount);
    // } else {
    //     println!("  - No balance transfer event found in this block");
    // }

    Ok(())
}
