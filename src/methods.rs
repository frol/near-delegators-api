use crate::extensions::{self, CallResultExt, RpcQueryResponseExt};

use color_eyre::{eyre, eyre::Context, Result};

use near_jsonrpc_client::JsonRpcClient;

use borsh::BorshDeserialize;

use futures::{stream::StreamExt, TryStreamExt};
use std::collections::BTreeSet;

pub const ATTEMPTS: u32 = 200000;
pub const LIMIT: usize = 500;

pub async fn get_receiver_id(
    beta_json_rpc_client: &JsonRpcClient,
    receipt_id: String,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    info!("Fetching receipt");

    for _ in 0..ATTEMPTS {
        let Ok(receipt_response) = beta_json_rpc_client
            .call(
                near_jsonrpc_client::methods::EXPERIMENTAL_receipt::RpcReceiptRequest {
                    receipt_reference: near_jsonrpc_primitives::types::receipts::ReceiptReference {
                        receipt_id: receipt_id.parse()?,
                    },
                },
            )
            .await
        else {
            warn!("Failed to get receiver_id for receipt_id: {receipt_id}. Retrying...");
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            continue;
        };

        return Ok(receipt_response.receiver_id.to_string());
    }

    Err(Box::new(
        near_jsonrpc_primitives::types::receipts::RpcReceiptError::InternalError {
            error_message: String::from("Failed to fetch receipt"),
        },
    ))
}

pub async fn get_block_id(
    beta_json_rpc_client: &JsonRpcClient,
    block_reference: near_primitives::types::BlockReference,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    info!("Fetching block ID");

    for _ in 0..ATTEMPTS {
        let Ok(block_response) = beta_json_rpc_client
            .call(near_jsonrpc_client::methods::block::RpcBlockRequest {
                block_reference: block_reference.clone(),
            })
            .await
        else {
            warn!("Failed to get block_id for block_reference: {block_reference:?}. Retrying...");
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            continue;
        };

        return Ok(block_response.header.height);
    }

    Err(Box::new(
        near_jsonrpc_primitives::types::receipts::RpcReceiptError::InternalError {
            error_message: String::from("Failed to fetch block id"),
        },
    ))
}

pub async fn get_all_validators(
    beta_json_rpc_client: &JsonRpcClient,
    block_id: u64,
) -> Result<BTreeSet<String>> {
    info!("Fetching all validators");

    let mut validators = BTreeSet::new();

    for account_id in ["pool.near", "poolv1.near"] {
        let query_view_method_response = beta_json_rpc_client
            .call(&near_jsonrpc_client::methods::any::<
                Result<serde_json::Value, near_jsonrpc_client::methods::query::RpcQueryError>,
            >(
                "view_state_paginated",
                serde_json::json!({
                    "block_id": block_id,
                    "account_id": account_id,
                }),
            ))
            .await
            .context("Failed to fetch query ViewState for <poolv1.near> on network <beta-rpc>")?;
        let query_view_method_response: near_jsonrpc_client::methods::query::RpcQueryResponse =
            serde_json::from_value(query_view_method_response["Ok"].clone()).unwrap();
        if let near_jsonrpc_primitives::types::query::QueryResponseKind::ViewState(result) =
            query_view_method_response.kind
        {
            info!("Parsing validators");

            validators.extend(result.values.iter().filter_map(|item| {
                if &item.key[..2] == b"se" {
                    String::try_from_slice(&item.value)
                        .ok()
                        .and_then(|result| result.parse().ok())
                } else {
                    None
                }
            }));
        } else {
            error!("Failed to parse validators");

            return Err(color_eyre::Report::msg("Error call result".to_string()));
        }
    }

    Ok(validators)
}

async fn get_number_of_delegators(
    beta_json_rpc_client: &JsonRpcClient,
    block_reference: near_primitives::types::BlockReference,
    validator_account_id: String,
) -> Result<usize> {
    let delegators_response = beta_json_rpc_client
        .call(near_jsonrpc_client::methods::query::RpcQueryRequest {
            block_reference,
            request: near_primitives::views::QueryRequest::CallFunction {
                account_id: validator_account_id.parse()?,
                method_name: "get_number_of_accounts".to_string(),
                args: near_primitives::types::FunctionArgs::from(serde_json::to_vec(
                    &serde_json::json!(null),
                )?),
            },
        })
        .await;

    match delegators_response {
        Ok(response) => response
            .call_result()?
            .parse_result_from_json::<usize>()
            .context("Failed to parse delegators"),
        Err(near_jsonrpc_client::errors::JsonRpcError::ServerError(
            near_jsonrpc_client::errors::JsonRpcServerError::HandlerError(
                near_jsonrpc_client::methods::query::RpcQueryError::NoContractCode { .. }
                | near_jsonrpc_client::methods::query::RpcQueryError::ContractExecutionError {
                    ..
                },
            ),
        )) => Ok(0),
        Err(err) => Err(err.into()),
    }
}

pub async fn get_delegators_by_validator_account_id(
    beta_json_rpc_client: &JsonRpcClient,
    validator_account_id: String,
    block_reference: near_primitives::types::BlockReference,
) -> Result<BTreeSet<String>> {
    let number_of_delegators = dbg!(
        get_number_of_delegators(
            beta_json_rpc_client,
            block_reference.clone(),
            validator_account_id.clone(),
        )
        .await
    )?;

    let delegators = futures::stream::iter((0..number_of_delegators).step_by(LIMIT)).map(|from| {
        let block_reference = block_reference.clone();
        let validator_account_id = validator_account_id.clone();

        async move {
            let mut error = Err(eyre::eyre!("unreachable"));
            for _ in 0..ATTEMPTS {
                let delegators_response = beta_json_rpc_client
                    .call(near_jsonrpc_client::methods::query::RpcQueryRequest {
                        block_reference: block_reference.clone(),
                        request: near_primitives::views::QueryRequest::CallFunction {
                            account_id: validator_account_id.parse()?,
                            method_name: "get_accounts".to_string(),
                            args: near_primitives::types::FunctionArgs::from(serde_json::to_vec(
                                &serde_json::json!({
                                    "from_index": from,
                                    "limit": LIMIT,
                                }),
                            )?),
                        },
                    })
                    .await;

                match delegators_response {
                    Ok(response) => return response
                        .call_result()?
                        .parse_result_from_json::<BTreeSet<extensions::Delegator>>()
                        .map(|delegators| {
                            delegators
                                .into_iter()
                                .map(|delegator| delegator.account_id.to_string())
                                .collect::<BTreeSet<_>>()
                        })
                        .context("Failed to parse delegators"),
                    Err(near_jsonrpc_client::errors::JsonRpcError::ServerError(
                        near_jsonrpc_client::errors::JsonRpcServerError::HandlerError(
                            near_jsonrpc_client::methods::query::RpcQueryError::NoContractCode { .. }
                            | near_jsonrpc_client::methods::query::RpcQueryError::ContractExecutionError {
                                ..
                            },
                        ),
                    )) => return Ok(BTreeSet::new()),
                    Err(err) => error = Err(err.into()),
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            error
        }
    })
        .buffer_unordered(3)
        .try_collect::<BTreeSet<_>>()
        .await?;

    Ok(delegators.into_iter().flatten().collect())
}
