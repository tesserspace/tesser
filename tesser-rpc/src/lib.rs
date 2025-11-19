pub mod client;
pub mod conversions;
pub mod strategy;
pub mod transport;

// Re-export generated protos so adapters can use them
pub mod proto {
    tonic::include_proto!("tesser.rpc.v1");
}

pub use strategy::RpcStrategy;
