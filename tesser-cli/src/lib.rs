pub mod alerts;
pub mod analyze;
pub mod app;
pub mod control;
pub mod data_validation;
pub mod live;
pub mod state;
pub mod telemetry;

pub use app::run as run_app;

#[cfg(feature = "bybit")]
pub use tesser_bybit::PublicChannel;

#[cfg(not(feature = "bybit"))]
pub use public_channel_stub::PublicChannel;

#[cfg(not(feature = "bybit"))]
mod public_channel_stub {
    use std::str::FromStr;

    use tesser_broker::BrokerError;

    #[derive(Clone, Copy, Debug)]
    pub enum PublicChannel {
        Linear,
        Inverse,
        Spot,
        Option,
        Spread,
    }

    impl PublicChannel {
        pub fn as_path(&self) -> &'static str {
            match self {
                Self::Linear => "linear",
                Self::Inverse => "inverse",
                Self::Spot => "spot",
                Self::Option => "option",
                Self::Spread => "spread",
            }
        }
    }

    impl FromStr for PublicChannel {
        type Err = BrokerError;

        fn from_str(value: &str) -> Result<Self, Self::Err> {
            match value.to_lowercase().as_str() {
                "linear" => Ok(Self::Linear),
                "inverse" => Ok(Self::Inverse),
                "spot" => Ok(Self::Spot),
                "option" => Ok(Self::Option),
                "spread" => Ok(Self::Spread),
                other => Err(BrokerError::InvalidRequest(format!(
                    "unsupported Bybit public channel '{other}'"
                ))),
            }
        }
    }
}
