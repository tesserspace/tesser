pub mod uppercase_key {
    use rust_decimal::Decimal;
    use serde::de::{Deserialize, Deserializer};
    use std::collections::HashMap;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<String, Decimal>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let map = HashMap::<String, Decimal>::deserialize(deserializer)?;
        Ok(map
            .into_iter()
            .map(|(k, v)| (k.to_uppercase(), v))
            .collect())
    }
}
