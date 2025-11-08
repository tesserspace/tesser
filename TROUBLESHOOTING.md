# Troubleshooting

## Bybit Kline Download Returns 403

**Symptom**

Running `tesser-cli data download ...` with the Bybit testnet/mainnet endpoints fails with:

```
Bybit responded with status 403 Forbidden: The Amazon CloudFront distribution is configured to block access from your country.
```

**Cause**

Bybit serves public REST APIs via CloudFront. Some regions/IP ranges are geo-blocked by CloudFront before the request reaches Bybit. This happens even for public endpoints and is unrelated to API key/secret configuration.

**Resolution**

1. Switch to a network that CloudFront allows (VPN, cloud VM, corporate network, etc.).
2. Use Bybit's public data mirrors (https://public.bybit.com/) instead of the REST endpoint for historical klines.
3. Configure an HTTPS proxy for `tesser-cli` (e.g., set `HTTPS_PROXY`) that egresses from an allowed region.

Once the request is routed through a permitted region, rerun the download command and the CLI will save the CSV under `data/<exchange>/<symbol>/...`.
