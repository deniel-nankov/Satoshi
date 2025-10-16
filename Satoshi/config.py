"""
Satoshi Data Pipeline Configuration

⚠️  SECURITY WARNING: This file contains API keys and secrets.
    - DO NOT commit this file to git
    - Keep this file secure and private
    - Use environment variables for production deployments

Add this file to .gitignore immediately.
"""

# =============================================================================
# 🏦 EXCHANGE CONNECTOR CONFIGURATION
# =============================================================================

EXCHANGE_CONFIG = {
    "venues": {
        # ===== COINBASE PRO =====
        "coinbase": {
            "enabled": True,
            "api_key_name": "organizations/4f9e821c-aac0-4532-9749-3d9fcfbe3251/apiKeys/2d687f42-1b83-46e1-8027-7b8ae66d9665",
            "private_key": """-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIJwQuzQbIoQMTwlYeE9C1J8MlK3MOgk7rQ7MNEqQINiioAoGCCqGSM49
AwEHoUQDQgAERP1PNinjCb/0XVUZwXu4A7RT7lS5aFjM1P91rOZa7x9TRubOE7hO
Qy6bDLkBfSwX/LQ777BuCtQF46kr7nQRAg==
-----END EC PRIVATE KEY-----""",
            # Major trading pairs - comprehensive list of available tickers
            "symbols": [
                # Top Market Cap (BTC, ETH, Major L1s)
                "BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD", "AVAX-USD", "DOT-USD",
                "MATIC-USD", "LINK-USD", "UNI-USD", "ATOM-USD", "XLM-USD", "ALGO-USD",
                
                # L2 Scaling & Infrastructure
                "ARB-USD", "OP-USD", "RNDR-USD", "FIL-USD", "ICP-USD", "APT-USD",
                "SUI-USD", "INJ-USD", "TIA-USD", "SEI-USD",
                
                # DeFi Blue Chips
                "AAVE-USD", "MKR-USD", "SNX-USD", "CRV-USD", "LDO-USD", "COMP-USD",
                "BAL-USD", "YFI-USD", "SUSHI-USD", "1INCH-USD",
                
                # Stablecoins (for pairs trading)
                "USDC-USD", "DAI-USD", "USDT-USD",
                
                # Meme & Community Tokens
                "DOGE-USD", "SHIB-USD", "PEPE-USD", "BONK-USD", "WIF-USD",
                
                # Gaming & Metaverse
                "SAND-USD", "MANA-USD", "AXS-USD", "GALA-USD", "IMX-USD",
                
                # AI & Data
                "FET-USD", "OCEAN-USD", "GRT-USD", "AGIX-USD",
                
                # Privacy & Security
                "ZEC-USD", "XMR-USD",
                
                # Other Major Tokens
                "BCH-USD", "LTC-USD", "ETC-USD", "XTZ-USD", "EOS-USD",
                "NEAR-USD", "FTM-USD", "HBAR-USD", "VET-USD", "CHZ-USD",
                
                # Cross-chain pairs (BTC/ETH pairs for relative value)
                "BTC-ETH", "ETH-BTC",
                
                # USDT pairs (higher volume alternatives)
                "BTC-USDT", "ETH-USDT", "SOL-USDT", "AVAX-USDT", "MATIC-USDT"
            ],
            "data_types": ["trades", "book"],
            "rate_limit_qps": 50,
            "rest_endpoint": "https://api.coinbase.com/api/v3",
            "ws_endpoint": "wss://advanced-trade-ws.coinbase.com"
        },
        
        # ===== BINANCE SPOT (DISABLED - Add your keys to enable) =====
        "binance": {
            "enabled": False,  # Set to True after adding keys
            "api_key": "YOUR_BINANCE_API_KEY",
            "api_secret": "YOUR_BINANCE_SECRET",
            "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
            "data_types": ["trades", "book"],
            "rate_limit_qps": 100,
            "rest_endpoint": "https://api.binance.com",
            "ws_endpoint": "wss://stream.binance.com:9443"
        },
        
        # ===== BINANCE FUTURES (DISABLED) =====
        "binance_futures": {
            "enabled": False,
            "api_key": "YOUR_BINANCE_FUTURES_API_KEY",
            "api_secret": "YOUR_BINANCE_FUTURES_SECRET",
            "symbols": ["BTCUSDT", "ETHUSDT"],
            "data_types": ["trades", "book", "funding", "oi"],
            "rate_limit_qps": 100,
            "rest_endpoint": "https://fapi.binance.com",
            "ws_endpoint": "wss://fstream.binance.com"
        },
        
        # Add more exchanges as needed (Gemini, Kraken, OKX)
    },
    
    # Global settings
    "circuit_breaker_failure_threshold": 5,
    "health_check_interval": 60.0,
    "max_retries": 5,
    "target_uptime_pct": 99.0
}

# =============================================================================
# 📈 OPTIONS CHAIN COLLECTOR CONFIGURATION
# =============================================================================

OPTIONS_CONFIG = {
    "venues": {
        "deribit": {
            "enabled": False,  # Add keys to enable
            "api_key": "YOUR_DERIBIT_API_KEY",
            "api_secret": "YOUR_DERIBIT_SECRET",
            "symbols": ["BTC", "ETH"]
        }
    },
    "collection_interval_sec": 60,
    "rate_limit_qps": 20
}

# =============================================================================
# ⛓️ ONCHAIN COLLECTOR CONFIGURATION
# =============================================================================

ONCHAIN_CONFIG = {
    "chains": {
        # ===== ETHEREUM (DISABLED - Add RPC URL to enable) =====
        "ethereum": {
            "enabled": False,  # Set to True after adding RPC
            "rpc_url": "https://eth-mainnet.g.alchemy.com/v2/YOUR_ALCHEMY_KEY",
            "fallback_rpcs": [
                "https://mainnet.infura.io/v3/YOUR_INFURA_KEY",
                "https://rpc.ankr.com/eth"
            ],
            "block_polling_interval": 12.0,
            "confirmations_required": 12,
            "chain_id": 1
        },
        
        # Add more chains as needed (BSC, Polygon, Arbitrum, etc.)
    },
    "rate_limit_qps": 10
}

# =============================================================================
# 📰 EVENTS COLLECTOR CONFIGURATION
# =============================================================================

EVENTS_CONFIG = {
    "sources": {
        # ===== GITHUB (DISABLED - Add token to enable) =====
        "github": {
            "enabled": False,  # Set to True after adding token
            "token": "YOUR_GITHUB_TOKEN",
            "tracked_repos": [
                "ethereum/go-ethereum",
                "bitcoin/bitcoin",
                "solana-labs/solana"
            ],
            "poll_interval_sec": 600
        },
        
        # Add more sources as needed (CryptoPanic, etc.)
    },
    "rate_limit_qps": 5
}

# =============================================================================
# 🚀 STREAMING BUS (KAFKA) CONFIGURATION
# =============================================================================

STREAMING_BUS_CONFIG = {
    "bootstrap_servers": ["localhost:9092"],
    "client_id": "satoshi-data-pipeline",
    "security_protocol": "PLAINTEXT",
    "compression_type": "lz4",
    "environment": "development"
}

# =============================================================================
# 📝 USAGE NOTES
# =============================================================================

"""
CURRENT CONFIGURATION STATUS:

✅ ACTIVE:
   - Coinbase: Configured with CDP API credentials
   
⚠️  DISABLED (Add keys to enable):
   - Binance Spot
   - Binance Futures
   - Ethereum onchain
   - GitHub events
   - Deribit options

TO ADD MORE EXCHANGES:
1. Get API keys from exchange
2. Add configuration block above
3. Set "enabled": True
4. Restart pipeline

SECURITY:
- This file should be in .gitignore
- Never commit API keys to version control
- Use environment variables for production
"""
