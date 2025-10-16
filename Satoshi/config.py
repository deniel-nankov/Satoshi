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
        
        # ===== GEMINI EXCHANGE =====
        "gemini": {
            "enabled": True,
            "api_key": "master-nnYtPydAmdPjufO8legi",
            "api_secret": "3hibQDRv5SM4juUCngNc58TdMPpU",
            # Major trading pairs available on Gemini
            "symbols": [
                # Top Market Cap
                "BTCUSD", "ETHUSD", "SOLUSD", "ADAUSD", "AVAXUSD", "DOTUSD",
                "MATICUSD", "LINKUSD", "UNIUSD", "ATOMUSD",
                
                # L2 & Infrastructure
                "ARBUSD", "OPUSD", "APTUSD",
                
                # DeFi Blue Chips
                "AAVEUSD", "MKRUSD", "COMPUSD", "YFIUSD",
                
                # Stablecoins
                "USDCUSD", "DAIUSD", "USDTUSD",
                
                # Meme Tokens
                "DOGEUSD", "SHIBUSD",
                
                # Other Major Assets
                "BCHUSD", "LTCUSD", "ETCUSD", "ZECUSD",
                "BATUSD", "ENJUSD", "MANAUSD", "SANDUSD",
                
                # Cross-pairs for arbitrage
                "ETHBTC", "LTCBTC"
            ],
            "data_types": ["trades", "book"],
            "rate_limit_qps": 100,
            "rest_endpoint": "https://api.gemini.com",
            "ws_endpoint": "wss://api.gemini.com/v1/marketdata"
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
#
# 🎯 INFRASTRUCTURE STRATEGY:
#
# PHASE 1 (Current - $49/month QuickNode Build Plan):
#   • Ethereum: QuickNode premium endpoint (90% of on-chain alpha)
#   • L2 chains: Free public RPCs (Arbitrum, Base, Polygon, Optimism)
#   • Expected alpha: 100-190 bps daily
#   • Cost: $49/month
#
# PHASE 2 (Upgrade to $499/month QuickNode Scale Plan when revenue justifies):
#   • Replace all free RPCs with dedicated QuickNode endpoints
#   • 5 premium endpoints = higher reliability, lower latency, no rate limits
#   • Expected alpha: 150-290 bps daily
#   • Upgrade trigger: $500k+ AUM or when free RPC rate limits hit
#
# =============================================================================

ONCHAIN_CONFIG = {
    "chains": {
        # ===== ETHEREUM MAINNET (QuickNode Premium - Build Plan) =====
        "ethereum": {
            "enabled": True,
            "rpc_url": "https://rough-boldest-bird.quiknode.pro/0b65e1be858da25e93b81fd776f043f1dc11501b/",
            "ws_url": "wss://rough-boldest-bird.quiknode.pro/0b65e1be858da25e93b81fd776f043f1dc11501b/",
            "fallback_rpcs": [
                "https://eth.llamarpc.com",
                "https://rpc.ankr.com/eth"
            ],
            "block_polling_interval": 12.0,
            "confirmations_required": 12,
            "chain_id": 1,
            "reorg_depth": 12,
            "finality_blocks": 32,
            # Alpha sources: Stablecoin mints/burns, CEX flows, LST discounts
            "data_types": ["flows", "lst_state", "bridge"],
            "priority": "critical"  # Highest alpha per your architecture
        },
        
        # ===== ARBITRUM (L2 - Free Public RPC → Upgrade to QuickNode at Scale Plan) =====
        "arbitrum": {
            "enabled": True,
            "rpc_url": "https://arb1.arbitrum.io/rpc",  # Free public RPC
            # TODO: Replace with QuickNode endpoint when upgrading to Scale plan ($499/mo)
            # "rpc_url": "https://YOUR-ARBITRUM-ENDPOINT.arbitrum-mainnet.quiknode.pro/YOUR-TOKEN/",
            "fallback_rpcs": [
                "https://rpc.ankr.com/arbitrum",
                "https://arbitrum.llamarpc.com"
            ],
            "block_polling_interval": 0.25,  # 250ms blocks
            "confirmations_required": 1,
            "chain_id": 42161,
            "reorg_depth": 1,
            "finality_blocks": 10,
            # Optimized for high-throughput L2 batching (900 batch size)
            "l2_optimized": True,
            "batch_size": 900,
            "queue_size": 60000,
            "publish_concurrency": 12,
            "data_types": ["flows", "bridge"],
            "priority": "high"
        },
        
        # ===== BASE (Coinbase L2 - Free Public RPC → Upgrade to QuickNode at Scale Plan) =====
        "base": {
            "enabled": True,
            "rpc_url": "https://mainnet.base.org",  # Free public RPC
            # TODO: Replace with QuickNode endpoint when upgrading to Scale plan ($499/mo)
            # "rpc_url": "https://YOUR-BASE-ENDPOINT.base-mainnet.quiknode.pro/YOUR-TOKEN/",
            "fallback_rpcs": [
                "https://base.llamarpc.com",
                "https://base.blockpi.network/v1/rpc/public"
            ],
            "block_polling_interval": 2.0,
            "confirmations_required": 1,
            "chain_id": 8453,
            "reorg_depth": 1,
            "finality_blocks": 10,
            # Synergistic with your Coinbase exchange data
            "l2_optimized": True,
            "data_types": ["flows", "bridge"],
            "priority": "high",
            "note": "Pairs with Coinbase market data for CEX<->L2 arbitrage"
        },
        
        # ===== POLYGON (Free Public RPC → Upgrade to QuickNode at Scale Plan) =====
        "polygon": {
            "enabled": True,
            "rpc_url": "https://polygon-rpc.com",  # Free public RPC
            # TODO: Replace with QuickNode endpoint when upgrading to Scale plan ($499/mo)
            # "rpc_url": "https://YOUR-POLYGON-ENDPOINT.matic.quiknode.pro/YOUR-TOKEN/",
            "fallback_rpcs": [
                "https://rpc.ankr.com/polygon",
                "https://polygon.llamarpc.com"
            ],
            "block_polling_interval": 2.0,
            "confirmations_required": 256,  # Polygon checkpoint system
            "chain_id": 137,
            "reorg_depth": 256,
            "finality_blocks": 256,
            # High stablecoin volume, bridge flows
            "l2_optimized": True,
            "batch_size": 750,
            "queue_size": 45000,
            "data_types": ["flows", "bridge"],
            "priority": "medium"
        },
        
        # ===== OPTIMISM (Free Public RPC → Upgrade to QuickNode at Scale Plan) =====
        "optimism": {
            "enabled": True,
            "rpc_url": "https://mainnet.optimism.io",  # Free public RPC
            # TODO: Replace with QuickNode endpoint when upgrading to Scale plan ($499/mo)
            # "rpc_url": "https://YOUR-OPTIMISM-ENDPOINT.optimism.quiknode.pro/YOUR-TOKEN/",
            "fallback_rpcs": [
                "https://rpc.ankr.com/optimism",
                "https://optimism.llamarpc.com"
            ],
            "block_polling_interval": 2.0,
            "confirmations_required": 1,
            "chain_id": 10,
            "reorg_depth": 1,
            "finality_blocks": 12,
            # Major DEX presence (Velodrome, Uniswap)
            "l2_optimized": True,
            "batch_size": 750,
            "queue_size": 45000,
            "publish_concurrency": 10,
            "data_types": ["flows", "bridge"],
            "priority": "medium"
        }
    },
    
    # Global on-chain settings
    "rate_limit_qps": 10,
    "circuit_breaker_failure_threshold": 5,
    "health_check_interval": 60.0,
    "max_retries": 5,
    
    # High-throughput L2 optimization (matches your architecture)
    "high_throughput_chains": ["arbitrum", "optimism", "polygon"],
    "l2_batch_size": 600,
    "l2_publish_concurrency": 8,
    "l2_auto_tune_enabled": True
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
