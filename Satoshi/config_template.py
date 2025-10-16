"""
Complete Data Pipeline Configuration Template

This file contains ALL implemented data sources in the Satoshi system.
Copy this to config.py and fill in your API keys.

📊 Implemented Data Sources:
- 6 Exchange Adapters (CEX/Futures)
- 5+ Blockchain Networks (EVM chains)
- Options Markets (Deribit, Binance Options)
- Off-chain Events (Governance, GitHub, Exchange Status)
"""

# =============================================================================
# 🏦 EXCHANGE CONNECTOR - 6 VENUES IMPLEMENTED
# =============================================================================

EXCHANGE_CONFIG = {
    "venues": {
        # ===== BINANCE SPOT =====
        "binance": {
            "enabled": True,  # Set to False to disable
            "api_key": "YOUR_BINANCE_API_KEY",
            "api_secret": "YOUR_BINANCE_SECRET",
            "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"],
            "data_types": ["trades", "book", "funding"],  # orderbook depth, trades, funding rates
            "rate_limit_qps": 100,  # Requests per second
            "rest_endpoint": "https://api.binance.com",
            "ws_endpoint": "wss://stream.binance.com:9443"
        },
        
        # ===== BINANCE FUTURES/PERPETUALS =====
        "binance_futures": {
            "enabled": True,
            "api_key": "YOUR_BINANCE_FUTURES_API_KEY",
            "api_secret": "YOUR_BINANCE_FUTURES_SECRET",
            "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
            "data_types": ["trades", "book", "funding", "oi"],  # + open interest
            "rate_limit_qps": 100,
            "rest_endpoint": "https://fapi.binance.com",
            "ws_endpoint": "wss://fstream.binance.com"
        },
        
        # ===== COINBASE PRO =====
        "coinbase": {
            "enabled": True,
            "api_key": "YOUR_COINBASE_API_KEY",
            "api_secret": "YOUR_COINBASE_SECRET",
            "passphrase": "YOUR_COINBASE_PASSPHRASE",  # Coinbase requires passphrase
            "symbols": ["BTC-USD", "ETH-USD", "SOL-USD"],
            "data_types": ["trades", "book"],
            "rate_limit_qps": 50,
            "rest_endpoint": "https://api.pro.coinbase.com",
            "ws_endpoint": "wss://ws-feed.pro.coinbase.com"
        },
        
        # ===== GEMINI =====
        "gemini": {
            "enabled": True,
            "api_key": "YOUR_GEMINI_API_KEY",
            "api_secret": "YOUR_GEMINI_SECRET",
            "symbols": ["BTCUSD", "ETHUSD"],
            "data_types": ["trades", "book"],
            "rate_limit_qps": 30,
            "rest_endpoint": "https://api.gemini.com",
            "ws_endpoint": "wss://api.gemini.com"
        },
        
        # ===== KRAKEN =====
        "kraken": {
            "enabled": True,
            "api_key": "YOUR_KRAKEN_API_KEY",
            "api_secret": "YOUR_KRAKEN_SECRET",
            "symbols": ["XBTUSD", "ETHUSD", "SOLUSD"],  # Kraken uses X prefix
            "data_types": ["trades", "book"],
            "rate_limit_qps": 20,
            "rest_endpoint": "https://api.kraken.com",
            "ws_endpoint": "wss://ws.kraken.com"
        },
        
        # ===== OKX =====
        "okx": {
            "enabled": True,
            "api_key": "YOUR_OKX_API_KEY",
            "api_secret": "YOUR_OKX_SECRET",
            "passphrase": "YOUR_OKX_PASSPHRASE",
            "symbols": ["BTC-USDT", "ETH-USDT", "SOL-USDT"],
            "data_types": ["trades", "book", "funding"],
            "rate_limit_qps": 60,
            "rest_endpoint": "https://www.okx.com",
            "ws_endpoint": "wss://ws.okx.com:8443"
        }
    },
    
    # Global exchange connector settings
    "circuit_breaker_failure_threshold": 5,
    "health_check_interval": 60.0,
    "max_retries": 5,
    "base_delay_ms": 100,
    "max_delay_ms": 30000,
    "target_uptime_pct": 99.5
}

# =============================================================================
# 📈 OPTIONS CHAIN COLLECTOR - 2 VENUES
# =============================================================================

OPTIONS_CONFIG = {
    "venues": {
        # ===== DERIBIT (Primary Options Exchange) =====
        "deribit": {
            "enabled": True,
            "api_key": "YOUR_DERIBIT_API_KEY",
            "api_secret": "YOUR_DERIBIT_SECRET",
            "symbols": ["BTC", "ETH"],  # Base currencies
            "rest_endpoint": "https://www.deribit.com",
            "ws_endpoint": "wss://www.deribit.com/ws/api/v2"
        },
        
        # ===== BINANCE OPTIONS =====
        "binance_options": {
            "enabled": True,
            "api_key": "YOUR_BINANCE_OPTIONS_API_KEY",
            "api_secret": "YOUR_BINANCE_OPTIONS_SECRET",
            "symbols": ["BTC", "ETH"],
            "rest_endpoint": "https://eapi.binance.com",
            "ws_endpoint": "wss://nbstream.binance.com/eoptions"
        }
    },
    
    "collection_interval_sec": 60,  # How often to poll options chain
    "rate_limit_qps": 20,
    "implied_vol_surface_resolution": 10  # Strike price intervals
}

# =============================================================================
# ⛓️ ONCHAIN COLLECTOR - 5+ BLOCKCHAIN NETWORKS
# =============================================================================

ONCHAIN_CONFIG = {
    "chains": {
        # ===== ETHEREUM MAINNET =====
        "ethereum": {
            "enabled": True,
            "rpc_url": "https://eth-mainnet.g.alchemy.com/v2/YOUR_ALCHEMY_API_KEY",
            "fallback_rpcs": [
                "https://mainnet.infura.io/v3/YOUR_INFURA_KEY",
                "https://rpc.ankr.com/eth"
            ],
            "block_polling_interval": 12.0,  # seconds
            "confirmations_required": 12,
            "chain_id": 1,
            "reorg_detection": True,
            "max_reorg_depth": 64,
            
            # Monitored contracts/addresses
            "watched_contracts": [
                "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
                "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
                "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",  # WBTC
            ],
            
            # Bridge contracts
            "bridge_contracts": [
                "0xa0c68c638235ee32657e8f720a23cec1bfc77c77",  # Polygon Bridge
                "0x8315177ab297ba92a06054ce80a67ed4dbd7ed3a",  # Arbitrum Bridge
                "0x99c9fc46f92e8a1c0dec1b1747d010903e884be1",  # Optimism Bridge
            ]
        },
        
        # ===== BINANCE SMART CHAIN =====
        "bsc": {
            "enabled": True,
            "rpc_url": "https://bsc-dataseed1.binance.org",
            "fallback_rpcs": [
                "https://bsc-dataseed2.binance.org",
                "https://bsc-dataseed.binance.org"
            ],
            "block_polling_interval": 3.0,  # BSC is faster
            "confirmations_required": 15,
            "chain_id": 56,
            "reorg_detection": True,
            "max_reorg_depth": 128
        },
        
        # ===== POLYGON =====
        "polygon": {
            "enabled": True,
            "rpc_url": "https://polygon-mainnet.g.alchemy.com/v2/YOUR_ALCHEMY_KEY",
            "fallback_rpcs": [
                "https://polygon-rpc.com",
                "https://rpc.ankr.com/polygon"
            ],
            "block_polling_interval": 2.0,
            "confirmations_required": 128,  # Polygon has higher reorg risk
            "chain_id": 137,
            "reorg_detection": True,
            "max_reorg_depth": 256
        },
        
        # ===== ARBITRUM =====
        "arbitrum": {
            "enabled": True,
            "rpc_url": "https://arb-mainnet.g.alchemy.com/v2/YOUR_ALCHEMY_KEY",
            "fallback_rpcs": [
                "https://arb1.arbitrum.io/rpc",
                "https://rpc.ankr.com/arbitrum"
            ],
            "block_polling_interval": 0.25,  # Arbitrum is very fast
            "confirmations_required": 10,
            "chain_id": 42161,
            "reorg_detection": True,
            "max_reorg_depth": 32
        },
        
        # ===== OPTIMISM =====
        "optimism": {
            "enabled": True,
            "rpc_url": "https://opt-mainnet.g.alchemy.com/v2/YOUR_ALCHEMY_KEY",
            "fallback_rpcs": [
                "https://mainnet.optimism.io",
                "https://rpc.ankr.com/optimism"
            ],
            "block_polling_interval": 2.0,
            "confirmations_required": 10,
            "chain_id": 10,
            "reorg_detection": True,
            "max_reorg_depth": 32
        },
        
        # ===== BASE (Coinbase L2) =====
        "base": {
            "enabled": False,  # Enable if needed
            "rpc_url": "https://base-mainnet.g.alchemy.com/v2/YOUR_ALCHEMY_KEY",
            "fallback_rpcs": [
                "https://mainnet.base.org",
                "https://base.llamarpc.com"
            ],
            "block_polling_interval": 2.0,
            "confirmations_required": 10,
            "chain_id": 8453,
            "reorg_detection": True
        },
        
        # ===== AVALANCHE =====
        "avalanche": {
            "enabled": False,  # Enable if needed
            "rpc_url": "https://api.avax.network/ext/bc/C/rpc",
            "fallback_rpcs": [
                "https://rpc.ankr.com/avalanche"
            ],
            "block_polling_interval": 2.0,
            "confirmations_required": 20,
            "chain_id": 43114,
            "reorg_detection": True
        }
    },
    
    # Global onchain settings
    "rate_limit_qps": 10,
    "max_block_batch_size": 100,
    "health_check_interval": 60.0
}

# =============================================================================
# 📰 EVENTS COLLECTOR - OFF-CHAIN DATA SOURCES
# =============================================================================

EVENTS_CONFIG = {
    "sources": {
        # ===== GOVERNANCE (DAO Proposals) =====
        "snapshot": {
            "enabled": True,
            "api_endpoint": "https://hub.snapshot.org/graphql",
            "tracked_spaces": [
                "aave.eth",
                "compound-governance.eth",
                "uniswap",
                "ens.eth",
                "gitcoindao.eth"
            ],
            "poll_interval_sec": 300  # 5 minutes
        },
        
        "compound_governance": {
            "enabled": True,
            "api_endpoint": "https://api.compound.finance/governance/proposals",
            "poll_interval_sec": 300
        },
        
        # ===== GITHUB RELEASES =====
        "github": {
            "enabled": True,
            "token": "YOUR_GITHUB_TOKEN",  # Required for higher rate limits
            "tracked_repos": [
                "ethereum/go-ethereum",      # Geth
                "bitcoin/bitcoin",           # Bitcoin Core
                "paritytech/substrate",      # Polkadot
                "solana-labs/solana",        # Solana
                "cosmos/cosmos-sdk",         # Cosmos
                "bnb-chain/bsc"              # BSC
            ],
            "poll_interval_sec": 600,  # 10 minutes
            "use_etag_caching": True
        },
        
        # ===== EXCHANGE STATUS/MAINTENANCE =====
        "binance_status": {
            "enabled": True,
            "api_endpoint": "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query",
            "poll_interval_sec": 300
        },
        
        "coinbase_status": {
            "enabled": True,
            "api_endpoint": "https://www.coinbasestatus.com/api/v2/summary.json",
            "poll_interval_sec": 180
        },
        
        # ===== TOKEN UNLOCK CALENDARS =====
        "token_unlocks": {
            "enabled": True,
            "api_endpoint": "https://token.unlocks.app/api",  # Example
            "api_key": "YOUR_TOKEN_UNLOCKS_API_KEY",
            "tracked_tokens": ["UNI", "DYDX", "APE", "OP"],
            "poll_interval_sec": 3600  # 1 hour
        },
        
        # ===== NEWS AGGREGATORS =====
        "cryptopanic": {
            "enabled": True,
            "api_key": "YOUR_CRYPTOPANIC_API_KEY",
            "api_endpoint": "https://cryptopanic.com/api/v1/posts/",
            "poll_interval_sec": 300,
            "filters": {
                "currencies": "BTC,ETH,SOL",
                "kind": "news"  # news, media, all
            }
        },
        
        "coindesk": {
            "enabled": False,  # RSS feed, no API key needed
            "rss_feed": "https://www.coindesk.com/arc/outboundfeeds/rss/",
            "poll_interval_sec": 600
        }
    },
    
    # Event processing settings
    "calendar_queue_size": 5000,
    "correlation_window_hours": 24,
    "health_check_interval": 300.0,
    "max_retries": 3,
    "base_delay_ms": 1000
}

# =============================================================================
# 🚀 STREAMING BUS (KAFKA) CONFIGURATION
# =============================================================================

STREAMING_BUS_CONFIG = {
    "bootstrap_servers": ["localhost:9092"],
    "client_id": "satoshi-data-pipeline",
    "security_protocol": "PLAINTEXT",
    
    # For production with SSL:
    # "security_protocol": "SSL",
    # "enable_ssl": True,
    # "ssl_cafile": "/path/to/ca-cert",
    # "ssl_certfile": "/path/to/client-cert",
    # "ssl_keyfile": "/path/to/client-key",
    
    # Compression
    "compression_type": "lz4",  # lz4, snappy, gzip, zstd
    
    # Performance tuning
    "max_batch_size": 1000000,  # 1MB
    "linger_ms": 10,  # Batch delay for throughput
    "acks": "all",  # Wait for all replicas (durability)
    
    # Consumer settings
    "auto_offset_reset": "latest",  # latest or earliest
    "enable_auto_commit": False,  # Manual commit for reliability
    "session_timeout_ms": 30000,
    "heartbeat_interval_ms": 3000
}

# =============================================================================
# 🎯 ORCHESTRATION MODES
# =============================================================================

ORCHESTRATION_MODES = {
    "development": {
        "quality_threshold": 90.0,  # Lenient
        "circuit_breaker_failure_threshold": 20,
        "circuit_breaker_recovery_timeout": 30,
        "pipeline_mode": "RESILIENT"
    },
    
    "institutional": {
        "quality_threshold": 99.0,  # Strict
        "circuit_breaker_failure_threshold": 5,
        "circuit_breaker_recovery_timeout": 60,
        "pipeline_mode": "STRICT"
    }
}

# =============================================================================
# 📝 USAGE INSTRUCTIONS
# =============================================================================

"""
SETUP STEPS:

1. Copy this file to config.py:
   cp config_template.py config.py

2. Fill in your API keys in config.py

3. Enable/disable data sources by setting "enabled": True/False

4. Run the test pipeline (no API keys needed):
   python3 test_data_pipeline.py

5. Run production pipeline:
   python3 run_data_pipeline.py development
   python3 run_data_pipeline.py institutional

PRIORITY SETUP (Start with these):
- Binance (most liquid CEX)
- Ethereum (most important chain)
- GitHub (critical release monitoring)

OPTIONAL SETUP (Add later):
- Additional exchanges (Gemini, Kraken, OKX)
- L2 chains (Arbitrum, Optimism, Base)
- Governance trackers
- News aggregators
"""
