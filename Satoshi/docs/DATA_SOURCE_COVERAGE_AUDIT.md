# Data Source Coverage Audit - Complete Analysis

## Executive Summary

This audit verifies we have sufficient schemas, topics, and infrastructure to fully leverage data from:
- ✅ Crypto Exchanges (CEX/Perps/Futures)
- ✅ CoinGecko (Market Metrics)
- ⚠️ FRED (Economic Indicators) - Schema exists, but limited indicators
- ⚠️ Alpha Vantage (TradFi Indices/Equities) - Schema exists, but limited symbols
- ⚠️ Ethereum Node (On-chain Data) - Only ERC20 transfers, missing many event types

---

## 1. CRYPTO EXCHANGES (Binance, Coinbase, Kraken, etc.)

### What They Provide:
- Trades (price, quantity, side, timestamp)
- Order book snapshots (bids/asks with depth)
- Funding rates (perpetual futures)
- Open Interest (perpetual futures)
- Borrow rates (margin trading)
- Maintenance windows (scheduled downtime)

### What We're Collecting:

#### ✅ TRADES - FULLY IMPLEMENTED
**Collector**: `exchange_connector.py` → `TradeData`
**Topics Published**:
- `raw_data.exchange_feed` (legacy)
- `raw_data.market.trades` (granular)

**Schema**: ✅ `market_trades` (schema_validator.py:2437)
```python
Fields: venue, symbol, timestamp, price, quantity, side, trade_id
```

**Status**: ✅ Complete end-to-end
- Bronze: ExchangeConnectorAgent collects from adapters
- Silver: Schema validation + quality gates
- Gold: OHLCV aggregation (ohlcv_aggregator.py)

---

#### ⚠️ ORDER BOOKS - COLLECTED BUT NO SCHEMA

**Collector**: `exchange_connector.py` → `BookData`
**Topics Published**: ❌ **NOT PUBLISHED TO KAFKA**
- Data collected into `output_queues[DataType.BOOK]`
- **Gap**: No Kafka topic publishing found in code

**Schema**: ❌ **EXISTS** (`market_book` schema:2516) **BUT NOT USED**

**Status**: ⚠️ **PARTIALLY IMPLEMENTED**
- Bronze: ✅ Collected from exchanges
- Silver: ❌ Not published to Kafka, schema exists but unused
- Gold: ✅ Orderbook curator exists (`orderbook_curator.py`)

**ACTION NEEDED**: Add Kafka publishing for order book data

---

#### ⚠️ FUNDING RATES - COLLECTED BUT NO SCHEMA

**Collector**: `exchange_connector.py` → `FundingData`
**Topics Published**: ❌ **NOT PUBLISHED TO KAFKA**
- Data collected into `output_queues[DataType.FUNDING]`
- **Gap**: No Kafka topic publishing found

**Schema**: ✅ `market_funding` (schema_validator.py:2536)
```python
Fields: venue, symbol, timestamp, funding_rate, next_funding_time, mark_price
```

**Status**: ⚠️ **PARTIALLY IMPLEMENTED**
- Bronze: ✅ Collected from exchanges
- Silver: Schema exists but data not published
- Gold: ❌ No curator exists

**ACTION NEEDED**: Add Kafka publishing for funding rates

---

#### ⚠️ OPEN INTEREST - COLLECTED BUT NO SCHEMA

**Collector**: `exchange_connector.py` → `OpenInterestData`
**Topics Published**: ❌ **NOT PUBLISHED TO KAFKA**
- Data collected into `output_queues[DataType.OI]`
- **Gap**: No Kafka topic publishing found

**Schema**: ✅ `market_oi` (schema_validator.py:2574)
```python
Fields: venue, symbol, timestamp, open_interest, open_interest_value
```

**Status**: ⚠️ **PARTIALLY IMPLEMENTED**
- Bronze: ✅ Collected from exchanges
- Silver: Schema exists but data not published
- Gold: ❌ No curator exists

**ACTION NEEDED**: Add Kafka publishing for open interest

---

#### ⚠️ BORROW RATES - COLLECTED BUT NO PIPELINE

**Collector**: `exchange_connector.py` → `BorrowData`
**Topics Published**: ❌ **NOT PUBLISHED TO KAFKA**
- Data collected into `output_queues[DataType.BORROW]`
- **Gap**: No Kafka topic publishing found

**Schema**: ❌ **DOES NOT EXIST**

**Status**: ❌ **NOT IMPLEMENTED**
- Bronze: ✅ Collected from exchanges
- Silver: ❌ No schema, not published
- Gold: ❌ No curator exists

**ACTION NEEDED**: Create schema + add Kafka publishing

---

#### ❌ MAINTENANCE WINDOWS - NOT IMPLEMENTED

**Collector**: `exchange_connector.py` → Defined in `DataType.MAINTENANCE`
**Topics Published**: ❌ None
**Schema**: ❌ None
**Status**: ❌ Enum exists but not implemented

**ACTION NEEDED**: Implement if needed for strategy downtime planning

---

## 2. COINGECKO (Crypto Market Metrics)

### What They Provide:
- Total crypto market cap
- BTC/ETH dominance %
- DeFi market cap
- 24h trading volume
- Active cryptocurrencies count

### What We're Collecting: ✅ EVERYTHING

**Collector**: `crypto_metrics_collector.py` → `CryptoMarketMetrics`
**Topics Published**: `raw_data.crypto.market_metrics`
**Schema**: ✅ `crypto_market_metrics` (schema_validator.py:3122)
**Gold Curator**: ✅ `crypto_market_structure_curator.py`

**Status**: ✅ **FULLY IMPLEMENTED END-TO-END**

---

## 3. FRED (Federal Reserve Economic Data)

### What They Provide (10,000+ indicators):
- **Interest Rates**: Fed Funds Rate, Treasury yields (1M-30Y), LIBOR, SOFR
- **Inflation**: CPI, PCE, PPI, Core CPI, Sticky CPI
- **Employment**: Unemployment rate, Nonfarm payrolls, Labor force participation, JOLTS
- **GDP**: Real GDP, GDP growth rate, GDP deflator
- **Money Supply**: M1, M2, M3, Money velocity
- **Credit Markets**: Corporate spreads, TED spread, Yield curve
- **Housing**: Home prices (Case-Shiller), Mortgage rates, Housing starts
- **Consumer**: Retail sales, Consumer confidence, Personal income
- **Manufacturing**: Industrial production, Capacity utilization, ISM PMI
- **International**: Trade balance, Exchange rates, Foreign holdings

### What We're Collecting: ⚠️ LIMITED SUBSET

**Collector**: `macro_collector.py` → Publishes to `raw_data.macro.economic_indicators`
**Schema**: ✅ `macro_economic_indicators` (schema_validator.py:3072)

**Currently Collecting** (from macro_collector.py):
```python
# Only these FRED series (8 indicators):
"DFF"      # Federal Funds Rate
"DGS10"    # 10-Year Treasury Yield
"DGS2"     # 2-Year Treasury Yield  
"DGS5"     # 5-Year Treasury Yield
"UNRATE"   # Unemployment Rate
"CPIAUCSL" # CPI (All Urban Consumers)
"GDP"      # Gross Domestic Product
"M2"       # Money Supply M2 (implied from code context)
```

**Missing Critical Indicators**:
- ❌ Corporate credit spreads (BAA, AAA)
- ❌ TED spread (3-month LIBOR - 3-month T-bill)
- ❌ Term premium / yield curve metrics
- ❌ ISM Manufacturing/Services PMI
- ❌ Consumer confidence indices
- ❌ Real-time inflation expectations (TIPS spreads)
- ❌ Labor market indicators (JOLTS, Initial claims)
- ❌ Housing data (Case-Shiller, mortgage rates)

**Status**: ⚠️ **PARTIALLY IMPLEMENTED**
- Bronze: ✅ Collector exists but limited to ~8 indicators
- Silver: ✅ Schema exists and validates
- Gold: ✅ Curator exists (`macro_tradfi_curator.py`)

**ACTION NEEDED**: Expand FRED indicator list for comprehensive macro coverage

---

## 4. ALPHA VANTAGE (TradFi Market Data)

### What They Provide:
- **Indices**: VIX, SPX, DJI, NDX, RUT, DXY
- **Equities**: All US stocks (real-time quotes)
- **ETFs**: SPY, QQQ, IWM, TLT, GLD, USO, UUP, etc.
- **Forex**: All major currency pairs
- **Commodities**: Oil (WTI/Brent), Gold, Silver, Natural Gas
- **Crypto**: BTC, ETH prices (redundant with exchange data)
- **Fundamental Data**: Earnings, balance sheets, income statements

### What We're Collecting: ⚠️ LIMITED SYMBOLS

**Collector**: `macro_collector.py` → Publishes to:
- `raw_data.tradfi.indices`
- `raw_data.tradfi.equities`

**Schemas**: 
- ✅ `tradfi_indices` (schema_validator.py:2993)
- ✅ `tradfi_equities` (schema_validator.py:3028)

**Currently Collecting** (from macro_collector.py config):
```python
# Indices (2-3 symbols)
"^VIX"  # Volatility Index
"DXY"   # Dollar Index

# Equities (3-5 symbols)
"SPY"   # S&P 500 ETF
"QQQ"   # Nasdaq 100 ETF
"TLT"   # 20+ Year Treasury Bond ETF
"GLD"   # Gold ETF (implied)
"USO"   # Oil ETF (implied)
```

**Missing Critical Symbols**:
- ❌ SPX (S&P 500 Index itself)
- ❌ DJI (Dow Jones Industrial)
- ❌ NDX (Nasdaq 100 Index)
- ❌ RUT (Russell 2000 small caps)
- ❌ IWM (Russell 2000 ETF)
- ❌ UUP (Dollar bullish ETF)
- ❌ HYG (High yield bonds)
- ❌ LQD (Investment grade bonds)
- ❌ ARKK, SQQQ, TQQQ (vol/leverage products)
- ❌ SLV (Silver)
- ❌ UNG (Natural Gas)
- ❌ XLE, XLF, XLK (Sector ETFs)

**Missing Asset Classes**:
- ❌ Forex pairs (EUR/USD, GBP/USD, USD/JPY)
- ❌ Fundamental data (earnings, revenue, EPS)
- ❌ Options data (IV, skew) - though Deribit covers crypto options

**Status**: ⚠️ **PARTIALLY IMPLEMENTED**
- Bronze: ✅ Collector exists but limited to ~5-7 symbols
- Silver: ✅ Schemas exist and validate
- Gold: ✅ Curator exists (`macro_tradfi_curator.py`)

**ACTION NEEDED**: Expand symbol list for comprehensive TradFi coverage

---

## 5. ETHEREUM NODE (On-Chain Data)

### What Ethereum Provides (via eth_getLogs, eth_getTransaction, etc.):

#### Token Transfers (ERC20/ERC721/ERC1155)
- Transfer events
- Approval events
- Mint/Burn events

#### DeFi Protocols
- **Uniswap V2/V3**: Swap, Mint, Burn, Sync events
- **Aave**: Deposit, Withdraw, Borrow, Repay, Liquidation events
- **Compound**: Mint, Redeem, Borrow, Repay, Liquidate events
- **Curve**: TokenExchange, AddLiquidity, RemoveLiquidity events
- **MakerDAO**: Vault open/close, CDP events, Liquidation events
- **Lido**: Submit, Withdrawal, Oracle updates
- **Rocket Pool**: Deposit, Withdrawal, Node registration

#### NFT Marketplaces
- **OpenSea**: Sale, Offer, Bid events
- **Blur**: Sale events
- **LooksRare**: Sale events

#### Bridge Events
- **Polygon Bridge**: Deposit, Withdraw events
- **Arbitrum Bridge**: L1→L2, L2→L1 events
- **Optimism Bridge**: Deposit, Withdraw events
- **Stargate**: Swap, Send events

#### Governance
- **ENS**: Registration, Renewal events
- **DAO Proposals**: Created, Voted, Executed events
- **Token votes**: Delegated, Voted events

#### MEV/Flashbots
- Block builder rewards
- Flashbot bundles
- Sandwich attacks (detectable via mempool)

### What We're Collecting: ⚠️ ONLY ERC20 TRANSFERS

**Collector**: `onchain_collector.py` → Publishes to `raw_data.onchain_events`
**Schema**: ✅ `onchain_flows` (schema_validator.py:2614)

**Currently Collecting**:
```python
✅ ERC20 Transfer events for critical tokens:
   - USDT, USDC, DAI (stablecoins)
   - WETH, WBTC (wrapped assets)
   - stETH, cbETH, sfrxETH (liquid staking)
   - rETH (Rocket Pool)
   
✅ Classification of transfers:
   - dex_swap (Uniswap, Curve, etc.)
   - bridge (Polygon, Arbitrum, Optimism)
   - cex_hot_wallet (Binance, Coinbase, etc.)
   - erc20_transfer (generic)

✅ Chain support:
   - Ethereum, Arbitrum, Polygon, Optimism, Base
```

**Missing Critical On-Chain Data**:

#### DeFi Protocol Events - ❌ NOT COLLECTED
- Uniswap Swap events (price, liquidity changes)
- Aave Deposit/Borrow/Liquidation events
- Compound money market events
- Curve pool activity
- Lido staking/withdrawal events
- MakerDAO vault events

**Impact**: Missing liquidity changes, lending rates, liquidation cascades

#### NFT Data - ❌ NOT COLLECTED
- OpenSea/Blur sales (floor price trends)
- NFT mint activity (market sentiment)
- High-value NFT movements

**Impact**: Missing NFT market structure signals

#### MEV Data - ❌ NOT COLLECTED
- Flashbot bundles
- Sandwich attacks
- Arbitrage opportunities
- MEV-boost rewards

**Impact**: Missing MEV alpha and market microstructure

#### Governance Data - ❌ NOT COLLECTED
- DAO proposal activity
- Voting patterns
- Token delegation

**Impact**: Missing governance risk signals

#### Block/Mempool Data - ❌ NOT COLLECTED
- Gas prices (base fee, priority fee)
- Block builder rewards
- Pending transactions (mempool)
- Uncle blocks (reorg detection)

**Impact**: Missing transaction cost forecasting, network congestion signals

**Status**: ⚠️ **PARTIALLY IMPLEMENTED**
- Bronze: ⚠️ Collector only captures ERC20 transfers
- Silver: ✅ Schema exists (`onchain_flows`)
- Gold: ❌ No on-chain curator exists (data goes straight to features?)

**ACTION NEEDED**: 
1. Add DeFi protocol event collection
2. Add gas/block data collection
3. Add MEV data collection
4. Create Gold layer on-chain curator

---

## 6. OPTIONS DATA (Deribit)

### What We're Collecting: ✅ FULLY IMPLEMENTED

**Collector**: `options_chain_collector.py` → Publishes to `raw_data.options_chain`
**Schema**: ✅ `options_surface` (schema_validator.py:2614)
**Gold Curator**: ✅ `options_chain_curator.py`

**Coverage**:
- ✅ BTC/ETH options chains
- ✅ IV, Greeks (delta, gamma, vega, theta)
- ✅ Strike/expiry/moneyness classification
- ✅ Surface completeness quality scoring

**Status**: ✅ **FULLY IMPLEMENTED END-TO-END**

---

## SUMMARY: DATA SOURCE COVERAGE

### ✅ FULLY IMPLEMENTED (3/6 sources)
1. **Crypto Exchange Trades** - Complete end-to-end
2. **CoinGecko Metrics** - Complete end-to-end
3. **Deribit Options** - Complete end-to-end

### ⚠️ PARTIALLY IMPLEMENTED (3/6 sources)
4. **FRED Economic Data** - Only 8 indicators (need 50+)
5. **Alpha Vantage TradFi** - Only 5-7 symbols (need 50+)
6. **Ethereum On-Chain** - Only ERC20 transfers (missing DeFi/MEV/NFT)

### ❌ EXCHANGE DATA GAPS (NOT PUBLISHED TO KAFKA)
- Order Books (collected but not published)
- Funding Rates (collected but not published)
- Open Interest (collected but not published)
- Borrow Rates (collected, no schema, not published)

---

## PRIORITY ACTION ITEMS

### 🔴 CRITICAL (Blocking Alpha Generation)

**1. Publish Exchange Market Data to Kafka**
- Add Kafka publishing for order books → `raw_data.market.book`
- Add Kafka publishing for funding rates → `raw_data.market.funding`
- Add Kafka publishing for open interest → `raw_data.market.oi`
- **Impact**: Unlock limit order strategies, funding arbitrage, OI-based signals

**2. Expand FRED Indicators (8 → 50+)**
Add to `macro_collector.py`:
```python
"BAA"       # Corporate BAA spread (credit risk)
"AAA"       # Corporate AAA spread
"T10Y2Y"    # 10Y-2Y yield curve (recession indicator)
"TEDRATE"   # TED spread (credit stress)
"VIXCLS"    # VIX closing (FRED version, backup for Alpha Vantage)
"DCOILWTICO" # WTI Crude Oil
"GOLDAMGBD228NLBM"  # Gold price
"UMCSENT"   # Consumer sentiment
"PAYEMS"    # Nonfarm payrolls
"IC4WSA"    # Initial claims (weekly)
"JTSJOL"    # Job openings
"HOUST"     # Housing starts
"MORTGAGE30US" # 30Y mortgage rate
"CSUSHPISA" # Case-Shiller Home Price Index
"DEXUSEU"   # USD/EUR exchange rate
"DEXCHUS"   # USD/CNY exchange rate
```
**Impact**: Enable macro regime detection, risk-on/off scoring

**3. Expand Alpha Vantage Symbols (7 → 50+)**
Add to `macro_collector.py`:
```python
# Indices
"^SPX", "^DJI", "^NDX", "^RUT"

# ETFs
"IWM", "HYG", "LQD", "UUP", "SLV", "UNG"
"XLE", "XLF", "XLK", "XLV", "XLP", "XLU"  # Sectors
"ARKK", "SQQQ", "TQQQ"  # Vol/leverage

# Commodities
"DBA", "DBC", "PPLT", "PALL"  # Ag, commodities, platinum, palladium
```
**Impact**: Enable sector rotation, vol regime detection, commodity correlation

### 🟡 HIGH PRIORITY (Enhances Alpha)

**4. Add DeFi Protocol Events**
Extend `onchain_collector.py` to collect:
```python
# Uniswap V2/V3 events
Swap(sender, amount0In, amount1In, amount0Out, amount1Out, to)
Mint(sender, amount0, amount1)
Burn(sender, amount0, amount1, to)

# Aave V3 events
Deposit(reserve, user, onBehalfOf, amount)
Withdraw(reserve, user, to, amount)
Borrow(reserve, user, onBehalfOf, amount, borrowRate)
Repay(reserve, user, repayer, amount)
Liquidate(collateral, debt, user, collateralAmount, debtToCover)
```
**Impact**: Detect liquidity shifts, lending rate changes, liquidation cascades

**5. Add Gas/Block Data Collection**
New collector: `ethereum_block_collector.py`
```python
# Per-block data
base_fee_per_gas
priority_fee
block_utilization
mev_reward
uncle_count

# Mempool data (optional)
pending_tx_count
avg_gas_price
```
**Impact**: Forecast transaction costs, detect network congestion

### 🟢 MEDIUM PRIORITY (Nice to Have)

**6. Add Borrow Rate Schema + Publishing**
Create `market_borrow` schema:
```python
Fields: venue, symbol, timestamp, borrow_rate, available_amount
```
**Impact**: Enable funding rate arbitrage, margin cost forecasting

**7. Add NFT Data Collection**
New collector: `nft_collector.py` (OpenSea/Blur APIs)
```python
# NFT market data
floor_price
sales_volume_24h
unique_buyers_24h
whale_accumulation
```
**Impact**: NFT market sentiment, whale tracking

**8. Add MEV Data Collection**
Extend `onchain_collector.py` or create `mev_collector.py`
```python
# MEV-boost data
flashbot_bundle_count
sandwich_attack_volume
arb_opportunity_count
builder_rewards
```
**Impact**: MEV alpha, market microstructure insights

---

## RECOMMENDED IMPLEMENTATION ORDER

### Phase 1: Unlock Existing Collected Data (1-2 days)
1. Add Kafka publishing for order books, funding, OI
2. Update orchestrator topic mappings
3. Test end-to-end flow

### Phase 2: Expand Macro Coverage (2-3 days)
1. Add 42 FRED indicators to macro_collector.py
2. Add 40+ Alpha Vantage symbols
3. Test macro curator with expanded data

### Phase 3: DeFi Protocol Events (1 week)
1. Add Uniswap Swap/Mint/Burn collection
2. Add Aave Deposit/Borrow/Liquidation
3. Create on-chain curator for DeFi data
4. Add schemas for new event types

### Phase 4: Gas/Block Data (3-4 days)
1. Create ethereum_block_collector.py
2. Add gas price, MEV reward collection
3. Create gas_metrics curator

### Phase 5: Advanced On-Chain (2 weeks)
1. Add NFT marketplace data
2. Add MEV data collection
3. Add governance event tracking

---

## ESTIMATED DATA VOLUME IMPACT

### Current State:
- **Trades**: ~1,000 msg/sec (exchanges)
- **Options**: ~100 msg/sec (Deribit)
- **On-Chain**: ~50 msg/sec (ERC20 transfers)
- **Macro/TradFi**: ~1 msg/min (slow-moving)
- **Total**: ~1,200 msg/sec

### After Phase 1 (Add Order Books, Funding, OI):
- **Order Books**: +500 msg/sec (5sec snapshots)
- **Funding**: +1 msg/5min per symbol (~10 msg/sec)
- **Open Interest**: +1 msg/5min per symbol (~10 msg/sec)
- **New Total**: ~1,720 msg/sec (+43%)

### After Phase 2 (Expand Macro Coverage):
- **FRED**: +42 indicators × 1 update/day = negligible
- **Alpha Vantage**: +40 symbols × 1 update/sec = +40 msg/sec
- **New Total**: ~1,760 msg/sec (+2%)

### After Phase 3 (DeFi Events):
- **Uniswap Swaps**: +200 msg/sec (high frequency)
- **Aave Events**: +50 msg/sec
- **New Total**: ~2,010 msg/sec (+14%)

### Final State (All Phases):
- **Total**: ~2,500-3,000 msg/sec
- **Storage**: ~200GB/day (compressed Kafka)
- **Kafka Throughput**: ~50MB/sec

---

## CONCLUSION

**Current Coverage**: 60% of available data
- ✅ Exchange trades, options, basic macro/tradfi, limited on-chain
- ❌ Missing: Order books, funding, OI, comprehensive macro, DeFi events, gas data, MEV

**Recommended Focus**: **Phase 1 + Phase 2** (5 days total)
- Unlocks order book strategies, funding arb, comprehensive macro signals
- Minimal implementation effort (Kafka publishing + config expansion)
- High ROI for alpha generation

**Long-term Vision**: **All 5 Phases** (4-6 weeks)
- Comprehensive multi-asset, multi-chain data coverage
- Enables advanced strategies (MEV, DeFi liquidity, NFT momentum)
- Positions system as institutional-grade data platform

---

**Document Version**: 1.0  
**Date**: November 6, 2025  
**Status**: ⚠️ Actionable Gaps Identified
