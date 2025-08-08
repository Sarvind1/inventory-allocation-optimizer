# Project Brain: Inventory Allocation Optimizer

## 🎯 **System Purpose**
A sophisticated **supply chain optimization engine** that predicts inventory shortages, calculates revenue impact, and generates actionable recommendations for a global e-commerce business. Processes ~5000 SKUs across 50+ weeks of demand planning.

---

## 🏗️ **System Architecture**

### **Core Components**
```
main.py                 → Orchestrates entire workflow
database_connector.py   → Redshift connection with threading (5-6 workers)
data_processor.py       → Business logic transformations  
calculations.py         → Weekly inventory waterfall engine
config_loader.py        → CSV-based configuration management
sql_query_loader.py     → Modular SQL file management
utils.py               → Helper functions
```

### **Data Flow**
```
1. LOAD    → 10 SQL queries in parallel from Redshift
2. PROCESS → Transform data, create ref columns, standardize 
3. CALCULATE → Weekly inventory waterfall (up to 104 weeks)
4. RECOMMEND → Generate TO checks, supply ops actions
5. OUTPUT  → Main CSV + optional debug files
```

---

## 📊 **Data Sources (10 Critical Queries)**

| Query | Purpose | Key Fields | Data Volume |
|-------|---------|------------|-------------|
| `demand_forecast.sql` | Monthly sales projections | razin, asin, mp, date, quantity | ~5000 rows |
| `inventory_sop.sql` | Current inventory state | asin, mp, total_inventory, in_amz, in_lm | ~5000 rows |
| `open_po.sql` | Purchase orders | PO#, quantity, status, crd, vendor | Variable |
| `inbound_shipments.sql` | Goods in transit | shipment, expected_delivery, quantity | Variable |
| `master_data.sql` | Product master | razin, asin, size_tier, brand | ~5000 rows |
| `vendor_master.sql` | Supplier data | vendor_id, shipping_region | Small |
| `target_sales_price.sql` | Pricing for revenue calc | ref, final_sales_price | ~5000 rows |
| `asin_vendor_mapping.sql` | Product-vendor links | asin, vendor_id | Variable |
| `otif_status.sql` | Delivery performance | document_number, line_id | Variable |
| `gfl_list.sql` | Go-forward products | razin, status | Small |

---

## 🔄 **Core Business Logic**

### **1. Reference Column Creation**
```python
# Universal product identifier across all tables
ref = (asin if asin_exists else razin) + mp
# Example: "B07XYZ123US", "RAZ-456EU"
```

### **2. Marketplace Standardization** 
```
Pan-EU → EU
DE → EU  
GB → UK
North America → US
```

### **3. Weekly Format (FIXED - DO NOT CHANGE)**
```
CW01-2025_demand    # Calendar week 1, 2025, demand column
CW25-2024_inbound   # Calendar week 25, 2024, inbound column
```
- Uses ISO calendar standard
- Format: `CW{week:02d}-{year}_{suffix}`
- Generates up to 104 weeks (2 years ahead)

### **4. PO Status Classification (FIXED BUSINESS RULES)**

**Signed POs** (Stages 12-23):
```
12. Ready for Batching Pending
13. Batch Creation Pending  
14. SM Sign-Off Pending
15. CI Approval Pending
16. CI Payment Pending
17. QC Schedule Pending
18. FFW Booking Missing
19. Supplier Pickup Date Pending
20. Pre Pickup Check
21. FOB Pickup Pending
22. Non FOB Pickup Pending
23. INB Creation Pending
```

**Unsigned POs** (Stages 1-11, A, B):
```
01. PO Approval Pending
02. Supplier Confirmation Pending
03-11. [Various production stages]
A. Anti PO Line
B. Compliance Blocked
```

### **5. Lead Time Calculations (CONFIGURABLE VIA CSV)**
```
Total Lead Time = Production Days + Transport Time + Port-to-Channel Buffer + Processing (15) + Safety Buffer (30)
```

**Transport Mapping**: `config/transport_leadtimes.csv`
- Maps shipping regions (CN→US: 39 days, EU→US: 45 days, etc.)
- 58+ routes configured

**Port-to-Channel Buffer**: `config/port_to_channel_buffer.csv`
- AMZ vs 3PL warehouse differences
- Regional variations

---

## ⚙️ **Key Algorithms**

### **1. Monthly-to-Weekly Demand Conversion**
```
Input:  Monthly quantities by ref
Output: Weekly CW columns with proportional distribution
Logic:  Distribute monthly quantity based on days per week in that month
```

### **2. Inventory Waterfall Calculation** 
```python
# For each week (up to 104 weeks):
inventory_end[week] = max(0, 
    inventory_start[week] + 
    inbound[week] + 
    po_signed[week] + 
    po_unsigned[week] - 
    demand[week]
)

sales_missed[week] = max(0,
    demand[week] - 
    inventory_start[week] - 
    po_signed[week] - 
    inbound[week]
)
```

### **3. Revenue Impact Calculation**
```python
# Until end of 2025
revenue_miss = sum(sales_missed_columns) * final_sales_price

# From OOS week onwards  
oos_revenue = sum(sales_missed_from_oos_week) * final_sales_price
```

### **4. Recommendation Logic**

**Transfer Order (TO) Checks**:
```python
# US/CA: 10-week demand vs fulfillable inventory
# EU/UK: 7-week demand vs fulfillable inventory
if (fulfillable + at_amz + on_the_way < future_demand) and (local_inventory > units_per_carton):
    recommendation = "TO to be checked/created"
```

**Supply Operations**:
```python
if (otw_35p_98d < future_demand_14w) and (manufacturing_28_126d > future_demand_18w):
    recommendation = "Expedite pick up goods from vendor"
```

---

## 🎛️ **Configuration Management (CSV-Based)**

### **Why CSV Configuration is Excellent for Business Users:**
- ✅ Non-technical users can modify lead times
- ✅ Version controlled in Git
- ✅ Easy to audit changes
- ✅ No code deployment needed for business rule changes

### **Configuration Files:**
```
config/transport_leadtimes.csv     → Shipping times between regions
config/port_to_channel_buffer.csv  → Warehouse-specific buffers  
config/country_region_mapping.csv  → 58 countries to logistics regions
config/asia_countries.csv          → Special routing countries
```

### **Key Business Thresholds (DO NOT CHANGE):**
- **5 Cartons**: AMZ vs 3PL splitting threshold
- **Asia Countries**: Special routing logic applies
- **CW Format**: Fixed calendar week format
- **PO Stages**: 23 predefined status classifications

---

## 🚀 **Performance Characteristics**

### **Current Performance:**
- **Data Volume**: 5000 SKUs × 50 weeks = ~250K cells
- **SQL Loading**: Main bottleneck (~60-70% of runtime)
- **Threading**: 5-6 workers for database queries
- **Memory**: Optimized with data type downcasting

### **Threading Strategy:**
```python
# Database queries run in parallel
ThreadPoolExecutor(max_workers=5)
# Queries prioritized by size (small reference data first)
```

### **Memory Optimization:**
```python
# Automatic downcasting in database_connector.py
float64 → float32 (where no precision loss)
int64 → int32/int16 (where values fit)
object → category (for low cardinality strings)
```

---

## 📈 **Business Outputs**

### **Primary Output**: `inventory_allocation_YYYYMMDD_HHMMSS.csv`
Contains weekly projections with:
- Inventory start/end positions by week
- Sales missed by week  
- Revenue impact calculations
- OOS (Out-of-Stock) predictions
- TO and Supply Ops recommendations
- ARM (At Risk Margin) calculations

### **Key Metrics Generated:**
- **Revenue Miss Until Dec 2025**: Total revenue at risk
- **OOS Week**: When stockout predicted to occur
- **DOH (Days on Hand)**: Inventory runway
- **Future Demand 7w/10w/14w**: Rolling demand windows
- **TO_Check_arm**: Revenue recoverable through transfers

### **Recommendation Categories:**
1. **TO Checks**: Transfer orders needed
2. **FFW + Supply Ops**: Expedite pickup  
3. **Supply Ops**: Prepone production
4. **ARM Calculations**: Revenue at risk quantification

---

## 🔧 **Code Quality & Architecture**

### **Strengths:**
- ✅ **Clean Separation**: Database, processing, calculations modular
- ✅ **Error Handling**: Connection retries, graceful failures
- ✅ **Logging**: Comprehensive performance tracking
- ✅ **Configuration**: Business rules externalized
- ✅ **Threading**: Already optimized for main bottleneck
- ✅ **Memory Management**: Data type optimization included

### **Architecture Patterns:**
- **Factory Pattern**: Database connector creation
- **Strategy Pattern**: Different processing for signed/unsigned POs
- **Template Method**: Consistent data processing workflow
- **Observer Pattern**: Progress logging throughout pipeline

### **Code Organization:**
```
Business Logic Layer    → calculations.py, data_processor.py
Data Access Layer      → database_connector.py, sql_query_loader.py  
Configuration Layer    → config_loader.py, CSV files
Orchestration Layer    → main.py
Utility Layer         → utils.py
```

---

## 🎯 **Optimization Opportunities**

### **1. SQL Loading (Primary Bottleneck)**
- **Current**: 5-6 workers, sequential processing
- **Potential**: 6-8 workers, query prioritization, result caching
- **Impact**: 60-70% of total runtime

### **2. Data Processing**
- **Current**: Some use of `.apply()` functions  
- **Potential**: Vectorized operations with NumPy
- **Impact**: 3-4x faster for large datasets

### **3. Memory Usage**
- **Current**: Good optimization in database layer
- **Potential**: Pre-allocated DataFrames, batch processing
- **Impact**: ~50% memory reduction possible

### **4. Calculation Engine**
- **Current**: Week-by-week loops (104 iterations)
- **Potential**: Vectorized operations, cached date calculations
- **Impact**: Significant for large SKU counts

---

## 🚨 **Critical Business Rules (DO NOT CHANGE)**

### **Fixed Formats:**
- ✅ CW week format: `CW01-2025_demand`
- ✅ PO splitting stages (1-11 vs 12-23)
- ✅ 5 carton threshold for AMZ/3PL
- ✅ Asia country routing logic
- ✅ Marketplace standardization rules

### **Configurable (via CSV):**
- ✅ Transport lead times (change frequently per business)
- ✅ Port-to-channel buffers
- ✅ Country-to-region mappings
- ✅ Asia countries list

### **Revenue Calculation Logic:**
- ✅ Until Dec 2025 timeframe
- ✅ OOS week identification method
- ✅ Final sales price fallback logic

---

## 📋 **Development Guidelines**

### **For Optimization:**
1. **Preserve all business logic exactly**
2. **Keep CW format and PO classification unchanged**
3. **Maintain CSV configuration approach**
4. **Focus on performance, not architecture changes**
5. **Ensure code remains readable for business users**

### **For Future Enhancements:**
1. **Add new configs as CSV files**
2. **Create new SQL files for additional data sources**
3. **Extend recommendation logic in calculations.py**
4. **Add validation functions in utils.py**

### **Testing Strategy:**
1. **Compare output CSV exactly before/after changes**
2. **Validate all business rules preserved**
3. **Performance benchmarking on actual data volumes**
4. **Configuration file validation**

---

## 📚 **Key Dependencies**
```
pandas>=1.3.0           → Core data processing
numpy>=1.21.0           → Numerical calculations  
redshift-connector>=2.0 → Database connectivity
python-dateutil>=2.8.0  → Date/time handling
openpyxl>=3.0.0         → Excel support
pyyaml>=5.4.0           → Configuration parsing
```

---

## 🔍 **Understanding Status: COMPLETE**

**✅ Architecture**: Modular, well-structured, appropriate for business users
**✅ Business Logic**: Complex supply chain rules understood and documented  
**✅ Data Flow**: 10-step pipeline from SQL to recommendations clear
**✅ Configuration**: CSV-based approach perfect for business rule changes
**✅ Performance**: Threading and optimization already implemented
**✅ Code Quality**: Clean, maintainable, properly error-handled

**Next Step**: Ready for performance optimizations while preserving all business logic.