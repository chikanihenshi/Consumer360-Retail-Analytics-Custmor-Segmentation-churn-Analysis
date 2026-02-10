import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    "mysql+pymysql://root:Riya1607@localhost:3306/retail_dw"
)

print("Database connected successfully!")

# Load fact_sales
fact_sales = pd.read_sql("SELECT * FROM fact_sales", engine)
print(fact_sales.head())
print(fact_sales.info())
print(fact_sales.columns)


fact_sales = pd.read_sql("SELECT * FROM fact_sales", engine)
print(fact_sales.head())
print(fact_sales.info())

# Load dim_customer (optional, for later)
dim_customer = pd.read_sql("SELECT * FROM dim_customer", engine)
print(dim_customer.head())


query = """
SELECT
    customer_id,
    invoice_date,
    total_amount
FROM fact_sales
"""

sales_df = pd.read_sql(query, engine)

print(sales_df.head())
print(sales_df.info())

# Remove nulls
sales_df = sales_df.dropna()

# Convert date column to datetime
sales_df['invoice_date'] = pd.to_datetime(sales_df['invoice_date'])

# Remove negative or zero sales
sales_df = sales_df[sales_df['total_amount'] > 0]

print(sales_df.describe())

analysis_date = sales_df['invoice_date'].max()
print("Analysis Date:", analysis_date)


# ================================
# STEP 2: Calculate RFM Metrics
# ================================

# Reference date for Recency
analysis_date = sales_df['invoice_date'].max()

# RFM calculation
rfm = sales_df.groupby('customer_id').agg({
    'invoice_date': lambda x: (analysis_date - x.max()).days,  # Recency
    'customer_id': 'count',                                     # Frequency
    'total_amount': 'sum'                                       # Monetary
})

# Rename columns
rfm.rename(columns={
    'invoice_date': 'Recency',
    'customer_id': 'Frequency',
    'total_amount': 'Monetary'
}, inplace=True)

# Preview RFM table
print(rfm.head())
print(rfm.describe())

# ================================
# STEP 3A: RFM Scoring
# ================================

# Create R, F, M scores using quantiles
rfm['R_score'] = pd.qcut(rfm['Recency'], 5, labels=[5,4,3,2,1])
rfm['F_score'] = pd.qcut(rfm['Frequency'].rank(method='first'), 5, labels=[1,2,3,4,5])
rfm['M_score'] = pd.qcut(rfm['Monetary'], 5, labels=[1,2,3,4,5])

# Convert scores to integer
rfm[['R_score','F_score','M_score']] = rfm[['R_score','F_score','M_score']].astype(int)

print(rfm.head())

# ================================
# STEP 3B: Customer Segmentation
# ================================

def rfm_segment(row):
    if row['R_score'] >= 4 and row['F_score'] >= 4 and row['M_score'] >= 4:
        return 'Champions'
    elif row['F_score'] >= 4 and row['M_score'] >= 3:
        return 'Loyal Customers'
    elif row['R_score'] >= 4:
        return 'Potential Loyalists'
    elif row['R_score'] <= 2 and row['F_score'] >= 3:
        return 'At Risk'
    else:
        return 'Hibernating'

rfm['Segment'] = rfm.apply(rfm_segment, axis=1)

print(rfm['Segment'].value_counts())


# ================================
# STEP 4A: Segment Validation
# ================================

segment_summary = (
    rfm
    .groupby('Segment')
    .agg(
        Customers=('Segment', 'count'),
        Avg_Recency=('Recency', 'mean'),
        Avg_Frequency=('Frequency', 'mean'),
        Avg_Monetary=('Monetary', 'mean'),
        Total_Revenue=('Monetary', 'sum')
    )
    .sort_values(by='Total_Revenue', ascending=False)
)

print(segment_summary)



rfm.to_csv("rfm_final_segments.csv", index=False)
