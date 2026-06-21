# ecommerce-medallion-project

Tech Stack

- Databricks Free Edition
- PySpark
- Medallion Architecture
- YAML Driven Configuration
- GitHub

Project Structure

config/
    dev.yaml

src/
    main.py
    reader.py
    silver.py
    dq.py
    gold.py
    writer.py
    utils.py

Pipeline Flow

Bronze
↓
DQ
↓
Silver
↓
Gold

Gold Outputs

- Daily Revenue
- Monthly Revenue
- Top 5 Customers
- Top 5 Products
- Running Revenue
