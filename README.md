# Technical Assessment


This is a generic dbt project that can be used to evaluate data modeling
and other technical skills at a high level. Based on TPC-H data, this project
leverages TPC-H data to create a platform for exploring how a candidate thinks
about data transformation, analyzes and weights technical approaches, and
communicates feedback.

## TPC-H Source Data Structures

```mermaid
erDiagram
   PART ||--o{ PARTSUPP : references
   SUPPLIER  ||--o{ PARTSUPP : references
   SUPPLIER }o--|| NATION : in
   LINEITEM }o--|{ PARTSUPP : references
   CUSTOMER }o--|| NATION : in
   NATION }o--|| REGION: in
   CUSTOMER ||--o{ ORDERS: places
   ORDERS ||--|{ LINEITEM: contains
   CUSTOMER }o--|| REGION: in
```

## dbt Project DAG

This is the current state of the dbt project's DAG. Generally. the project follows
[dbt Labs' recommended project structure guidelines][dbt labs structure], with some inherrited
structures that require maintainence.

```mermaid
flowchart LR

    classDef newModel stroke:#DAF7A6, fill:#DAF7A644
    classDef removedModel stroke:#F7A6B1, fill:#F7A6B144

    tpch.customer --> stg_tpch__customers;
    tpch.lineitem --> stg_tpch__line_items;
    tpch.nation --> stg_tpch__nations;
    tpch.order --> stg_tpch__orders;
    tpch.partsupp --> stg_tpch__part_suppliers;
    tpch.part --> stg_tpch__parts;
    tpch.region --> stg_tpch__regions;
    tpch.supplier --> stg_tpch__suppliers;

    stg_tpch__suppliers --> suppliers
    stg_tpch__nations --> suppliers
    stg_tpch__regions --> suppliers

    stg_tpch__orders --> lost_revenue
    stg_tpch__customers --> lost_revenue
    stg_tpch__nations --> lost_revenue

    stg_tpch__orders --> int_order_line_items__mapped
    stg_tpch__line_items --> int_order_line_items__mapped


    int_order_line_items__mapped --> monthly_return_revenue

    stg_tpch__customers --> client_purchase_status;
    int_order_line_items__mapped --> client_purchase_status;
    lost_revenue --> client_purchase_status;

    min_supply_cost
    tpch.part --> min_supply_cost
    tpch.supplier --> min_supply_cost
    tpch.partsupp --> min_supply_cost
    tpch.nation --> min_supply_cost

    eur_lowcost_brass_suppliers
    tpch.part --> eur_lowcost_brass_suppliers
    tpch.supplier --> eur_lowcost_brass_suppliers
    tpch.partsupp --> eur_lowcost_brass_suppliers
    tpch.nation --> eur_lowcost_brass_suppliers
    tpch.region --> eur_lowcost_brass_suppliers
    min_supply_cost --> eur_lowcost_brass_suppliers

    uk_lowcost_brass_suppliers
    eur_lowcost_brass_suppliers --> uk_lowcost_brass_suppliers
    tpch.supplier --> uk_lowcost_brass_suppliers

    stg_tpch__suppliers --> supplier_returns
    stg_tpch__parts --> supplier_returns
    int_order_line_items__mapped --> supplier_returns

```

# Development

To get started developing in this project, please make sure that you have the
following dependencies installed:

## duckdb

An embedded columnar database that can be used for lightweight analytics queries.
This project leverages duckdb as our datastore.

Can be installed via homebrew

```console
brew install duckdb
```

or via https://duckdb.org/docs/installation/.

## dbt-duckdb

The `dbt-duckdb` package is an adapter that allows dbt to leverage duckdb as
a datastore. It can be installed via pip

```console
pip install dbt-duckdb
```


## Using the project
Once your dependencies have been installed, you can use all of your standard
dbt commands for building, testing, and compiling your project.

For example, here's the command to build the project.
```console
dbt build
```

[dbt labs structure]: https://docs.getdbt.com/guides/best-practices/how-we-structure/1-guide-overview
