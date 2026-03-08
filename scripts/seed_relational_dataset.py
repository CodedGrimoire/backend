import asyncio
import random
import sys
from datetime import date, timedelta
from pathlib import Path

from sqlalchemy import (
    Column,
    Date,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    select,
    text,
)
from sqlalchemy.ext.asyncio import create_async_engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from app.core.config import settings


metadata = MetaData()

customers = Table(
    "customers",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String),
    Column("city", String),
    Column("signup_date", Date),
)

products = Table(
    "products",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("product_name", String),
    Column("category", String),
    Column("price", Integer),
)

orders = Table(
    "orders",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("customer_id", Integer, ForeignKey("customers.id")),
    Column("order_date", Date),
)

order_items = Table(
    "order_items",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("order_id", Integer, ForeignKey("orders.id")),
    Column("product_id", Integer, ForeignKey("products.id")),
    Column("quantity", Integer),
)

payments = Table(
    "payments",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("order_id", Integer, ForeignKey("orders.id")),
    Column("payment_method", String),
    Column("amount", Integer),
)


CITIES = ["Dhaka", "Chittagong", "Khulna", "Rajshahi", "Sylhet"]
CATEGORIES = ["Electronics", "Clothing", "Home", "Books"]
PAY_METHODS = ["Cash", "Card", "Mobile Banking"]


def random_date(start: date, end: date) -> date:
    delta = end - start
    return start + timedelta(days=random.randrange(delta.days + 1))


async def main():
    engine = create_async_engine(settings.database_url, future=True, echo=False)

    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
        await conn.execute(
            text(
                "TRUNCATE payments, order_items, orders, products, customers RESTART IDENTITY CASCADE"
            )
        )

    # generate customers
    customers_data = [
        {
            "name": f"Customer {i+1}",
            "city": random.choice(CITIES),
            "signup_date": random_date(date(2022, 1, 1), date(2024, 12, 31)),
        }
        for i in range(50)
    ]

    # generate products
    products_data = [
        {
            "product_name": f"Product {i+1}",
            "category": random.choice(CATEGORIES),
            "price": random.randint(10, 500) * 10,
        }
        for i in range(30)
    ]

    async with engine.begin() as conn:
        await conn.execute(customers.insert(), customers_data)
        await conn.execute(products.insert(), products_data)

        # fetch ids
        cust_ids = [
            row[0] for row in (await conn.execute(select(customers.c.id))).all()
        ]
        prod_rows = (await conn.execute(select(products.c.id, products.c.price))).all()
        prod_prices = {row.id: row.price for row in prod_rows}

        # orders
        orders_data = []
        for _ in range(200):
            orders_data.append(
                {
                    "customer_id": random.choice(cust_ids),
                    "order_date": random_date(date(2023, 1, 1), date(2024, 12, 31)),
                }
            )
        await conn.execute(orders.insert(), orders_data)
        order_ids = [row[0] for row in (await conn.execute(select(orders.c.id))).all()]

        # order_items and payments
        order_items_data = []
        order_totals = {}
        for oid in order_ids:
            num_items = random.randint(1, 3)
            total = 0
            for _ in range(num_items):
                pid = random.choice(list(prod_prices.keys()))
                qty = random.randint(1, 5)
                price = prod_prices[pid]
                total += price * qty
                order_items_data.append(
                    {"order_id": oid, "product_id": pid, "quantity": qty}
                )
            order_totals[oid] = total

        await conn.execute(order_items.insert(), order_items_data)

        payments_data = [
            {
                "order_id": oid,
                "payment_method": random.choice(PAY_METHODS),
                "amount": amount,
            }
            for oid, amount in order_totals.items()
        ]
        await conn.execute(payments.insert(), payments_data)

    print("Inserted:")
    print(" 50 customers")
    print(" 30 products")
    print(f" {len(order_ids)} orders")
    print(f" {len(order_items_data)} order_items")
    print(f" {len(payments_data)} payments")


if __name__ == "__main__":
    asyncio.run(main())
