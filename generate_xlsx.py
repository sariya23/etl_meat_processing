from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

N = 100_000
OUTPUT = Path("data.xlsx")
RANDOM_SEED = 42
np.random.seed(RANDOM_SEED)


regions = ["ЦФО", "СЗФО", "ЮФО", "ПФО", "УФО", "СФО", "ДФО"]
warehouses = ["Склад №1", "Склад №2", "Склад №3", "Холодильник А", "Холодильник Б"]
transport_modes = ["Авто-рефрижератор", "Ж/д рефрижератор", "Авиа"]
payment_terms = ["Предоплата", "7 дней", "14 дней", "30 дней"]
payment_statuses = ["Оплачено", "Частично", "Не оплачено"]
approval_statuses = ["Согласовано", "Ожидает", "Отклонено"]
quality_grades = ["A", "B", "C"]
incoterms = ["EXW", "FCA", "CPT", "DAP", "DDP"]
units = ["кг", "т", "шт", "л"]

suppliers = ["1", "2", "3", "4 5", "6"]
item_categories = {
    "Говядина": ["Оковалок", "Вырезка", "Лопатка"],
    "Свинина": ["Окорок", "Шея", "Корейка"],
    "Птица": ["Курица", "Индейка"],
    "Субпродукты": ["Печень", "Сердце"],
}
cat_keys = list(item_categories.keys())


# -------- ВСПОМОГАТЕЛЬНОЕ --------
def random_dates(start: datetime, end: datetime, n: int) -> pd.Series:
    start_u = int(pd.Timestamp(start).timestamp())
    end_u = int(pd.Timestamp(end).timestamp())
    return pd.to_datetime(np.random.randint(start_u, end_u, n), unit="s")


def gen_ids(prefix: str, n: int, width: int = 6) -> list:
    return [
        f"{prefix}-{str(x).zfill(width)}" for x in np.random.randint(0, 10**width, n)
    ]


start_date = datetime(2023, 1, 1)
end_date = datetime(2025, 6, 30)

order_date = random_dates(start_date, end_date, N)
lead_time_days = np.random.randint(1, 21, N)
expected_delivery_date = order_date + pd.to_timedelta(lead_time_days, unit="D")

delay_distribution = np.random.normal(loc=1.2, scale=3.0, size=N).astype(int)
actual_delivery_date = expected_delivery_date + pd.to_timedelta(
    delay_distribution, unit="D"
)
delivery_on_time = (delay_distribution <= 0).astype(int)


po_number = [
    f"PO-{d.year}-{str(i).zfill(6)}"
    for i, d in zip(np.random.randint(0, 1_000_000, N), order_date)
]
contract_id = gen_ids("CT", N, width=7)
batch_id = gen_ids("LOT", N, width=8)
supplier_id = gen_ids("SUP", N, width=4)
supplier_name = np.random.choice(suppliers, size=N)
supplier_country = np.full(N, "Россия")
supplier_region = np.random.choice(
    regions, size=N, p=[0.35, 0.12, 0.1, 0.18, 0.1, 0.1, 0.05]
)
warehouse_choice = np.random.choice(warehouses, size=N, p=[0.35, 0.25, 0.2, 0.15, 0.05])
transport_choice = np.random.choice(transport_modes, size=N, p=[0.75, 0.2, 0.05])
incoterm_choice = np.random.choice(incoterms, size=N, p=[0.25, 0.25, 0.25, 0.2, 0.05])


mgr_first = [
    "Алексей",
    "Мария",
    "Иван",
    "Ольга",
    "Дмитрий",
    "Екатерина",
    "Сергей",
    "Анна",
    "Никита",
    "Ирина",
    "Павел",
    "Светлана",
]
mgr_last = [
    "Иванов",
    "Петров",
    "Сидоров",
    "Кузнецова",
    "Смирнов",
    "Попова",
    "Соколова",
    "Морозов",
    "Васильев",
    "Новикова",
    "Фёдоров",
    "Алексеева",
]
manager = np.array(
    [f"{np.random.choice(mgr_last)} {np.random.choice(mgr_first)}" for _ in range(N)]
)

category_choice = np.random.choice(cat_keys, size=N, p=[0.35, 0.35, 0.2, 0.1])
subcategory_choice = np.array(
    [np.random.choice(item_categories[c]) for c in category_choice]
)
item_code = [f"ITM-{np.random.randint(1000, 9999)}" for _ in range(N)]
unit_choice = np.random.choice(units, size=N, p=[0.8, 0.02, 0.12, 0.06])

quantity = np.where(
    unit_choice == "кг",
    np.random.gamma(shape=2.0, scale=12.0, size=N),
    np.where(
        unit_choice == "т",
        np.random.gamma(shape=2.0, scale=0.5, size=N),  # небольшие партии в тоннах
        np.where(
            unit_choice == "шт",
            np.random.randint(5, 120, size=N),
            np.random.gamma(shape=2.0, scale=10.0, size=N),
        ),
    ),
).astype(float)
quantity = np.maximum(1, np.round(quantity, 2))


base_price_rub = np.where(
    category_choice == "Говядина",
    np.random.normal(520, 60, N),
    np.where(
        category_choice == "Свинина",
        np.random.normal(330, 45, N),
        np.where(
            category_choice == "Птица",
            np.random.normal(220, 30, N),
            np.random.normal(150, 25, N),
        ),
    ),
)

unit_mult = np.where(unit_choice == "т", 1000, 1)
unit_price = (base_price_rub / unit_mult) * np.random.normal(1.0, 0.05, N)
unit_price = np.maximum(5, unit_price).round(2)


total_cost_rub = (quantity * unit_price).round(2)
vat_rate = np.random.choice([0.1, 0.2], size=N, p=[0.15, 0.85])
vat_amount = (total_cost_rub * vat_rate).round(2)
total_with_vat = (total_cost_rub + vat_amount).round(2)


quality_choice = np.random.choice(quality_grades, size=N, p=[0.72, 0.23, 0.05])
defects_rate = np.clip(
    np.random.beta(a=1.2, b=28, size=N)
    + (quality_choice == "C") * 0.02
    + (quality_choice == "B") * 0.005,
    0,
    0.12,
)
rejected_qty = np.floor(quantity * defects_rate).astype(int)
accepted_qty = np.maximum(0, np.round(quantity - rejected_qty, 2))


temp_on_delivery = np.random.normal(loc=2.0, scale=1.0, size=N)
temp_on_delivery += np.where(transport_choice == "Авиа", 0.3, 0.0)
temp_on_delivery = np.round(temp_on_delivery, 1)


pay_terms = np.random.choice(payment_terms, size=N, p=[0.25, 0.25, 0.25, 0.25])
pay_status = np.random.choice(payment_statuses, size=N, p=[0.7, 0.2, 0.1])
appr_status = np.random.choice(approval_statuses, size=N, p=[0.9, 0.08, 0.02])


weekday_order = order_date.weekday
month_order = order_date.month
year_order = order_date.year


df = pd.DataFrame(
    {
        "ID_записи": np.arange(1, N + 1),
        "Номер_заказа": po_number,
        "ID_контракта": contract_id,
        "ID_партии": batch_id,
        "Дата_заказа": pd.to_datetime(order_date.date),
        "Ожидаемая_дата_поставки": pd.to_datetime(expected_delivery_date.date),
        "Фактическая_дата_поставки": pd.to_datetime(actual_delivery_date.date),
        "Задержка_поставки_дн": delay_distribution.astype(int),
        "Поставка_вовремя": delivery_on_time.astype(int),
        "ID_поставщика": supplier_id,
        "Поставщик": supplier_name,
        "Страна_поставщика": supplier_country,
        "Регион_поставщика": supplier_region,
        "Склад": warehouse_choice,
        "Менеджер": manager,
        "Инкотермс": incoterm_choice,
        "Категория_товара": category_choice,
        "Подкатегория_товара": subcategory_choice,
        "Код_товара": item_code,
        "Единица_изм": unit_choice,
        "Количество": quantity,
        "Количество_отказано": rejected_qty,
        "Количество_принято": accepted_qty,
        "Валюта": np.full(N, "RUB"),
        "Цена_за_ед": unit_price,
        "Курс_к_руб": np.full(N, 1.0),
        "Сумма_руб": total_cost_rub,
        "Ставка_НДС": vat_rate,
        "Сумма_НДС": vat_amount,
        "Сумма_с_НДС_руб": total_with_vat,
        "Тип_транспорта": transport_choice,
        "Темп_при_доставке_C": temp_on_delivery,
        "Класс_качества": quality_choice,
        "Доля_брака": np.round(defects_rate, 4),
        "Условия_оплаты": pay_terms,
        "Статус_оплаты": pay_status,
        "Статус_согласования": appr_status,
        "День_недели_заказа": weekday_order,
        "Месяц_заказа": month_order,
        "Год_заказа": year_order,
    }
)


with pd.ExcelWriter(
    OUTPUT, engine="xlsxwriter", datetime_format="yyyy-mm-dd"
) as writer:
    df.to_excel(writer, index=False)
    workbook = writer.book
    worksheet = writer.sheets["Sheet1"]
    worksheet.set_column(0, len(df.columns) - 1, 15)

print(f"Готово: сохранён файл {OUTPUT.resolve()}")
print(f"Строк: {len(df):,}".replace(",", " "))
print("Пример колонок:", ", ".join(df.columns[:10]), "...")
