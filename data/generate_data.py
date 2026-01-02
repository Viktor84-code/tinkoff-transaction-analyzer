"""
Генератор тестовых данных для анализатора транзакций.
"""

import pandas as pd
from datetime import datetime, timedelta
import random
import os


def generate_transactions():
    """Генерирует тестовые транзакции."""

    # Создаём папку если нет
    os.makedirs('data', exist_ok=True)

    # Данные для генерации
    categories = ['Супермаркеты', 'Кафе', 'Транспорт', 'Здоровье', 'Переводы']
    cards = ['5814', '7512', '9632']

    transactions = []
    start_date = datetime(2023, 1, 1)

    # Генерируем 200 транзакций
    for i in range(200):
        days = random.randint(0, 365)
        date = start_date + timedelta(days=days)

        # 80% расходов, 20% доходов
        if random.random() < 0.8:
            amount = -round(random.uniform(50, 15000), 2)  # расход
            cashback = round(abs(amount) * 0.01, 2)
        else:
            amount = round(random.uniform(1000, 50000), 2)  # доход
            cashback = 0

        transaction = {
            'Дата операции': date.strftime('%Y-%m-%d'),
            'Дата платежа': (date + timedelta(days=random.randint(0, 2))).strftime('%Y-%m-%d'),
            'Номер карты': random.choice(cards),
            'Статус': 'OK',
            'Сумма операции': amount,
            'Валюта операции': 'RUB',
            'Сумма платежа': abs(amount),
            'Валюта платежа': 'RUB',
            'Кешбэк': cashback,
            'Категория': random.choice(categories),
            'MCC': random.randint(1000, 9999),
            'Описание': f'Транзакция {i + 1}',
            'Бонусы (включая кешбэк)': cashback * 2,
            'Округление на Инвесткопилку': random.choice([0, 10, 50, 100]),
            'Сумма операции с округлением': round(amount, -1)
        }
        transactions.append(transaction)

    # Создаём DataFrame
    df = pd.DataFrame(transactions)

    # Сохраняем в Excel
    filepath = 'data/operations.xlsx'
    df.to_excel(filepath, index=False)

    print(f"✅ Сгенерировано {len(df)} транзакций")
    print(f"📁 Файл: {filepath}")
    print(f"📊 Колонки: {', '.join(df.columns)}")

    return df


if __name__ == '__main__':
    print("🎯 Генерация тестовых данных для анализатора транзакций...")
    df = generate_transactions()
    print("✅ Готово!")
