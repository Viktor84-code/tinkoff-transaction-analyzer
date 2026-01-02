"""
Генератор тестовых данных для проекта анализатора транзакций.
"""

import pandas as pd
from datetime import datetime, timedelta
import random


def generate_transactions(num=200):
    """Генерация транзакций."""
    categories = ['Супермаркеты', 'Кафе', 'Транспорт', 'Здоровье', 'Переводы']
    cards = ['5814', '7512', '9632']

    data = []
    start_date = datetime(2023, 1, 1)

    for i in range(num):
        date = start_date + timedelta(days=random.randint(0, 365))
        amount = -round(random.uniform(100, 10000), 2)  # расходы отрицательные

        transaction = {
            'Дата операции': date.strftime('%Y-%m-%d'),
            'Номер карты': random.choice(cards),
            'Статус': 'OK',
            'Сумма операции': amount,
            'Валюта операции': 'RUB',
            'Категория': random.choice(categories),
            'Описание': f'Покупка #{i + 1}',
            'Кешбэк': round(abs(amount) * 0.01, 2),
            'Округление на Инвесткопилку': random.choice([0, 10, 50, 100]),
        }
        data.append(transaction)

    return pd.DataFrame(data)


def main():
    """Основная функция."""
    print("Генерация тестовых данных...")
    df = generate_transactions(250)

    # Сохраняем в Excel
    df.to_excel('data/operations.xlsx', index=False)
    print(f"Создано {len(df)} транзакций в data/operations.xlsx")
    print(f"Колонки: {', '.join(df.columns)}")


if __name__ == "__main__":
    main()
