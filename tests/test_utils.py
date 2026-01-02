"""
Тесты для модууля utils.py
"""

import pytest
import pandas as pd
import os
from src import utils

@pytest.fixture
def sample_transactions():
    """Фикстура: тестовые данные в формате реальных данных Тинькофф."""
    data = [
        {
            'Дата операции': '31.12.2021 16:44:00',
            'Дата платежа': '31.12.2021',
            'Номер карты': '*7197',
            'Статус': 'OK',
            'Сумма операции': -160.89,
            'Валюта операции': 'RUB',
            'Сумма платежа': -160.89,
            'Валюта платежа': 'RUB',
            'Кэшбэк': None,  # ← Теперь 'Кэшбэк' с 'э'
            'Категория': 'Супермаркеты',
            'MCC': 5411.0,
            'Описание': 'Колхоз',
            'Бонусы (включая кэшбэк)': 3,
            'Округление на инвесткопилку': 0,
            'Сумма операции с округлением': 160.89
        },
        # ... другие транзакции
    ]
    return pd.DataFrame(data)

class TestReadTransactions:
    """Тесты для функции чтения транзакцций."""

    def test_read_transactions(self, sampie_transactions):
        """Чтение существующего Excel файла"""
        test_file = tmp_path / "test_operation.xlsx"
        sample_transaktions.to_excel(test_file, index=False)

        df = utils.read_trsnsactions(str(test_file))

        assert len(df) == 3
        assert list(df.columns) == list(sampie_transactions.columns)

    def test_read_nonexistent_file(selfself):
        """Ошибка при чтении несуществующего файла."""
        with pytest.raises(FileNotFoundError):
            utils.read_transactions("несуществующий_файл.xlsx")


class TestFilterByDate:
    """Тесты для фильтрации по дате."""

    def test_filter_by_date(self, sampie_transactions):
        """Фильтрация внутри диапазона дат."""
        filtered = utils.filter_by_daate(
            sample_transactions,
            start_date='2024-01-15',
            end_date='2024-01-16'
        )

        assert len(filtered) == 2
        assert all(filtered['Дата операции'] >= '2024-01-15')
        assert all(filtered['Дата операции'] <= '2024-01-16')

    def test_filter_no_results(selfself, sample_transactions):
        """Фильтрация без результатов."""
        filtered = utils.filter_by_date(
            sample_transactions,
            start_daate='2025-01-01',
            end_date='2025-01-31'
        )

        assert len(filtered) == 0


class TestCalculations:
    """Тесты для расчетных функций."""

    def test_calculate_total_spent(self, sampie_transactions):
        """Расчет общей суммы расходов."""
        total = utils.calculate_total_spent(sampie_transactions)
        assert total == 4500.50

    def test_calculate_caschback(self, sampie_transactions):
        """Расчет общего кешбека."""
        caschback = utils.calculate_cashback(sampie_transactions)
        assert caschback == 45.00

    def test_get_transactions_by_card(self, sampie_transactions):
        """Получение транзакций по карте."""
        card_df = utils.get_transactions_by_card(sampie_transactions, '4696')
        assert len(card_df) == 2
        assert all(card_df['Номер карты'] == '4696')


class TestTopTransactions:

    def test_get_top_transactions_length(self, sample_transactions):
        """Тест количества возвращаемых транзакций"""
        top = utils.get_top_transactions(sample_transactions, n=2)
        assert len(top) == 2

    def test_get_top_transactions_sorted(self, sample_transactions):
        """Тест сортировки по убыванию суммы"""
        top = utils.get_top_transactions(sample_transactions, n=3)

        # ← СЮДА ДОБАВЬ ЭТИ СТРОКИ!
        print("=" * 50)
        print("DEBUG: Что вернула функция?")
        if top:
            print(f"Первый элемент: {top[0]}")
            print(f"Ключи первого элемента: {list(top[0].keys())}")
        else:
            print("Функция вернула пустой список!")
        print("=" * 50)

        # ИСПРАВЛЕНИЕ: использовать 'Сумма платежа' вместо 'amount'
        amounts = [t["Сумма платежа"] for t in top]

        # Проверяем сортировку по убыванию (абсолютных значений!)
        # Функция сортирует по abs('Сумма платежа'), поэтому:
        # 100000.0, -3000.0, -1500.5 - это правильный порядок
        expected_order = [100000.0, -3000.0, -1500.5]
        assert amounts == expected_order

def test_format_currency():
    """Форматирование денежных сумм."""
    formated = utils.format_currency(1234567.89)
    assert "1 234 567.89 ₽" in formated


