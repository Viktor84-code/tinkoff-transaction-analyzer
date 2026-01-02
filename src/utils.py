"""
Утилиты для работы с банковскими транзакциями.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import logging
import os

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_transactions(filepath: str = "data/operations.xlsx") -> pd.DataFrame:
    """
    Читает транзакции из Excel файла.

    Args:
        filepath: путь к Excel файлу

    Returns:
        DataFrame с транзакциями

    Raises:
        FileNotFoundError: если файл не существует
    """
    if not os.path.exists(filepath):
        error_msg = f"Файл не найден: {filepath}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    logger.info(f"Чтение транзакций из {filepath}")
    df = pd.read_excel(filepath)
    logger.info(f"Прочитано {len(df)} транзакций")

    return df


def filter_by_date(
        df: pd.DataFrame,
        start_date: str,
        end_date: str
) -> pd.DataFrame:
    """
    Фильтрует транзакции по диапазону дат.

    Args:
        df: DataFrame с транзакциями
        start_date: начальная дата (YYYY-MM-DD)
        end_date: конечная дата (YYYY-MM-DD)

    Returns:
        Отфильтрованный DataFrame
    """
    # Конвертируем даты в правильный формат
    df_copy = df.copy()

    # Преобразуем строки дат в datetime
    # Формат в данных: '31.12.2021 16:44:00'
    df_copy['Дата операции'] = pd.to_datetime(
        df_copy['Дата операции'],
        format='%d.%m.%Y %H:%M:%S',
        errors='coerce'  # Если ошибка - ставим NaT
    )

    # Преобразуем входные даты
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    # Фильтруем
    mask = (df_copy['Дата операции'] >= start) & (df_copy['Дата операции'] <= end)
    filtered = df_copy[mask]

    logger.info(f"Фильтрация: {start_date} - {end_date}, найдено {len(filtered)} транзакций")
    return filtered


def get_transactions_by_card(
        df: pd.DataFrame,
        card_last_digits: str
) -> pd.DataFrame:
    """
    Возвращает транзакции по конкретной карте.

    Args:
        df: DataFrame с транзакциями
        card_last_digits: последние 4 цифры карты

    Returns:
        Транзакции указанной карты
    """
    card_transactions = df[df['Номер карты'] == card_last_digits]
    logger.info(f"Транзакции карты {card_last_digits}: {len(card_transactions)} записей")
    return card_transactions


def calculate_total_spent(df: pd.DataFrame) -> float:
    """
    Рассчитывает общую сумму расходов.

    Args:
        df: DataFrame с транзакциями

    Returns:
        Сумма расходов (отрицательные значения)
    """
    # Расходы — отрицательные значения
    expenses = df[df['Сумма операции'] < 0]
    total = expenses['Сумма операции'].sum() * -1  # Делаем положительным

    logger.info(f"Общая сумма расходов: {total:.2f} ₽")
    return total


def calculate_cashback(df: pd.DataFrame) -> float:
    """
    Рассчитывает общий кешбэк.

    Args:
        df: DataFrame с транзакциями

    Returns:
        Сумма кешбэка
    """
    total_cashback = df['Кешбэк'].sum()
    logger.info(f"Общий кешбэк: {total_cashback:.2f} ₽")
    return total_cashback


def get_top_transactions(df: pd.DataFrame, n: int = 5) -> List[Dict[str, Any]]:
    """
    Возвращает топ-N транзакций по сумме платежа.

    Args:
        df: DataFrame с транзакциями
        n: количество транзакций в топе

    Returns:
        Список словарей с топ транзакциями
    """
    # Берем абсолютное значение суммы для сортировки
    top_df = df.copy()
    top_df['abs_amount'] = top_df['Сумма платежа'].abs()
    top_df = top_df.sort_values('abs_amount', ascending=False).head(n)

    top_transactions = []
    for _, row in top_df.iterrows():
        transaction = {
            'date': row['Дата операции'].strftime('%d.%m.%Y') if hasattr(row['Дата операции'], 'strftime') else str(
                row['Дата операции']),
            'amount': float(row['Сумма операции']),
            'category': row['Категория'],
            'description': row['Описание'],
            'card': row['Номер карты']
        }
        top_transactions.append(transaction)

    logger.info(f"Получено топ-{n} транзакций")
    return top_transactions


def format_currency(amount: float) -> str:
    """
    Форматирует денежную сумму.

    Args:
        amount: сумма

    Returns:
        Отформатированная строка
    """
    return f"{amount:,.2f} ₽".replace(",", " ")


def main():
    """Демонстрация работы утилит."""
    print("=== ДЕМОНСТРАЦИЯ УТИЛИТ ===")

    try:
        # 1. Читаем данные
        df = read_transactions()
        print(f"✅ Прочитано {len(df)} транзакций")

        # 2. Общая статистика
        total_spent = calculate_total_spent(df)
        total_cashback = calculate_cashback(df)
        print(f"💰 Общие расходы: {format_currency(total_spent)}")
        print(f"🎁 Общий кешбэк: {format_currency(total_cashback)}")

        # 3. Транзакции по карте
        card_df = get_transactions_by_card(df, '5814')
        print(f"💳 Транзакции карты 5814: {len(card_df)} записей")

        # 4. Топ транзакций
        top = get_top_transactions(df, 3)
        print(f"🏆 Топ-3 транзакции:")
        for i, tx in enumerate(top, 1):
            print(f"   {i}. {tx['date']} - {format_currency(tx['amount'])} - {tx['category']}")

        print("\n✅ Все утилиты работают!")

    except Exception as e:
        print(f"❌ Ошибка: {e}")

def get_top_transactions(transactions_df: pd.DataFrame, n: int = 5) -> List[Dict[str, Any]]:
    """
    Возвращает топ-N транзакций по абсолютной сумме платежа.

    Args:
        transactions_df: DataFrame с транзакциями
        n: количество возвращаемых транзакций

    Returns:
        List[Dict]: Список словарей с транзакциями
    """
    if transactions_df.empty:
        logger.info("Пустой DataFrame, возвращаем пустой список")
        return []

    # Создаем копию чтобы не менять оригинал
    top_df = transactions_df.copy()

    # Добавляем столбец с абсолютными значениями
    # Используем 'Сумма платежа' если есть, иначе 'Сумма операции'
    if 'Сумма платежа' in top_df.columns:
        amount_col = 'Сумма платежа'
    elif 'Сумма операции' in top_df.columns:
        amount_col = 'Сумма операции'
    else:
        error_msg = "Не найден столбец с суммой. Доступные столбцы: " + str(list(top_df.columns))
        logger.error(error_msg)
        raise KeyError(error_msg)

    logger.info(f"Используем столбец '{amount_col}' для сортировки")
    top_df['abs_amount'] = top_df[amount_col].abs()

    # Сортируем по убыванию абсолютной суммы
    top_df = top_df.sort_values('abs_amount', ascending=False).head(n)

    # Удаляем временный столбец
    top_df = top_df.drop(columns=['abs_amount'])

    # Логируем результат
    logger.info(f"Возвращаем топ-{n} транзакций")

    # Возвращаем как список словарей
    return top_df.to_dict('records')


if __name__ == "__main__":
    main()
