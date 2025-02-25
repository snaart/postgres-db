#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import time
import os
import json
from datetime import datetime
import argparse
import sys

# Параметры подключения к базе данных (можно настроить через аргументы командной строки)
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}

# Настройки расчета интерполяции по умолчанию
DEFAULT_SETTINGS = {
    "min_temperature": -20,  # Ограничим для демонстрации
    "max_temperature": 20,
    "temperature_step": 1.0,
    "clear_previous_results": True
}


def parse_arguments():
    """Обработка аргументов командной строки"""
    parser = argparse.ArgumentParser(description='Анализ производительности интерполяций метеоданных')

    # Параметры базы данных
    db_group = parser.add_argument_group('Параметры базы данных')
    db_group.add_argument('--dbname', help='Имя базы данных')
    db_group.add_argument('--user', help='Имя пользователя')
    db_group.add_argument('--password', help='Пароль')
    db_group.add_argument('--host', help='Хост', default='localhost')
    db_group.add_argument('--port', help='Порт', default='5432')

    # Параметры расчета
    calc_group = parser.add_argument_group('Параметры расчета')
    calc_group.add_argument('--min-temp', type=float,
                            help=f'Минимальная температура (по умолчанию: {DEFAULT_SETTINGS["min_temperature"]})')
    calc_group.add_argument('--max-temp', type=float,
                            help=f'Максимальная температура (по умолчанию: {DEFAULT_SETTINGS["max_temperature"]})')
    calc_group.add_argument('--step', type=float,
                            help=f'Шаг температуры (по умолчанию: {DEFAULT_SETTINGS["temperature_step"]})')
    calc_group.add_argument('--keep-previous', action='store_true', help='Сохранять предыдущие результаты')

    # Параметры вывода
    vis_group = parser.add_argument_group('Параметры вывода')
    vis_group.add_argument('--output-dir', default='performance_results', help='Директория для сохранения результатов')
    vis_group.add_argument('--no-plots', action='store_true', help='Не создавать графики')
    vis_group.add_argument('--dpi', type=int, default=300, help='DPI для сохранения графиков')

    # Дополнительные параметры
    parser.add_argument('--skip-calculation', action='store_true', help='Пропустить расчет, только визуализация')
    parser.add_argument('--verbose', action='store_true', help='Подробный вывод')

    return parser.parse_args()


def get_config(args):
    """Подготовка конфигурации на основе аргументов командной строки"""
    # Конфигурация базы данных
    db_config = DB_CONFIG.copy()
    if args.dbname: db_config["dbname"] = args.dbname
    if args.user: db_config["user"] = args.user
    if args.password: db_config["password"] = args.password
    if args.host: db_config["host"] = args.host
    if args.port: db_config["port"] = args.port

    # Настройки расчета
    calc_settings = DEFAULT_SETTINGS.copy()
    if args.min_temp is not None: calc_settings["min_temperature"] = args.min_temp
    if args.max_temp is not None: calc_settings["max_temperature"] = args.max_temp
    if args.step is not None: calc_settings["temperature_step"] = args.step
    if args.keep_previous: calc_settings["clear_previous_results"] = False

    return db_config, calc_settings


def print_header(title):
    """Печать красивого заголовка в консоль"""
    width = 80
    print("\n" + "=" * width)
    print(f" {title} ".center(width))
    print("=" * width)


def connect_to_db(config):
    """Подключение к базе данных PostgreSQL"""
    print_header("ПОДКЛЮЧЕНИЕ К БАЗЕ ДАННЫХ")

    print(f"Подключение к базе данных {config['dbname']} на {config['host']}:{config['port']}...")
    try:
        conn = psycopg2.connect(**config)
        print("✓ Подключение успешно установлено")
        return conn
    except psycopg2.Error as e:
        print(f"✗ Ошибка подключения к базе данных: {e}")
        return None


def run_interpolation_procedure(conn, settings, verbose=False):
    """Запуск хранимой процедуры для расчета интерполяций"""
    print_header("РАСЧЕТ ИНТЕРПОЛЯЦИЙ")

    print("Параметры расчета:")
    print(f"- Диапазон температур: от {settings['min_temperature']} до {settings['max_temperature']} °C")
    print(f"- Шаг расчета: {settings['temperature_step']} °C")
    print(f"- Очистка предыдущих результатов: {'Да' if settings['clear_previous_results'] else 'Нет'}")

    try:
        cursor = conn.cursor()

        # Анимация процесса выполнения
        print("\nЗапуск процедуры расчета...")
        print("Это может занять некоторое время. Пожалуйста, подождите...\n")

        animation = "|/-\\"
        idx = 0
        start_time = time.time()

        # Запускаем процедуру
        cursor.execute(
            """
            CALL snaart.calculate_all_interpolations(
                %s, %s, %s, %s
            )
            """,
            (
                settings['min_temperature'],
                settings['max_temperature'],
                settings['temperature_step'],
                settings['clear_previous_results']
            )
        )

        # Простая анимация во время выполнения
        while cursor.connection and not cursor.closed:
            sys.stdout.write(f"\rВыполнение... {animation[idx % len(animation)]}")
            sys.stdout.flush()
            idx += 1
            time.sleep(0.1)

            # Проверяем статус запроса
            if not cursor.connection.is_busy():
                break

        conn.commit()
        end_time = time.time()

        # Очищаем строку анимации
        sys.stdout.write("\r" + " " * 30 + "\r")

        print(f"✓ Процедура успешно выполнена за {end_time - start_time:.2f} секунд")

        # Вывод уведомлений, если включен подробный режим
        if verbose and conn.notices:
            print("\nУведомления:")
            for notice in conn.notices:
                print(f"  {notice.strip()}")

        cursor.close()
        return True
    except psycopg2.Error as e:
        print(f"✗ Ошибка выполнения процедуры: {e}")
        conn.rollback()
        return False


def fetch_performance_metrics(conn):
    """Получение метрик производительности из базы данных"""
    print_header("ПОЛУЧЕНИЕ МЕТРИК ПРОИЗВОДИТЕЛЬНОСТИ")

    try:
        cursor = conn.cursor()

        # Основные метрики
        print("Получение основных метрик...")
        cursor.execute("""
            SELECT 
                id,
                total_time_ms,
                total_calculations,
                successful_calculations,
                avg_calculation_time_ms,
                min_calculation_time_ms,
                max_calculation_time_ms,
                parameters,
                created_at
            FROM snaart.interpolation_performance 
            ORDER BY created_at DESC 
            LIMIT 1;
        """)
        metrics = cursor.fetchone()

        if not metrics:
            print("✗ Метрики производительности не найдены")
            return None, None, None, None

        print(f"✓ Основные метрики получены")

        # Статистика по высотам
        print("Получение статистики по высотам...")
        cursor.execute("""
            SELECT 
                height, 
                COUNT(*) as count, 
                AVG(calculation_time) as avg_time,
                MIN(calculation_time) as min_time,
                MAX(calculation_time) as max_time,
                COUNT(*) FILTER (WHERE result_value IS NULL) as error_count
            FROM snaart.interpolation_results
            GROUP BY height
            ORDER BY height;
        """)
        height_stats = cursor.fetchall()
        print(f"✓ Получена статистика для {len(height_stats)} высот")

        # Статистика по температурам
        print("Получение статистики по температурам...")
        cursor.execute("""
            SELECT 
                FLOOR(temperature/10)*10 as temp_range,
                COUNT(*) as count, 
                AVG(calculation_time) as avg_time,
                MIN(calculation_time) as min_time,
                MAX(calculation_time) as max_time,
                COUNT(*) FILTER (WHERE result_value IS NULL) as error_count
            FROM snaart.interpolation_results
            GROUP BY FLOOR(temperature/10)*10
            ORDER BY temp_range;
        """)
        temp_stats = cursor.fetchall()
        print(f"✓ Получена статистика для {len(temp_stats)} диапазонов температур")

        # Данные для тепловой карты
        print("Получение данных для тепловой карты...")
        cursor.execute("""
            SELECT 
                height, 
                FLOOR(temperature/5)*5 as temp_group,
                AVG(calculation_time) as avg_time,
                COUNT(*) as count
            FROM snaart.interpolation_results
            GROUP BY height, FLOOR(temperature/5)*5
            ORDER BY height, temp_group;
        """)
        heatmap_data = cursor.fetchall()
        print(f"✓ Получены данные для тепловой карты ({len(heatmap_data)} точек)")

        cursor.close()
        return metrics, height_stats, temp_stats, heatmap_data
    except psycopg2.Error as e:
        print(f"✗ Ошибка получения метрик: {e}")
        return None, None, None, None


def create_performance_charts(metrics, height_stats, temp_stats, heatmap_data, output_dir="performance_results",
                              dpi=300):
    """Создание визуализаций производительности"""
    if not metrics or not height_stats or not temp_stats:
        print("✗ Недостаточно данных для создания визуализаций")
        return False

    print_header("СОЗДАНИЕ ВИЗУАЛИЗАЦИЙ")

    # Создаем директорию для сохранения, если её нет
    os.makedirs(output_dir, exist_ok=True)

    # Метка времени для имени файлов
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Извлекаем параметры из JSON
    params = json.loads(metrics[7]) if metrics[7] else {}

    # 1. График по высотам
    print("- Создание графика по высотам...")
    plt.figure(figsize=(12, 6))

    heights = [row[0] for row in height_stats]
    avg_times = [row[2] for row in height_stats]
    error_counts = [row[5] for row in height_stats]

    ax1 = plt.subplot(111)
    ax1.bar(heights, avg_times, width=100, alpha=0.7, color='skyblue', label='Среднее время (мс)')
    ax1.set_xlabel('Высота (м)', fontsize=12)
    ax1.set_ylabel('Среднее время расчета (мс)', color='blue', fontsize=12)
    ax1.tick_params(axis='y', labelcolor='blue')
    ax1.grid(axis='y', linestyle='--', alpha=0.3)

    # Добавляем линию с ошибками
    ax2 = ax1.twinx()
    error_line = ax2.plot(heights, error_counts, 'r-', linewidth=2, label='Количество ошибок')
    ax2.set_ylabel('Количество ошибок', color='red', fontsize=12)
    ax2.tick_params(axis='y', labelcolor='red')

    # Объединение легенд
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + lines2, labels + labels2, loc='upper right')

    plt.title('Анализ производительности по высотам', fontsize=14)
    plt.tight_layout()

    height_chart = f"{output_dir}/height_performance_{timestamp}.png"
    plt.savefig(height_chart, dpi=dpi)
    plt.close()

    # 2. График по температурам
    print("- Создание графика по температурам...")
    plt.figure(figsize=(12, 6))

    temp_ranges = [f"{int(row[0])}..{int(row[0]) + 10}" for row in temp_stats]
    temp_avg_times = [row[2] for row in temp_stats]
    temp_error_counts = [row[5] for row in temp_stats]

    ax1 = plt.subplot(111)
    ax1.bar(range(len(temp_ranges)), temp_avg_times, alpha=0.7, color='lightgreen', label='Среднее время (мс)')
    ax1.set_xticks(range(len(temp_ranges)))
    ax1.set_xticklabels(temp_ranges, rotation=45)
    ax1.set_xlabel('Диапазон температур (°C)', fontsize=12)
    ax1.set_ylabel('Среднее время расчета (мс)', color='green', fontsize=12)
    ax1.tick_params(axis='y', labelcolor='green')
    ax1.grid(axis='y', linestyle='--', alpha=0.3)

    # Линия с ошибками
    ax2 = ax1.twinx()
    error_line = ax2.plot(range(len(temp_ranges)), temp_error_counts, 'r-', linewidth=2, label='Количество ошибок')
    ax2.set_ylabel('Количество ошибок', color='red', fontsize=12)
    ax2.tick_params(axis='y', labelcolor='red')

    # Объединение легенд
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + lines2, labels + labels2, loc='upper right')

    plt.title('Анализ производительности по температурам', fontsize=14)
    plt.tight_layout()

    temp_chart = f"{output_dir}/temperature_performance_{timestamp}.png"
    plt.savefig(temp_chart, dpi=dpi)
    plt.close()

    # 3. Тепловая карта
    if heatmap_data:
        print("- Создание тепловой карты...")

        # Преобразуем данные для тепловой карты
        df = pd.DataFrame(heatmap_data, columns=['height', 'temp_group', 'avg_time', 'count'])
        pivot_df = df.pivot(index='height', columns='temp_group', values='avg_time')

        plt.figure(figsize=(14, 8))

        # Создаем тепловую карту
        ax = plt.subplot(111)
        heatmap = ax.pcolormesh(pivot_df.columns, pivot_df.index, pivot_df.values,
                                cmap='viridis', shading='auto')

        # Цветовая шкала
        cbar = plt.colorbar(heatmap)
        cbar.set_label('Среднее время расчета (мс)', fontsize=12)

        plt.title('Тепловая карта времени расчета', fontsize=14)
        plt.xlabel('Температура (°C)', fontsize=12)
        plt.ylabel('Высота (м)', fontsize=12)
        plt.tight_layout()

        heatmap_file = f"{output_dir}/heatmap_performance_{timestamp}.png"
        plt.savefig(heatmap_file, dpi=dpi)
        plt.close()

    # 4. Итоговый комбинированный график
    print("- Создание итогового графика...")
    plt.figure(figsize=(14, 10))

    # Статистические данные
    total_time = metrics[1]  # total_time_ms
    avg_time = metrics[4]  # avg_calculation_time_ms
    min_time = metrics[5]  # min_calculation_time_ms
    max_time = metrics[6]  # max_calculation_time_ms

    # График 1: Высоты (верхний левый)
    plt.subplot(2, 2, 1)
    plt.bar(heights, avg_times, width=100, alpha=0.7, color='skyblue')
    plt.xlabel('Высота (м)')
    plt.ylabel('Среднее время (мс)')
    plt.title('Время расчета по высотам')
    plt.grid(axis='y', linestyle='--', alpha=0.3)

    # График 2: Температуры (верхний правый)
    plt.subplot(2, 2, 2)
    plt.bar(range(len(temp_ranges)), temp_avg_times, alpha=0.7, color='lightgreen')
    plt.xticks(range(len(temp_ranges)), temp_ranges, rotation=45)
    plt.xlabel('Диапазон температур (°C)')
    plt.ylabel('Среднее время (мс)')
    plt.title('Время расчета по температурам')
    plt.grid(axis='y', linestyle='--', alpha=0.3)

    # График 3: Метрики времени (нижний левый)
    plt.subplot(2, 2, 3)
    stat_values = [avg_time, min_time, max_time]
    stat_labels = ['Среднее', 'Минимальное', 'Максимальное']
    colors = ['#3498db', '#2ecc71', '#e74c3c']

    bars = plt.bar(stat_labels, stat_values, alpha=0.7, color=colors)

    # Добавляем значения над столбцами
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2., height + 0.02,
                 f'{height:.4f}', ha='center', va='bottom', fontsize=9)

    plt.ylabel('Время (мс)')
    plt.title('Метрики времени выполнения')
    plt.grid(axis='y', linestyle='--', alpha=0.3)

    # График 4: Текстовая информация (нижний правый)
    plt.subplot(2, 2, 4)
    plt.axis('off')

    total_calcs = metrics[2]
    successful_calcs = metrics[3]

    info_text = (
        f"Анализ производительности расчетов\n"
        f"Дата и время: {metrics[8].strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        f"Параметры расчета:\n"
        f"- Диапазон температур: от {params.get('min_temperature', 'н/д')} до {params.get('max_temperature', 'н/д')} °C\n"
        f"- Шаг расчета: {params.get('temperature_step', 'н/д')} °C\n"
        f"- Количество высот: {params.get('heights_count', 'н/д')}\n\n"
        f"Результаты расчетов:\n"
        f"- Всего расчетов: {total_calcs}\n"
        f"- Успешных расчетов: {successful_calcs}\n"
        f"- Процент успешных: {successful_calcs / total_calcs * 100:.2f}%\n\n"
        f"Время выполнения:\n"
        f"- Общее время: {total_time:.2f} мс ({total_time / 1000:.2f} с)\n"
        f"- Среднее время: {avg_time:.4f} мс\n"
        f"- Минимальное время: {min_time:.4f} мс\n"
        f"- Максимальное время: {max_time:.4f} мс"
    )

    plt.text(0.05, 0.95, info_text, fontsize=10, verticalalignment='top')
    plt.title('Сводная информация')

    plt.tight_layout()
    plt.suptitle('Анализ производительности расчета интерполяций', fontsize=16, y=1.02)

    combined_file = f"{output_dir}/combined_performance_{timestamp}.png"
    plt.savefig(combined_file, bbox_inches='tight', dpi=dpi)
    plt.close()

    print("\n✓ Графики успешно созданы и сохранены:")
    print(f"1. {height_chart}")
    print(f"2. {temp_chart}")

    if heatmap_data:
        print(f"3. {heatmap_file}")

    print(f"4. {combined_file} (итоговый)")

    return combined_file


def display_summary(metrics):
    """Вывод сводной информации по расчетам"""
    if not metrics:
        return

    print_header("СВОДНАЯ ИНФОРМАЦИЯ ПО РАСЧЕТАМ")

    params = json.loads(metrics[7]) if metrics[7] else {}
    total_time = metrics[1]
    total_calcs = metrics[2]
    successful_calcs = metrics[3]
    avg_time = metrics[4]
    min_time = metrics[5]
    max_time = metrics[6]

    print(f"Параметры расчета:")
    print(
        f"- Диапазон температур: от {params.get('min_temperature', 'н/д')} до {params.get('max_temperature', 'н/д')} °C")
    print(f"- Шаг расчета: {params.get('temperature_step', 'н/д')} °C")
    print(f"- Количество высот: {params.get('heights_count', 'н/д')}")

    print(f"\nРезультаты расчетов:")
    print(f"- Всего расчетов: {total_calcs}")
    print(f"- Успешных расчетов: {successful_calcs}")
    print(f"- Процент успешных: {successful_calcs / total_calcs * 100:.2f}%")

    print(f"\nВремя выполнения:")
    print(f"- Общее время: {total_time:.2f} мс ({total_time / 1000:.2f} с)")
    print(f"- Среднее время: {avg_time:.4f} мс")
    print(f"- Минимальное время: {min_time:.4f} мс")
    print(f"- Максимальное время: {max_time:.4f} мс")


def main():
    """Основная функция приложения"""
    # Разбор аргументов командной строки
    args = parse_arguments()

    # Подготовка конфигурации
    db_config, calc_settings = get_config(args)

    print_header("ПРИЛОЖЕНИЕ ДЛЯ АНАЛИЗА ИНТЕРПОЛЯЦИЙ")
    print("Это приложение выполняет расчет всех вариантов интерполяции")
    print("и анализирует производительность для включения в Pull Request")

    # Подключение к базе данных
    conn = connect_to_db(db_config)
    if not conn:
        return

    try:
        # Запуск хранимой процедуры, если не указано пропустить расчет
        if not args.skip_calculation:
            success = run_interpolation_procedure(conn, calc_settings, args.verbose)
            if not success:
                print("✗ Не удалось выполнить процедуру интерполяции")
                return
        else:
            print("\nРасчет интерполяций пропущен по запросу пользователя.")
            print("Будут использованы существующие результаты.")

        # Получение метрик производительности
        metrics, height_stats, temp_stats, heatmap_data = fetch_performance_metrics(conn)

        if not metrics:
            print("✗ Не удалось получить метрики производительности")
            return

        # Вывод сводной информации
        display_summary(metrics)

        # Создание визуализаций
        if not args.no_plots:
            result_file = create_performance_charts(
                metrics,
                height_stats,
                temp_stats,
                heatmap_data,
                args.output_dir,
                args.dpi
            )

            if result_file:
                print(f"\n✓ Анализ производительности успешно завершен!")
                print(f"  Итоговый график: {result_file}")
        else:
            print("\nСоздание графиков пропущено по запросу пользователя.")

    finally:
        # Закрытие соединения с базой данных
        if conn:
            conn.close()
            print("\nСоединение с базой данных закрыто")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nПриложение остановлено пользователем.")
    except Exception as e:
        print(f"\n\nПроизошла непредвиденная ошибка: {e}")
    finally:
        print("\nРабота приложения завершена.")