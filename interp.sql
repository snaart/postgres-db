/**
 * Хранимая процедура для расчета всех вариантов интерполяции температур
 * 
 * Выполняет расчет интерполяции для всех комбинаций:
 * - высот из таблицы temperature_deviations
 * - температур в заданном диапазоне с указанным шагом
 * 
 * Результаты сохраняются в таблицу interpolation_results
 * Метрики производительности сохраняются в таблицу interpolation_performance
 */
CREATE OR REPLACE PROCEDURE public.calculate_all_interpolations(
    p_min_temperature NUMERIC DEFAULT -50,
    p_max_temperature NUMERIC DEFAULT 40,
    p_temperature_step NUMERIC DEFAULT 0.5,
    p_clear_previous_results BOOLEAN DEFAULT TRUE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_height INTEGER;
    v_temp NUMERIC;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_total_calculations INTEGER := 0;
    v_successful_calculations INTEGER := 0;
    v_heights INTEGER[];
    v_calculation_result NUMERIC[];
    v_calc_start TIMESTAMP;
    v_calc_end TIMESTAMP;
    v_calc_time NUMERIC;
    v_error_msg TEXT;
BEGIN
    -- Фиксируем время начала выполнения
    RAISE NOTICE 'Начало расчета интерполяций';
    RAISE NOTICE 'Параметры: мин. температура = %, макс. температура = %, шаг = %', 
                 p_min_temperature, p_max_temperature, p_temperature_step;
    
    -- Если требуется, очищаем предыдущие результаты
    IF p_clear_previous_results THEN
        DROP TABLE IF EXISTS public.interpolation_results;
        DROP TABLE IF EXISTS public.interpolation_performance;
    END IF;
    
    -- Создаем таблицу для хранения результатов
    CREATE TABLE IF NOT EXISTS public.interpolation_results (
        id SERIAL PRIMARY KEY,
        height INTEGER NOT NULL,
        temperature NUMERIC(8,2) NOT NULL,
        tens_value INTEGER,
        ones_value INTEGER,
        dev_tens NUMERIC,
        dev_ones NUMERIC,
        result_value NUMERIC,
        calculation_time NUMERIC(10,3), -- в миллисекундах
        error_message TEXT DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Создаем таблицу для метрик производительности
    CREATE TABLE IF NOT EXISTS public.interpolation_performance (
        id SERIAL PRIMARY KEY,
        total_time_ms NUMERIC(10,3),
        total_calculations INTEGER,
        successful_calculations INTEGER,
        avg_calculation_time_ms NUMERIC(10,3),
        min_calculation_time_ms NUMERIC(10,3),
        max_calculation_time_ms NUMERIC(10,3),
        parameters JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Фиксируем время начала
    v_start_time := clock_timestamp();
    
    -- Получаем список всех высот
    SELECT array_agg(height ORDER BY height) 
    INTO v_heights 
    FROM public.temperature_deviations;
    
    RAISE NOTICE 'Найдено % различных высот для расчета', array_length(v_heights, 1);
    
    -- Оптимизация: создаем индексы для ускорения вставки
    CREATE INDEX IF NOT EXISTS idx_interpolation_results_height ON public.interpolation_results(height);
    CREATE INDEX IF NOT EXISTS idx_interpolation_results_temp ON public.interpolation_results(temperature);
    
    -- Для каждой высоты и температуры выполняем расчет
    FOREACH v_height IN ARRAY v_heights LOOP
        RAISE NOTICE 'Обработка высоты: % м', v_height;
        
        -- Перебираем температуры с заданным шагом
        v_temp := p_min_temperature;
        WHILE v_temp <= p_max_temperature LOOP
            v_total_calculations := v_total_calculations + 1;
            
            -- Замеряем время расчета
            v_calc_start := clock_timestamp();
            
            -- Выполняем расчет с обработкой ошибок
            BEGIN
                v_calculation_result := public.calculate_temperature_deviation(v_height, v_temp);
                v_error_msg := NULL;
                
                -- Если расчет успешный, увеличиваем счетчик
                IF v_calculation_result IS NOT NULL THEN
                    v_successful_calculations := v_successful_calculations + 1;
                END IF;
            EXCEPTION 
                WHEN OTHERS THEN
                    v_calculation_result := NULL;
                    v_error_msg := SQLERRM;
            END;
            
            v_calc_end := clock_timestamp();
            v_calc_time := EXTRACT(EPOCH FROM (v_calc_end - v_calc_start)) * 1000; -- в миллисекундах
            
            -- Сохраняем результат расчета
            INSERT INTO public.interpolation_results (
                height, 
                temperature, 
                tens_value, 
                ones_value, 
                dev_tens, 
                dev_ones, 
                result_value,
                calculation_time,
                error_message
            ) 
            VALUES (
                v_height, 
                v_temp, 
                CASE WHEN v_calculation_result IS NOT NULL THEN v_calculation_result[1] ELSE NULL END,
                CASE WHEN v_calculation_result IS NOT NULL THEN v_calculation_result[2] ELSE NULL END,
                CASE WHEN v_calculation_result IS NOT NULL THEN v_calculation_result[3] ELSE NULL END,
                CASE WHEN v_calculation_result IS NOT NULL THEN v_calculation_result[4] ELSE NULL END,
                CASE WHEN v_calculation_result IS NOT NULL THEN v_calculation_result[5] ELSE NULL END,
                v_calc_time,
                v_error_msg
            );
            
            -- Переходим к следующей температуре
            v_temp := v_temp + p_temperature_step;
        END LOOP;
    END LOOP;
    
    -- Фиксируем время окончания
    v_end_time := clock_timestamp();
    
    -- Записываем метрики производительности
    INSERT INTO public.interpolation_performance (
        total_time_ms,
        total_calculations,
        successful_calculations,
        avg_calculation_time_ms,
        min_calculation_time_ms,
        max_calculation_time_ms,
        parameters
    )
    SELECT
        EXTRACT(EPOCH FROM (v_end_time - v_start_time)) * 1000,
        v_total_calculations,
        v_successful_calculations,
        AVG(calculation_time),
        MIN(calculation_time),
        MAX(calculation_time),
        jsonb_build_object(
            'min_temperature', p_min_temperature,
            'max_temperature', p_max_temperature,
            'temperature_step', p_temperature_step,
            'heights_count', array_length(v_heights, 1),
            'calculation_date', NOW()::TEXT
        )
    FROM
        public.interpolation_results;
    
    -- Выводим сводную информацию
    RAISE NOTICE 'Расчет интерполяций завершен:';
    RAISE NOTICE '  Всего выполнено расчетов: %', v_total_calculations;
    RAISE NOTICE '  Успешных расчетов: %', v_successful_calculations;
    RAISE NOTICE '  Общее время выполнения: % мс', EXTRACT(EPOCH FROM (v_end_time - v_start_time)) * 1000;
END;
$$;

-- Создаем функцию в схеме public для обеспечения совместимости
CREATE OR REPLACE FUNCTION public.calculate_all_interpolations(
    p_min_temperature NUMERIC DEFAULT -50,
    p_max_temperature NUMERIC DEFAULT 40,
    p_temperature_step NUMERIC DEFAULT 0.5,
    p_clear_previous_results BOOLEAN DEFAULT TRUE
) RETURNS VOID AS $$
BEGIN
    CALL public.calculate_all_interpolations(
        p_min_temperature,
        p_max_temperature,
        p_temperature_step,
        p_clear_previous_results
    );
END;
$$ LANGUAGE plpgsql;