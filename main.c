#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <time.h>
#include <math.h>

/* Структура для хранения одной точки интерполяции */
typedef struct {
    double temperature;
    double correction;
} CorrectionPoint;

/* Функция сравнения для сортировки массива точек по температуре */
int compare_points(const void *a, const void *b) {
    CorrectionPoint *pa = (CorrectionPoint*) a;
    CorrectionPoint *pb = (CorrectionPoint*) b;
    if (pa->temperature < pb->temperature)
        return -1;
    else if (pa->temperature > pb->temperature)
        return 1;
    else
        return 0;
}

/* Функция линейной интерполяции:
   Если t точно совпадает с одной из точек – возвращает её correction,
   иначе находит две ближайшие точки и вычисляет значение по формуле */
double interpolate(double t, CorrectionPoint *points, int n) {
    if(t <= points[0].temperature)
        return points[0].correction;
    if(t >= points[n-1].temperature)
        return points[n-1].correction;

    for(int i = 0; i < n - 1; i++) {
        if(t >= points[i].temperature && t <= points[i+1].temperature) {
            double x0 = points[i].temperature;
            double x1 = points[i+1].temperature;
            double y0 = points[i].correction;
            double y1 = points[i+1].correction;
            /* Защита от деления на 0 */
            if(fabs(x1 - x0) < 1e-9)
                return y0;
            return y0 + (y1 - y0) * (t - x0) / (x1 - x0);
        }
    }
    return 0.0; // В теории сюда не дойдём
}

int main() {
    /* Параметры подключения к БД.
       Отредактируйте строку подключения в соответствии с вашими настройками. */
    const char *conninfo = "dbname=mydb user=myuser password=12345 host=localhost port=5432";
    PGconn *conn = PQconnectdb(conninfo);
    if(PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Ошибка подключения: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return EXIT_FAILURE;
    }

    /* Запрос данных из таблицы calc_temperatures_correction */
    PGresult *res = PQexec(conn, "SELECT temperature, correction FROM public.calc_temperatures_correction ORDER BY temperature ASC");
    if(PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Ошибка запроса: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return EXIT_FAILURE;
    }

    int n_points = PQntuples(res);
    CorrectionPoint *points = malloc(n_points * sizeof(CorrectionPoint));
    if(points == NULL) {
        fprintf(stderr, "Ошибка выделения памяти\n");
        PQclear(res);
        PQfinish(conn);
        return EXIT_FAILURE;
    }

    /* Чтение данных из результата запроса */
    for(int i = 0; i < n_points; i++) {
        points[i].temperature = atof(PQgetvalue(res, i, 0));
        points[i].correction = atof(PQgetvalue(res, i, 1));
    }
    /* Если данные не гарантированно отсортированы, можно отсортировать массив */
    qsort(points, n_points, sizeof(CorrectionPoint), compare_points);

    PQclear(res);

    /* Начало замера времени расчета */
    clock_t start = clock();

    /* Переменные для проверки (например, суммирование результатов) */
    double sum = 0.0;
    int count = 0;

    /* Расчет интерполяции от 0 до 40 градусов с шагом 0.01.
       Для каждого значения температура вычисляется "на лету" без кеширования. */
    for(double t = 0.0; t <= 40.0; t += 0.01) {
        double corr = interpolate(t, points, n_points);
        sum += corr;  // суммирование для проверки корректности работы алгоритма
        count++;
        /* Раскомментируйте следующую строку для вывода каждого результата (замедлит расчет) */
        // printf("t = %.2f, correction = %.4f\n", t, corr);
    }

    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;

    /* Вывод результатов */
    printf("Выполнено %d интерполяционных вычислений.\n", count);
    printf("Общая сумма коррекций (для проверки): %.4f\n", sum);
    printf("Время расчета: %.6f секунд.\n", elapsed);

    free(points);
    PQfinish(conn);

    return EXIT_SUCCESS;
}
