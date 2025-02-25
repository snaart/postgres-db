#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <pthread.h>
#include <time.h>

// Структура для результатов вычислений
typedef struct {
    double temperature;
    double correction;
} Result;

typedef struct {
    PGconn *conn;
    const char *conninfo;
    int start_index;
    int end_index;
    const char *stmt_name;
    Result *results;  // Массив для хранения результатов
} ThreadArgs;

void* process_temperature_range(void *arg) {
    ThreadArgs *args = (ThreadArgs*)arg;
    double t, lower_temp, lower_corr, upper_temp, upper_corr, interp;
    char param[64];
    PGresult *res;

    PGconn *thread_conn = PQconnectdb(args->conninfo);
    if (PQstatus(thread_conn) != CONNECTION_OK) {
        fprintf(stderr, "Ошибка подключения в потоке: %s\n", PQerrorMessage(thread_conn));
        PQfinish(thread_conn);
        return NULL;
    }

    const char *stmt =
        "SELECT "
        "  (SELECT temperature FROM calc_temperatures_correction WHERE temperature <= $1 ORDER BY temperature DESC LIMIT 1) AS lower_temp, "
        "  (SELECT correction  FROM calc_temperatures_correction WHERE temperature <= $1 ORDER BY temperature DESC LIMIT 1) AS lower_corr, "
        "  (SELECT temperature FROM calc_temperatures_correction WHERE temperature >= $1 ORDER BY temperature ASC LIMIT 1) AS upper_temp, "
        "  (SELECT correction  FROM calc_temperatures_correction WHERE temperature >= $1 ORDER BY temperature ASC LIMIT 1) AS upper_corr";

    res = PQprepare(thread_conn, args->stmt_name, stmt, 1, NULL);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Ошибка подготовки запроса в потоке: %s\n", PQerrorMessage(thread_conn));
        PQclear(res);
        PQfinish(thread_conn);
        return NULL;
    }
    PQclear(res);

    for (int i = args->start_index; i <= args->end_index; i++) {
        t = i * 0.01;
        snprintf(param, sizeof(param), "%lf", t);
        const char *params[1] = { param };

        res = PQexecPrepared(thread_conn, args->stmt_name, 1, params, NULL, NULL, 0);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            fprintf(stderr, "Ошибка выполнения запроса для t = %.2lf: %s\n", t, PQerrorMessage(thread_conn));
            PQclear(res);
            continue;
        }

        if (PQntuples(res) != 1) {
            fprintf(stderr, "Неверное число строк для t = %.2lf\n", t);
            PQclear(res);
            continue;
        }

        int lower_isnull = PQgetisnull(res, 0, 0);
        int upper_isnull = PQgetisnull(res, 0, 2);

        if (lower_isnull) {
            interp = atof(PQgetvalue(res, 0, 3));
        } else if (upper_isnull) {
            interp = atof(PQgetvalue(res, 0, 1));
        } else {
            lower_temp = atof(PQgetvalue(res, 0, 0));
            lower_corr = atof(PQgetvalue(res, 0, 1));
            upper_temp = atof(PQgetvalue(res, 0, 2));
            upper_corr = atof(PQgetvalue(res, 0, 3));

            if (upper_temp - lower_temp == 0)
                interp = lower_corr;
            else
                interp = lower_corr + (upper_corr - lower_corr) * (t - lower_temp) / (upper_temp - lower_temp);
        }

        // Сохраняем результат в массив
        int result_index = i - args->start_index;
        args->results[result_index].temperature = t;
        args->results[result_index].correction = interp;

        PQclear(res);
    }

    PQfinish(thread_conn);
    return NULL;
}

int main(void) {
    clock_t start_time = clock();

    const char *conninfo = "dbname=mydb user=myuser password=12345 host=localhost port=5432";
    PGconn *conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Ошибка подключения: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return EXIT_FAILURE;
    }

    const int num_threads = 4;
    pthread_t threads[num_threads];
    ThreadArgs thread_args[num_threads];

    // Выделяем память под результаты для каждого потока
    Result *all_results[num_threads];

    int range_size = 4001 / num_threads;
    for (int i = 0; i < num_threads; i++) {
        int thread_range = (i == num_threads - 1) ?
            4000 - i * range_size + 1 :
            range_size;

        all_results[i] = (Result*)malloc(thread_range * sizeof(Result));

        thread_args[i].conn = conn;
        thread_args[i].conninfo = conninfo;
        thread_args[i].start_index = i * range_size;
        thread_args[i].end_index = (i == num_threads - 1) ? 4000 : (i + 1) * range_size - 1;
        thread_args[i].stmt_name = "interp";
        thread_args[i].results = all_results[i];

        if (pthread_create(&threads[i], NULL, process_temperature_range, &thread_args[i]) != 0) {
            fprintf(stderr, "Ошибка создания потока %d\n", i);
            return EXIT_FAILURE;
        }
    }

    // Ожидаем завершения всех потоков
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    clock_t end_time = clock();
    double execution_time = (double)(end_time - start_time) / CLOCKS_PER_SEC;

    printf("Общее время выполнения: %.2f секунд\n", execution_time);

    // Освобождаем память
    for (int i = 0; i < num_threads; i++) {
        free(all_results[i]);
    }

    PQfinish(conn);
    return EXIT_SUCCESS;
}