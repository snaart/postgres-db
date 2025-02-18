#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>

int main(void) {
    /* Параметры подключения – отредактируйте по необходимости */
    const char *conninfo = "dbname=mydb user=myuser password=12345 host=localhost port=5432";
    PGconn *conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Ошибка подключения: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return EXIT_FAILURE;
    }

    /* Готовим запрос с параметром (t – текущая температура)
       Он возвращает:
         lower_temp – максимальная температура, не превышающая t
         lower_corr – соответствующая корректировка,
         upper_temp – минимальная температура, не меньшая t
         upper_corr – соответствующая корректировка */
    const char *stmtName = "interp";
    const char *stmt =
      "SELECT "
      "  (SELECT temperature FROM calc_temperatures_correction WHERE temperature <= $1 ORDER BY temperature DESC LIMIT 1) AS lower_temp, "
      "  (SELECT correction  FROM calc_temperatures_correction WHERE temperature <= $1 ORDER BY temperature DESC LIMIT 1) AS lower_corr, "
      "  (SELECT temperature FROM calc_temperatures_correction WHERE temperature >= $1 ORDER BY temperature ASC LIMIT 1) AS upper_temp, "
      "  (SELECT correction  FROM calc_temperatures_correction WHERE temperature >= $1 ORDER BY temperature ASC LIMIT 1) AS upper_corr";

    PGresult *res = PQprepare(conn, stmtName, stmt, 1, NULL);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Ошибка подготовки запроса: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return EXIT_FAILURE;
    }
    PQclear(res);

    /* Цикл по температуре от 0 до 40 с шагом 0.01.
       Для каждого значения мы выполняем запрос, получаем границы и вычисляем интерполяцию. */
    double t, lower_temp, lower_corr, upper_temp, upper_corr, interp;
    char param[64];

    /* Всего шагов: 4000 шагов + начальное значение (0.00) */
    for (int i = 0; i <= 4000; i++) {
        t = i * 0.01;
        snprintf(param, sizeof(param), "%lf", t);
        const char *params[1] = { param };

        res = PQexecPrepared(conn, stmtName, 1, params, NULL, NULL, 0);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            fprintf(stderr, "Ошибка выполнения запроса для t = %.2lf: %s\n", t, PQerrorMessage(conn));
            PQclear(res);
            continue;
        }

        /* Ожидается ровно одна строка */
        if (PQntuples(res) != 1) {
            fprintf(stderr, "Неверное число строк для t = %.2lf\n", t);
            PQclear(res);
            continue;
        }

        /* Если значение ниже минимального в таблице – используем корректировку верхней точки,
           если выше максимального – используем корректировку нижней точки.
           Функция PQgetisnull() проверяет, возвращено ли значение. */
        int lower_isnull = PQgetisnull(res, 0, 0);
        int upper_isnull = PQgetisnull(res, 0, 2);

        if (lower_isnull) {
            interp = atof(PQgetvalue(res, 0, 3));  // t ниже минимума
        } else if (upper_isnull) {
            interp = atof(PQgetvalue(res, 0, 1));  // t выше максимума
        } else {
            lower_temp = atof(PQgetvalue(res, 0, 0));
            lower_corr = atof(PQgetvalue(res, 0, 1));
            upper_temp = atof(PQgetvalue(res, 0, 2));
            upper_corr = atof(PQgetvalue(res, 0, 3));

            if (upper_temp - lower_temp == 0)
                interp = lower_corr;  // Предотвращаем деление на ноль
            else
                interp = lower_corr + (upper_corr - lower_corr) * (t - lower_temp) / (upper_temp - lower_temp);
        }

        printf("t = %.2lf, interpolated correction = %.4lf\n", t, interp);
        PQclear(res);
    }

    PQfinish(conn);
    return EXIT_SUCCESS;
}
