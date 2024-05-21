import psycopg2
from abc import ABC, abstractmethod


class DataBase(ABC):
    """Абстрактный класс для DBManager"""
    @abstractmethod
    def execute_and_fetch(self):
        pass

    @abstractmethod
    def get_companies_and_vacancies_count(self):
        pass

    @abstractmethod
    def get_all_vacancies(self):
        pass

    @abstractmethod
    def get_avg_salary(self):
        pass

    @abstractmethod
    def get_vacancies_with_higher_salary(self):
        pass

    @abstractmethod
    def get_vacancies_with_keyword(self):
        pass


class DBManager(DataBase):
    """Класс для работы с существующей базой данных на localhost"""

    def __init__(self, host='localhost', database='course_work_5', port=5432, username='postgres', password='1234') -> None:
        """Именные параметры уже внутри функции, однако их можно изменить на свои при создании экземпляра класса."""
        self.host = host
        self.database = database
        self.port = port
        self.user = username
        self.password = password
        self.params = dict(host=self.host, database=self.database, port=self.port, user=self.user,
                           password=self.password)

    def __enter__(self):
        """Создает соединение для ЭК"""
        try:
            self.conn = psycopg2.connect(**self.params)
            return self.conn

        except psycopg2.OperationalError:
            print(f"Ошибка соединения с базой данных. Проверьте введенные настройки.")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Выполняет коммиты и закрытие Базы данных"""
        self.conn.commit()
        self.conn.close()

    def execute_and_fetch(func):
        """Функция-декоратор, чтобы не писать однотипный код к каждому методу.
        в функции func возвращаются SQL запросы от всех методов класса."""
        def wrapper(self, *args, **kwargs):
            cur = self.conn.cursor()
            cur.execute(func(self, *args, **kwargs))
            result = cur.fetchall()
            [print(*_) for _ in result]
            cur.close()
            return result
        return wrapper

    @execute_and_fetch
    def get_companies_and_vacancies_count(self):
        """Печатает и возвращает список всех компаний и количество вакансий у каждой компании."""
        return """
        SELECT e.name_, COUNT(v.vacancy_id) as quantity_vacancies
        FROM vacancies v
        INNER JOIN employers e USING(vacancy_id)
        GROUP BY e.name_;
        """

    @execute_and_fetch
    def get_all_vacancies(self):
        """Печатает и возвращает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и
        ссылки на вакансию."""
        return """
        SELECT e.name_ AS entity_name, v.name_ AS vacancy_name, s.from_ AS salary_from, s.to_ AS salary_to, v.alternate_url as url
        FROM vacancies v
        INNER JOIN salaries s USING(vacancy_id)
        INNER JOIN employers e USING(vacancy_id);
        """

    @execute_and_fetch
    def get_avg_salary(self):
        """Печатает и возвращает среднюю зарплату по вакансиям,
        в которых есть данные отличные от нуля, если в обоих колонках from_/to_ нулевые значения, то не учитываем
        строку, если только в from_ значение отличное от нуля, то учитываем только его, если значение отличное от нуля
        только в to_, то _from не учитываем, т.к. там ноль. Примеры, 0.00 - 100000 = 100000, 100000 - 200000 = 100000,
        100000 - 0.00 = 100000. Учитываем только такой формат"""
        return """
        SELECT AVG(avarage)
        FROM(
        SELECT CASE WHEN from_ <> 0 THEN from_ ELSE to_ END AS avarage 
        FROM(SELECT from_, to_
        FROM salaries
        WHERE from_ <> 0 OR to_ <> 0))
        """

    @execute_and_fetch
    def get_vacancies_with_higher_salary(self):
        """
        Функция возвращает и печатает список вакансий, зарплата которых выше средней заработной платы
        всех вакансий таблицы vacancies.
        """
        return """
        SELECT v.name_
        FROM vacancies v
        INNER JOIN salaries s USING(vacancy_id)
        WHERE 
	        (s.from_ <> 0 AND s.from_ > (SELECT AVG(avarage)
		        FROM(SELECT CASE WHEN from_ <> 0 THEN from_ ELSE to_ END AS avarage 
			        FROM(SELECT from_, to_
				        FROM salaries
				        WHERE from_ <> 0 OR to_ <> 0))))
        OR 
	        (s.to_ <> 0 AND s.to_ > (SELECT AVG(avarage)
		        FROM(SELECT CASE WHEN from_ <> 0 THEN from_ ELSE to_ END AS avarage 
			        FROM(SELECT from_, to_
				        FROM salaries
				        WHERE from_ <> 0 OR to_ <> 0))));
        """

    @execute_and_fetch
    def get_vacancies_with_keyword(self, *args, **kwargs):
        """Принимает значение или значения( через запятую), возвращает список всех
        вакансий, в названии которых присутствуют переданные в функцию слово
        или список слов"""
        self.lst = [_ for _ in args]
        if len(self.lst) > 1:
            for _ in args[1:]:
                result = f"""
            SELECT name_
            FROM vacancies
            WHERE name_ like '%{args[0]}%'
            """ + f"""OR name_ like '%{_}%'"""
            return result + ';'
        elif len(self.lst) == 1:
            return f"""
            SELECT name_
            FROM vacancies
            WHERE name_ like '%{args[0]}%';
            """
        else:
            print(f"Неверно введены данные в функцию get_vacancies_with_keyword")

