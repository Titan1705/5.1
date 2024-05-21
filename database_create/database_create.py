from source.funcs import get_id_employees
from source.funcs import get_vacancies
from source.constants import hh_company_names, employee_id_API, vacancies_API
import psycopg2

# Переменная со списком ID компаний, а так же списком вакансий в формате JSON.
id_employees_list = get_id_employees(hh_company_names, employee_id_API)
get_vacancy_list = get_vacancies(id_employees_list, vacancies_API)

#  Вводим свои локальные параметры базы данных Postgres SQL.
try:
    params = dict(host='localhost', database='course_work_5', port=5432, user='postgres', password='1234')
    with psycopg2.connect(**params) as connection:
        with connection.cursor() as cursor:

            #  Удаляем старые таблицы, если ранее существовали.
            cursor.execute(f"""
            DROP TABLE IF EXISTS areas;
            DROP TABLE IF EXISTS salaries;
            DROP TABLE IF EXISTS types;
            DROP TABLE IF EXISTS addresses;
            DROP TABLE IF EXISTS employers;
            DROP TABLE IF EXISTS vacancies;
            """)

            # Создаем новые таблицы.
            cursor.execute(f"""

            CREATE TABLE vacancies (vacancy_id INT PRIMARY KEY, --modified name
            premium BOOL NOT NULL,
            name_ VARCHAR(100) NOT NULL, --modified name
            area_id SERIAL NOT NULL, 
            salary_id SERIAL, 
            vacancy_type_id SERIAL NOT NULL, --modified name
            address_id SERIAL NOT NULL, 
            published_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP NOT NULL,
            url VARCHAR(200) NOT NULL,
            alternate_url VARCHAR(200) NOT NULL,
            employer_id SERIAL NOT NULL
            );

            CREATE TABLE areas(
            vacancy_id INT REFERENCES vacancies(vacancy_id),
            area_id SERIAL PRIMARY KEY,
            employer_area_id INT NOT NULL, --modified name
            name_ VARCHAR(50), --modified name
            url VARCHAR(100) NOT NULL
            );

            CREATE TABLE salaries(
            vacancy_id INT REFERENCES vacancies(vacancy_id),
            salary_id SERIAL PRIMARY KEY,
            from_ REAL, --modified name
            to_ REAL, --modified name
            currency VARCHAR(5) NOT NULL,
            gross BOOL NOT NULL
            );

            CREATE TABLE types(
            vacancy_id INT REFERENCES vacancies(vacancy_id),
            type_id SERIAL PRIMARY KEY,
            vacancy_type_id VARCHAR(10) NOT NULL, --modified name
            name_ VARCHAR(20) NOT NULL --modified name
            );

            CREATE TABLE addresses(
            vacancy_id INT REFERENCES vacancies(vacancy_id),
            address_ID SERIAL PRIMARY KEY,
            city VARCHAR(100),
            street VARCHAR(100),
            building VARCHAR(100),
            lat DOUBLE PRECISION,
            lng DOUBLE PRECISION,
            description TEXT,
            raw TEXT,
            id_ INT NOT NULL
            );

            CREATE TABLE employers (
            vacancy_id INT REFERENCES vacancies(vacancy_id),
            employer_id SERIAL PRIMARY KEY NOT NULL,
            company_id INT NOT NULL, --modified name
            name_ VARCHAR(100) NOT NULL, --modified name
            url VARCHAR(100) NOT NULL,
            alternate_url VARCHAR(100) NOT NULL,
            logo_urls TEXT NOT NULL,
            vacancies_url VARCHAR(100),
            accredited_it_employer BOOL,
            trusted_ BOOL --modified name
            );

            """)
            connection.commit()
            for company_vacancies in get_vacancy_list:  # Список компаний
                for vacancy in company_vacancies:  # Список вакансий по каждой отдельной компании

                    #  добавляем данные в таблицы
                    #  vacancies table
                    add_vacancy = (
                        vacancy['id'], vacancy['premium'], vacancy['name'], vacancy['published_at'],
                        vacancy['created_at'],
                        vacancy['url'], vacancy['alternate_url'])
                    cursor.execute(f"""
                                            INSERT INTO vacancies(vacancy_id, premium, name_, published_at, created_at, url, 
                                            alternate_url) VALUES (%s, %s, %s, %s, %s, %s, %s) returning *;
                                            """, add_vacancy)
                    #  areas table
                    add_area = (vacancy['id'], vacancy['area']['id'], vacancy['area']['name'], vacancy['area']['url'])
                    cursor.execute(f"""
                            INSERT INTO areas (vacancy_id, employer_area_id, name_, url) VALUES (%s, %s, %s, %s) returning *;
                            """, add_area)

                    # salaries table. Если значение ключа salary None, то вносим вручную данные в таблицу.
                    if vacancy.get('salary'):
                        add_salary = (
                            vacancy['id'], vacancy['salary']['from'], vacancy['salary']['to'],
                            vacancy['salary']['currency'],
                            vacancy['salary']['gross'])
                    else:
                        add_salary = (vacancy['id'], 0, 0, 'null', 'false')
                    cursor.execute(f"""
                            INSERT INTO salaries (vacancy_id, from_, to_, currency, gross) VALUES (%s, %s, %s, %s, %s) returning *;
                            """, add_salary)
                    # types table
                    add_type = (vacancy['id'], vacancy['type']['id'], vacancy['type']['name'])
                    cursor.execute(f"""
                                            INSERT INTO types (vacancy_id, vacancy_type_id, name_) VALUES (%s, %s, %s) returning *;
                                            """, add_type)
                    #  addresses table
                    if vacancy.get('address'):
                        add_address = (vacancy['id'], vacancy['address'].get('city', 'null'),
                                       vacancy['address'].get('street', 'null'),
                                       vacancy['address'].get('building', 'null'),
                                       vacancy['address'].get('lat', 'null'),
                                       vacancy['address'].get('lng', 'null'),
                                       vacancy['address'].get('description', 'null'),
                                       vacancy['address'].get('raw', 'null'), vacancy['address'].get('id', 0))
                        cursor.execute(f"""
                                            INSERT INTO addresses(vacancy_id, city, street, building, lat, lng, description, raw, 
                                            id_) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) returning *;
                                            """, add_address)

                    # employers table
                    add_employer = (
                        vacancy['id'], vacancy['employer'].get('id'), vacancy['employer'].get('name'),
                        vacancy['employer'].get('url'), vacancy['employer'].get('alternate_url'),
                        str(vacancy['employer'].get('logo_urls')), vacancy['employer'].get('vacancies_url'),
                        vacancy['employer'].get('accredited_it_employer'), vacancy['employer'].get('trusted'))
                    cursor.execute(f"""
                                            INSERT INTO employers(vacancy_id, company_id, name_, url, alternate_url, logo_urls, 
                                            vacancies_url, accredited_it_employer, trusted_) VALUES 
                                            (%s, %s, %s, %s, %s, %s, %s, %s, %s) returning *;
                                            """, add_employer)

                    connection.commit()
    print(f"База данных создана, данные в БД созданы.")
    connection.close()
except psycopg2.OperationalError:
    print("Ошибка соединения с базой данных. Проверьте настройки.")
