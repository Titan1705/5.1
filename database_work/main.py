from source.dbmanager import DBManager


def main(db_instance: DBManager) -> None:
    """Функция выполнения программы"""
    print(f"Привет, пользователь. Я - меню приложения."
          f"У меня 5 функций, по работе с базой данных, которую ранее ты\n"
          f"должен был установить в локальном компьютере:\n"
          f"Если ты этого не сделал, прочитай README.md файл.")
    print("""Выбери один из пунктов в меню.
-----------------------------------------------------------------------""")
    user_answer = 0
    load_func = (db_instance.get_companies_and_vacancies_count,
                 db_instance.get_all_vacancies, db_instance.get_avg_salary,
                 db_instance.get_vacancies_with_higher_salary,
                 db_instance.get_vacancies_with_keyword)
    while True:
        try:
            user_answer = int(input("""
1. Функция возвращает и печатает список всех компаний и количество вакансий в каждой компании.
2. Функция возвращает и печатает список всех вакансий с указанием названия компании, названия вакансии, и 
зарплаты, и ссылки на вакансию.
3. Функция возвращает и печатает  среднюю зарплату по вакансиям.
4. Функция возвращает и печатает список вакансий, зарплата которых выше средней заработной платы
всех вакансий таблицы vacancies.
5. Функция возвращает и печатает список всех названий вакансий, где присутствует введенное ключевое/ключевые 
слова.
6. Выход из приложения.
            """))

        except ValueError:
            print(f"Введите одно из чисел выше.")

        if user_answer in range(1, 5):
            load_func[user_answer - 1]()
        elif user_answer == 5:
            print(f"Введите в поиск ключевые слова через пробел или одно ключевое слово(Поиск зависит от регистра).")
            key_word = input()
            while '  ' in key_word:
                key_word = key_word.replace('  ', ' ')
            key_word = key_word.split()
            load_func[user_answer - 1](*key_word) if len(key_word) >= 1 else \
            print(f"Ввели неверное представление аргументов в функцию.")
        elif user_answer == 6:
            print(f"Приложение закрывается...")
            break
        else:
            print(f"Ввел неверное число из меню. Попробуй заново.")
            continue


if __name__ == '__main__':
    db_conn = DBManager()
    with db_conn:
        main(db_conn)