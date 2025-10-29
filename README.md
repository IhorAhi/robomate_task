# robomate_task

Для запуску проекту потрібно:

- compose.yaml
- Dockerfile
- main.py
- requirements.txt

Помістити в папку проекту, а також:

- init.sql
- password.txt

Помістити в папку db всередині проекту

Для запуску достатньо у папці виконати docker compose up.

---

Для тестування потрібні файли в одній папці:

- test.py
- events_sample.csv
- events_sample_200k.csv - це файл events_sample.csv, повторений 40 раз

Для запуску тестування достатньо виконати pytest test.py

Це виконає пункти *Тести* та *Продуктивність*. Вузьким місцем стало спроба подати запит аналітики під час інсерту даних
