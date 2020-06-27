# Тестовое задание для стажеров аналитиков-технологов

Необходимо разработать некоторые части приложения для учета книг в библиотеке. Описание данных, с которыми будет работать приложение – ниже.

Приложение для учета книг в библиотеке должно:

1. Хранить названия книг, ФИО авторов, наименования издательств, год издания.
2. Учитывать имеющиеся в библиотеке экземпляры конкретной книги.
3. Учитывать студентов, которым выдавалась конкретная книга. При каждой выдаче книги студенту, фиксируется дата выдачи. При возврате – дата возврата книги.

## Задания:

1. Опишите модель данных (в любом удобном для вас представлении) для обслуживания библиотеки. Это может быть описание таблиц с типами данных, диаграмма – что угодно.

Указанную модель данных удобно представить в виде следующей ER-диаграммы:

![ER-диаграмма БД](diag.png)

2. Напишите SQL-запрос, который бы возвращал самого популярного автора за год. Запрос должен основываться на модели данных, которую вы описали в задании 1. 

Запишем указанный запрос и, для наглядности, выполним его средствами Python:


```python
import psycopg2 as pg
import pandas as pd

class DBexecutor:
    
    def __init__(self, params):
        self.params = params
        self.conn, self.curs = self.connect_db()
        
    def connect_db(self):
        try:
            conn = pg.connect(**self.params)
            conn.set_session(autocommit=True)
            if conn:
                curs = conn.cursor()
                return conn, curs
        except Exception:
            print('Ошибка подключения к БД')
            return None, None
        
    def __del__(self):
        if self.conn:
            self.curs.close()
            self.conn.close()
```


```python
params = {
    'dbname': 'library',
    'user': 'homutovan',
    'password': ''
        }

db = DBexecutor(params)

df = pd.read_sql("""
                select first_name, middle_name, last_name
                from books_students bs
                inner join books b on book_id = b.id
                inner join author a2 on author_id = a2.id
                inner join first_name fn on first_name_id = fn.id
                inner join middle_name mn on middle_name_id = mn.id
                inner join last_name ln2 on last_name_id = ln2.id
                where bs.date_of_issue between '2019-01-01' and '2019-12-31'
                group by first_name, middle_name, last_name 
                order by count(bs.id) desc
                limit 1;
                """, db.conn)

df
```




<div> 
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>first_name</th>
      <th>middle_name</th>
      <th>last_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Вячеслав</td>
      <td>Иванович</td>
      <td>Букур</td>
    </tr>
  </tbody>
</table>
</div>



3. Определите понятие «злостный читатель».  Предложите алгоритм для поиска самого злостного читателя библиотеки. На любом языке программирования опишите алгоритм поиска такого читателя. Алгоритм должен основываться на модели данных, которую вы описали в задании 1.

Определим понятие «злостный читатель» - пусть «злостным» является такой читатель, который с начала года допускал задержку возврата книг, в среднем, более чем на 15 дней. Максимально допустимый срок возврата книг примем равным 30 дням. Выполним запрос для поиска таких читателей в БД:


```python
db = DBexecutor(params)

df = pd.read_sql("""
                select first_name, middle_name, last_name, faculty 
                from books_students bs 
                inner join student s on student_id = s.id
                inner join first_name fn on first_name_id = fn.id
                inner join middle_name mn on middle_name_id = mn.id
                inner join last_name ln2 on last_name_id = ln2.id
                inner join faculty f on faculty_id = f.id
                where bs.date_of_issue between '2020-01-01' and '2020-06-26'
                group by first_name, middle_name, last_name, faculty 
                having sum(date_of_return - date_of_issue - 30) / count(bs.id) > 15
                order by last_name;
                """, db.conn)
df
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>first_name</th>
      <th>middle_name</th>
      <th>last_name</th>
      <th>faculty</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Адольф</td>
      <td>Нестеровна</td>
      <td>Абрамович</td>
      <td>Факультет автоматики и вычислительной техники ...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Виталий</td>
      <td>Эльевич</td>
      <td>Агафонов</td>
      <td>Факультет энергетики (ФЭН)</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Эммануил</td>
      <td>Самойлович</td>
      <td>Бондарев</td>
      <td>Механико -технологический факультет (МТФ)</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Татьяна</td>
      <td>Соломоновна</td>
      <td>Вершинин</td>
      <td>Факультет летательных аппаратов (ФЛА)</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Александра</td>
      <td>Павлович</td>
      <td>Горб</td>
      <td>Факультет мехатроники и автоматизации (ФМА)</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Станислав</td>
      <td>Борисовна</td>
      <td>Козлов</td>
      <td>Механико -технологический факультет (МТФ)</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Феоктист</td>
      <td>Макарович</td>
      <td>Куприна</td>
      <td>Факультет радиотехники и электроники (РЭФ)</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Рашель</td>
      <td>Федотович</td>
      <td>Ляпин</td>
      <td>Факультет летательных аппаратов (ФЛА)</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Рашель</td>
      <td>Арсеньевич</td>
      <td>Мельников-Печерский</td>
      <td>Факультет прикладной математики и информатики ...</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Анатолий</td>
      <td>Константиновна</td>
      <td>Назаренко</td>
      <td>Факультет летательных аппаратов (ФЛА)</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Рустам</td>
      <td>Максимович</td>
      <td>Наумович</td>
      <td>Механико -технологический факультет (МТФ)</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Эльвира</td>
      <td>Дамианович</td>
      <td>Олеша</td>
      <td>Факультет прикладной математики и информатики ...</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Герберт</td>
      <td>Викентьевич</td>
      <td>Тадтаев</td>
      <td>Факультет радиотехники и электроники (РЭФ)</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Галина</td>
      <td>Бейшеналиевич</td>
      <td>Тотров</td>
      <td>Институт социальных технологий (ИСТ)</td>
    </tr>
    <tr>
      <th>14</th>
      <td>Всеволод</td>
      <td>Мономах</td>
      <td>Хамхоев</td>
      <td>Факультет энергетики (ФЭН)</td>
    </tr>
    <tr>
      <th>15</th>
      <td>Инна</td>
      <td>Исмаилович</td>
      <td>Цей</td>
      <td>Факультет радиотехники и электроники (РЭФ)</td>
    </tr>
  </tbody>
</table>
</div>



В с помощью данного запроса мы объединяем таблицы books_students, student, first_name, middle_name, last_name и faculty, группируем записи по ФИО студентов, для каждой полученной группы выполняем расчет отношения числа дней просрочки к числу записей о получении и возврате книги и фильтруем полученный результат, отсекая значения менее 15.

Аналогичного результата мы можем достичь запросив данные из БД и выполнив дальнейшую обработку средствами Python:


```python
import datetime

db = DBexecutor(params)

df = pd.read_sql("""
                select * 
                from books_students bs 
                inner join student s on student_id = s.id
                inner join first_name fn on first_name_id = fn.id
                inner join middle_name mn on middle_name_id = mn.id
                inner join last_name ln2 on last_name_id = ln2.id
                inner join faculty f on faculty_id = f.id
                """, db.conn)
data = df[['date_of_issue', 'date_of_return', 'first_name', 'middle_name', 'last_name', 'faculty']]
filter_after = data['date_of_issue'] >= datetime.date(2020, 1, 1)
filter_before = data['date_of_issue'] <= datetime.date(2020, 6, 26)
data = data.loc[filter_before & filter_after]
```


```python
data['delta'] = data['date_of_return'] - data['date_of_issue'] - pd.Timedelta(30, unit='d')
group_data = data.groupby(['first_name', 'middle_name', 'last_name', 'faculty']).agg(['sum', 'count']).reset_index()
group_data['av_delay'] = group_data[['delta'][0]]['sum'] / group_data[['delta'][0]]['count']
filter_delay = group_data['av_delay'].dt.round(freq = 'D') > pd.Timedelta(15, unit='d')
group_data.loc[filter_delay].sort_values('last_name').reset_index()[['first_name', 'middle_name', 'last_name', 'faculty']]
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr>
      <th></th>
      <th>first_name</th>
      <th>middle_name</th>
      <th>last_name</th>
      <th>faculty</th>
    </tr>
    <tr>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Адольф</td>
      <td>Нестеровна</td>
      <td>Абрамович</td>
      <td>Факультет автоматики и вычислительной техники ...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Виталий</td>
      <td>Эльевич</td>
      <td>Агафонов</td>
      <td>Факультет энергетики (ФЭН)</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Эммануил</td>
      <td>Самойлович</td>
      <td>Бондарев</td>
      <td>Механико -технологический факультет (МТФ)</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Татьяна</td>
      <td>Соломоновна</td>
      <td>Вершинин</td>
      <td>Факультет летательных аппаратов (ФЛА)</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Александра</td>
      <td>Павлович</td>
      <td>Горб</td>
      <td>Факультет мехатроники и автоматизации (ФМА)</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Станислав</td>
      <td>Борисовна</td>
      <td>Козлов</td>
      <td>Механико -технологический факультет (МТФ)</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Феоктист</td>
      <td>Макарович</td>
      <td>Куприна</td>
      <td>Факультет радиотехники и электроники (РЭФ)</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Рашель</td>
      <td>Федотович</td>
      <td>Ляпин</td>
      <td>Факультет летательных аппаратов (ФЛА)</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Рашель</td>
      <td>Арсеньевич</td>
      <td>Мельников-Печерский</td>
      <td>Факультет прикладной математики и информатики ...</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Анатолий</td>
      <td>Константиновна</td>
      <td>Назаренко</td>
      <td>Факультет летательных аппаратов (ФЛА)</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Рустам</td>
      <td>Максимович</td>
      <td>Наумович</td>
      <td>Механико -технологический факультет (МТФ)</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Эльвира</td>
      <td>Дамианович</td>
      <td>Олеша</td>
      <td>Факультет прикладной математики и информатики ...</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Герберт</td>
      <td>Викентьевич</td>
      <td>Тадтаев</td>
      <td>Факультет радиотехники и электроники (РЭФ)</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Галина</td>
      <td>Бейшеналиевич</td>
      <td>Тотров</td>
      <td>Институт социальных технологий (ИСТ)</td>
    </tr>
    <tr>
      <th>14</th>
      <td>Всеволод</td>
      <td>Мономах</td>
      <td>Хамхоев</td>
      <td>Факультет энергетики (ФЭН)</td>
    </tr>
    <tr>
      <th>15</th>
      <td>Инна</td>
      <td>Исмаилович</td>
      <td>Цей</td>
      <td>Факультет радиотехники и электроники (РЭФ)</td>
    </tr>
  </tbody>
</table>
</div>




```python
del db
```
