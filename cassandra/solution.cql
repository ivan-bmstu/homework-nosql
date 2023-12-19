describe web_user_posting;

CREATE KEYSPACE web_user_posting WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};
USE web_user_posting;

/*
создадим две таблицы, т.к. в cassandr'е таблицы создаются под запросы, а 1-3 запросы исключают возможность использовать
4 запрос для той же таблицы.
*/
CREATE TABLE user_post (
                         user_id INT,
                         user_name TEXT,
                         email TEXT,
                         post_id INT,
                         content TEXT,
                         created_at TIMESTAMP,
                         PRIMARY KEY ( user_id, created_at)
) WITH CLUSTERING ORDER BY (created_at DESC );

CREATE TABLE thematic_post (
                             user_id INT,
                             user_name TEXT,
                             post_id INT,
                             created_at TIMESTAMP,
                             topic_id INT,
                             created_at_date DATE,
                             PRIMARY KEY ( (topic_id, created_at_date), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);

/*
 Запишем данные в таблицы. Прочитал, что можно использовать BATCH как аналог транзакций, мне кажется он здесь уместен,
потому что может возникнуть ситуация, что мы записали в таблицу user_post новый пост, но не в thematic_post. И уже
в дальнейшем, мы не сможем по 4 запросу найти добавленный пост,
а значит, не сможем найти данный пост среди тематических. Но с другой стороны,
это может сильно снизить производительность, поэтому я тут неуверен.
*/
BEGIN BATCH
INSERT INTO user_post(user_id, user_name, email, post_id, content, created_at)
VALUES (1, 'Ivan', '1@ru', 1, 'Quod licet Iovi, non licet bovi', '2022-12-17 13:23:41');
INSERT INTO thematic_post(user_id, user_name, post_id, created_at, topic_id, created_at_date)
VALUES (1, 'Ivan', 1, '2022-12-17 13:23:41', 1, '2022-12-17');
APPLY BATCH;

BEGIN BATCH
INSERT INTO user_post(user_id, user_name, email, post_id, content, created_at)
VALUES (1, 'Ivan', '1@ru', 2, 'Scientia potentia est', '2023-01-22 13:23:46');
INSERT INTO thematic_post(user_id, user_name, post_id, created_at, topic_id, created_at_date)
VALUES (1, 'Ivan', 2, '2023-01-22 13:23:46', 2, '2023-01-22');
APPLY BATCH;

BEGIN BATCH
INSERT INTO user_post(user_id, user_name, email, post_id, content, created_at)
VALUES (1, 'Ivan', '1@ru', 3, 'In vene veritas', '2023-01-22 15:23:49');
INSERT INTO thematic_post(user_id, user_name, post_id, created_at, topic_id, created_at_date)
VALUES (1, 'Ivan', 3, '2023-01-22 15:23:49', 2, '2023-01-22');
APPLY BATCH;


BEGIN BATCH
INSERT INTO user_post(user_id, user_name, email, post_id, content, created_at)
VALUES (2, 'Alex', '2@ru', 4, 'Medice, cura te ipsum!', '2022-12-17 15:23:45');
INSERT INTO thematic_post(user_id, user_name, post_id, created_at, topic_id, created_at_date)
VALUES (2, 'Alex', 4, '2022-12-17 15:23:45', 2, '2022-12-17');
APPLY BATCH;

BEGIN BATCH
INSERT INTO user_post(user_id, user_name, email, post_id, content, created_at)
VALUES (2, 'Alex', '2@ru', 5, 'Ab ovo ad mala', '2023-01-22 15:23:45');
INSERT INTO thematic_post(user_id, user_name, post_id, created_at, topic_id, created_at_date)
VALUES (2, 'Alex', 5, '2023-01-22 15:23:45', 2, '2023-01-22');
APPLY BATCH;

BEGIN BATCH
INSERT INTO user_post(user_id, user_name, email, post_id, content, created_at)
VALUES (2, 'Alex', '2@ru', 6, 'Non sum qualis eram', '2023-01-22 16:23:45');
INSERT INTO thematic_post(user_id, user_name, post_id, created_at, topic_id, created_at_date)
VALUES (2, 'Alex', 6, '2023-01-22 16:23:45', 2, '2023-01-22');
APPLY BATCH;

/*
1
Запрос, который считывает посты заданного пользователя упорядоченные по времени создания поста - сначала новые.
Запрос возвращает post_id, content, дату создания поста.
*/
SELECT
  post_id,
  content,
  created_at
FROM user_post
WHERE user_id = 1;

/*
2
Запрос, который считывает последний пост заданного пользователя.
Запрос возвращает post_id, content, дату создания поста, username
*/
SELECT
  post_id,
  content,
  created_at,
  user_name
FROM user_post
WHERE user_id = 1
LIMIT 1;

/*
3
Запрос, который считывает посты заданного пользователя, опубликованные после заданной даты.
Запрос возвращает post_id, content, дату создания поста.
*/
SELECT
  post_id,
  content,
  created_at
FROM user_post
WHERE
    user_id = 1
  AND created_at > '2022-12-17 23:59:59';

/*
4
Запрос, который считывает пользователей, которые сделали пост в заданный день по заданной теме,
упорядоченные по времени - сначала новые. Запрос возвращает user_id, username, post_id, время создания поста.
*/
SELECT
  user_id,
  user_name,
  post_id,
  created_at
FROM thematic_post
WHERE
    topic_id = 2
  AND created_at_date = '2023-01-22';
