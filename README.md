
# Watset для PySpark

В данном репозитории находится реализация Watset -- метода обнаружения понятий в графе синонимов, основанный на кластеризации графов значений слов.

Подробное описание алгоритма и оригинальная реализация алгоритма:

 * https://nlpub.ru/Watset
 * http://depot.nlpub.ru/ustalov.jct2017.pdf
 * https://github.com/dustalov/watset

## Использование
Перед началом, нужно исправить в `watset_spark.sh`:

 * путь до `spark_submit` и его параметры (по-умолчанию происходит запуск на локальной машине)
 * задать нужную версию python (по-умолчанию используется python3.5)

Узнать все доступные опции можно так:

```
$ ./watset_spark.sh -h
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
usage: watset.py [-h] [--local_cw_maxsteps LOCAL_CW_MAXSTEPS]
                 [--local_cw_weighting {TOP,DIST_LOG,DIST_NOLOG}]
                 [--global_cw_maxsteps GLOBAL_CW_MAXSTEPS]
                 [--global_cw_weighting {TOP,DIST_LOG,DIST_NOLOG}]
                 [--no_parallel_cw]
                 input_file output_dir

positional arguments:
  input_file            path to input file
  output_dir            path to output directory

optional arguments:
  -h, --help            show this help message and exit
  --local_cw_maxsteps LOCAL_CW_MAXSTEPS
  --local_cw_weighting {TOP,DIST_LOG,DIST_NOLOG}
  --global_cw_maxsteps GLOBAL_CW_MAXSTEPS
  --global_cw_weighting {TOP,DIST_LOG,DIST_NOLOG}
  --no_parallel_cw      Use pure python Chinise Whispers instead of pyspark
                        implementation, probably works faster

```

Примеры запуска:

```
./watset_spark.sh ../watset/data/ru/edges.count.txt test1

./watset_spark.sh --global_cw_maxsteps 5 --local_cw_weighting DIST_LOG --global_cw_weighting DIST_LOG --no_parallel_cw ../watset/data/ru/edges.count.txt test2
```

## Пример формата входных данных

Граф синонимов, каждое ребро в отдельной строке
Ребро это слово, синоним и вес ребра, разделённые символом табуляции

```
абсурд  чепуха  14.0
абсурд  чушь    14.0
абсурдизм   абсурд  1.0
абсурдно    бессмысленно    1.0
...
```

Вес ребра может быть как целым, так и числом с плавающей точкой.

## Формат выходных данных
Найденые синсеты (synsets), по одному на строке.

Каждая строка содержит записанные через табуляцию: номер синсета, количество слов в синсете, слова в синсете.
Слова в синсете разделяются запятыми.

При выводе синсеты сортируются по количеству слов в них, начиная с самых больших.

```
...
7160    4       телохранитель, бодигард, охранник, секьюрити
7161    4       неспециалист, дилетант, непрофессионал, профан
7162    4       развернуть, изложить, развертеть, развинтить
7163    4       семка, семя, семечко, семячко
7164    4       самоубийственный, саморазрушительный, самоуничтожительный, суицидальный
...
```

При записи Spark разделяет вывод на части, записывает их в файлы `part-*` и кладёт их в заданную папку.

## Отличия от оригинальной реализации (https://github.com/dustalov/watset)

 * В оригинальной реализации ограничивается количество рёбер при построении EGO-сетей. Здесь -- нет.

 * В оригинальной версии проверяются вроде бы невозможные случаи, когда либо у слова нет кандидатов слов-смыслов на этапе связывания значений слов, либо
 у всех кандидатов нулевой косинусный коэффициент(cosine similarity). Здесь этих проверок нет. Возможно, имеет смысл добавить.

## Доп. материалы

В папке `notebooks` находятся наработки в виде jupyter-ноутбуков.
В том числе реализация Watset на чистом питоне (без Spark'а).

## FAQ

#### - А почему всё в одном файле?

Чтобы было легко запускать при помощи `spark-submit`. Решил, что так будет проще.
Исходник разделён большими комментариями-заголовками на части чтобы было легкче ориентироваться по нему.

#### - Зачем нужен ключ --no_parallel_cw?
У PySpark'а всё же похоже очень большие накладные расходы. Чем меньше его используем и делаем всё на чистом потоке и в одном потоке...
тем быстрее выходит в этой задаче. Ну упс.
