# SCD2_storage

## Описание задачи.
Разработать ETL процесс, получающий ежедневную выгрузку данных (предоставляется за 3 дня), загружающий ее в хранилище данных и ежедневно строящий отчет.

## Выгрузка данных.
Ежедневно некие информационные системы выгружают три следующих файла:

Список транзакций за текущий день. Формат – CSV.

Список терминалов полным срезом. Формат – XLSX.

Список паспортов, включенных в «черный список» - с накоплением с начала месяца. Формат – XLSX.

Сведения о картах, счетах и клиентах хранятся в СУБД Oracle в схеме BANK.

## Структура хранилища.
Данные должны быть загружены в хранилище со следующей структурой (имена сущностей указаны по существу, без особенностей правил нейминга, указанных далее):

![image](https://user-images.githubusercontent.com/95413398/168186057-fc53008a-9db0-47d0-8f2c-f6f4502994d9.png)

## Построение отчета.
По результатам загрузки ежедневно необходимо строить витрину отчетности по мошенническим операциям. Витрина строится накоплением, каждый новый отчет укладывается в эту же таблицу с новым report_dt.
В витрине должны содержаться следующие поля:
![image](https://user-images.githubusercontent.com/95413398/168186122-43b02b7c-7ba5-4d48-a718-12dee498055b.png)

**Признаки мошеннических операций.**
1) Совершение операции при просроченном или заблокированном паспорте.
2) Совершение операции при недействующем договоре.
3) Совершение операций в разных городах в течение одного часа.
4) Попытка подбора суммы. В течение 20 минут проходит более 3х операций со следующим шаблоном – каждая последующая меньше предыдущей, при этом отклонены все кроме последней. Последняя операция (успешная) в такой цепочке считается мошеннической.

## Обработка файлов
Выгружаемые файлы именуются согласно следующему шаблону:

transactions_DDMMYYYY.txt

passport_blacklist_DDMMYYYY.xlsx

terminals_DDMMYYYY.xlsx

Предполагается что в один день приходит по одному такому файлу. После загрузки соответствующего файла он должен быть переименован в файл с расширением .backup чтобы при следующем запуске файл не искался и перемещен в каталог archive:

## Краткое решение

Каталоги:

**Zero** - исходные файлы проекта;

**All** - сюда выкладываются все исходные файлы для начала работы;

**Perday** - промежуточный каталог для имитации ежедневного импорта файлов;

**Warning** - сюда складываются файлы с ошибками для дальнейшей обработки, например файлы дубликаты, которые уже хранятся в архиве и т.д.;

**Archive** - сюда складываются отработанные файлы в формате .backup

Скрипт **Zero** заново собирает весь проект:

1) Очистка папок с файлами, очистка логов, удаление всех связанных таблиц проекта из БД.
2) Создание таблиц слоев в БД: stages / meta / scd2_hist.

Скрипт **Main**:

Проверка дубликатов файлов в папке , поиск самых старых файлов - перенос из каталога All в каталог Perday. Запуск скриптов Conn и далее запуск скриптов из папки sql_scripts. 

Скрипт **Conn**:

Предварительная обработка файлов, формирование датафреймов и заливка в слой Stage в  Oracle DB. 

Каталог **sql_scripts**:

Импорт в SCD2 хранилище. В финале построение отчета о мошеннических операциях. 



