<h1>Задание: Реализовать сценарий бронирования машины</h1>
    Представим, что мы эмулируем поведения мобильного приложения по подбору каршеринга.

Для реализации этого сценария нам нужно:
- Собрать список предложений, запросить из различных источников доступные варианты
- Отфильтровать этот список по интересующим нас параметрам
- Забронировать машину

Примерно так же выглядит наш сценарий в коде.
<b>chain_combine_service_offers -> chain_filter_offers -> chain_book_car</b>


<h2>chain_combine_service_offers</h2>

```python
async def chain_combine_service_offers(inbound: Queue, outbound: Queue, **kw):
pass
```

В этом звене нужно запросить с помощью функций
get_offers_from_sourses и get_offers запросить все предложения о машинах. get_offers_from_sourses принимает список источников данных и по каждому из них нужно вызвать get_offers.
Ограничения:
Нельзя одновременно ожидать более 5 get_offers_from_sourses, иначе вас заблокируют и ваш запрос будет обрабатывать в 10 раз дольше

<h2>chain_filter_offers</h2>
    В этом звене нужно отфильтровать список предложений по заданным параметрам

```python
async def chain_filter_offers(
    inbound: Queue,
    outbound: Queue,
    brand: Optional[str] = None,
    price: Optional[int] = None,
    **kw,
):
    pass
```

<h2>chain_book_car</h2>
   Звено отвечает за бронирование автомобиля.

```python
async def chain_book_car(inbound: Queue, outbound: Queue, **kw):
    pass
```

В нем необходимо для всех предложений вызвать метод book_request. Предложение, запрос на  бронирование которого отработает быстрее - передать в  outbound, остальные предложения нужно отменить, используя метод cancel_book_request.

<h2>Требования к выполнению</h2>
- Для прохождения задания необходимо пройти все тесты.
- Тесты нельзя изменять. Нельзя стирать или переписывать изначальный код

<h2>Советы по выполнению задания</h2>
- Повторите всю теоретическую часть главы.
- Начните реализовывать логику работы звеньев последовательно.
- Внимательно изучите тесты.
- Постарайтесь найти общие моменты с примерами из урока и элементами задания.
- Подумайте, какие механизмы asyncio необходимо применить в той или иной функции.
- Возможно для успешного выполнение тестов на время работы программы - нужно поднять n воркеров внутри звена.
- Примеры входящих и исходящих данных можно посмотреть в тестах.

<h2>Для проверки заданий нужно запустить команду pytest в terminal.</h2>
`pytest tests/app -vv -s`

<h1>Последовательность выполнения задания</h1>
<h2>chain_combine_service_offers</h2>
- Начать следует с первого звена chain_combine_service_offers
Есть 2 очереди:

`inbound: asyncio.Queue — очередь, из которой мы будем получать данные внутрь звена.`

`outbound: asyncio.Queue — очередь, в которую мы будет передавать результат выполнения работы.`


- Во все очереди передаются объекты класса **PipelineContext**

``` python
class PipelineContext:
   def __init__(self, user_id: int, data: Optional[Any] = None):
       self._user_id = user_id
       self.data = data # data - полезная информация для обработки в звене

   @property
   def user_id(self):
       return self._user_id

class PipelineContext:
   def __init__(self, user_id: int, data: Optional[Any] = None):
       self._user_id = user_id
       self.data = data # data - полезная информация для обработки в звене

   @property
   def user_id(self):
       return self._user_id
```

- Например, на вход в chain_combine_service_offers из inbound получим

```python
PipelineContext(
    user_id = 1,
    data = [
       "somewhere1",
       "somewhere2",
       "somewhere",
       "somewhere",
    ]
)

PipelineContext(
    user_id = 1,
    data = [
       "somewhere1",
       "somewhere2",
       "somewhere",
       "somewhere",
    ]
)
```

- Чтобы уметь обрабатывать данные внутри звена непрерывно — нужно сделать функции-воркеры, которые будут разгребать нашу inbound очередь
- Пример такого воркера:
```python
async def worker_combine_service_offers(
   inbound: Queue, outbound: Queue):
   while True:
       ctx: PipelineContext = await inbound.get()
       pass
       await outbound.put(ctx)

async def worker_combine_service_offers(
   inbound: Queue, outbound: Queue):
   while True:
       ctx: PipelineContext = await inbound.get()
       pass
       await outbound.put(ctx)
```

Он будет в бесконечном цикле обрабатывать данные из очереди. Т.е. внутри **chain_combine_service_offers**
нужно сделать несколько воркеров, которые бесконечно будут обрабатывать наши данные.
Нужно запустить их конкурентно для работы в фоне любым из известных способов.
У нас есть ограничение на количество параллельных запросов **get_offers_from_sourses**.Нужно подобрать такой примитив синхронизации, который ограничивал бы вызов этой функции для N воркеров.

```python
async def worker_combine_service_offers(
   inbound: Queue, outbound: Queue, something
):
   while True:
        pass

async def worker_combine_service_offers(
   inbound: Queue, outbound: Queue, something
):
   while True:
        pass
```

Внутри get_offers_from_sourses нужно для каждого источника (source)вызвать get_offers(source) и, объединив данные в 1 список - вернуть из функции.

```python
async def get_offers_from_sourses(sources: list[str]) -> list[dict]:
    pass

async def get_offers_from_sourses(sources: list[str]) -> list[dict]:
    pass
```

Т.к. get_offers длительная по выполнению, нужно минимизироать ожидание ответа ранее рассмотренным механизмом запуска нескольких короутин. После того, как мы выполнили всю полезную работу с данными, необходимо передать их в очередь outbound.
У объекта типа PipelineContext нужно заменить значение атрибута data. Пример передаваемого объекта в outbound очередь:

```python
PipelineContext(
    user_id = 1,
    data = [
       {"url": "http://source/car?id=1", "price": 1_000, "brand": "LADA"},
       {"url": "http://source/car?id=2", "price": 5_000, "brand": "MITSUBISHI"},
       {"url": "http://source/car?id=3", "price": 3_000, "brand": "KIA"},
       {"url": "http://source/car?id=4", "price": 2_000, "brand": "DAEWOO"},
       {"url": "http://source/car?id=5", "price": 10_000, "brand": "PORSCHE"},
    ]
)

PipelineContext(
    user_id = 1,
    data = [
       {"url": "http://source/car?id=1", "price": 1_000, "brand": "LADA"},
       {"url": "http://source/car?id=2", "price": 5_000, "brand": "MITSUBISHI"},
       {"url": "http://source/car?id=3", "price": 3_000, "brand": "KIA"},
       {"url": "http://source/car?id=4", "price": 2_000, "brand": "DAEWOO"},
       {"url": "http://source/car?id=5", "price": 10_000, "brand": "PORSCHE"},
    ]
)
```

Рассмотрим тесты в файле **tests/test_unit.py**, позволяющие нам проверить программу.
**TestChainCombineService** — класс, в котором перечислены все тестовые сценарии для функции **chain_combine_service_offers**.
- TestChainCombineService

    - **test_correct_output** — тест проверки верного значения в **outbound** очередь
    - **test_time_execution** — тест на время выполнения
    - **test_gathers_count** — тест на ограничение параллельных запросов **get_offers_from_sourses**


chain_filter_offers
В этом звене нужно отфильтровать список предложений по заданным параметрам:

``` python
brand: Optional[str] = None
price: Optional[int] = None
```

**outbound** очередь звена **chain_combine_service_offers** — она же inbound очередь звена **chain_filter_offers**. Отфильтруем список по соответствующим параметрам словаря. Brand при наличии значения — по соответствию, а **price** меньше или равно значению параметра звена при его наличии.
Пример передаваемого объекта в outbound очередь c условием price = 3000 :

```python
PipelineContext(
    user_id = 1,
    data = [
       {"url": "http://source/car?id=1", "price": 1_000, "brand": "LADA"},
       {"url": "http://source/car?id=3", "price": 3_000, "brand": "KIA"},
       {"url": "http://source/car?id=4", "price": 2_000, "brand": "DAEWOO"},
    ]
)

PipelineContext(
    user_id = 1,
    data = [
       {"url": "http://source/car?id=1", "price": 1_000, "brand": "LADA"},
       {"url": "http://source/car?id=3", "price": 3_000, "brand": "KIA"},
       {"url": "http://source/car?id=4", "price": 2_000, "brand": "DAEWOO"},
    ]
)

```

Посмотрите внимательно на тесты этого звена **test/test_unit.py**.
Внутри **@pytest.mark.parametrize()** перечислены варианты входных параметров и того, что ожидается получить.

<h2>chain_book_car</h2>
Аналогично звену **chain_combine_service_offers** нужно поднять воркеров для обработки данных из очереди.
Получаем данные из очереди:
```python
PipelineContext(
    user_id = 1,
    data = [
       {"url": "http://source/car?id=1", "price": 1_000, "brand": "LADA"},
       {"url": "http://source/car?id=2", "price": 5_000, "brand": "MITSUBISHI"},
       {"url": "http://source/car?id=3", "price": 3_000, "brand": "KIA"},
       {"url": "http://source/car?id=4", "price": 2_000, "brand": "DAEWOO"},
       {"url": "http://source/car?id=5", "price": 10_000, "brand": "PORSCHE"},
    ]
)

PipelineContext(
    user_id = 1,
    data = [
       {"url": "http://source/car?id=1", "price": 1_000, "brand": "LADA"},
       {"url": "http://source/car?id=2", "price": 5_000, "brand": "MITSUBISHI"},
       {"url": "http://source/car?id=3", "price": 3_000, "brand": "KIA"},
       {"url": "http://source/car?id=4", "price": 2_000, "brand": "DAEWOO"},
       {"url": "http://source/car?id=5", "price": 10_000, "brand": "PORSCHE"},
    ]
)
```

Для каждого предложения (словаря из списка data) вызываем book_request. Вызовы нужно сделать параллельными и вернуть управление, как только завершится какой-либо запрос. Остальные задачи нужно отменить, вызвав метод cancel() у asyncio.Task, и дождаться корректного завершения работы этих задач.
Внутри book_request нужно перехватить исключение **asyncio.CancelledError** и вызвать метод cancel_book_request.
То бронирование (словарь например {"url": "http://source/car?id=1", "price": 1_000, "brand": "LADA"}), которое произошло первым, передаем в PipelineContext.data, обновленный объект передаем в outbound очередь.

Рассмотрим тесты в файле **tests/test_unit.py**
<h2>TestChainBooking</h2>

**test_time_execution** — проверка времени выполнения
**test_1_booked_car** — проверка того, что по итогу забронирована только 1 машина


Тест проверки всей цепочки
**test/test_general.py**
Для прохождения этого теста нужно реализовать функцию **run_pipeline**.
Внутри создаем задачи всех звеньев и инициализируем необходимые очереди.
```python
def run_pipeline(inbound: Queue) -> Queue:
    pass

def run_pipeline(inbound: Queue) -> Queue:
    pass
```

Прежде, чем проходить тесты — рекомендем создать отдельный файл,
импортировать все необходимые зависимости и попробовать в нем отлаживать более мелкие участки кода.

Успехов в выполнении задания!
