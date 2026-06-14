# Thread Pool
Курсовая работа. Многопоточное и асинхронное программирование на Java.

## Параметры
- `corePoolSize` — сколько потоков создается сразу.
- `maxPoolSize` — максимум потоков.
- `keepAliveTime` — лишний поток завершается, если долго ничего не делает.
- `queueSize` — размер очереди у одного воркера.
- `minSpareThreads` — если свободных воркеров мало, создается новый поток.
- У каждого воркера своя очередь.
- Балансировка задач сделана через Round Robin.
- Есть `execute`, `submit`, `shutdown`, `shutdownNow`.
- Есть логирование создания потоков, выполнения задач, отказов и idle timeout.

## Запуск
```bash
Run Main
```
Или:
```bash
mvn compile exec:java -Dexec.mainClass=ru.example.Main
```
