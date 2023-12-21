import time
from sqlalchemy import create_engine, Column, Integer, Float, String, MetaData, Table
from sqlalchemy.orm import sessionmaker
import pyautogui

# Подключение к ClickHouse
engine = create_engine('clickhouse://username:password@clickhouse-server:8123/mouse_data', echo=True)

# Определение метаданных и таблицы
metadata = MetaData()
mouse_movements = Table('mouse_movements', metadata,
                        Column('x', Integer),
                        Column('y', Integer),
                        Column('deltaX', Integer),
                        Column('deltaY', Integer),
                        Column('clientTimeStamp', Float),
                        Column('button', Integer),
                        Column('target', String)
                        )

metadata.create_all(engine)

# Отслеживание движений мыши и запись в ClickHouse
while True:
    x, y = pyautogui.position()
    deltaX, deltaY = pyautogui.dragTo(x + 1, y + 1, duration=0.1)  # Пример движения мыши
    client_time_stamp = time.time()
    button = 1  # Пример: левая кнопка мыши
    target = "YourTargetWindow"  # Пример: название окна

    # Запись данных в ClickHouse
    with engine.connect() as connection:
        query = mouse_movements.insert().values(
            x=x, y=y, deltaX=deltaX, deltaY=deltaY,
            clientTimeStamp=client_time_stamp, button=button, target=target
        )
        connection.execute(query)

    time.sleep(1)  # Задержка для уменьшения нагрузки на систему
