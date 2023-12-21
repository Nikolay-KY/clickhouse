from datetime import datetime
from pynput import mouse, keyboard
from ewmh import EWMH
import clickhouse_connect
import pandahouse as ph
import pandas as pd


def create_data_frame():
    return pd.DataFrame(columns=["x", "y", "deltaX", "deltaY", "clientTimeStamp", "button", "target"],
                        dtype={"x": "int16", "y": "int16", "deltaX": "int16", "deltaY": "int16",
                               "clientTimeStamp": "float32", "button": "int8", "target": "string"})


def get_current_window_title():
    ewmh = EWMH()
    active_window = ewmh.getActiveWindow()
    if active_window:
        return ewmh.getWmName(active_window).decode('utf-8') or None
    return None


def record_movement(x, y, button):
    global currX, currY
    deltaX, deltaY = abs(currX - x) if currX != -1 else 0, abs(currY - y) if currY != -1 else 0
    currX, currY = x, y
    curr_datetime = datetime.now().timestamp()
    window_title = get_current_window_title()
    return [x, y, deltaX, deltaY, curr_datetime, button, window_title]


def flush_buff(buff, buff_size, connection):
    if len(buff) >= buff_size:
        ph.to_clickhouse(buff, "mouse_movements_buffer", connection=connection, index=False)
        return create_data_frame()
    return buff


def on_move(x, y):
    global df_buff
    df_buff = df_buff.append(record_movement(x, y, "None"), ignore_index=True)
    df_buff = flush_buff(df_buff, df_buff_size, pdh_conn)


def on_click(x, y, button, pressed):
    global df_buff
    df_buff = df_buff.append(record_movement(x, y, button), ignore_index=True)
    df_buff = flush_buff(df_buff, df_buff_size, pdh_conn)


def on_scroll(x, y, dx, dy):
    global df_buff
    df_buff = df_buff.append(record_movement(x, y, "None"), ignore_index=True)
    df_buff = flush_buff(df_buff, df_buff_size, pdh_conn)


def on_key_release(key):
    if key == keyboard.Key.esc:
        mouse_listener.stop()
        keyboard_listener.stop()
        print("Listeners stopped!")
        return False


# Global Variables
df_buff, df_buff_size = create_data_frame(), 50
currX, currY = -1, -1

# ClickHouse Connection
pdh_conn = {'host': 'http://127.0.0.1:8123', 'database': 'test', 'user': 'default', 'password': 'qw123456'}

# Drop and Create Tables
with clickhouse_connect.get_client(host="127.0.0.1", port=8123, database="test", username="default", password="qw123456") as client:
    client.query("DROP TABLE IF EXISTS test.mouse_movements")
    client.query("""
        CREATE TABLE IF NOT EXISTS test.mouse_movements (
            x Int16, y Int16, deltaX Int16, deltaY Int16,
            clientTimeStamp Float32, button Enum8('None' = 0, 'Button.left' = 1, 'Button.middle' = 2, 'Button.right' = 3),
            target String
        ) ENGINE = MergeTree ORDER BY clientTimeStamp
    """)
    client.query("DROP TABLE IF EXISTS test.mouse_movements_buffer")
    client.query("""
        CREATE TABLE test.mouse_movements_buffer AS test.mouse_movements
        ENGINE = Buffer('test', 'mouse_movements', 2, 10, 100, 1000, 10000, 100000, 1000000)
    """)

# Mouse and Keyboard Listeners
with mouse.Listener(on_move=on_move, on_click=on_click, on_scroll=on_scroll) as mouse_listener, \
        keyboard.Listener(on_release=on_key_release) as keyboard_listener:
    mouse_listener.join()
    keyboard_listener.join()

# Execute queries
with clickhouse_connect.get_client(host="127.0.0.1", port=8123, database="test", username="default", password="qw123456") as client:
    print("Number of mouse movements with non-zero delta:", client.query("SELECT COUNT(*) FROM test.mouse_movements WHERE (deltaX + deltaY) != 0"))
    print("Number of mouse movements within a range:", client.query("""
        SELECT target, COUNT(*)
        FROM test.mouse_movements
        WHERE x < 1000 AND y < 1000
        GROUP BY target
    """))
    print("Maximum delta for each target:", client.query("""
        SELECT target, MAX(plus(abs(deltaX), abs(deltaY))) AS max_delta
        FROM test.mouse_movements
        GROUP BY target
    """))
