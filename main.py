import requests
import os
from dotenv import load_dotenv, find_dotenv
from datetime import date, timedelta
import pandas as pd
import psycopg2

URL_NASA = "https://api.nasa.gov/neo/rest/v1/feed"
load_dotenv(find_dotenv())


def get_response() -> dict:
    """Получить список астероидов за последние 3 дня (вкл. сегодня)"""
    end_date = date.today()
    start_date = end_date - timedelta(days=2)

    params = {
        "api_key": os.environ.get("KEY_NASA"),
        "start_date": start_date,
        "end_date": end_date,
    }
    resp = requests.get(URL_NASA, params=params)
    if resp.status_code == 200:
        return resp.json()["near_earth_objects"]
    else:
        print("Error!")


def transform_data(report: dict) -> pd.DataFrame:
    """Преобразовать полученный ответ в DataFrame"""
    days = report.keys()
    df_ovr = pd.DataFrame(
        columns=[
            "id",
            "name",
            "is_potentially_hazardous_asteroid",
            "estimated_diameter",
            "cpd.relative_velocity.kilometers_per_second",
            "cpd.miss_distance.kilometers",
            "cpd.close_approach_date",
        ]
    )

    for day in days:
        df = pd.json_normalize(
            report[day],
            record_path="close_approach_data",
            meta=["id", "name", "is_potentially_hazardous_asteroid", "estimated_diameter"],
            record_prefix="cpd.",
        )
        df = df[
            [
                "id",
                "name",
                "is_potentially_hazardous_asteroid",
                "estimated_diameter",
                "cpd.relative_velocity.kilometers_per_second",
                "cpd.miss_distance.kilometers",
                "cpd.close_approach_date",
            ]
        ]
        df_ovr = pd.concat([df_ovr, df], ignore_index=True)

    df_ovr["estimated_diameter_min_km"] = df_ovr["estimated_diameter"].apply(
        lambda x: x["kilometers"]["estimated_diameter_min"]
    )
    df_ovr["estimated_diameter_max_km"] = df_ovr["estimated_diameter"].apply(
        lambda x: x["kilometers"]["estimated_diameter_max"]
    )

    df_ovr = df_ovr[
        [
            "id",
            "name",
            "is_potentially_hazardous_asteroid",
            "estimated_diameter_min_km",
            "estimated_diameter_max_km",
            "cpd.relative_velocity.kilometers_per_second",
            "cpd.miss_distance.kilometers",
            "cpd.close_approach_date",
        ]
    ]
    df_ovr = df_ovr.rename(
        columns={
            "cpd.relative_velocity.kilometers_per_second": "relative_velocity_km_sec",
            "cpd.miss_distance.kilometers": "miss_distance_km",
            "cpd.close_approach_date": "searching_date",
        }
    )

    df_ovr.to_csv("report_NASA.csv", index=False)
    return df_ovr


def make_dict(df: pd.DataFrame) -> dict:
    """Получить словарь нужными ключами"""
    hazard_count = df.loc[df["is_potentially_hazardous_asteroid"] == True, "is_potentially_hazardous_asteroid"].count()
    max_name = df[df["estimated_diameter_max_km"] == df["estimated_diameter_max_km"].max()].iloc[0]["name"]
    min_hours = ((df["miss_distance_km"].astype(float) / df["relative_velocity_km_sec"].astype(float)) / 360).min()

    report_dict = {
        "potentially_hazardous_count": hazard_count,
        "name_with_max_estimated_diam": max_name,
        "min_collision_hours": round(min_hours),
    }
    return report_dict


def create_connection() -> psycopg2.extensions.connection:
    """Создать connection к БД"""
    conn = psycopg2.connect(
        dbname=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        host=os.environ.get("DB_HOST"),
    )
    return conn


def table_init(connection: psycopg2.extensions.connection) -> None:
    """Инициализировать таблицу asteroids, если не существует"""
    cur = connection.cursor()
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS asteroids (
        id INTEGER NOT NULL,
        name VARCHAR,
        is_potentially_hazardous_asteroid BOOLEAN,
        estimated_diameter_min_km REAL,
        estimated_diameter_max_km REAL,
        relative_velocity_km_sec REAL,
        miss_distance_km REAL,
        searching_date DATE
    );"""
    )
    connection.commit()


def insert_data(connection: psycopg2.extensions.connection, df: pd.DataFrame) -> None:
    """Вставить в таблицу asteroids полученные данные"""
    arr = list(df.to_records(index=False))
    values = ", ".join(map(str, arr))
    cur = connection.cursor()
    cur.execute(f"INSERT INTO asteroids VALUES {values}")
    connection.commit()


def get_information(connection: psycopg2.extensions.connection, searching_date: str, miss_distance_km: str) -> list:
    """Получить из таблицы список всех имен астероидов по заданным условиям"""
    cur = connection.cursor()
    cur.execute(
        f"SELECT name FROM asteroids WHERE searching_date = '{searching_date}' AND miss_distance_km = {miss_distance_km}"
    )
    names = [row[0] for row in cur.fetchall()]
    return names


if __name__ == "__main__":
    response = get_response()
    df = transform_data(response)

    d = make_dict(df)
    print(d)

    conn = create_connection()
    table_init(conn)
    insert_data(conn, df)
    names = get_information(conn, searching_date="2023-01-23", miss_distance_km="4.8695064e+07")
    print(names)
    conn.close()
