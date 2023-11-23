import sqlite3
import datetime
import pandas as pd
import random

class DB():
    def __init__(self):
        """
        初期設定
        """
        self.dbname = "agri.db"                                         # データベース名
        self.get_config()                                               # 設定データを読み込む

    def get_config(self):
        """
        設定データを取得する
        """
        conn = sqlite3.connect(self.dbname)
        sql = f"SELECT * FROM config"
        df = pd.read_sql_query(sql, conn)                               # sql実行しpandas形式で格納する
        conn.close()
        df = df.set_index("index")                                      # index列をインデックスに設定する
        dict = {}
        for index, row in df.iterrows():                                # dataframeを辞書にする
            dict[index] = row["value"]
        # 辞書の中でよく使う値を変数として設定する
        self.sunlight_from =  dict["sunlight_from"]                     # LED点灯時間累計の始点
        self.temperature_from =  dict["temperature_from"]               # 温度累計の始点
        self.ephem_config = {   "place": dict["place"],
                                "lat": dict["lat"],
                                "lon": dict["lon"],
                                "elev": dict["elev"],
                            }
        return dict


    def set_config(self, dict):
        """
        設定データを書き込む
        """
        df = pd.DataFrame(index=[], columns=["index", "value"])         # 空のデータフレームを用意する
        for key, value in dict.items():                                 # 辞書をデータフレームにする
            df.loc[key] = [key, value]
        conn = sqlite3.connect(self.dbname)
        cur = conn.cursor()
        df.to_sql("config", conn, if_exists="replace", index=None)      # dfをデータベースに書き込む
        cur.close()
        conn.close()


    def set_temperature(self, temp, humi, dt=None):
        """
        温湿度をデータベースに登録する
        Args:
            temp: 温度
            humi: 湿度
            dt  : 日時（文字列） 未指定ならば今
        """
        conn = sqlite3.connect(self.dbname)
        cur = conn.cursor()

        if dt is None:                                                  # 日時がNoneだったら
            dt = datetime.datetime.now()                                # 現在時刻
            strdt = dt.strftime("%Y/%m/%d %H:%M")                       # 日時の文字列
            strdate = dt.strftime("%Y/%m/%d")                           # 日付の文字列
        else:                                                           # 日時が文字列として与えられていたら
            strdt = dt                                                  # それが日時の文字列
            strdate = dt.split(" ")[0]                                  # スペースで区切った最初のほうが日付

        sql = f"INSERT INTO temperature VALUES('{strdate}','{strdt}', {temp}, {humi})"
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
        self.set_summary(strdate)                                       # その日のサマリーデータを更新する


    def set_summary(self, date):
        """
        サマリーデータを登録する
        Args:
            date: 日付（文字列）
        """
        conn = sqlite3.connect(self.dbname)
        cur = conn.cursor()
        df = self.get_temperature(date)                                 # 指定した日の温湿度データを取得する
        max_temp = df["temperature"].max()                              # その日の最高気温
        min_temp = df["temperature"].min()                              # その日の最低気温
        mean_temp = (max_temp+min_temp)/2                               # 最高気温と最低気温の中間

        df = self.get_LED(date)                                         # 指定した日のLEDデータを取得する
        lighting_minutes = df["minute"].sum()                           # その日のLED点灯時間の合計

        latest_summary_data = self.get_latest_date("summary")           # サマリーデータの直近の日付
        if latest_summary_data is None:                                 # データがなければ挿入する
            sql = f"INSERT INTO summary(date, max_temp, min_temp, mean_temp, lighting_minutes) "\
                    f"VALUES('{date}','{max_temp}', {min_temp}, {mean_temp}, {lighting_minutes})"
        else:                                                           # データがあればその日のデータを更新する
            sql = f"UPDATE summary "\
                    f"SET max_temp={max_temp}, min_temp={min_temp}, mean_temp={mean_temp}, lighting_minutes={lighting_minutes} "\
                    f"WHERE date='{date}'"
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()


    def get_temperature(self, date):
        """
        データベースから指定した日の温湿度データを取り出す
        Args:
            date : 日付（文字列）Noneならば今日
        Returns:
            df   : dataframe
        """
        conn = sqlite3.connect(self.dbname)
        if date is None:                                                # 日付がNoneだったら
            date = datetime.date.today().strftime("%Y/%m/%d")           # 今日の文字列

        sql = f"SELECT * FROM temperature WHERE date='{date}'"
        df = pd.read_sql_query(sql, conn)                               # sql実行しpandas形式で格納する
        df["datetime"] = pd.to_datetime(df["datetime"])                 # 文字列の日時をdatetimeに変換する
        conn.close()
        return df


    def get_LED(self, date):
        """
        データベースから指定した日のLEDデータを取り出す
        Args:
            date : 日付（文字列）Noneならば今日
        Returns:
            df   : dataframe
        """
        conn = sqlite3.connect(self.dbname)
        if date is None:                                                # 日付がNoneだったら
            date = datetime.date.today().strftime("%Y/%m/%d")           # 今日の文字列

        sql = f"SELECT * FROM LED WHERE date='{date}'"
        df = pd.read_sql_query(sql, conn)                               # sql実行しpandas形式で格納する
        conn.close()
        return df

    def get_summary(self, sunlight_from, temperature_from, date=None, days=7):
        """
        温度データのまとめデータを取得する
        Args:
            sunlight_from: LED点灯時間の累計の始点
            temperature_from: 温度の累計の始点
            date_to: 日付（文字列）未指定ならば今日
            days   : 何日前までか
        """
        if date is None:                                                # 日付がNoneだったら
            date = datetime.date.today()                                # 今日（datetime型）
        else:                                                           # 日付が文字列として与えられていたら
            date = datetime.datetime.strptime(date, "%Y/%m/%d")         # 日付の計算をするためにdatetime型にする

        date_from = date - datetime.timedelta(days = days-1)            # 何日前（datetime型）
        date_from = date_from.strftime("%Y/%m/%d")                      # datetime型を文字列にする
        date_to = date.strftime("%Y/%m/%d")                             # datetime型を文字列にする
        conn = sqlite3.connect(self.dbname)

        sql = f"SELECT date, lighting_minutes FROM summary WHERE date BETWEEN '{sunlight_from}' AND '{date_to}'"
        df_sunlight = pd.read_sql_query(sql, conn)                      # LED点灯時間のみのデータフレーム
        df_sunlight = df_sunlight.set_index("date")                     # date列をインデックスに設定する
        df_sunlight = df_sunlight.cumsum()                              # 各日のデータを累積和にする
        df_sunlight = df_sunlight.rename(columns={"lighting_minutes": "lighting_minutes_sum"})      # 列名変更

        sql = f"SELECT date, mean_temp FROM summary WHERE date BETWEEN '{temperature_from}' AND '{date_to}'"
        df_temp = pd.read_sql_query(sql, conn)                          # 平均気温のみのデータフレーム
        df_temp = df_temp.set_index("date")                             # date列をインデックスに設定する
        df_temp = df_temp.cumsum()                                      # 各日のデータを累積和にする
        df_temp = df_temp.rename(columns={"mean_temp": "mean_temp_sum"})        # 列名変更

        sql = f"SELECT * FROM summary WHERE date BETWEEN '{date_from}' AND '{date_to}'"
        df = pd.read_sql_query(sql, conn)                               # サマリーのデータフレーム
        df = df.set_index("date")                                       # date列をインデックスに設定する
        conn.close()

        df = df.join(df_sunlight, how="left")                           # サマリーにLED点灯時間累計データをジョインする
        df = df.join(df_temp, how="left")                               # サマリーに温度累計データをジョインする
        dates = df.index.tolist()                                       # インデックス（日付）のリスト
        dict = {}
        for d in dates:                                                 # 各日付において
            dict[d] = { "max_temp": df.at[d, "max_temp"],
                        "min_temp": df.at[d, "min_temp"],
                        "mean_temp": df.at[d, "mean_temp"],
                        "lighting_minutes": df.at[d, "lighting_minutes"],
                        "lighting_minutes_sum": df.at[d, "lighting_minutes_sum"],
                        "mean_temp_sum": df.at[d, "mean_temp_sum"],
                        }                                               # 日ごとの辞書として登録する
        return dict

    def get_latest_date(self, table):
        """
        テーブルの最新日付を取得する
        Args:
            table : テーブル temperatureもしくはsummary
        Returns:
            date: 日付（文字列） データがない場合はNone
        """
        conn = sqlite3.connect(self.dbname)
        cur = conn.cursor()
        sql = f"SELECT MAX(date) FROM {table}"
        cur.execute(sql)
        date = cur.fetchone()[0]                                        # fetchは要素1のタプルを返すので、その要素を取り出す
        cur.close()
        conn.close()
        return date



    def set_LED(self, minute):
        """
        LED点灯時間をDBに追加する
        Args:
            minute : 時間（分）
            _      : 登録日時指定不可（今を点灯終了時刻とする）
        """
        conn = sqlite3.connect(self.dbname)
        cur = conn.cursor()
        now = datetime.datetime.now()                                   # 今
        date = now.strftime("%Y/%m/%d")                                 # 日付
        dt_to = now.strftime("%Y/%m/%d %H:%M")                          # 点灯終了時刻（今）
        df_from = (now - datetime.timedelta(minutes=minute)).strftime("%Y/%m/%d %H:%M")     # 点灯開始時刻
        sql = f"INSERT INTO LED VALUES('{date}','{df_from}', '{dt_to}', {minute})"
        print(sql)
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()


    def getLED(self, date=None):
        """
        LEDデータを取得する
        Args:
            date: 日付（文字列）Noneならば今日
        """
        if date is None:                                                # 日付がNoneだったら
            date = datetime.date.today().strftime("%Y/%m/%d")           # 今日の文字列
        conn = sqlite3.connect(self.dbname)
        cur = conn.cursor()
        sql = f"SELECT * FROM LED WHERE date='{date}'"
        df = pd.read_sql_query(sql, conn)                               # sql実行しpandas形式で格納する
        cur.close()
        conn.close()
        return df


    def toCSV(self, table, date=None, days=0):
        """
        DBをcsvとして保存する
        Args:
            table : テーブル名
            date  : 日付（テキスト）
            days  : dateから何日前まで
        """        
        conn = sqlite3.connect(self.dbname)
        if date is None:                                                # 日付がNoneだったら
            date = datetime.date.today()                                # 今日まで
        else:                                                           # 日付が文字列として与えられていたら
            date = datetime.datetime.strptime(date, "%Y/%m/%d")         # それをdatetimeにする

        date_to = date.strftime("%Y/%m/%d")                             # datetimeを文字列にする
        date_from = date - datetime.timedelta(days = days)              # 何日前
        date_from = date_from.strftime("%Y/%m/%d")                      # datetimeを文字列にする
        sql = f"SELECT * FROM {table} WHERE date BETWEEN '{date_from}' AND '{date_to}'"
        df = pd.read_sql_query(sql, conn)                               # sql実行しpandas形式で格納する
        df.to_csv(f"{table}.csv", index=False ,header=True)             # インデックス無しでcsv保存する
        conn.close()


    def set_ephem(self, dict):
        """
        日の出・日の入り時刻をサマリーに登録する
        Args:
            dict : 辞書
        """
        date = datetime.datetime.today().strftime("%Y/%m/%d")           # 今日の日付（文字列）
        sunrise_time = dict["sunrise_time"]
        sunset_time = dict["sunset_time"]
        moon_phase = dict["moon_phase"]
        latest_summary_data = self.get_latest_date("summary")           # サマリーの最新日付
        if latest_summary_data != date:                                 # 今日のデータがなければ挿入する
            conn = sqlite3.connect(self.dbname)
            cur = conn.cursor()
            sql = f"INSERT INTO summary(date, sunrise_time, sunset_time, moon_phase) "\
                    f"VALUES('{date}','{sunrise_time}', '{sunset_time}', '{moon_phase}')"
            print("="*100)
            print(sql)
            print("="*100)
            cur.execute(sql)
            conn.commit()
            cur.close()
            conn.close()
        else:                                                           # データがあれば何もしない
            pass
    
    def delete(self, date_from):
        """
        指定した日以前のデータベースを削除する
        Args:
            date_from : 日付（文字列）
        """
        conn = sqlite3.connect(self.dbname)
        cur = conn.cursor()
        sql = "SELECT name FROM sqlite_master WHERE type='table'"       # DB内の全テーブル取得するSQL
        cur.execute(sql)
        tables = cur.fetchall()                                         # DB内の全テーブル　要素1のタプルのリスト
        tables = [table[0] for table in tables]                         # タプルのリストを単純なリストにする

        for table in tables:                                            # 各テーブルにおいて
            if table != "config":                                       # configでなかったら
                sql = f"DELETE FROM {table} WHERE date<='{date_from}'"  # データ削除するSQL
                print(sql)
                cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()


db = DB()

def main():
    """
    # 温湿度のデモ
    sunlight_from = "2023/11/15"
    temperature_from = "2023/11/15"
    temp = random.randint(10, 30)
    humi = random.randint(40, 100)
    db.set_temperature(temp, humi)
    # db.getHumiSummary(days=5)
    """
    
    # DB削除のデモ
    db.delete("2023/11/10")

    
    # CSV出力のデモ
    # db.toCSV("summary", days=2)
    # db.toCSV("temperature")
    # db.toCSV("LED")

    #"""

    """
    # LEDのデモ
    df = db.getLED()
    print(df.head())
    """


if __name__ == "__main__":
    main()

