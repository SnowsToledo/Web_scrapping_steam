import psycopg2 as psy2
from datetime import date, datetime
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium import webdriver
from os.path import exists
import plotly.express as px
import pandas as pd


class ConnectionDrive:
    def __init__(self):
        if exists("connection.txt"):
            with open("connection.txt", 'r') as f:
                lines = f.readlines()
                f.close()

            self.host = lines[0].replace("\n", "")
            self.port = lines[1].replace("\n", "")
            self.dbname = lines[2].replace("\n", "")
            self.schema = lines[3].replace("\n", "")
            self.user = lines[4].replace("\n", "")
            self.password = lines[5].replace("\n", "")
            self.driver_path = lines[6].replace("\n", "")
            self.conn = self.connect()
            self.scrap_steam()

        else:
            self.driver_path: str = input("Edge driver path: ")
            self.host = input("Host: ")
            self.port = int(input("Port(int): "))
            self.dbname = input("Database name: ")
            self.schema = input("Schema name: ")
            self.user = input("User: ")
            self.password = input("Password: ")
            with open("connection.txt", "w") as f:
                f.write(self.host + "\n" + str(
                    self.port) + "\n" + self.dbname + "\n" + self.schema + "\n" + self.user + "\n" + self.password + '\n' + self.driver_path)
                f.close()
            self.get_from_zero()

    def connect(self):
        try:
            return psy2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
        except psy2.Error as error:
            print("ConnectionDrive.connect:Não foi possível estabelecer uma conexão. ERROR: ", error.__str__())

    def get_from_zero(self):
        self.create_jogo_table()  # Cria a tabela 'jogo'
        self.create_tags_table()  # Cria a tabela 'tags'
        self.create_day_stats_table()  # Cria a tabela 'day_stats'
        self.insert_steam()  # Na tabela 'jogo' insere uma linha para a steam.
        self.scrap_steam()

    # Cria a tabela 'jogo'
    def create_jogo_table(self):
        cur = self.conn.cursor()
        try:
            cur.execute("""
                CREATE TABLE """ + self.schema + """.jogo(
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    price FLOAT NOT NULL,
                    lowest_price FLOAT NULL,
                    top_pick_players INT NULL,
                    day_top_pick DATE NULL);
            """)
            self.conn.commit()
        except psy2.Error as error:
            print("Não foi possível criar a tabela 'jogo'. ERROR: ", error.__str__())

    # Cria a tabela 'tags'
    def create_tags_table(self):
        cur = self.conn.cursor()
        try:
            cur.execute("""
                CREATE TABLE """ + self.schema + """.tags(
                    tag_id INT NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    id_game int NOT NULL,
                    CONSTRAINT tags_jogo_fkey FOREIGN KEY (id_game) REFERENCES """ + self.schema + """.jogo(id),
                    CONSTRAINT tags_pkey PRIMARY KEY (tag_id, id_game));
            """)
            self.conn.commit()
        except psy2.Error as error:
            print("Não foi possível criar a tabela 'tags'. ERROR: ", error.__str__())

    # Cria a tabela 'day_stats'
    def create_day_stats_table(self):
        cur = self.conn.cursor()
        try:
            cur.execute("""
                CREATE TABLE """ + self.schema + """.day_stats(
                    day DATE NOT NULL,
                    top_pick_players INT NOT NULL,
                    rank INT NOT NULL,
                    jogado_vendido INT NOT NULL,
                    id_game INT NOT NULL,
                    CONSTRAINT stats_jogo_fkey FOREIGN KEY (id_game) REFERENCES """ + self.schema + """.jogo(id),
                    CONSTRAINT stats_jogo PRIMARY KEY (day, id_game, jogado_vendido));
            """)
            self.conn.commit()
        except psy2.Error as error:
            print("Não foi possível criar a tabela 'day_stats'. ERROR: ", error.__str__())

    # Insere um jogo para a steam
    def insert_steam(self):
        cur = self.conn.cursor()
        try:
            cur.execute("INSERT INTO " + self.schema + ".jogo(name,price) VALUES ('steam', 0)")
            self.conn.commit()
        except psy2.Error as error:
            print("Não foi possível inserir a linha da 'steam'. ERROR: ", error.__str__())

    @staticmethod
    def open_driver() -> webdriver.Edge:
        with open("connection.txt", 'r') as f:
            lines = f.readlines()
        options = Options()
        options.add_argument("--headless")
        driver = webdriver.Edge(service=Service(lines[5]), options=options)
        return driver

    # Faz o scrap da steam
    def scrap_steam(self):

        self.get_top_steam_players()
        games = self.get_games()

        result = self.get_names()

        for game in games:
            if game.name not in result:
                self.insert_game(game)
            self.insert_stats(game)

        self.get_sum_players_by_tags()
        self.get_sum_players_by_tags_day()
        self.get_count_tags_game()
        self.get_top_pick_game()
        self.get_top_pick_game_csgo()
        self.get_top_pick_for_game()

        self.conn.close()

    def get_top_steam_players(self):
        driver = self.open_driver()
        driver.get("https://store.steampowered.com/charts")

        time.sleep(2)

        max_online = driver.find_element(By.CLASS_NAME, 'onlineuserchart_StatsTitle_TY7Qb').text
        driver.close()
        today = date.today().strftime("%Y-%m-%d")
        max_online = max_online.replace(',', '')
        conn = self.connect()
        cur = conn.cursor()

        try:
            sql = f"INSERT INTO {self.schema}.day_stats(day, top_pick_players, rank, jogado_vendido, id_game) VALUES ('{today}', {max_online}, {0}, 0, 1)"
            cur.execute(sql)
            conn.commit()
        except psy2.IntegrityError:
            conn.commit()
            sql = f"UPDATE {self.schema}.day_stats set top_pick_players={max_online} WHERE day = '{today}'"
            cur.execute(sql)
            conn.commit()
        except psy2.Error as error:
            print("ConnectionDrive.get_top_steam_players: Algo deu errado...", error.__str__())

    def get_games(self) -> list:
        driver = self.open_driver()
        driver.get("https://store.steampowered.com/charts/mostplayed")

        time.sleep(2)
        driver.find_element(By.CSS_SELECTOR, ".DialogDropDown_CurrentDisplay").click()
        driver.find_element(By.CSS_SELECTOR, ".dropdown_DialogDropDownMenu_Item_1R-DV:nth-child(2)").click()

        result = self.get_names()

        games = []

        for i in range(100):
            time.sleep(2)
            ranks = driver.find_elements(By.CLASS_NAME, 'weeklytopsellers_TableRow_2-RN6')
            name = [n.text for n in driver.find_elements(By.CLASS_NAME, 'weeklytopsellers_GameName_1n_4-')]
            game_page = ranks[i].find_element(By.CLASS_NAME, 'weeklytopsellers_CapsuleCell_18kGH').find_element(
                By.TAG_NAME, 'a')
            tags = []
            if name[i] not in result:
                driver.get(game_page.get_attribute('href'))
                time.sleep(3)
                tags_elements = driver.find_elements(By.CLASS_NAME, 'app_tag')
                for tag in tags_elements:
                    if tag.text != '' and tag.text != '+':
                        tags.append(tag.text)
                driver.get('https://store.steampowered.com/charts/mostplayed')
                time.sleep(1)
                driver.find_element(By.CSS_SELECTOR, ".DialogDropDown_CurrentDisplay").click()
                driver.find_element(By.CSS_SELECTOR, ".dropdown_DialogDropDownMenu_Item_1R-DV:nth-child(2)").click()
                time.sleep(3)
                ranks = driver.find_elements(By.CLASS_NAME, 'weeklytopsellers_TableRow_2-RN6')
            try:
                game = ranks[i].text.split("\n")
                today = date.today().strftime("%Y-%m-%d")
                if len(game) == 4:
                    jogo = Jogo(int(game[0]), game[1], game[2], today, game[3].split(' ')[1], tags, self.schema)
                    try:
                        jogo.set_id_game(self.conn)
                    except psy2.OperationalError:
                        self.conn = self.connect()
                        jogo.set_id_game(self.conn)
                    games.append(jogo)
                if len(game) == 6:
                    jogo = Jogo(int(game[0]), game[1], game[3], today, game[5].split(' ')[1], tags, self.schema)
                    jogo.set_lowest_price(game[4], today, game[2])
                    try:
                        jogo.set_id_game(self.conn)
                    except psy2.OperationalError:
                        self.conn = self.connect()
                        jogo.set_id_game(self.conn)
                    games.append(jogo)
            except IndexError:
                continue

        driver.close()
        return games

    def insert_game(self, game):
        try:
            game.insert_jogo_sql(self.conn)
            game.insert_tags(self.conn)
        except psy2.Error as error:
            print("ConnectionDrive.insert_game: Algo deu errado...\n", error, "\nTentando novamente...")
            self.conn = self.connect()
            self.insert_game(game)

    def insert_stats(self, game):
        try:
            game.insert_rank(self.conn)
            game.update_jogo_pick_players(self.conn)
        except psy2.OperationalError as error:
            print("ConnectionDrive.insert_stats : Algo deu errado...\n", error, "\nTentando novamente...")
            self.conn = self.connect()
            self.insert_stats(game)

    def get_names(self) -> list:
        try:
            cur = self.conn.cursor()
            cur.execute('SELECT "name" from ' + self.schema + '.jogo')
            result = [r[0] for r in cur.fetchall()]
            cur.close()
            return result
        except psy2.OperationalError:
            self.conn = self.connect()
            return self.get_names()

    def delete_tags(self, table):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("DELETE FROM " + self.schema + f".{table}")
        conn.commit()
        cur.close()
        conn.close()

    def get_sum_players_by_tags(self):
        cur = self.conn.cursor()
        try:
            cur.execute("""SELECT SUM(ds.top_pick_players) AS sum_p, tags."name" FROM """ + self.schema + """.day_stats ds 
                            INNER JOIN """ + self.schema + """.tags ON ds.id_game=tags.id_game 
                            GROUP BY tags."name" 
                            ORDER BY sum_p DESC;""")
            x, y = [], []
            for r in cur.fetchall():
                y.append(r[0])
                x.append(r[1])
            cur.close()
            fig = px.bar(x=x, y=y, labels={'y': 'Jogadores', 'x': 'Tag'}, height=800)
            fig.update_xaxes(tickangle=45)
            fig.write_html("output/Sum players by tag.html")
        except psy2.OperationalError as error:
            print("ConnectionDrive.get_sum_players_by_tags: Algo deu errado...\n", error, "\nTentando novamente...")
            self.conn = self.connect()
            self.get_sum_players_by_tags()
        except psy2.Error as error:
            print("Não foi possível selecionar os dados da base de dados. ERROR: ", error.__str__())

    def get_sum_players_by_tags_day(self):
        cur = self.conn.cursor()
        try:
            cur.execute("""SELECT SUM(ds.top_pick_players) AS sum_p, ds."day", tags."name" FROM """ + self.schema + """.day_stats ds 
                            INNER JOIN """ + self.schema + """.tags ON ds.id_game=tags.id_game 
                            GROUP BY (tags."name", ds."day") 
                            ORDER BY sum_p DESC;""")
            x, y, d = [], [], []
            for r in cur.fetchall()[:100]:
                y.append(r[0])
                x.append(r[2])
                d.append(r[1].strftime("%d/%m/%Y"))
            cur.close()
            fig = px.bar(x=x, y=y, color=d, barmode='group', labels={'y': 'Jogadores', 'x': 'Tag'}, height=800)
            fig.write_html("output/Sum players by tag per day.html")
        except psy2.OperationalError as error:
            print("ConnectionDrive.get_sum_players_by_tags_day: Algo deu errado...\n", error, "\nTentando novamente...")
            self.conn = self.connect()
            self.get_sum_players_by_tags_day()
        except psy2.Error as error:
            print("Não foi possível selecionar os dados da base de dados. ERROR: ", error.__str__())

    def get_count_tags_game(self):
        cur = self.conn.cursor()
        try:
            cur.execute(
                'SELECT count(tags.tag_id), tags."name"  from ' + self.schema + '.tags group by tags."name" ORDER BY count DESC;')
            x, y = [], []
            for r in cur.fetchall()[:50]:
                y.append(r[0])
                x.append(r[1])
            cur.close()
            fig = px.bar(x=x, y=y, labels={'y': 'Tags count', 'x': 'Tag'}, height=800)
            fig.update_xaxes(tickangle=45)
            fig.write_html("output/Tags count.html")
        except psy2.OperationalError as error:
            print("ConnectionDrive.get_count_tags_game: Algo deu errado...\n", error, "\nTentando novamente...")
            self.conn = self.connect()
            self.get_count_tags_game()
        except psy2.Error as error:
            print("Não foi possível selecionar os dados da base de dados. ERROR: ", error.__str__())

    def get_top_pick_game(self):
        cur = self.conn.cursor()
        try:
            cur.execute("""SELECT ds.top_pick_players, ds."day", jogo."name" FROM """ + self.schema + """.day_stats ds
                        INNER JOIN """ + self.schema + """.jogo ON ds.id_game=jogo.id where ds.id_game<=10 and ds.id_game != 1
                        ORDER BY ds."day" ASC;""")
            x, y, n = [], [], []
            for r in cur.fetchall():
                y.append(r[0])
                x.append(r[1])
                n.append(r[2])
            cur.close()
            data = {"Name": n, "Pick players": y, "Day": x}
            df = pd.DataFrame(data)
            fig = px.line(df, x="Day", y="Pick players", color="Name", title='Jogos pick players')
            fig.write_html("output/Pick players during the days.html")
        except psy2.OperationalError as error:
            print("ConnectionDrive.get_top_pick_game: Algo deu errado...\n", error, "\nTentando novamente...")
            self.conn = self.connect()
            self.get_top_pick_game()
        except psy2.Error as error:
            print("Não foi possível selecionar os dados da base de dados. ERROR: ", error.__str__())

    def get_top_pick_game_csgo(self):
        cur = self.conn.cursor()
        try:
            cur.execute("""SELECT ds.top_pick_players, ds."day" FROM """ + self.schema + """.day_stats ds
                                    INNER JOIN """ + self.schema + """.jogo ON ds.id_game=jogo.id where ds.id_game=2
                                    ORDER BY ds."day" ASC;""")
            x, y = [], []
            for r in cur.fetchall():
                y.append(r[0])
                x.append(r[1])
            cur.close()
            fig = px.line(x=x, y=y, title='Counter-Strike pick players', labels={'y': 'Pick players', 'x': 'Dias'})
            fig.write_html("output/Pick_players_days_CS:GO.html")
        except psy2.OperationalError as error:
            print("ConnectionDrive.get_top_pick_game_csgo: Algo deu errado...\n", error, "\nTentando novamente...")
            self.conn = self.connect()
            self.get_top_pick_game_csgo()
        except psy2.Error as error:
            print("Não foi possível selecionar os dados da base de dados. ERROR: ", error.__str__())

    def get_top_pick_for_game(self):
        cur = self.conn.cursor()
        try:
            cur.execute(
                'SELECT name, top_pick_players, day_top_pick from ' + self.schema + '.jogo WHERE name != \'steam\' ORDER BY top_pick_players DESC;')
            x, y, n = [], [], []
            for r in cur.fetchall():
                n.append(r[0])
                y.append(r[1])
                x.append(r[2])
            fig = px.scatter(x=x, y=y, color=n, labels={"x": "Dias", "y": "Pick players", "color": "Jogo"})
            fig.write_html("output/Pick_players_days_CS:GO.html")
        except psy2.OperationalError as error:
            print("ConnectionDrive.get_top_pick_for_game: Algo deu errado...\n", error, "\nTentando novamente...")
            self.conn = self.connect()
            self.get_top_pick_for_game()
        except psy2.Error as error:
            print("Não foi possível selecionar os dados da base de dados. ERROR: ", error.__str__())


class Jogo:
    def __init__(self, rank, name, actual_price, actual_price_date, top_actual_pick_players, tags, schema):
        self.rank = rank
        self.name = name
        if actual_price == 'Gratuito para jogar' or actual_price == 'Free To Play':
            self.actual_price = 0
        else:
            try:
                self.actual_price = float(actual_price.replace("R$", "").replace(',', '.'))
            except (ValueError, TypeError):
                self.actual_price = actual_price
        self.actual_price_date = date.fromisoformat(actual_price_date)
        self.top_actual_pick_players = top_actual_pick_players.replace(",", "")
        self.top_pick_players = None
        self.top_pick_date = None
        self.lowest_price = None
        self.lowest_price_date = None
        self.top_discount = None
        self.tags = tags
        self.id_game = None
        self.schema = schema

    def __str__(self):
        return f"name: {self.name}\nrank: {self.rank}\npick players: {self.top_actual_pick_players}\nPrice: {self.actual_price}"

    def set_lowest_price(self, lowest_price, lowest_price_date, top_discount):
        if lowest_price == 'Gratuito para jogar':
            self.lowest_price = 0
        else:
            self.lowest_price = lowest_price
        self.lowest_price_date = date.fromisoformat(lowest_price_date)
        self.top_discount = top_discount

    def set_top_pick_players(self, top_pick_players, top_pick_date):
        self.top_pick_players = top_pick_players
        self.top_pick_date = top_pick_date

    def set_id_game(self, conn):
        cur = conn.cursor()
        name = self.name.replace("'", "''")
        sql = f"SELECT (id) FROM {self.schema}.jogo where name='{name}'"
        cur.execute(sql)
        try:
            result = cur.fetchone()[0]
            self.id_game = int(result)
        except TypeError:
            self.insert_jogo_sql(conn)

        cur.close()

    def insert_jogo_sql(self, conn):
        cur = conn.cursor()
        sql = f"SELECT (name) FROM {self.schema}.jogo"
        cur.execute(sql)
        result = [r[0] for r in cur.fetchall()]
        if not (self.name in result):
            if self.actual_price == 'Free To Play' or self.actual_price == 'Free Demo':
                self.actual_price = 0
            if self.actual_price == '(not available in your region)':
                self.actual_price = -1
            name = self.name.replace("'", "''")
            sql = f"INSERT INTO {self.schema}.jogo(name, price) VALUES ('{name}', {self.actual_price})"

            cur.execute(sql)
            conn.commit()
        self.set_id_game(conn)
        cur.close()

    def update_jogo_pick_players(self, conn):
        cur = conn.cursor()
        sql = f'SELECT day_stats.top_pick_players, day_stats."day", jogo.top_pick_players, jogo.day_top_pick FROM {self.schema}.day_stats INNER JOIN {self.schema}.jogo ON day_stats.id_game = jogo.id  WHERE id_game = {self.id_game} ORDER BY day_stats.top_pick_players DESC;'
        cur.execute(sql)
        top = cur.fetchone()
        try:
            if top[0] > top[2]:
                self.set_top_pick_players(top[0], top[1])
                sql = f"UPDATE {self.schema}.jogo SET top_pick_players = {top[0]}, day_top_pick = '{top[1]}' WHERE id = {self.id_game}"
                cur.execute(sql)
                conn.commit()
            else:
                self.set_top_pick_players(top[2], top[3])
            cur.close()
        except TypeError:
            self.set_top_pick_players(top[2], top[3])
            cur.close()

    def update_lowest_price(self, conn):
        cur = conn.cursor()
        name = self.name.replace("'", "''")
        sql = f"UPDATE {self.schema}.jogo SET lowest_price = {self.lowest_price} WHERE name = '{name}'"
        cur.execute(sql)
        conn.commit()
        cur.close()

    def insert_rank(self, conn):
        cur = conn.cursor()
        name = self.name.replace("'", "''")
        sql = f"SELECT (id) FROM {self.schema}.jogo where name='{name}'"
        cur.execute(sql)
        result = int(cur.fetchone()[0])
        today = date.today().strftime("%Y-%m-%d")
        sql = f"INSERT INTO {self.schema}.day_stats(day, top_pick_players, rank, jogado_vendido, id_game) VALUES ('{today}', {self.top_actual_pick_players}, {self.rank}, 0, {result})"
        try:
            cur.execute(sql)
        except psy2.IntegrityError as e:
            print("Jogo.insert_rank: Já foi inserido...\n", e)
        except psy2.OperationalError:
            conn = ConnectionDrive().connect()
        conn.commit()
        cur.close()
        self.update_jogo_pick_players(conn)

    def insert_tags(self, conn):
        cur = conn.cursor()
        sql = f"select Max(tag_id) from {self.schema}.tags"
        cur.execute(sql)
        last_id = ((0, cur.fetchone()[0])[cur.fetchone() is None]) + 1
        cur.close()
        for tag in self.tags:
            cur = conn.cursor()
            sql = f"select tag_id from {self.schema}.tags where name = '{tag}'"
            cur.execute(sql)
            tag_id = cur.fetchone()
            if tag_id is None:
                tag_id = last_id
                last_id += 1
            else:
                tag_id = tag_id[0]
            tag = tag.replace("'", "''")
            sql = f"INSERT INTO {self.schema}.tags(tag_id, name, id_game) VALUES ({tag_id}, '{tag}', {self.id_game})"
            try:
                cur.execute(sql)
            except psy2.IntegrityError as e:
                print("Jogo.insert_tags: Já foi inserido...\n", e)
            conn.commit()
            cur.close()


class Vendido(Jogo):
    def __init__(self, rank_vendido, num_vendidos, rank, name, actual_price, actual_price_date, top_actual_pick_players,
                 tags, schema):
        super().__init__(rank, name, actual_price, actual_price_date, top_actual_pick_players, tags, schema)
        self.rank_vendido = rank_vendido
        self.num_vendidos = num_vendidos

    def __str__(self):
        return f"name: {self.name}\nrank: {self.rank_vendido}\nnum vendidos: {self.num_vendidos}"


if __name__ == '__main__':
    print(datetime.now())
    ConnectionDrive()
    print(datetime.now())