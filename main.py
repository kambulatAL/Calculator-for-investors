import csv
import sqlite3
import logging

MAIN_MENU = {'0': 'Exit', '1': 'CRUD operations', '2': 'Show top ten companies by criteria'}
CRUD_MENU = {'0': 'Back', '1': 'Create a company', '2': 'Read a company', '3': 'Update a company',
             '4': 'Delete a company', '5': 'List all companies'}
TOP_TEN_MENU = {'0': 'Back', '1': 'List by ND/EBITDA', '2': 'List by ROE', '3': 'List by ROA'}


class Database:
    def __new__(cls, db_name):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Database, cls).__new__(cls)
        return cls.instance

    def __init__(self, db_name):
        self.db = sqlite3.connect(db_name)
        self.cur = self.db.cursor()

    def create_companies_table(self):
        self.cur.execute("""
                        create table if not exists companies(
                        ticker TEXT primary key,
                        name TEXT,
                        sector TEXT);""")
        self.db.commit()

        self.cur.execute("""select ticker from companies;""")
        return self.cur.fetchone()

    def create_financial_table(self):
        self.cur.execute("""
                        create table if not exists financial(
                        ticker TEXT primary key,
                        ebitda float,
                        sales float,
                        net_profit float,
                        market_price float,
                        net_debt float,
                        assets float,
                        equity float, 
                        cash_equivalents float,
                        liabilities float);""")
        self.db.commit()

        self.cur.execute("""select ticker from financial;""")
        return self.cur.fetchone()

    def insert_into_companies(self, ticker, name, sector):
        self.cur.execute("""
                        insert into companies(ticker, name, sector) 
                        values ('{0}', '{1}', '{2}');
                        """.format(ticker, name, sector))

        self.db.commit()

    def insert_into_financial(self, ticker, ebitda, sales, net_profit, market_price, net_debt, assets, equity,
                              cash_equivalents, liabilities):
        self.cur.execute("""
                        insert into financial
                        values ('{0}', {1}, {2},{3}, {4}, {5},{6}, {7}, {8}, {9});
                        """.format(ticker, ebitda, sales, net_profit, market_price, net_debt, assets, equity,
                                   cash_equivalents, liabilities))
        self.db.commit()

    def update_financial(self, ebitda, sales, net_profit, market_price, net_debt, assets, equity,
                         cash_equivalents, liabilities, ticker):
        self.cur.execute("""
                        update financial 
                        set 
                            ebitda = {0},
                            sales = {1},
                            net_profit = {2},
                            market_price = {3},
                            net_debt = {4},
                            assets = {5}, 
                            equity = {6},
                            cash_equivalents = {7},
                            liabilities = {8}
                        where ticker = '{9}';""".format(ebitda, sales, net_profit, market_price, net_debt, assets,
                                                        equity, cash_equivalents, liabilities, ticker))
        self.db.commit()

    def del_company(self, ticker):
        self.cur.execute("""
                    delete from companies
                    where ticker = '{0}';""".format(ticker))
        self.db.commit()

    def del_financial(self, ticker):
        self.cur.execute("""
                    delete from financial
                    where ticker = '{0}';""".format(ticker))
        self.db.commit()

    def list_companies(self):
        self.cur.execute("""
                    select *
                    from companies
                    order by ticker""")
        return self.cur.fetchall()

    def find_company(self, name):
        self.cur.execute("""
                    select ticker, name 
                    from companies 
                    where lower(name) like '%{0}%';""".format(name))
        return self.cur.fetchall()

    def find_financial(self, ticker):
        self.cur.execute("""
                        select * 
                        from financial
                        where ticker = '{0}';""".format(ticker))
        return self.cur.fetchone()

    def topten_by_indic(self, indicator):
        if indicator == 'ND/EBITDA':
            self.cur.execute("""
                        select ticker, round(net_debt/ebitda, 2)
                        from financial
                        order by net_debt/ebitda desc
                        limit 10;""")
        elif indicator == 'ROE':
            self.cur.execute("""
                        select ticker, round(net_profit/equity, 2)
                        from financial
                        order by net_profit/equity desc
                        limit 10;""")
        elif indicator == 'ROA':
            self.cur.execute("""
                        select ticker, round(net_profit/assets, 2)
                        from financial
                        order by net_profit/assets desc
                        limit 10;""")
        return self.cur.fetchall()


class FinCalc:
    def __init__(self, db):
        self.db = db

    @staticmethod
    def handle_empty_vals(row, cols):
        for col in cols:
            if row[col] == '':
                row[col] = 'null'

    def insert_companies_to_sql(self, file):
        with open(file, newline='') as f:
            data = csv.DictReader(f, delimiter=',')
            for row in data:
                self.handle_empty_vals(row, ['ticker', 'name', 'sector'])
                logging.log(0, row['ticker'])
                self.db.insert_into_companies(row['ticker'], row['name'], row['sector'])

    def insert_financial_to_sql(self, file):
        with open(file, newline='') as f:
            data = csv.DictReader(f, delimiter=',')
            for row in data:
                self.handle_empty_vals(row,
                                       ['ticker', 'ebitda', 'sales', 'net_profit', 'market_price', 'net_debt', 'assets',
                                        'equity', 'cash_equivalents', 'liabilities'])
                self.db.insert_into_financial(row['ticker'], row['ebitda'], row['sales'], row['net_profit'],
                                              row['market_price'], row['net_debt'], row['assets'], row['equity'],
                                              row['cash_equivalents'], row['liabilities'])

    @staticmethod
    def print_menu(menu_name, menu):
        print(menu_name)
        for key, section in menu.items():
            print(key, section)
        print()

    @staticmethod
    def proc_comp_creation_inp(update=False):
        input_description = ["Enter ticker (in the format 'MOON'):", "Enter company (in the format 'Moon Corp'):",
                             "Enter industries (in the format 'Technology'):",
                             "Enter ebitda (in the format '987654321'):", "Enter sales (in the format '987654321'):",
                             "Enter net profit (in the format '987654321'):",
                             "Enter market price (in the format '987654321'):",
                             "Enter net debt (in the format '987654321'):",
                             "Enter assets (in the format '987654321'):",
                             "Enter equity (in the format '987654321'):",
                             "Enter cash equivalents (in the format '987654321'):",
                             "Enter liabilities (in the format '987654321'):"]
        inputs = []

        if update:
            input_description = input_description[3:]
            for desc in input_description:
                print(desc)
                inputs.append(float(input()))
        else:
            counter = 0
            for desc in input_description:
                print(desc)
                if counter < 3:
                    inputs.append(input())
                else:
                    inputs.append(float(input()))
        return inputs

    def create_company(self):
        inputs = self.proc_comp_creation_inp()
        self.db.insert_into_companies(*inputs[:3])
        self.db.insert_into_financial(inputs[0], *inputs[3:])
        print('Company created successfully!')

    def calc_company_stats(self, ticker):
        comp_fin = self.db.find_financial(ticker)
        keys = ['P/E', 'P/S', 'P/B', 'ND/EBITDA', 'ROE', 'ROA', 'L/A']
        stats = dict().fromkeys(keys, None)

        if comp_fin[3] is not None and comp_fin[4] is not None:
            stats['P/E'] = round(comp_fin[4] / comp_fin[3], 2)
        if comp_fin[2] is not None and comp_fin[4] is not None:
            stats['P/S'] = round(comp_fin[4] / comp_fin[2], 2)
        if comp_fin[6] is not None and comp_fin[4] is not None:
            stats['P/B'] = round(comp_fin[4] / comp_fin[6], 2)
        if comp_fin[1] is not None and comp_fin[5] is not None:
            stats['ND/EBITDA'] = round(comp_fin[5] / comp_fin[1], 2)
        if comp_fin[3] is not None and comp_fin[7] is not None:
            stats['ROE'] = round(comp_fin[3] / comp_fin[7], 2)
        if comp_fin[3] is not None and comp_fin[6] is not None:
            stats['ROA'] = round(comp_fin[3] / comp_fin[6], 2)
        if comp_fin[9] is not None and comp_fin[6] is not None:
            stats['L/A'] = round(comp_fin[9] / comp_fin[6], 2)

        for key in keys:
            print(f'{key} = {stats[key]}')

    def select_company(self):
        print('Enter company name:')
        name = input()
        companies = self.db.find_company(name)
        if len(companies) == 0:
            print('Company not found!')
            return None
        else:
            for counter, comp in enumerate(companies):
                print(counter, comp[1])

            print('Enter company number:')
            com_num = int(input())
            return companies[com_num]

    def update_company(self):
        if (selected_company := self.select_company()) is not None:
            inputs = self.proc_comp_creation_inp(True)
            self.db.update_financial(*inputs, selected_company[0])
            print('Company updated successfully!')

    def read_company(self):
        if (selected_company := self.select_company()) is not None:
            print(selected_company[0], selected_company[1])
            self.calc_company_stats(selected_company[0])

    def delete_company(self):
        if (selected_company := self.select_company()) is not None:
            self.db.del_company(selected_company[0])
            self.db.del_financial(selected_company[0])
            print('Company deleted successfully!')

    def list_companies(self):
        print('COMPANY LIST')
        companies = self.db.list_companies()
        for comp in companies:
            print(*comp)

    def process_crud_menu(self):
        while True:
            self.print_menu('CRUD MENU', CRUD_MENU)
            print('Enter an option:')
            crud_inp = input()

            if crud_inp not in CRUD_MENU:
                print('Invalid option!\n')
            elif crud_inp == '1':
                self.create_company()
            elif crud_inp == '2':
                self.read_company()
            elif crud_inp == '3':
                self.update_company()
            elif crud_inp == '4':
                self.delete_company()
            elif crud_inp == '5':
                self.list_companies()
            return

    def get_topten(self, indicator):
        print('TICKER', indicator)
        top = self.db.topten_by_indic(indicator)
        for row in top:
            print(*row)

    def process_topten_menu(self):
        while True:
            self.print_menu('TOP TEN MENU', TOP_TEN_MENU)
            print('Enter an option:')
            topten_inp = input()

            if topten_inp not in TOP_TEN_MENU:
                print('Invalid option!\n')
            elif topten_inp == '1':
                self.get_topten('ND/EBITDA')
            elif topten_inp == '2':
                self.get_topten('ROE')
            elif topten_inp == '3':
                self.get_topten('ROA')
            return

    def start_calc(self):
        print('Welcome to the Investor Program!')
        while True:
            self.print_menu('MAIN MENU', MAIN_MENU)
            print('Enter an option:')
            main_inp = input()

            if main_inp not in MAIN_MENU:
                print('Invalid option!\n')
                continue
            elif main_inp == '0':
                print('Have a nice day!')
                self.db.db.close()
                break
            elif main_inp == '1':
                self.process_crud_menu()
            elif main_inp == '2':
                self.process_topten_menu()


database = Database('investor.db')
com_is_created = database.create_companies_table()
fin_is_created = database.create_financial_table()

calc = FinCalc(database)

if com_is_created is None:
    calc.insert_companies_to_sql('companies.csv')
if fin_is_created is None:
    calc.insert_financial_to_sql('financial.csv')
calc.start_calc()
