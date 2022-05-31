import json
from datetime import datetime
import requests
import mysql.connector
from mysql.connector import Error
import pandas as pd

# переменные для адепта
host = 'ink-adept-db'
database = 'adept_id'
object_id = 3158 # тестовый
journal_id = 2131
# переменные для велдбука
weldbook_url = 'https://weldbook.ru'
contractor_dict = {
    'nzm': '9cb49b0841e588c2ebedfc1af876cb2f',
    'gemont': '1165e02c2943d10a76a95d2aacfa0fb4',
    'tps': 'fef15edbe711c12a0cf4725e4eaf8edf',
}
# переменные для неосинтеза
neosintez_url = 'https://construction.irkutskoil.ru/'
root_class_id = '30f70be7-1480-ec11-911c-005056b6948b'
root_id = '30f70be7-1480-ec11-911c-005056b6948b'
material_class_id = '35d52d09-0583-ec11-911c-005056b6948b'
line_attribute_id = '667bbac0-3f82-ec11-911c-005056b6948b'
title_attribute_id = '6d0e274e-1a09-eb11-9110-005056b6948b'
name_attribute_id = '10548523-4356-ec11-911a-005056b6948b'
amount_attribute_id = 'fdde6847-c6cf-ea11-9110-005056b6948b'

weldable_types =[
    'Кольцо',
    'Заглушка',
    'Отвод',
    'Переход',
    'Тройник',
    'Труба',
    'Фланец',
    'Фланец',
]


class Plant:

    TOKEN = None
    ADEPT = None

    def __init__(self, root, adept_id):
        self.adept_object_id = adept_id
        self.neosintez_id = root
        self.__materials_dict = None
        self.materials = []
        # self.lines = set()
        self.welds = []

    @staticmethod
    def get_neosintez_token():
        """метод для получения токена для авторизации в api неосинтез"""
        #  открыть файл и получить учетные данные
        with open('auth_neosintez.txt') as a:
            aut_string = a.read()
        req_url = neosintez_url + 'connect/token'
        payload = aut_string  # строка вида grant_type=password&username=????&password=??????&client_id=??????&client_secret=??????
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        response = requests.post(req_url, data=payload, headers=headers)
        if response.status_code == 200:
            Plant.TOKEN = json.loads(response.text)['access_token']

    @staticmethod
    def create_adept_connection():
        if Plant.ADEPT is None:
            with open('auth_adept.txt') as a:
                auth = [line.rstrip() for line in a]
            try:
                Plant.ADEPT = mysql.connector.connect(
                    host=host,
                    user=auth[0],
                    passwd=auth[1],
                    database=database
                )
                print('Connection successful')
            except Error as e:
                print(f'The error {e}', file=log)

    def __get_welds(self, contractor):
        token = contractor_dict[contractor]
        headers = {
            'Authorization': f'Bearer {token}',
        }
        count_url = weldbook_url + '/newApi/welds/count'
        response = requests.get(count_url, headers=headers, verify=False)
        if response.text:
            limit = int(response.text)
            report_url = weldbook_url + f'/newApi/welds/cumulativeReportDataList?offset=0&limit={limit}'
            response = requests.get(report_url, headers=headers, verify=False)
            self.welds.extend(json.loads(response.text))

    def get_welds_from_weldbook(self):
        for contractor in contractor_dict:
            self.__get_welds(contractor)

    def get_materials_from_neosintez(self):
        if not Plant.TOKEN:
            Plant.get_neosintez_token()
        req_url = neosintez_url + 'api/objects/search?take=300'
        payload = json.dumps({
            "Filters": [
                {
                    "Type": 4,
                    "Value": self.neosintez_id  # id узла поиска в Неосинтез
                },
                {
                    "Type": 5,
                    "Value": material_class_id  # id класса в Неосинтез
                }
            ],
            "Conditions": [
                {
                    'Operator': 7,
                    'Type': 1,
                    'Attribute': f'{line_attribute_id}'
                }
            ]
        })
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {Plant.TOKEN}',
            'Content-Type': 'application/json-patch+json',
            'X-HTTP-Method-Override': 'GET'
        }
        # поисковый запрос
        response = requests.post(req_url, headers=headers, data=payload)
        response = json.loads(response.text)
        if response['Total'] > 0:
            self.__materials_dict = response['Result']

        return self.__materials_dict

    @property
    def neosintez_material_amount(self):
        return len(self.__materials_dict)

    @staticmethod
    def __get_value(attributes: dict, attribute_id: str, ref=False, type='str'):
        result = attributes.get(attribute_id, None)
        if result:
            if not ref:
                return attributes[attribute_id]['Value']
            else:
                return attributes[attribute_id]['Value']['Name']
        else:
            if type=='int':
                return 0
            else:
                return ''

    def create_materials(self):
        for material in self.__materials_dict:
            attributes = material['Object']['Attributes']
            name = self.__get_value(attributes, name_attribute_id)
            amount = self.__get_value(attributes, amount_attribute_id, type='int')
            line = self.__get_value(attributes, line_attribute_id, ref=True)
            title = self.__get_value(attributes, title_attribute_id)
            new_material = Material(name, amount, line, title, attributes)
            if new_material in self.materials:
                index = self.materials.index(new_material)
                exist_material = self.materials[index]
                exist_material.amount += new_material.amount
                self.materials[index] = exist_material
            else:
                self.materials.append(new_material)

    # def get_lines(self):
    #     for material in self.__materials_dict:
    #         line_name = material['Object']['Attributes'][line_attribute_id]['Value']['Name']
    #         line_title = material['Object']['Attributes'][title_attribute_id]['Value']
    #         self.lines.add(Line(line_name, line_title))

    def push_materials_to_adept(self):
        """"метод для передачи записи в адепт"""
        if not Plant.ADEPT:
            Plant.create_adept_connection()
        for material in self.materials:

            material.exist_in_adept(Plant.ADEPT)
            if material.exist or material.attributes['type'] in weldable_types:
                continue

            line = Line(material.line, material.title)
            structure_id = line.get_adept_structure_level_id(Plant.ADEPT)


            query = f"""
            INSERT INTO 
            adept_id.jobs (company_id, objectID, name, qty, structureLevelID, journalID)
            VALUES
            (525,
            {object_id},
            'Монтаж материала {material.name}',
            {material.amount},            
            {structure_id},
            {journal_id}            
            ); 
            """
            query = query.split()
            query = ' '.join(query)

            cursor = Plant.ADEPT.cursor()
            cursor.execute(query)
            Plant.ADEPT.commit()
            cursor.close()


class Line:

    def __init__(self, name, title):
        self.name = name
        self.title = title
        self.__adept_structure_level_id = None
        self.__parent_level_id = None
        self.__structure_id = None

    def __hash__(self):
        return hash(self.name + self.title)

    def get_adept_structure_level_id(self, adept_connection):
        if self.__adept_structure_level_id:
            return self.__adept_structure_level_id

        query = f"""
            SELECT sl.id AS levelId
            FROM structure_level AS sl
            LEFT JOIN structure AS st ON sl.structureID = st.id 
            LEFT JOIN structure_level AS sl2 ON sl.parentID = sl2.id
            WHERE st.objectID = {object_id} AND sl.name = '{self.name}' AND sl2.name LIKE '%{self.title}%' 
                        """
        cursor = adept_connection.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()
        if len(result) == 1:
            self.__adept_structure_level_id = result[0]['levelId']
        elif len(result) > 1:
            print(f"Найдено более одного уровня структуры для линии {self.name}", file=log)
            raise Exception(f"Найдено более одного уровня структуры для линии {self.name}")
        else:
            cursor.close()
            self.__create_into_adept(adept_connection)
            self.get_adept_structure_level_id(adept_connection)
        cursor.close()
        return self.__adept_structure_level_id


    def __get_structure_parent(self, adept_connection):
        if not self.__parent_level_id:
            query = f"""
                        SELECT sl.id AS levelId, sl.structureID
                        FROM structure_level AS sl
                        LEFT JOIN structure AS st ON sl.structureID = st.id
                        WHERE st.objectID = {object_id} AND sl.name like 'Эстакада {self.title}'
                        """
            cursor = adept_connection.cursor(dictionary=True)
            cursor.execute(query)
            result = cursor.fetchall()
            if len(result) == 1:

                self.__parent_level_id = result[0]['levelId']
                self.__structure_id = result[0]['structureID']
            else:
                print(f"Не найдено или найдено более одного уровня структуры с именем {self.title}", file=log)
                raise Exception(f"Не найдено или найдено более одного уровня структуры с именем Эстакада {self.title}")
            cursor.close()

    def __create_into_adept(self, adept_connection):
        """"метод для создания линии в адепт"""
        if not self.__structure_id or not self.__parent_level_id:
            self.__get_structure_parent(adept_connection)
        query = f"""
                INSERT INTO 
                adept_id.structure_level (structureID, name, parentID)
                VALUES
                ({self.__structure_id},
                '{self.name}',
                {self.__parent_level_id}
                ); 
                """
        #query = query.split()
        # query = ' '.join(query)

        cursor = adept_connection.cursor()
        cursor.execute(query)
        adept_connection.commit()
        cursor.close()


class Material:

    ATTRIBUTES = {
        'designation': '2cb21840-d51f-ea11-910b-005056b6948b',
        'code': 'b1461a5b-0603-eb11-9110-005056b6948b',
        'type': '532d2888-3582-ec11-911c-005056b6948b',
        'isometry': '060c4af7-4256-ec11-911a-005056b6948b',
    }

    def __init__(self, name, amount, line, title, attributes:dict):
        self.attributes = {}
        for attribute in Material.ATTRIBUTES:
            attribute_dict = attributes.get(Material.ATTRIBUTES[attribute], None)
            if attribute_dict:
                if attribute_dict['Type'] != 8:
                    self.attributes[attribute] = attribute_dict['Value']
                else:
                    self.attributes[attribute] = attribute_dict['Value']['Name']
        self.name = name
        self.amount = amount
        self.line = line
        self.title = title

    def __str__(self):
        return self.name

    def __hash__(self):
        return hash(self.name + self.line + self.title)

    def __eq__(self, other):
        if isinstance(other, Material):
            return self.name == other.name and self.line == other.line and self.title == other.title

    def exist_in_adept(self, adept_connection):
        query = f"""
                SELECT j.id
                FROM adept_id.jobs AS j
                LEFT JOIN adept_id.structure_level AS sl ON j.structureLevelID = sl.id
                LEFT JOIN adept_id.structure_level AS sl2 ON sl.parentID = sl2.id
                WHERE j.objectID = {object_id} and sl.name = '{self.line}' AND j.name LIKE '%{self.name}%'  AND sl2.name LIKE '%{self.title}%'
                """
        query = query.split()
        query = ' '.join(query)
        cursor = adept_connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        if len(result):
            self.exist = True
            print(f"Материал найден в Адепт {self.name} {self.line}")

        else:
            self.exist = False
        cursor.close()


def get_time():
    """Функция возвращает текущую дату и время в строке формата
    Y-m-d_H.M.S"""
    return f'{datetime.now().strftime("%Y-%m-%d_%H.%M.%S")}'


start_time = datetime.now()

# создание файла для логов
file_name = f'log/{get_time()}.txt'
log = open(file_name, 'w')

plant = Plant(root_id, object_id)
plant.get_materials_from_neosintez()
plant.create_materials()

print(f'Обработка объекта Неосинтез id: {plant.neosintez_id} Адепт id{plant.adept_object_id}', file=log)
print(f'Всего сущностей в неосинтез {plant.neosintez_material_amount}.' , end=' ', file=log)
print(f'Уникальных материалов в неосинтезе {len(plant.materials)}.', end=' ', file=log)
print(f'Контрольная сумма по количеству {sum(map(lambda x: x.amount, plant.materials))}', file=log)

plant.push_materials_to_adept()

print(f'Количество существующих в адепт {len(tuple(filter(lambda x: x.exist, plant.materials)))}', file=log)
print(f'Время работы {datetime.now() - start_time}', file=log)



Plant.ADEPT.close()
log.close()

print(plant.neosintez_material_amount)
print(len(plant.materials))
print(datetime.now() - start_time)