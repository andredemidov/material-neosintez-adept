import json
import sys
from datetime import datetime
import requests
import mysql.connector
from mysql.connector import Error

# переменные для адепта
host = 'ink-adept-db'
database = 'adept_id'
object_id = 2198
journal_id = 2265
# object_id = 3158  # тестовый
# journal_id = 2131  # тестовый


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
unit_attribute_id = '9904c66d-f66f-e911-8115-817c3f53a992'
type_attribute_id = '532d2888-3582-ec11-911c-005056b6948b'
revision_attribute_id = 'e0ec82e0-f360-e911-8115-817c3f53a992'
actual_attribute_id = '0f1c8267-801a-ea11-910b-005056b6948b'

# конфигурация
only_not_weldable = False

weldable_types = [
    'Кольцо',
    'Заглушка',
    'Отвод',
    'Переход',
    'Тройник',
    'Труба',
    'Фланец',
]


class Plant:
    TOKEN = None
    ADEPT = None

    def __init__(self, root, adept_id):
        self.adept_object_id = adept_id
        self.neosintez_id = root
        self.__materials_data_neosintez = None
        self.__materials_data_adept = None
        self.materials_from_neosintez = []
        self.materials_from_adept = []
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

    def __get_materials_from_neosintez(self):
        if not Plant.TOKEN:
            Plant.get_neosintez_token()
        req_url = neosintez_url + 'api/objects/search?take=30000'
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
                    'Operator': 1,
                    'Type': 1,
                    'Attribute': actual_attribute_id,
                    'Logic': 0,
                    'Value': 'f15978b1-d193-e911-80cd-9783b3495d40',
                },
                {
                    'Operator': 7,
                    'Type': 1,
                    'Attribute': line_attribute_id,
                    'Logic': 2,
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
            self.__materials_data_neosintez = response['Result']

        return self.__materials_data_neosintez

    @property
    def neosintez_material_amount(self):
        return len(self.__materials_data_neosintez)

    @staticmethod
    def __get_value(attributes: dict, attribute_id: str, ref=False, attribute_type='str'):
        result = attributes.get(attribute_id, None)
        if result:
            type = result['Type']
            if type == 8:
                return attributes[attribute_id]['Value']['Name']
            else:
                return attributes[attribute_id]['Value']
        else:
            if attribute_type == 'int':
                return 0
            else:
                return ''

    def init_materials_from_neosintez(self):
        if self.__materials_data_neosintez is None:
            self.__get_materials_from_neosintez()
        for material in self.__materials_data_neosintez:
            attributes = material['Object']['Attributes']
            if only_not_weldable and self.__get_value(attributes, type_attribute_id, ref=True) in weldable_types:
                continue
            name = self.__get_value(attributes, name_attribute_id)
            amount = self.__get_value(attributes, amount_attribute_id, attribute_type='int')
            line = self.__get_value(attributes, line_attribute_id, ref=True)
            title = self.__get_value(attributes, title_attribute_id)
            unit = self.__get_value(attributes, unit_attribute_id, ref=True)
            revision = self.__get_value(attributes, revision_attribute_id, ref=True)
            next_material = Material(name, amount, line, title, unit, attributes)
            next_material.revision = revision
            if next_material in self.materials_from_neosintez:
                index = self.materials_from_neosintez.index(next_material)
                exist_material = self.materials_from_neosintez[index]
                if next_material.revision_int > exist_material.revision_int:
                    self.materials_from_neosintez[index] = next_material
                elif next_material.revision_int == exist_material.revision_int:
                    exist_material.amount += next_material.amount
                    self.materials_from_neosintez[index] = exist_material
                else:
                    continue
            else:
                self.materials_from_neosintez.append(next_material)

    def init_materials_from_adept(self):
        if self.__materials_data_adept is None:
            self.__get_materials_from_adept()
        for material_data in self.__materials_data_adept:
            material = Material(material_data['name'],
                                material_data['amount'],
                                material_data['line'],
                                material_data['title'],
                                material_data['unit'],
                                {})
            material.adept_id = material_data['id']
            material.exist = True

            self.materials_from_adept.append(material)

    @property
    def control_total_from_adept(self):
        query = f"""
                    SELECT sum(j.qty) as total, COUNT(*) as amount
                    FROM jobs as j
                    WHERE j.objectID = {object_id}
                     AND j.name like 'Монтаж материала%'
                     AND j.name not like '%(del)'
        """
        cursor = self.ADEPT.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()

        total = result[0]['total']
        amount = result[0]['amount']

        return total, amount

    @property
    def tagged_exist(self):
        return len(tuple(filter(lambda x: x.exist, plant.materials_from_neosintez)))

    @property
    def tagged_del(self):
        query = f"""
                            SELECT sum(j.qty) as total, COUNT(*) as amount
                            FROM jobs as j
                            WHERE j.objectID = {object_id}
                             AND j.name like 'Монтаж материала%'
                             AND j.name like '%(del)'
                                        """
        cursor = self.ADEPT.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()

        total = result[0]['total']
        amount = result[0]['amount']

        return amount

    @property
    def tagged_new(self):
        query = f"""
                            SELECT sum(j.qty) as total, COUNT(*) as amount
                            FROM jobs as j
                            WHERE j.objectID = {object_id}
                             AND j.name like 'Монтаж материала%'
                             AND j.name like '%(new)'
                                        """
        cursor = self.ADEPT.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()

        total = result[0]['total']
        amount = result[0]['amount']

        return amount


    @property
    def control_total_from_neosintez(self):
        total = sum(map(lambda x: x.amount, self.materials_from_neosintez))
        amount = len(self.materials_from_neosintez)
        return total, amount

    def __get_materials_from_adept(self):
        query = f"""
                        SELECT j.id, j.name, sl.name as line, j.qty as amount,
                        sl2.name as title, j.BUPlan as unit
                        FROM adept_id.jobs AS j
                        LEFT JOIN adept_id.structure_level AS sl ON j.structureLevelID = sl.id
                        LEFT JOIN adept_id.structure_level AS sl2 ON sl.parentID = sl2.id
                        WHERE j.objectID = {object_id} 
                        AND j.name like 'Монтаж материала%'
                        """
        query = query.split()
        query = ' '.join(query)
        cursor = Plant.ADEPT.cursor(dictionary=True)
        cursor.execute(query)
        self.__materials_data_adept = cursor.fetchall()
        cursor.close()

    def tag_material_as_del(self):
        if not Plant.ADEPT:
            Plant.create_adept_connection()
        for material_adept in self.materials_from_adept:
            if material_adept not in self.materials_from_neosintez:
                material_adept.suffix = '(del)'
                material_adept.update_into_adept(Plant.ADEPT)

    def push_materials_to_adept(self):
        """"метод для передачи записи в адепт"""
        if not Plant.ADEPT:
            Plant.create_adept_connection()
        for material in self.materials_from_neosintez:

            material.exist_in_adept(Plant.ADEPT)
            if material.exist:
                material.update_into_adept(Plant.ADEPT)
            else:
                if configuration == 'update':
                    material.suffix = '(new)'
                material.push_into_adept(Plant.ADEPT)


class Line:

    def __init__(self, line, title):
        self.line = line
        self.__title = title
        self.__adept_structure_level_id = None
        self.__parent_level_id = None
        self.__structure_id = None

    @property
    def adept_title(self):
        return 'Эстакада ' + self.title

    @property
    def title(self):
        return self.__title.replace('Эстакада ', '')


    def __hash__(self):
        return hash(self.line + self.title)

    def get_adept_structure_level_id(self, adept_connection):
        if self.__adept_structure_level_id:
            return self.__adept_structure_level_id

        query = f"""
            SELECT sl.id AS levelId
            FROM structure_level AS sl
            LEFT JOIN structure AS st ON sl.structureID = st.id 
            LEFT JOIN structure_level AS sl2 ON sl.parentID = sl2.id
            WHERE st.objectID = {object_id} AND sl.name = '{self.line}' AND sl2.name = '{self.adept_title}' 
                        """
        cursor = adept_connection.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()
        if len(result) == 1:
            self.__adept_structure_level_id = result[0]['levelId']
        elif len(result) > 1:
            print(f"Найдено более одного уровня структуры для линии {self.line} титул {self.title}", file=log)

            raise Exception(f"Найдено более одного уровня структуры для линии {self.line} титул {self.title}")
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
                        WHERE st.objectID = {object_id} AND sl.name = '{self.adept_title}'
                        """
            cursor = adept_connection.cursor(dictionary=True)
            cursor.execute(query)
            result = cursor.fetchall()
            if len(result) == 1:

                self.__parent_level_id = result[0]['levelId']
                self.__structure_id = result[0]['structureID']
            else:
                print(f"Не найдено или найдено более одного уровня структуры с именем {self.title}", file=log)
                raise Exception(f"Не найдено или найдено более одного уровня структуры с именем {self.title}")
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
                '{self.line}',
                {self.__parent_level_id}
                ); 
                """
        # query = query.split()
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

    def __init__(self, name, amount, line, title, unit, attributes: dict):
        self.attributes = {}
        for attribute in Material.ATTRIBUTES:
            attribute_dict = attributes.get(Material.ATTRIBUTES[attribute], None)
            if attribute_dict:
                if attribute_dict['Type'] != 8:
                    self.attributes[attribute] = attribute_dict['Value']
                else:
                    self.attributes[attribute] = attribute_dict['Value']['Name']
        self.__name = name

        self.amount = amount
        self.line = line
        self.__title = title
        self.unit = unit
        self.structure_id = None
        self.adept_id = None
        self.suffix = ''
        self.revision = ''


    @property
    def adept_title(self):
        return 'Эстакада ' + self.title

    @property
    def revision_int(self):
        if self.revision.isdigit():
            return int(self.revision)
        else:
            return ord(self.revision.upper()) - 1000

    @property
    def title(self):
        return self.__title.replace('Эстакада ', '')

    @property
    def adept_name(self):
        return 'Монтаж материала ' + self.name

    @property
    def name(self):
        return self.__name.replace('(new)', '').replace('(del)', '').replace('Монтаж материала ', '')

    def __str__(self):
        return self.name

    def __hash__(self):
        return hash(self.name + self.line + self.title + self.unit)

    def __eq__(self, other):
        if isinstance(other, Material):
            return self.name == other.name and self.line == other.line \
                   and self.title == other.title and self.unit == other.unit
        else:
            raise TypeError

    def exist_in_adept(self, adept_connection):
        query = f"""
                SELECT j.id, j.name
                FROM adept_id.jobs AS j
                LEFT JOIN adept_id.structure_level AS sl ON j.structureLevelID = sl.id
                LEFT JOIN adept_id.structure_level AS sl2 ON sl.parentID = sl2.id
                WHERE j.objectID = {object_id} 
                and sl.name = '{self.line}' 
                AND (
                    j.name = '{self.adept_name}' 
                    or j.name like '{self.adept_name}(___)'
                )   
                AND sl2.name = '{self.adept_title}'
                AND j.BUPlan = '{self.unit}'

                """
        query = query.split()
        query = ' '.join(query)
        cursor = adept_connection.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()
        if len(result) == 1:
            self.exist = True
            self.adept_id = result[0]['id']
            self.suffix = '(new)' if '(new)' in result[0]['name'] else ''

            # print(f"Материал найден в Адепт {self.name} {self.line}")

        elif len(result) == 0:
            self.exist = False

        else:
            self.exist = True
            print(f'Найдено более одного материала {self.adept_title} {self.suffix}, {self.line}, {self.name}', file=log)
            raise Exception(f'Найдено более одного материала {self.adept_title} {self.suffix}, {self.line}, {self.name}')
        cursor.close()

        return self.structure_id

    def push_into_adept(self, adept_connection):
        if self.structure_id is None:
            line = Line(self.line, self.title)
            self.structure_id = line.get_adept_structure_level_id(adept_connection)

        query = f"""
                    INSERT INTO 
                    adept_id.jobs (company_id, objectID, name, qty, structureLevelID, journalID, BUPlan, BUFact)
                    VALUES
                    (525,
                    {object_id},
                    '{self.adept_name + self.suffix}',
                    {self.amount},            
                    {self.structure_id},
                    {journal_id},
                    '{self.unit}',            
                    '{self.unit}'            
                    ); 
                    """
        query = query.split()
        query = ' '.join(query)

        cursor = adept_connection.cursor()
        cursor.execute(query)
        adept_connection.commit()
        cursor.close()

    def update_into_adept(self, adept_connection):
        if self.structure_id is None:
            line = Line(self.line, self.adept_title)
            self.structure_id = line.get_adept_structure_level_id(adept_connection)

        query = f"""
                    UPDATE jobs
                    SET
                    name = '{self.adept_name + self.suffix}',
                    qty = {self.amount} 
                    where id = {self.adept_id}; 
                    """

        cursor = adept_connection.cursor()
        cursor.execute(query)
        adept_connection.commit()
        cursor.close()


def get_time():
    """Функция возвращает текущую дату и время в строке формата
    Y-m-d_H.M.S"""
    return f'{datetime.now().strftime("%Y-%m-%d_%H.%M.%S")}'


start_time = datetime.now()

argv = sys.argv
if 'update' in argv:
    configuration = 'update'
else:
    configuration = 'create'


# создание файла для логов
file_name = f'log/{get_time()}.txt'
log = open(file_name, 'w')

plant = Plant(root_id, object_id)

plant.init_materials_from_neosintez()

print(f'Только материалы НЕ под сварку {only_not_weldable}. Конфигурация {configuration}', file=log)
print(f'Обработка объекта Неосинтез id: {plant.neosintez_id} Адепт id{plant.adept_object_id}', file=log)
print(f'Всего сущностей в неосинтез {plant.neosintez_material_amount}.', end=' ', file=log)
print(f'Контрольная сумма по количеству в неосинтез {plant.control_total_from_neosintez}', file=log)

plant.push_materials_to_adept()

print(f'Контрольная сумма по количеству в адепт {plant.control_total_from_adept}', file=log)
print(f'Количество существующих в адепт {plant.tagged_exist}', file=log)

plant.init_materials_from_adept()
plant.tag_material_as_del()

print(f'Помеченных как del: {plant.tagged_del}. Помеченных как new: {plant.tagged_new}', file=log)

print(f'Время работы {datetime.now() - start_time}', file=log)

Plant.ADEPT.close()
log.close()

print(plant.neosintez_material_amount)
print(len(plant.materials_from_neosintez))
print(datetime.now() - start_time)
