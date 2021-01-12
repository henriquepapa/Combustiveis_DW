from unidecode import unidecode


def normalize_columns(colunas: list) -> list:
    colunas_out = []

    for coll in colunas:
        coll = unidecode(coll)
        coll = coll.replace(' - ', '_')
        coll = coll.replace(' ', '_')
        coll = coll.lower()
        colunas_out.append(coll)

    return colunas_out


def convert_state(state: str) -> str:
    dicio_state = {
        'Distrito Federal': 'DF',
        'Goiás': 'GO',
        'Mato Grosso do Sul': 'MS',
        'Mato Grosso': 'MT',
        'Acre': 'AC',
        'Amazonas': 'AM',
        'Amapá': 'AP',
        'Pará': 'PA',
        'Rondônia': 'RO',
        'Roraima': 'RR',
        'Tocantins': 'TO',
        'Alagoas': 'AL',
        'Bahia': 'BA',
        'Ceará': 'CE',
        'Maranhão': 'MA',
        'Paraíba': 'PB',
        'Pernambuco': 'PE',
        'Piauí': 'PI',
        'Rio Grande do Norte': 'RN',
        'Sergipe': 'SE',
        'Paraná': 'PR',
        'Rio Grande do Sul': 'RS',
        'Santa Catarina': 'SC',
        'Espírito Santo': 'ES',
        'Minas Gerais': 'MG',
        'Rio de Janeiro': 'RJ',
        'São Paulo': 'SP'
    }
    return dicio_state[state]


def normalize_city(city: str) -> str:
    city = unidecode(city)
    if "'" in city:
        city = city.replace("'", '')
    return city.upper()
