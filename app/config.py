class Settings:
    APP_TITLE = "Demand-Allocator"
    APP_DESCRIPTION = "API to allocate demands to different opportunities based on geographic data using geodesic distance and real distance calculations."
    APP_VERSION = "1.0.0"

    DEMAND_ID_POSSIBLE_COLUMNS = [
        'CD_SETOR', 'ID', 'SETOR', 'SECTOR_ID', 'SECTOR_CODE', 
        'NEIGHBORHOOD', 'QUARTIER', 'BARRIO', 'DISTRICT', 'DISTRICT_CODE', 
        'BARRIO_ID', 'BARRIO_CODE', 'NEIGHBORHOOD_ID', 'NEIGHBORHOOD_CODE',
        'QUARTIER_ID', 'QUARTIER_CODE', 'KIEZ', 'VIERTEL', 'BAIRRO', 
        'LOCALIDAD', 'QUARTIERE', 'COLONIA', 'KRAJ', 'RIONE', 'ARRONDISSEMENT',
        'SUBDIVISION', 'SEKTOR', 'SCT', 'CD_BAIRRO', 'NM_BAIRRO'  
    ]

    NAME_POSSIBLE_COLUMNS = [
        'NOME', 'NAME', 'NOME_ESTABELECIMENTO', 'ESTABELECIMENTO', 
        'NOME_UBS', 'UBS', 'FACILITY_NAME', 'HOSPITAL_NAME', 'SCHOOL_NAME', 
        'CLINIC_NAME', 'NOME_CLINICA', 'NOME_ESCOLA', 'NOME_HOSPITAL',
        'NOMBRE', 'NOM', 'NOME_STRUTTURA', 'NOME_OSPEDALE', 'NOMBRE_HOSPITAL', 
        'NOMBRE_ESCUELA', 'NOMBRE_CLINICA', 'NOMBRE_ESTABLECIMIENTO', 
        'FACILIDAD', 'NOME_DO_HOSPITAL', 'NOMBRE_DE_LA_CLINICA'
    ]

    CITY_POSSIBLE_COLUMNS = [
        'MUNICIPIO', 'CIDADE', 'MUNICIPALITY', 'CITY', 'BOROUGH', 'COMMUNE', 
        'GEMEINDE', 'COMUNE', 'MUNIC', 'TOWN', 'VILLAGE', 'DISTRICT', 'REGION',
        'VILLE', 'CIUDAD', 'CITÉ', 'CITTÀ', 'POBLACIÓN', 'POVOADO', 'PUEBLO', 
        'VILLA', 'URBE', 'VILLEGGIO', 'SETTLEMENT', 'SETOR', 'LOCALIDAD', 
        'LUGAR', 'LIEU'
    ]

    STATE_POSSIBLE_COLUMNS = [
        'NM_UF', 'UF', 'ST', 'State', 'Province', 'Territory', 'Provincia', 
        'Estado', 'Regiao', 'REGION_CODE', 'STATE_CODE', 'PROVINCE_CODE',
        'ESTADO', 'PROVINCIA', 'ETAT', 'STATO', 'LAND', 'PROVINZ', 'ESTADO_DE',
        'PROVINCIA_DI', 'STATO_DI'
    ]
    
    LATITUDE_POSSIBLE_COLUMNS = [
        'LAT', 'LATITUDE', 'COORD_LAT', 'GEO_LAT', 'Y', 'Y_COORD'
    ]

    LONGITUDE_POSSIBLE_COLUMNS = [
        'LON', 'LONG', 'LONGITUDE', 'COORD_LON', 'GEO_LON', 'X', 'X_COORD'
    ]

    POPULATION_POSSIBLE_COLUMNS = [
        'POP', 'POPULATION', 'POPULACAO', 'POPULAÇÃO', 'DEMANDA', 
        'HABITANTES', 'RESIDENTS'
    ]

    BLACK_POPULATION_POSSIBLE_COLUMNS = [
        'RAÇA NEGRA TOTAL'
    ]

    BROWN_POPULATION_POSSIBLE_COLUMNS = [
        'RAÇA PARDA TOTAL'
    ]

    INDIGENOUS_POPULATION_POSSIBLE_COLUMNS = [
        'RAÇA INDÍGENA TOTAL'
    ]

    YELLOW_POPULATION_POSSIBLE_COLUMNS = [
        'RAÇA AMARELA TOTAL'
    ]

    RACE_COLUMNS = [
        'RAÇA NEGRA TOTAL', 'RAÇA PARDA TOTAL', 'RAÇA INDÍGENA TOTAL', 'RAÇA AMARELA TOTAL'
    ]

    ESTABLISHMENT_ID_POSSIBLE_COLUMNS = [
        'CNES', 'ID_ESTABELECIMENTO'
    ]

    ADDRESS_POSSIBLE_COLUMNS= [
        'ADDRESS', 'ENDEREÇO', 'DIRECCIÓN', 'LOGRADOURO', 'RUA'
    ]

    DISTANCE_POSSIBLE_COLUMNS = [
        'DISTANCE', 'MEAN_DISTANCE', 'GEO_DISTANCE'
    ]

    COVERAGE_RADIUS_POSSIBLE_COLUMNS = [
        'Radius', 'Coverage_Radius'
    ]

settings = Settings()
