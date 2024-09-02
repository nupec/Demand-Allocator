class Settings:
    APP_TITLE = "Demand-Allocator"
    APP_DESCRIPTION = "API for allocating demands to different establishments based on geographic data using geodesic distance calculations."
    APP_VERSION = "1.0.0"

    DEMAND_ID_POSSIBLE_COLUMNS = [
        'CD_SETOR', 'ID', 'SETOR', 'SECTOR_ID', 'SECTOR_CODE', 
        'NEIGHBORHOOD', 'QUARTIER', 'BARRIO', 'DISTRICT', 'DISTRICT_CODE', 
        'BARRIO_ID', 'BARRIO_CODE', 'NEIGHBORHOOD_ID', 'NEIGHBORHOOD_CODE',
        'QUARTIER_ID', 'QUARTIER_CODE', 'KIEZ', 'VIERTEL', 'BAIRRO', 
        'LOCALIDAD', 'QUARTIERE', 'COLONIA', 'KRAJ', 'RIONE', 'ARRONDISSEMENT',
        'SUBDIVISION', 'SEKTOR', 'SCT'
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

settings = Settings()
